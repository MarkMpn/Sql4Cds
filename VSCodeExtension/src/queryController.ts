import * as path from "node:path";
import * as vscode from "vscode";
import { State } from "vscode-languageclient/node";
import { DocumentConnectionManager, errorMessage } from "./documentConnections";
import {
  CellValue, MessageParams, Methods, QueryCancelResult, QueryCompleteParams, QueryDisposeResult,
  ResultSetEventParams, ResultSetSummary, SaveResultRequestResult, SubsetResult
} from "./protocol";
import { createExportParams, exportMethod, opensInTextEditor, resultExportChoices } from "./resultExport";
import { buildDataverseRecordUrl, extractDataverseRecordReference } from "./dataverseRecordLink";
import { resultsHtml } from "./resultWebview";
import { Sql4CdsService } from "./serviceClient";
import type { ExecutionPlanGraph, ExportFormat } from "../../MarkMpn.Sql4Cds.PPTB/src/resultsProtocol";
import type { ResultsState } from "./webview/protocol";

const pageSize = 200;
const exportFormatNames = { Csv: "csv", Excel: "xlsx", Json: "json", Markdown: "md", Xml: "xml" } as const;
type QueryStatus = ResultsState["status"];

interface QueryState {
  runId: number;
  messages: MessageParams["message"][];
  results: Map<string, ResultSetSummary>;
  plans: Map<string, ExecutionPlanGraph[]>;
  planVersion: number;
  status: QueryStatus;
  cancelRequested: boolean;
  estimatedPlan: boolean;
  started: number;
  ended?: number;
  batchElapsed?: string;
}

interface WebviewMessage {
  type?: string;
  requestId?: number;
  ownerUri?: string;
  runId?: number;
  key?: string;
  start?: number;
  count?: number;
  reference?: unknown;
  text?: string;
  format?: ExportFormat;
}

export class QueryController implements vscode.Disposable, vscode.WebviewViewProvider {
  private readonly states = new Map<string, QueryState>();
  private readonly cleanupPending = new Map<string, Promise<void>>();
  private readonly disposables: vscode.Disposable[] = [];
  private readonly queryStatus: vscode.StatusBarItem;
  private resultsView?: vscode.WebviewView;
  private selectedUri?: string;
  private nextRunId = 1;
  private readonly preparing = new Set<string>();
  private actualPlan = false;

  constructor(private readonly service: Sql4CdsService, private readonly connections: DocumentConnectionManager,
    private readonly extensionUri?: vscode.Uri) {
    const client = service.languageClient;
    this.queryStatus = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 90);
    this.disposables.push(
      this.queryStatus,
      client.onNotification(Methods.queryMessage, (params: MessageParams) => this.onMessage(params)),
      client.onNotification(Methods.resultSetAvailable, (params: ResultSetEventParams) => this.onResultSummary(params)),
      client.onNotification(Methods.resultSetUpdated, (params: ResultSetEventParams) => this.onResultSummary(params)),
      client.onNotification(Methods.resultSetComplete, (params: ResultSetEventParams) => this.onResultSummary(params)),
      client.onNotification(Methods.queryComplete, (params: QueryCompleteParams) => this.onQueryComplete(params)),
      client.onDidChangeState(event => { if (event.newState === State.Stopped) { this.onServiceStopped(); } }),
      vscode.window.onDidChangeActiveTextEditor(editor => this.onActiveEditorChanged(editor)),
      vscode.workspace.onDidChangeConfiguration(event => {
        if (event.affectsConfiguration("SQL4CDS.maxResultRows") && this.selectedUri && this.states.has(this.selectedUri)) {
          this.publishState(this.selectedUri);
        }
      }),
      vscode.workspace.onDidCloseTextDocument(document => {
        const uri = document.uri.toString();
        if (this.states.has(uri)) { void this.cleanupUri(uri, true); }
      })
    );
  }

  public resolveWebviewView(view: vscode.WebviewView): void {
    const extensionUri = this.extensionUri ?? vscode.extensions.getExtension("markcarrington.vscode-sql4cds")?.extensionUri;
    if (!extensionUri) { throw new Error("The SQL 4 CDS extension location could not be found."); }
    this.resultsView = view;
    view.webview.options = { enableScripts: true, localResourceRoots: [vscode.Uri.joinPath(extensionUri, "dist")] };
    view.webview.onDidReceiveMessage((message: WebviewMessage) => {
      void this.onWebviewMessage(message, view.webview).catch(error => vscode.window.showErrorMessage(`SQL 4 CDS: ${errorMessage(error)}`));
    }, undefined, this.disposables);
    view.onDidDispose(() => {
      if (this.resultsView === view) { this.resultsView = undefined; }
    }, undefined, this.disposables);
    const asset = (name: string) => view.webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, "dist", name)).toString();
    view.webview.html = resultsHtml(view.webview, {
      script: asset("results.js"), styles: asset("results.css"), images: asset("plan-images")
    });
  }

  public toggleActualPlan(): void {
    this.actualPlan = !this.actualPlan;
    void vscode.commands.executeCommand("setContext", "sql4cds.actualPlan", this.actualPlan);
  }

  public async execute(estimatedPlan = false): Promise<void> {
    const editor = vscode.window.activeTextEditor;
    if (!editor || editor.document.languageId !== "sql4cds") {
      void vscode.window.showInformationMessage("Open a SQL 4 CDS query editor first.");
      return;
    }
    const uri = editor.document.uri.toString();
    if (this.preparing.has(uri)) { return; }
    this.preparing.add(uri);
    try { await this.executeEditor(editor, uri, estimatedPlan); }
    finally { this.preparing.delete(uri); }
  }

  private async executeEditor(editor: vscode.TextEditor, uri: string, estimatedPlan: boolean): Promise<void> {
    const cleanup = this.cleanupPending.get(uri);
    if (cleanup) { await cleanup; }
    if (!this.connections.get(uri) && !await this.connections.connectEditor(undefined, editor)) { return; }
    const selection = editor.selection;
    const query = selection.isEmpty ? editor.document.getText() : editor.document.getText(selection);
    const querySelection = selection.isEmpty ? undefined : {
      startLine: selection.start.line, startColumn: selection.start.character,
      endLine: selection.end.line, endColumn: selection.end.character
    };
    if (!query.trim()) {
      void vscode.window.showInformationMessage("There is no SQL to execute.");
      return;
    }
    const previous = this.states.get(uri);
    if (previous && isRunning(previous.status)) {
      void vscode.window.showWarningMessage(previous.status === "cancelling"
        ? "Wait for the current query cancellation to finish." : "A query is already running for this editor.");
      return;
    }
    if (previous) { await this.disposeStoredResults(uri, false); }
    if (editor.document.isClosed) { return; }
    const state: QueryState = {
      runId: this.nextRunId++, messages: [], results: new Map(), plans: new Map(), planVersion: 0,
      status: "running", cancelRequested: false, estimatedPlan, started: Date.now()
    };
    this.states.set(uri, state);
    this.selectedUri = uri;
    try {
      await this.showResultsView();
      if (this.states.get(uri) !== state || editor.document.isClosed) { return; }
      this.publishState(uri);
      this.updateRunningContext();
      await this.service.languageClient.sendRequest(Methods.executeDocumentSelection, {
        ownerUri: uri, querySelection, getFullColumnSchema: false,
        executionPlanOptions: estimatedPlan || this.actualPlan ? {
          includeEstimatedExecutionPlanXml: estimatedPlan, includeActualExecutionPlanXml: !estimatedPlan && this.actualPlan
        } : undefined
      });
    } catch (error) {
      if (this.states.get(uri) !== state) { return; }
      state.status = "failed";
      state.ended = Date.now();
      state.messages.push({ isError: true, message: errorMessage(error) });
      this.publishState(uri);
      this.updateRunningContext();
    }
  }

  public async cancel(): Promise<void> {
    const uri = vscode.window.activeTextEditor?.document.uri.toString();
    const state = uri ? this.states.get(uri) : undefined;
    if (!uri || !state || state.status !== "running") { return; }
    state.status = "cancelling";
    state.cancelRequested = true;
    this.publishState(uri);
    this.updateRunningContext();
    try {
      const response = await this.service.languageClient.sendRequest<QueryCancelResult>(Methods.cancelQuery, { ownerUri: uri });
      if (response.messages) { throw new Error(response.messages); }
    } catch (error) {
      if (this.states.get(uri) !== state || state.status !== "cancelling") { return; }
      state.status = "running";
      state.cancelRequested = false;
      const message = `Could not cancel the query: ${errorMessage(error)}`;
      state.messages.push({ isError: true, message });
      void vscode.window.showErrorMessage(message);
      this.publishState(uri);
      this.updateRunningContext();
    }
  }

  private onMessage(params: MessageParams): void {
    const state = this.states.get(params.ownerUri);
    if (!state) { return; }
    state.messages.push(params.message);
    this.publishState(params.ownerUri);
  }

  private onResultSummary(params: ResultSetEventParams): void {
    const state = this.states.get(params.ownerUri);
    if (!state) { return; }
    const summary = params.resultSetSummary;
    const key = `${summary.batchId}:${summary.id}`;
    if (summary.specialAction?.expectYukonXMLShowPlan || state.plans.has(key) || params.executionPlans?.length) {
      if (params.executionPlans?.length) {
        state.plans.set(key, params.executionPlans);
        state.planVersion++;
      }
      else if (!state.plans.has(key)) { state.plans.set(key, []); }
      state.results.delete(key);
    } else {
      state.results.set(key, summary);
    }
    this.publishState(params.ownerUri);
  }

  private onQueryComplete(params: QueryCompleteParams): void {
    const state = this.states.get(params.ownerUri);
    if (!state) { return; }
    state.ended = Date.now();
    state.batchElapsed = params.batchSummaries?.map(batch => batch.executionElapsed).filter(Boolean).join(" + ");
    const hasErrors = state.messages.some(message => message.isError) || Boolean(params.batchSummaries?.some(batch => batch.hasError));
    state.status = state.cancelRequested ? "cancelled" : hasErrors ? "failed" : "completed";
    this.publishState(params.ownerUri);
    this.updateRunningContext();
  }

  private onServiceStopped(): void {
    for (const [uri, state] of this.states) {
      if (!isRunning(state.status)) { continue; }
      state.status = "failed";
      state.ended = Date.now();
      state.messages.push({ isError: true, message: "The SQL 4 CDS language service stopped before the query completed." });
      this.publishState(uri);
    }
    this.updateRunningContext();
  }

  private async showResultsView(): Promise<void> {
    if (!this.resultsView) { await vscode.commands.executeCommand("sql4cdsQueryResults.focus", { preserveFocus: true }); }
    this.resultsView?.show(true);
  }

  private async onWebviewMessage(message: WebviewMessage, webview: vscode.Webview): Promise<void> {
    if (!message || typeof message !== "object") { return; }
    if (message.type === "ready") {
      if (this.selectedUri && this.states.has(this.selectedUri)) { this.publishState(this.selectedUri); }
      else { void webview.postMessage({ type: "empty" }); }
      return;
    }
    if (message.type === "error" && typeof message.text === "string") {
      void vscode.window.showErrorMessage(`SQL 4 CDS: ${message.text}`);
      return;
    }
    if (!Number.isSafeInteger(message.requestId)) { throw new Error("Invalid results request."); }
    try {
      const value = await this.handleResultRequest(message);
      void webview.postMessage({ type: "response", requestId: message.requestId, value });
    } catch (error) {
      void webview.postMessage({ type: "response", requestId: message.requestId, error: errorMessage(error) });
    }
  }

  private async handleResultRequest(message: WebviewMessage): Promise<unknown> {
    const uri = message.ownerUri;
    const state = uri ? this.states.get(uri) : undefined;
    if (!uri || !state || message.runId !== state.runId) { throw new Error("The query results are no longer available."); }
    switch (message.type) {
      case "rows": {
        const result = message.key ? state.results.get(message.key) : undefined;
        const maxRows = vscode.workspace.getConfiguration("SQL4CDS").get<number>("maxResultRows", 10000);
        if (!result || !Number.isSafeInteger(message.start) || message.start! < 0 ||
          !Number.isSafeInteger(message.count) || message.count! < 1 || message.count! > pageSize ||
          message.start! >= Math.min(result.rowCount, maxRows)) { throw new Error("Invalid result row range."); }
        const response = await this.service.languageClient.sendRequest<SubsetResult>(Methods.subset, {
          ownerUri: uri, batchIndex: result.batchId, resultSetIndex: result.id, rowsStartIndex: message.start,
          rowsCount: Math.min(message.count!, Math.min(result.rowCount, maxRows) - message.start!)
        });
        if (this.states.get(uri) !== state) { throw new Error("The query results changed while loading rows."); }
        if (!response.resultSubset) { throw new Error("The language service returned no result subset."); }
        return response.resultSubset.rows.map(row => row.map(cell => ({ ...cell, displayValue: formatCell(cell) })));
      }
      case "copy":
        if (typeof message.text !== "string") { throw new Error("No clipboard text was supplied."); }
        await vscode.env.clipboard.writeText(message.text);
        return;
      case "export": {
        const format = message.format;
        const choice = resultExportChoices.find(choice =>
          format && Object.hasOwn(exportFormatNames, format) && choice.format === exportFormatNames[format]);
        if (!message.key || !choice) { throw new Error("Invalid export format."); }
        await this.exportResult(uri, message.key, choice);
        return;
      }
      case "openRecord": {
        const reference = extractDataverseRecordReference(message.reference);
        if (!reference) { throw new Error("Invalid Dataverse record reference."); }
        const profile = this.connections.findByName(reference.dataSource);
        if (!profile?.url) { throw new Error(`No connection URL is available for '${reference.dataSource}'.`); }
        if (!await vscode.env.openExternal(vscode.Uri.parse(buildDataverseRecordUrl(profile.url, reference)))) {
          throw new Error("The Dataverse record could not be opened.");
        }
        return;
      }
      case "openFetchXml":
        if (typeof message.text !== "string") { throw new Error("No FetchXML was supplied."); }
        await vscode.window.showTextDocument(await vscode.workspace.openTextDocument({ language: "xml", content: message.text }),
          { viewColumn: vscode.ViewColumn.Beside, preview: false });
        return;
      default: throw new Error("Unknown results action.");
    }
  }

  private async exportResult(uri: string, key: string, choice: typeof resultExportChoices[number]): Promise<void> {
    const state = this.states.get(uri);
    const result = state?.results.get(key);
    if (!state || !result || !result.complete || isRunning(state.status)) {
      throw new Error("Wait for the query to finish before exporting its results.");
    }
    const resultOrdinal = [...state.results.keys()].indexOf(key) + 1;
    const document = vscode.workspace.textDocuments.find(item => item.uri.toString() === uri);
    const name = `${path.parse(document?.fileName || "query").name || "query"}-result-${resultOrdinal}.${choice.extension}`;
    let defaultUri: vscode.Uri | undefined;
    if (document?.uri.scheme === "file") { defaultUri = vscode.Uri.file(path.join(path.dirname(document.uri.fsPath), name)); }
    else if (vscode.workspace.workspaceFolders?.[0]) { defaultUri = vscode.Uri.joinPath(vscode.workspace.workspaceFolders[0].uri, name); }
    const target = await vscode.window.showSaveDialog({
      defaultUri, filters: { [choice.filterName]: [choice.extension], "All files": ["*"] }, saveLabel: "Export full result set"
    });
    if (!target) { return; }
    if (this.states.get(uri) !== state) { throw new Error("The query results changed before export."); }
    const request = createExportParams(choice.format, {
      ownerUri: uri, filePath: target.fsPath, batchIndex: result.batchId, resultSetIndex: result.id
    });
    const response = await vscode.window.withProgress(
      { location: vscode.ProgressLocation.Notification, title: "Exporting SQL 4 CDS results..." },
      () => this.service.languageClient.sendRequest<SaveResultRequestResult>(exportMethod(choice.format), request)
    );
    if (response.messages) { throw new Error(response.messages); }
    const exported = `Exported ${result.rowCount.toLocaleString()} rows to ${target.fsPath}.`;
    if (opensInTextEditor(choice.format)) {
      try {
        await vscode.window.showTextDocument(await vscode.workspace.openTextDocument(target), { preview: false });
      } catch (error) {
        const action = await vscode.window.showWarningMessage(
          `${exported} VS Code could not open the exported file: ${errorMessage(error)}`, "Open in Default App", "Reveal in Finder/Explorer");
        await this.handleExportAction(action, target);
      }
    } else {
      const action = await vscode.window.showInformationMessage(exported, "Open in Default App", "Reveal in Finder/Explorer");
      await this.handleExportAction(action, target);
    }
  }

  private async handleExportAction(action: string | undefined, target: vscode.Uri): Promise<void> {
    if (action === "Open in Default App") {
      if (!await vscode.env.openExternal(target)) { void vscode.window.showWarningMessage(`No application could open ${target.fsPath}.`); }
    } else if (action === "Reveal in Finder/Explorer") { await vscode.commands.executeCommand("revealFileInOS", target); }
  }

  private publishState(uri: string): void {
    const state = this.states.get(uri);
    if (!state || uri !== this.selectedUri) { return; }
    const maxRows = vscode.workspace.getConfiguration("SQL4CDS").get<number>("maxResultRows", 10000);
    const returnedRows = [...state.results.values()].reduce((total, result) => total + result.rowCount, 0);
    const affectedRows = state.messages.map(message => affectedRowCount(message.message)).filter((value): value is number => value !== undefined);
    this.updateQueryStatus(state, returnedRows, affectedRows.length ? affectedRows.reduce((total, value) => total + value, 0) : undefined);
    void this.resultsView?.webview.postMessage({
      type: "state", ownerUri: uri, runId: state.runId, status: state.status, estimatedPlan: state.estimatedPlan,
      returnedRows, batchElapsed: state.batchElapsed,
      planVersion: state.planVersion,
      autoSizeColumns: vscode.workspace.getConfiguration("SQL4CDS").get<boolean>("autoSizeColumns", true),
      plans: [...state.plans].map(([key, graphs]) => ({ key, graphs })),
      results: [...state.results].map(([key, result]) => ({
        key, rowCount: result.rowCount, complete: result.complete,
        columns: result.columnInfo.map(column => column.columnName ?? column.name ?? ""),
        displayRowCount: Math.min(result.rowCount, maxRows), truncated: result.rowCount > maxRows
      })),
      messages: state.messages.map(message => ({ isError: message.isError, message: message.message }))
    } satisfies ResultsState);
  }

  private updateQueryStatus(state: QueryState, returnedRows: number, affectedRows: number | undefined): void {
    const parts: string[] = [];
    if (state.results.size) { parts.push(`${returnedRows.toLocaleString()} ${returnedRows === 1 ? "row" : "rows"} returned`); }
    if (affectedRows !== undefined) { parts.push(`${affectedRows.toLocaleString()} ${affectedRows === 1 ? "row" : "rows"} affected`); }
    if (isRunning(state.status)) { parts.push(state.status === "cancelling" ? "$(sync~spin) Cancelling" : "$(sync~spin) Running"); }
    else if (state.ended) { parts.push(`$(clock) ${formatDuration(state.ended - state.started)}`); }
    this.queryStatus.text = parts.join("  ");
    this.queryStatus.tooltip = "Query result summary";
    this.queryStatus.show();
  }

  private updateRunningContext(): void {
    const uri = vscode.window.activeTextEditor?.document.uri.toString();
    void vscode.commands.executeCommand("setContext", "sql4cds.queryRunning", Boolean(uri && isRunning(this.states.get(uri)?.status)));
  }

  private onActiveEditorChanged(editor: vscode.TextEditor | undefined): void {
    this.updateRunningContext();
    // Focusing the results panel must not detach it from its SQL editor.
    if (!editor) { return; }
    if (editor.document.languageId !== "sql4cds") {
      this.selectedUri = undefined;
      this.queryStatus.hide();
      void this.resultsView?.webview.postMessage({ type: "empty" });
      return;
    }
    const uri = editor.document.uri.toString();
    this.selectedUri = uri;
    if (this.states.has(uri)) { this.publishState(uri); }
    else {
      this.queryStatus.hide();
      void this.resultsView?.webview.postMessage({ type: "empty" });
    }
  }

  private async disposeStoredResults(uri: string, reportErrors: boolean): Promise<void> {
    try {
      const response = await this.service.languageClient.sendRequest<QueryDisposeResult>(Methods.disposeQuery, { ownerUri: uri });
      if (response.messages && reportErrors) { void vscode.window.showWarningMessage(`SQL 4 CDS could not release query results: ${response.messages}`); }
    } catch (error) {
      if (reportErrors) { void vscode.window.showWarningMessage(`SQL 4 CDS could not release query results: ${errorMessage(error)}`); }
    }
  }

  private async cleanupUri(uri: string, cancelRunning: boolean): Promise<void> {
    const pending = this.cleanupPending.get(uri);
    if (pending) { return pending; }
    const cleanup = this.performCleanup(uri, cancelRunning);
    this.cleanupPending.set(uri, cleanup);
    try { await cleanup; }
    finally { if (this.cleanupPending.get(uri) === cleanup) { this.cleanupPending.delete(uri); } }
  }

  private async performCleanup(uri: string, cancelRunning: boolean): Promise<void> {
    const state = this.states.get(uri);
    this.states.delete(uri);
    if (this.selectedUri === uri) {
      this.selectedUri = undefined;
      this.queryStatus.hide();
      void this.resultsView?.webview.postMessage({ type: "empty" });
    }
    this.updateRunningContext();
    if (cancelRunning && state && isRunning(state.status)) {
      try { await this.service.languageClient.sendRequest<QueryCancelResult>(Methods.cancelQuery, { ownerUri: uri }); }
      catch { /* The service may already be shutting down. */ }
    }
    await this.disposeStoredResults(uri, false);
  }

  public dispose(): void {
    for (const disposable of this.disposables) { disposable.dispose(); }
    for (const uri of this.states.keys()) { void this.cleanupUri(uri, true); }
    this.resultsView = undefined;
  }
}

function formatCell(cell: CellValue): string {
  return cell.isNull ? "NULL" : cell.displayValue ?? cell.invariantCultureDisplayValue ?? String(cell.rawObject ?? "");
}

function affectedRowCount(message: string): number | undefined {
  const match = message.match(/^\(?\s*([\d,]+)\s+.+?\s+(?:created|inserted|updated|deleted|affected)\)?\s*$/i);
  if (!match) { return undefined; }
  const value = Number(match[1].replaceAll(",", ""));
  return Number.isSafeInteger(value) ? value : undefined;
}

function formatDuration(milliseconds: number): string {
  if (milliseconds < 1000) { return `${milliseconds} ms`; }
  if (milliseconds < 60000) { return `${(milliseconds / 1000).toFixed(1)} s`; }
  const minutes = Math.floor(milliseconds / 60000);
  const seconds = Math.floor((milliseconds % 60000) / 1000);
  return `${minutes}:${seconds.toString().padStart(2, "0")}`;
}

function isRunning(status: QueryStatus | undefined): boolean { return status === "running" || status === "cancelling"; }
