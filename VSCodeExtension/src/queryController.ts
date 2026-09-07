import * as path from "node:path";
import * as vscode from "vscode";
import { State } from "vscode-languageclient/node";
import { DocumentConnectionManager, errorMessage } from "./documentConnections";
import {
  CellValue,
  MessageParams,
  Methods,
  QueryCancelResult,
  QueryCompleteParams,
  QueryDisposeResult,
  ResultSetEventParams,
  ResultSetFilter,
  ResultSetSort,
  ResultSetSummary,
  SaveResultRequestResult,
  SubsetResult
} from "./protocol";
import { createExportParams, exportMethod, opensInTextEditor, resultExportChoices } from "./resultExport";
import { ClipboardFormat, ClipboardValue, projectClipboardRows, serializeClipboard } from "./resultClipboard";
import { resultsHtml } from "./resultWebview";
import { Sql4CdsService } from "./serviceClient";
import { detectStructuredValue } from "./structuredValue";
import { StructuredValueViewer } from "./structuredValueViewer";

const pageSize = 200;

type QueryStatus = "running" | "cancelling" | "completed" | "cancelled" | "failed";

interface DisplayResult { summary: ResultSetSummary; }
interface QueryState {
  runId: number;
  messages: MessageParams["message"][];
  results: Map<string, DisplayResult>;
  status: QueryStatus;
  cancelRequested: boolean;
  started: number;
  ended?: number;
  batchElapsed?: string;
}

interface WebviewMessage {
  type?: string;
  ownerUri?: string;
  key?: string;
  page?: number;
  runId?: number;
  text?: string;
  format?: ClipboardFormat;
  headers?: boolean;
  searchText?: string;
  filters?: ResultSetFilter[];
  sort?: ResultSetSort;
  viewVersion?: number;
  row?: number;
  columnIndex?: number;
  selection?: {
    ranges?: Array<{ rowStart: number; rowEnd: number; columnStart: number; columnEnd: number }>;
    columnOrder?: number[];
  };
}

export class QueryController implements vscode.Disposable, vscode.WebviewViewProvider {
  private readonly states = new Map<string, QueryState>();
  private readonly cleanupPending = new Map<string, Promise<void>>();
  private readonly disposables: vscode.Disposable[] = [];
  private readonly queryStatus: vscode.StatusBarItem;
  private readonly structuredValueViewer: StructuredValueViewer;
  private resultsView?: vscode.WebviewView;
  private selectedUri?: string;
  private nextRunId = 1;
  private readonly preparing = new Set<string>();

  constructor(private readonly service: Sql4CdsService, private readonly connections: DocumentConnectionManager) {
    const client = service.languageClient;
    this.queryStatus = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 90);
    this.structuredValueViewer = new StructuredValueViewer();
    this.disposables.push(
      this.queryStatus,
      this.structuredValueViewer,
      client.onNotification(Methods.queryMessage, (params: MessageParams) => this.onMessage(params)),
      client.onNotification(Methods.resultSetAvailable, (params: ResultSetEventParams) => this.onResultSummary(params)),
      client.onNotification(Methods.resultSetUpdated, (params: ResultSetEventParams) => this.onResultSummary(params)),
      client.onNotification(Methods.resultSetComplete, (params: ResultSetEventParams) => this.onResultSummary(params)),
      client.onNotification(Methods.queryComplete, (params: QueryCompleteParams) => this.onQueryComplete(params)),
      client.onDidChangeState(event => { if (event.newState === State.Stopped) { this.onServiceStopped(); } }),
      vscode.window.onDidChangeActiveTextEditor(editor => this.onActiveEditorChanged(editor)),
      vscode.workspace.onDidCloseTextDocument(document => {
        const uri = document.uri.toString();
        if (this.states.has(uri)) { void this.cleanupUri(uri, true); }
      })
    );
  }

  public resolveWebviewView(view: vscode.WebviewView): void {
    this.resultsView = view;
    view.webview.options = { enableScripts: true, localResourceRoots: [] };
    view.webview.onDidReceiveMessage((message: WebviewMessage) => {
      void this.onWebviewMessage(message).catch(error => vscode.window.showErrorMessage(`SQL 4 CDS: ${errorMessage(error)}`));
    }, undefined, this.disposables);
    view.onDidDispose(() => {
      if (this.resultsView === view) { this.resultsView = undefined; }
    }, undefined, this.disposables);
    view.webview.html = resultsHtml(view.webview);
  }

  public async execute(): Promise<void> {
    const editor = vscode.window.activeTextEditor;
    if (!editor || editor.document.languageId !== "sql4cds") {
      void vscode.window.showInformationMessage("Open a SQL 4 CDS query editor first.");
      return;
    }

    const uri = editor.document.uri.toString();
    if (this.preparing.has(uri)) { return; }
    this.preparing.add(uri);
    try { await this.executeEditor(editor, uri); }
    finally { this.preparing.delete(uri); }
  }

  private async executeEditor(editor: vscode.TextEditor, uri: string): Promise<void> {
    const cleanup = this.cleanupPending.get(uri);
    if (cleanup) { await cleanup; }
    if (!this.connections.get(uri) && !await this.connections.connectEditor(undefined, editor)) { return; }
    const selection = editor.selection;
    const query = selection.isEmpty ? editor.document.getText() : editor.document.getText(selection);
    if (!query.trim()) {
      void vscode.window.showInformationMessage("There is no SQL to execute.");
      return;
    }

    const previous = this.states.get(uri);
    if (previous && isRunning(previous.status)) {
      void vscode.window.showWarningMessage(previous.status === "cancelling"
        ? "Wait for the current query cancellation to finish."
        : "A query is already running for this editor.");
      return;
    }

    if (previous) { await this.disposeStoredResults(uri, false); }
    if (editor.document.isClosed) { return; }

    const state: QueryState = {
      runId: this.nextRunId++,
      messages: [],
      results: new Map(),
      status: "running",
      cancelRequested: false,
      started: Date.now()
    };
    this.states.set(uri, state);
    this.selectedUri = uri;
    try {
      await this.showResultsView();
      if (this.states.get(uri) !== state || editor.document.isClosed) { return; }
      this.publishState(uri);
      this.updateRunningContext();
      await this.service.languageClient.sendRequest(Methods.executeString, {
        ownerUri: uri,
        query,
        getFullColumnSchema: false,
        executionPlanOptions: undefined
      });
    } catch (error) {
      if (this.states.get(uri) !== state) { return; }
      state.status = "failed";
      state.ended = Date.now();
      const message = errorMessage(error);
      state.messages.push({ isError: true, message });
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
      if (this.states.get(uri) !== state || state.status !== "cancelling") { return; }
      if (response.messages) {
        state.status = "running";
        state.cancelRequested = false;
        const message = `Could not cancel the query: ${response.messages}`;
        state.messages.push({ isError: true, message });
        void vscode.window.showErrorMessage(message);
        this.publishState(uri);
        this.updateRunningContext();
      }
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
    state.results.set(resultKey(params.resultSetSummary), { summary: params.resultSetSummary });
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
    if (!this.resultsView) {
      await vscode.commands.executeCommand("sql4cdsQueryResults.focus", { preserveFocus: true });
    }
    this.resultsView?.show(true);
  }

  private async onWebviewMessage(message: WebviewMessage): Promise<void> {
    if (!message || typeof message !== "object") { return; }
    if (message.type === "ready") {
      if (this.selectedUri && this.states.has(this.selectedUri)) {
        this.publishState(this.selectedUri);
      } else {
        void this.resultsView?.webview.postMessage({ type: "empty" });
      }
      return;
    }
    const uri = message.ownerUri ?? this.selectedUri;
    if (!uri) { return; }
    switch (message.type) {
      case "page":
        await this.sendPage(uri, message);
        break;
      case "copy":
        if (typeof message.text === "string") {
          await vscode.env.clipboard.writeText(message.text);
          void vscode.window.setStatusBarMessage("SQL 4 CDS: Results copied", 2000);
        }
        break;
      case "copySelection":
        await this.copySelection(uri, message);
        break;
      case "viewCell":
        await this.viewCell(uri, message);
        break;
      case "export":
        if (message.key && message.runId === this.states.get(uri)?.runId) { await this.exportResult(uri, message.key); }
        break;
    }
  }

  private async sendPage(uri: string, message: WebviewMessage): Promise<void> {
    const state = this.states.get(uri);
    const result = message.key ? state?.results.get(message.key) : undefined;
    if (!state || !result || message.runId !== state.runId || !message.key || !isViewSpec(message, result.summary.columnInfo.length)) { return; }

    const maxRows = vscode.workspace.getConfiguration("SQL4CDS").get<number>("maxResultRows", 10000);
    const displayRows = Math.min(result.summary.rowCount, maxRows);
    const lastPage = Math.max(0, Math.ceil(displayRows / pageSize) - 1);
    const requestedPage = Number.isInteger(message.page) ? message.page! : 0;
    const page = Math.max(0, Math.min(requestedPage, lastPage));
    const start = page * pageSize;
    const count = Math.min(pageSize, Math.max(0, displayRows - start));

    try {
      const response = count === 0
        ? { resultSubset: { rowCount: 0, rows: [] } }
        : await this.service.languageClient.sendRequest<SubsetResult>(Methods.subset, {
          ownerUri: uri,
          batchIndex: result.summary.batchId,
          resultSetIndex: result.summary.id,
          rowsStartIndex: start,
          rowsCount: count,
          searchText: message.searchText,
          filters: message.filters,
          sort: message.sort,
          viewVersion: message.viewVersion
        });
      if (this.states.get(uri) !== state) { return; }
      const subset = response.resultSubset;
      const rows = (subset?.rows ?? []).map(row => row.map(formatCell));
      const filteredRows = subset?.totalRowCount ?? displayRows;
      const transformedRows = Math.min(filteredRows, maxRows);
      void this.resultsView?.webview.postMessage({
        type: "page",
        runId: state.runId,
        key: message.key,
        page,
        rows,
        start,
        displayRows: transformedRows,
        filteredRows,
        totalRows: result.summary.rowCount,
        truncated: filteredRows > transformedRows,
        viewVersion: subset?.viewVersion ?? message.viewVersion
      });
    } catch (error) {
      if (this.states.get(uri) !== state) { return; }
      void this.resultsView?.webview.postMessage({
        type: "pageError",
        runId: state.runId,
        key: message.key,
        page,
        message: `Could not retrieve this result page: ${errorMessage(error)}`,
        viewVersion: message.viewVersion
      });
    }
  }

  private async copySelection(uri: string, message: WebviewMessage): Promise<void> {
    const state = this.states.get(uri);
    const result = message.key ? state?.results.get(message.key) : undefined;
    const ranges = message.selection?.ranges;
    const columnOrder = message.selection?.columnOrder;
    const format = message.format;
    const maxRows = vscode.workspace.getConfiguration("SQL4CDS").get<number>("maxResultRows", 10000);
    if (!state || !result || message.runId !== state.runId || !message.key || !ranges?.length || !columnOrder?.length || !isClipboardFormat(format)) { return; }
    if (!isViewSpec(message, result.summary.columnInfo.length) || !validateSelection(ranges, columnOrder, result.summary.columnInfo.length, Math.min(result.summary.rowCount, maxRows))) {
      void vscode.window.showErrorMessage("The result selection is no longer valid. Select the cells again and retry.");
      return;
    }

    const rowStart = Math.min(...ranges.map(range => range.rowStart));
    const rowEnd = Math.max(...ranges.map(range => range.rowEnd));
    const selectedVisualColumns = columnOrder.filter((_, visualIndex) => ranges.some(range => visualIndex >= range.columnStart && visualIndex <= range.columnEnd));
    if (format === "sqlIn" && selectedVisualColumns.length !== 1) {
      void vscode.window.showInformationMessage("Copy as SQL IN requires a single selected column.");
      return;
    }
    const rowCount = rowEnd - rowStart + 1;
    if (rowCount > 50_000) {
      const choice = await vscode.window.showWarningMessage(
        `Copying ${rowCount.toLocaleString()} result rows may use significant memory.`,
        { modal: true },
        "Copy"
      );
      if (choice !== "Copy" || this.states.get(uri) !== state) { return; }
    }

    try {
      const text = await vscode.window.withProgress(
        { location: vscode.ProgressLocation.Notification, title: "Copying SQL 4 CDS results…", cancellable: true },
        async (progress, token) => {
          const rows: ClipboardValue[][] = [];
          for (let chunkStart = rowStart; chunkStart <= rowEnd; chunkStart += pageSize) {
            if (token.isCancellationRequested) { throw new CopyCancelledError(); }
            const chunkCount = Math.min(pageSize, rowEnd - chunkStart + 1);
            const response = await this.service.languageClient.sendRequest<SubsetResult>(Methods.subset, {
              ownerUri: uri,
              batchIndex: result.summary.batchId,
              resultSetIndex: result.summary.id,
              rowsStartIndex: chunkStart,
              rowsCount: chunkCount,
              searchText: message.searchText,
              filters: message.filters,
              sort: message.sort,
              viewVersion: message.viewVersion
            });
            if (this.states.get(uri) !== state || response.resultSubset?.viewVersion !== message.viewVersion) { throw new Error("The result view changed while it was being copied."); }
            const sourceRows = response.resultSubset?.rows ?? [];
            rows.push(...projectClipboardRows(sourceRows.map(row => row.map(clipboardCell)), chunkStart, ranges, columnOrder));
            progress.report({ increment: chunkCount / rowCount * 100 });
          }
          if (token.isCancellationRequested) { throw new CopyCancelledError(); }
          const headers = selectedVisualColumns.map(index => result.summary.columnInfo[index]?.columnName ?? result.summary.columnInfo[index]?.name ?? "");
          return serializeClipboard(format, { headers, rows }, { includeHeaders: message.headers });
        }
      );
      if (this.states.get(uri) !== state) { throw new Error("The query changed while the selection was being copied."); }
      await vscode.env.clipboard.writeText(text);
      void vscode.window.setStatusBarMessage("SQL 4 CDS: Selection copied", 2000);
    } catch (error) {
      if (!(error instanceof CopyCancelledError)) { void vscode.window.showErrorMessage(`Could not copy results: ${errorMessage(error)}`); }
    }
  }

  private async viewCell(uri: string, message: WebviewMessage): Promise<void> {
    const state = this.states.get(uri);
    const result = message.key ? state?.results.get(message.key) : undefined;
    if (!state || !result || message.runId !== state.runId || !Number.isSafeInteger(message.row) || message.row! < 0 ||
      !Number.isSafeInteger(message.columnIndex) || message.columnIndex! < 0 || message.columnIndex! >= result.summary.columnInfo.length || !isViewSpec(message, result.summary.columnInfo.length)) { return; }
    let response: SubsetResult;
    try {
      response = await this.service.languageClient.sendRequest<SubsetResult>(Methods.subset, {
        ownerUri: uri,
        batchIndex: result.summary.batchId,
        resultSetIndex: result.summary.id,
        rowsStartIndex: message.row!,
        rowsCount: 1,
        searchText: message.searchText,
        filters: message.filters,
        sort: message.sort,
        viewVersion: message.viewVersion
      });
    } catch (error) {
      void vscode.window.showErrorMessage(`Could not retrieve the cell value: ${errorMessage(error)}`);
      return;
    }
    if (this.states.get(uri) !== state || response.resultSubset?.viewVersion !== message.viewVersion) { return; }
    const cell = response.resultSubset?.rows[0]?.[message.columnIndex!];
    if (!cell || cell.isNull) { return; }
    const structured = detectStructuredValue(formatCell(cell) ?? "");
    if (!structured) {
      void vscode.window.showInformationMessage("This cell does not contain a valid JSON object, JSON array, or XML document.");
      return;
    }
    const title = Number.isInteger(message.columnIndex) ? `Result column ${message.columnIndex! + 1}` : "Result value";
    await this.structuredValueViewer.open(structured, title);
  }

  private async exportResult(uri: string, key: string): Promise<void> {
    const state = this.states.get(uri);
    const result = state?.results.get(key);
    if (!state || !result || !result.summary.complete) {
      void vscode.window.showInformationMessage("Wait for this result set to finish before exporting it.");
      return;
    }

    const choice = await vscode.window.showQuickPick(resultExportChoices, {
      title: "Export full result set",
      placeHolder: "Select a file format"
    });
    if (!choice || this.states.get(uri) !== state) { return; }

    const resultOrdinal = [...state.results.keys()].indexOf(key) + 1;
    const document = vscode.workspace.textDocuments.find(item => item.uri.toString() === uri);
    const name = `${path.parse(document?.fileName || "query").name || "query"}-result-${resultOrdinal}.${choice.extension}`;
    let defaultUri: vscode.Uri | undefined;
    if (document?.uri.scheme === "file") {
      defaultUri = vscode.Uri.file(path.join(path.dirname(document.uri.fsPath), name));
    } else if (vscode.workspace.workspaceFolders?.[0]) {
      defaultUri = vscode.Uri.joinPath(vscode.workspace.workspaceFolders[0].uri, name);
    }
    const target = await vscode.window.showSaveDialog({
      defaultUri,
      filters: { [choice.filterName]: [choice.extension], "All files": ["*"] },
      saveLabel: "Export full result set"
    });
    if (!target || this.states.get(uri) !== state) { return; }

    const request = createExportParams(choice.format, {
      ownerUri: uri,
      filePath: target.fsPath,
      batchIndex: result.summary.batchId,
      resultSetIndex: result.summary.id
    });
    try {
      const response = await vscode.window.withProgress(
        { location: vscode.ProgressLocation.Notification, title: "Exporting SQL 4 CDS results…" },
        () => this.service.languageClient.sendRequest<SaveResultRequestResult>(exportMethod(choice.format), request)
      );
      if (response.messages) { throw new Error(response.messages); }
    } catch (error) {
      void vscode.window.showErrorMessage(`Could not export results: ${errorMessage(error)}`);
      return;
    }

    const exported = `Exported ${result.summary.rowCount.toLocaleString()} rows to ${target.fsPath}.`;
    if (opensInTextEditor(choice.format)) {
      try {
        const exportedDocument = await vscode.workspace.openTextDocument(target);
        await vscode.window.showTextDocument(exportedDocument, { preview: false });
      } catch (error) {
        const action = await vscode.window.showWarningMessage(
          `${exported} VS Code could not open the exported file: ${errorMessage(error)}`,
          "Open in Default App",
          "Reveal in Finder/Explorer"
        );
        await this.handleExportAction(action, target);
      }
      return;
    }

    const action = await vscode.window.showInformationMessage(
      exported,
      "Open in Default App",
      "Reveal in Finder/Explorer"
    );
    await this.handleExportAction(action, target);
  }

  private async handleExportAction(action: string | undefined, target: vscode.Uri): Promise<void> {
    if (action === "Open in Default App") {
      const opened = await vscode.env.openExternal(target);
      if (!opened) { void vscode.window.showWarningMessage(`No application could open ${target.fsPath}.`); }
    } else if (action === "Reveal in Finder/Explorer") {
      await vscode.commands.executeCommand("revealFileInOS", target);
    }
  }

  private publishState(uri: string): void {
    const state = this.states.get(uri);
    if (!state || uri !== this.selectedUri) { return; }
    const maxRows = vscode.workspace.getConfiguration("SQL4CDS").get<number>("maxResultRows", 10000);
    const returnedRows = [...state.results.values()].reduce((total, result) => total + result.summary.rowCount, 0);
    const affectedRows = state.messages.map(message => affectedRowCount(message.message)).filter((value): value is number => value !== undefined);
    this.updateQueryStatus(state, returnedRows, affectedRows.length ? affectedRows.reduce((total, value) => total + value, 0) : undefined);
    if (!this.resultsView) { return; }
    void this.resultsView.webview.postMessage({
      type: "state",
      ownerUri: uri,
      runId: state.runId,
      status: state.status,
      started: state.started,
      ended: state.ended,
      batchElapsed: state.batchElapsed,
      returnedRows,
      affectedRows: affectedRows.length ? affectedRows.reduce((total, value) => total + value, 0) : undefined,
      results: [...state.results.entries()].map(([key, result], index) => ({
        key,
        ordinal: index + 1,
        rowCount: result.summary.rowCount,
        complete: result.summary.complete,
        columns: (result.summary.columnInfo ?? []).map(column => column.columnName ?? column.name ?? ""),
        displayRowCount: Math.min(result.summary.rowCount, maxRows),
        truncated: result.summary.rowCount > maxRows,
        pageSize
      })),
      messages: state.messages.map(message => ({
        isError: message.isError,
        message: message.message,
        time: message.time
      }))
    });
  }

  private updateQueryStatus(state: QueryState, returnedRows: number, affectedRows: number | undefined): void {
    const parts: string[] = [];
    if (state.results.size) { parts.push(`${returnedRows.toLocaleString()} ${returnedRows === 1 ? "row" : "rows"} returned`); }
    if (affectedRows !== undefined) { parts.push(`${affectedRows.toLocaleString()} ${affectedRows === 1 ? "row" : "rows"} affected`); }
    if (isRunning(state.status)) {
      parts.push(state.status === "cancelling" ? "$(sync~spin) Cancelling" : "$(sync~spin) Running");
    } else if (state.ended) {
      parts.push(`$(clock) ${formatDuration(state.ended - state.started)}`);
    }
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
    if (!editor || editor.document.languageId !== "sql4cds") {
      this.selectedUri = undefined;
      this.queryStatus.hide();
      void this.resultsView?.webview.postMessage({ type: "empty" });
      return;
    }
    const uri = editor.document.uri.toString();
    this.selectedUri = uri;
    if (this.states.has(uri)) {
      this.publishState(uri);
    } else {
      this.queryStatus.hide();
      void this.resultsView?.webview.postMessage({ type: "empty" });
    }
  }

  private async disposeStoredResults(uri: string, reportErrors: boolean): Promise<void> {
    try {
      const response = await this.service.languageClient.sendRequest<QueryDisposeResult>(Methods.disposeQuery, { ownerUri: uri });
      if (response.messages && reportErrors) {
        void vscode.window.showWarningMessage(`SQL 4 CDS could not release query results: ${response.messages}`);
      }
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

function formatCell(cell: CellValue): string | null {
  if (cell.isNull) { return null; }
  return cell.displayValue ?? cell.invariantCultureDisplayValue ?? String(cell.rawObject ?? "");
}

function clipboardCell(cell: CellValue | undefined): ClipboardValue {
  if (!cell || cell.isNull) { return null; }
  const raw = cell.rawObject;
  if (raw === null) { return null; }
  if (typeof raw === "string" || typeof raw === "number" || typeof raw === "boolean" || typeof raw === "bigint") { return raw; }
  return cell.invariantCultureDisplayValue ?? cell.displayValue ?? String(raw ?? "");
}

function isViewSpec(message: WebviewMessage, columnCount: number): boolean {
  const operators = new Set(["contains", "notContains", "equals", "notEquals", "startsWith", "endsWith", "isEmpty", "isNotEmpty", "greaterThan", "greaterThanOrEqual", "lessThan", "lessThanOrEqual"]);
  return Number.isSafeInteger(message.viewVersion) && message.viewVersion! >= 0 &&
    (message.searchText === undefined || typeof message.searchText === "string") &&
    (message.filters === undefined || (Array.isArray(message.filters) && message.filters.every(filter => Number.isSafeInteger(filter.columnIndex) && filter.columnIndex >= 0 && filter.columnIndex < columnCount && operators.has(filter.operator) && (filter.value === undefined || typeof filter.value === "string")))) &&
    (message.sort === undefined || (Number.isSafeInteger(message.sort.columnIndex) && message.sort.columnIndex >= 0 && message.sort.columnIndex < columnCount && (message.sort.direction === "asc" || message.sort.direction === "desc")));
}

function isClipboardFormat(value: unknown): value is ClipboardFormat {
  return value === "tsv" || value === "csv" || value === "json" || value === "xml" || value === "markdown" || value === "sqlIn";
}

function validateSelection(
  ranges: Array<{ rowStart: number; rowEnd: number; columnStart: number; columnEnd: number }>,
  columnOrder: number[],
  columnCount: number,
  rowCount: number
): boolean {
  if (new Set(columnOrder).size !== columnOrder.length || columnOrder.some(column => !Number.isSafeInteger(column) || column < 0 || column >= columnCount)) { return false; }
  return rowCount > 0 && ranges.every(range => Number.isSafeInteger(range.rowStart) && Number.isSafeInteger(range.rowEnd) && range.rowStart >= 0 && range.rowEnd >= range.rowStart && range.rowEnd < rowCount &&
    Number.isSafeInteger(range.columnStart) && Number.isSafeInteger(range.columnEnd) && range.columnStart >= 0 && range.columnEnd >= range.columnStart && range.columnEnd < columnOrder.length);
}

class CopyCancelledError extends Error {}

function affectedRowCount(message: string): number | undefined {
  const match = message.match(/^\(?\s*([\d,]+)\s+.+?\s+(?:created|inserted|updated|deleted|affected)\)?\s*$/i);
  if (!match) { return undefined; }
  const value = Number(match[1].replace(/,/g, ""));
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
function resultKey(summary: ResultSetSummary): string { return `${summary.batchId}:${summary.id}`; }
