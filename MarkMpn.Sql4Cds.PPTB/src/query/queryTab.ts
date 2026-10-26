import { monaco, applyDiagnostics } from "../editor/monacoSetup";
import { ConnectionRegistry } from "../connections";
import type { Sql4CdsClient } from "../lsp/client";
import type { ConfirmationParams, ConnectionCompleteParams, EntityReferenceValue, ExecutionPlanNode, PptbConnectionOptions, ResultSetEventParams, ResultSetSummary, SelectionData } from "../lsp/protocol";
import { exportFormats, type ExportFormat } from "../lsp/protocol";
import { ExecutionPlanView } from "../executionPlan/executionPlanView";
import { getNodeImageUrl } from "../executionPlan/nodeImages";
import { ResultsGrid } from "./resultsGrid";

export interface QueryTabHost {
    readonly client: Sql4CdsClient;
    /** Indicates if queries should be executed with the actual execution plan */
    readonly includeActualPlan: boolean;
    /** Gets the connection options to use for a PPTB connection slot */
    getConnectionOptions(slot: number): PptbConnectionOptions | null;
    confirm(params: ConfirmationParams): void;
    onStateChanged(tab: QueryTab): void;
    onStatusChanged(tab: QueryTab): void;
    /** Called when the execution plan node to show in the Properties panel changes */
    onSelectedPlanNodeChanged(tab: QueryTab): void;
    /** Opens a read-only tab with the FetchXML from a plan node */
    openFetchXml(fetchXml: string): void;
    copyToClipboard(text: string): void;
    openRecord(reference: EntityReferenceValue): void;
    copyRecordUrl(reference: EntityReferenceValue): void;
    getRecordSelectStatement(reference: EntityReferenceValue, currentDataSource?: string): Promise<string>;
}

type ResultsView = "results" | "messages" | "plan";

let nextTabId = 1;

/**
 * A single query editor tab. Each tab is a separate document in the language server with its own connection,
 * and keeps its own results and messages.
 */
export class QueryTab {
    readonly id: number;
    readonly uri: string;
    readonly model: monaco.editor.ITextModel;
    readonly resultsElement: HTMLElement;

    title: string;
    readonly readOnly: boolean;
    readonly isFetchXmlPreview: boolean;
    filePath: string | null = null;
    viewState: monaco.editor.ICodeEditorViewState | null = null;
    connectionSlot: number | null = null;
    connected = false;
    connecting = false;
    running = false;
    startTime = 0;
    connectionSummary: ConnectionCompleteParams["connectionSummary"];
    statusMessage = "Not connected";
    statusKind: "success" | "error" | "warning" | "busy" = "warning";
    gridStatus = "";
    gridStatusIsError = false;
    resultsMode: "grid" | "text" = "grid";

    private readonly resultsPane: HTMLElement;
    private readonly textPane: HTMLElement;
    private readonly messagesPane: HTMLElement;
    private readonly planPane: HTMLElement;
    private readonly planView: ExecutionPlanView;
    private readonly resultsTabButtons = new Map<ResultsView, HTMLButtonElement>();
    private readonly grids = new Map<string, ResultsGrid>();
    private readonly rowCounts = new Map<string, number>();
    private readonly planResultSets = new Set<string>();
    private readonly textResultSets = new Map<string, { summary: ResultSetSummary; element: HTMLElement }>();
    private executionResultsMode: "grid" | "text" = "grid";
    private resultsGeneration = 0;
    private readonly changeListener: monaco.IDisposable;
    private savedText: string;
    private pendingConnect: ((params: ConnectionCompleteParams) => void) | null = null;
    private hasErrors = false;
    private cancelled = false;
    private estimatedPlan = false;
    private endTime = 0;
    private engineError: string | null = null;
    private view: ResultsView = "results";

    get elapsedMilliseconds(): number {
        return this.startTime === 0 ? 0 : (this.running ? Date.now() : this.endTime) - this.startTime;
    }

    get totalRowCount(): number {
        return Array.from(this.rowCounts.values()).reduce((total, count) => total + count, 0);
    }

    /** The execution plan node to show in the Properties panel, if the execution plan is currently visible */
    get selectedPlanNode(): ExecutionPlanNode | null {
        return this.view === "plan" ? this.planView.selectedNode : null;
    }

    /** Indicates if the execution plan is currently being shown */
    get showingPlan(): boolean {
        return this.view === "plan";
    }

    constructor(
        private readonly host: QueryTabHost,
        initialText = "",
        options: { language?: string; readOnly?: boolean; isFetchXmlPreview?: boolean } = {},
    ) {
        this.id = nextTabId++;
        this.uri = `pptb://query/${this.id}`;
        this.title = `Query ${this.id}`;
        this.readOnly = options.readOnly ?? false;
        this.isFetchXmlPreview = options.isFetchXmlPreview ?? false;

        this.model = monaco.editor.createModel(initialText, options.language ?? "sql", monaco.Uri.parse(this.uri));
        // The language server splits documents on \n, so keep the model line endings consistent
        this.model.setEOL(monaco.editor.EndOfLineSequence.LF);
        this.savedText = this.model.getValue();

        this.resultsElement = document.createElement("div");
        this.resultsElement.className = "query-results";

        const resultsTabs = document.createElement("div");
        resultsTabs.className = "results-tabs";
        for (const [view, label] of [["results", "Results"], ["messages", "Messages"], ["plan", "Execution Plan"]] as const) {
            const button = document.createElement("button");
            button.textContent = label;
            button.addEventListener("click", () => this.showResultsView(view));
            this.resultsTabButtons.set(view, button);
            resultsTabs.appendChild(button);
        }
        this.resultsTabButtons.get("plan")!.hidden = true;

        this.resultsPane = document.createElement("div");
        this.resultsPane.className = "results-pane";

        this.textPane = document.createElement("div");
        this.textPane.className = "results-text";
        this.textPane.tabIndex = 0;
        this.textPane.setAttribute("aria-label", "Query results as text");

        this.messagesPane = document.createElement("div");
        this.messagesPane.className = "messages-pane";

        this.planView = new ExecutionPlanView(
            getNodeImageUrl,
            () => host.onSelectedPlanNodeChanged(this),
            undefined,
            (fetchXml) => host.openFetchXml(fetchXml),
        );
        this.planPane = document.createElement("div");
        this.planPane.className = "plan-pane";
        this.planPane.appendChild(this.planView.element);

        this.resultsElement.append(resultsTabs, this.resultsPane, this.textPane, this.messagesPane, this.planPane);
        this.showResultsView("results");

        if (!this.readOnly) {
            host.client.addListener(this.uri, {
                connectionComplete: (p) => this.onConnectionComplete(p),
                batchStart: () => {},
                message: (p) => this.addMessage(p.message.message, p.message.isError),
                resultSetAvailable: (p) => this.onResultSetAvailable(p),
                resultSetUpdated: (p) => this.onResultSetUpdated(p),
                resultSetComplete: (p) => this.updateResultSet({ ...p.resultSetSummary, complete: true }),
                batchComplete: (p) => {
                    if (p.batchSummary.hasError) this.hasErrors = true;
                },
                queryComplete: (p) => {
                    for (const batch of p.batchSummaries) {
                        if (batch.hasError) this.hasErrors = true;
                        for (const summary of batch.resultSetSummaries ?? []) this.updateResultSet(summary);
                    }
                    this.onQueryComplete();
                },
                confirmation: (p) => host.confirm(p),
                diagnostics: (p) => applyDiagnostics(this.model, p),
            });

            void host.client.openDocument(this.uri, this.model.getValue());
            this.changeListener = this.model.onDidChangeContent(() => {
                void host.client.changeDocument(this.uri, this.model.getValue()).catch(() => {});
                host.onStateChanged(this);
            });
        } else {
            this.changeListener = { dispose() {} };
        }
    }

    get isDirty(): boolean {
        return this.model.getValue() !== this.savedText;
    }

    setFile(path: string): void {
        this.filePath = path;
        this.title = path.split(/[\\/]/).pop() || path;
        this.host.onStateChanged(this);
    }

    markSaved(savedText = this.model.getValue()): void {
        this.savedText = savedText;
        this.host.onStateChanged(this);
    }

    /** Connects this tab to one of the PPTB connection slots */
    async connect(slot: number): Promise<void> {
        if (this.engineError) throw new Error(this.engineError);
        const options = this.host.getConnectionOptions(slot);
        if (!options) throw new Error("The selected connection is not available");

        if (this.connected) await this.disconnect();

        this.connectionSlot = slot;
        this.connectionSummary = undefined;
        this.connecting = true;
        this.statusMessage = "Connecting…";
        this.statusKind = "busy";
        this.host.onStateChanged(this);

        try {
            const complete = new Promise<ConnectionCompleteParams>((resolve) => (this.pendingConnect = resolve));
            const accepted = await this.host.client.associateConnection(this.uri, ConnectionRegistry.ownerUri(slot));
            if (!accepted) throw new Error("The connection request was rejected");

            const result = await complete;
            if (this.engineError) throw new Error(this.engineError);
            if (result.errorMessage) throw new Error(result.errorMessage);

            this.connected = true;
            this.connectionSummary = result.connectionSummary;
            this.statusMessage = "Connected";
            this.statusKind = "success";
        } catch (err) {
            this.statusMessage = this.engineError ?? `Connection failed: ${err instanceof Error ? err.message : String(err)}`;
            this.statusKind = "error";
            throw err;
        } finally {
            this.pendingConnect = null;
            this.connecting = false;
            this.host.onStateChanged(this);
        }
    }

    async disconnect(): Promise<void> {
        if (!this.connected) return;
        this.connected = false;
        this.connectionSummary = undefined;
        this.statusMessage = "Not connected";
        this.statusKind = "warning";
        this.host.onStateChanged(this);
        try {
            await this.host.client.disconnect(this.uri);
        } catch {
            // Ignore failures - the connection is being discarded anyway
        }
    }

    private onConnectionComplete(params: ConnectionCompleteParams): void {
        this.pendingConnect?.(params);
    }

    /**
     * Executes the selected text, or the entire document if nothing is selected. If `estimatedPlan` is set the query
     * is not run and the estimated execution plan is shown instead.
     */
    async execute(selection: monaco.Selection | null, estimatedPlan = false): Promise<void> {
        if (this.engineError) throw new Error(this.engineError);
        if (this.running) return;
        if (!this.connected) throw new Error("Select a connection before running a query");

        await this.clearResults();
        if (this.engineError) throw new Error(this.engineError);

        this.executionResultsMode = this.resultsMode;
        this.showResultsView("results");
        this.running = true;
        this.hasErrors = false;
        this.cancelled = false;
        this.estimatedPlan = estimatedPlan;
        this.startTime = Date.now();
        this.endTime = 0;
        this.statusMessage = estimatedPlan ? "Generating estimated execution plan…" : "Executing query…";
        this.statusKind = "busy";
        this.host.onStateChanged(this);

        let querySelection: SelectionData | undefined;
        if (selection && !selection.isEmpty()) {
            querySelection = {
                startLine: selection.startLineNumber - 1,
                startColumn: selection.startColumn - 1,
                endLine: selection.endLineNumber - 1,
                endColumn: selection.endColumn - 1,
            };
        }

        try {
            await this.host.client.executeDocumentSelection(this.uri, querySelection, {
                includeEstimatedExecutionPlanXml: estimatedPlan,
                includeActualExecutionPlanXml: !estimatedPlan && this.host.includeActualPlan,
            });
        } catch (err) {
            if (this.engineError) return;
            this.addMessage(err instanceof Error ? err.message : String(err), true);
            this.onQueryComplete();
        }
    }

    async cancel(): Promise<void> {
        if (!this.running) return;
        this.cancelled = true;
        this.statusMessage = "Cancelling query…";
        this.host.onStatusChanged(this);
        try {
            const result = await this.host.client.cancelQuery(this.uri);
            if (result?.messages) {
                this.cancelled = false;
                this.addMessage(result.messages, true);
            }
        } catch (err) {
            if (this.engineError) return;
            this.cancelled = false;
            this.addMessage(`Unable to cancel query: ${err instanceof Error ? err.message : String(err)}`, true);
        }
    }

    private onQueryComplete(): void {
        if (!this.running) return;
        this.running = false;
        for (const grid of this.grids.values()) grid.setExportEnabled(true);
        this.endTime = Date.now();

        const elapsed = (this.elapsedMilliseconds / 1000).toFixed(1);
        this.statusMessage = this.cancelled ? "Query cancelled"
            : this.hasErrors ? "Query completed with errors"
            : this.estimatedPlan ? "Estimated execution plan generated"
            : "Query completed successfully";
        this.statusKind = this.cancelled ? "warning" : this.hasErrors ? "error" : "success";
        this.addMessage(`${this.statusMessage} in ${elapsed}s`, false, true);

        if (this.hasErrors) this.showResultsView("messages");
        else if (this.estimatedPlan && this.planView.count > 0) this.showResultsView("plan");
        else if (this.grids.size > 0 || this.textResultSets.size > 0) this.showResultsView("results");
        else this.showResultsView("messages");
        if (this.executionResultsMode === "text") void this.renderTextResults(this.resultsGeneration);
        this.host.onStateChanged(this);
    }

    engineStopped(message: string): void {
        if (this.engineError) return;
        this.engineError = message;
        if (this.running) this.endTime = Date.now();
        this.running = false;
        this.connected = false;
        this.connecting = false;
        this.connectionSummary = undefined;
        this.cancelled = false;
        this.resultsGeneration++;
        this.gridStatus = "";
        this.gridStatusIsError = false;
        for (const grid of this.grids.values()) grid.setExportEnabled(false);
        this.pendingConnect?.({ ownerUri: this.uri, errorMessage: message });
        this.pendingConnect = null;
        this.addMessage(message, true);
        this.showResultsView("messages");
        this.host.onStateChanged(this);
    }

    private isPlanResultSet(summary: ResultSetSummary): boolean {
        return summary.specialAction?.expectYukonXMLShowPlan === true || this.planResultSets.has(this.resultSetKey(summary));
    }

    private onResultSetAvailable(params: ResultSetEventParams): void {
        const summary = params.resultSetSummary;

        // Execution plans are returned as special result sets which are shown in the Execution Plan tab instead of a grid
        if (this.isPlanResultSet(summary)) {
            this.planResultSets.add(this.resultSetKey(summary));
            this.addPlans(params);
            return;
        }

        if (this.executionResultsMode === "text") {
            const element = document.createElement("pre");
            element.textContent = `${summary.columnInfo.map((column) => column.columnName).join("\t")}\n`;
            this.textResultSets.set(this.resultSetKey(summary), { summary, element });
            this.textPane.appendChild(element);
            this.updateResultSet(summary);
            this.showResultsView("results");
            return;
        }

        const generation = this.resultsGeneration;
        const grid = new ResultsGrid(
            summary.columnInfo,
            async (start, count) => {
                const result = await this.host.client.subset(this.uri, summary.batchId, summary.id, start, count);
                return result.resultSubset.rows;
            },
            (text) => this.host.copyToClipboard(text),
            (reference) => this.host.openRecord(reference),
            (text, isError) => {
                this.gridStatus = text;
                this.gridStatusIsError = isError;
                this.host.onStatusChanged(this);
            },
            (format) => this.exportResults(summary, generation, format),
            {
                copyUrl: (reference) => this.host.copyRecordUrl(reference),
                createSelect: (reference) => void this.appendRecordSelectStatement(reference),
            },
        );
        grid.setExportEnabled(!this.running);
        this.grids.set(this.resultSetKey(summary), grid);
        this.updateResultSet(summary);
        this.resultsPane.appendChild(grid.element);
        this.resultsPane.dataset.count = String(this.grids.size);
        this.showResultsView("results");
    }

    private async appendRecordSelectStatement(reference: EntityReferenceValue): Promise<void> {
        try {
            const source = this.connectionSlot === null ? undefined : this.host.getConnectionOptions(this.connectionSlot)?.connectionName;
            const sql = await this.host.getRecordSelectStatement(reference, source);
            if (this.model.isDisposed()) return;
            if (this.readOnly) throw new Error("The query editor is read-only");
            const line = this.model.getLineCount();
            const column = this.model.getLineMaxColumn(line);
            const text = `${this.model.getValue() ? "\n\n" : ""}${sql}`;
            this.model.pushStackElement();
            this.model.pushEditOperations([], [{
                range: new monaco.Range(line, column, line, column),
                text,
                forceMoveMarkers: true,
            }], () => null);
            this.model.pushStackElement();
        } catch (err) {
            this.gridStatus = `Unable to create SELECT statement: ${err instanceof Error ? err.message : String(err)}`;
            this.gridStatusIsError = true;
            this.host.onStatusChanged(this);
            await window.toolboxAPI.utils.showNotification({ title: "Unable to create SELECT statement", body: this.gridStatus, type: "error" });
        }
    }

    private addMessage(text: string, isError: boolean, isSummary = false): void {
        if (isError) {
            this.hasErrors = true;
            this.statusMessage = text;
            this.statusKind = "error";
            this.host.onStatusChanged(this);
        }

        const line = document.createElement("div");
        line.className = isError ? "message error" : isSummary ? "message summary" : "message";
        line.textContent = text;
        this.messagesPane.appendChild(line);
        this.messagesPane.scrollTop = this.messagesPane.scrollHeight;
    }

    private async exportResults(summary: ResultSetSummary, generation: number, format: ExportFormat): Promise<void> {
        const exportFormat = exportFormats.find((entry) => entry.format === format);
        if (!exportFormat) throw new Error("Unsupported export format");
        try {
            if (generation !== this.resultsGeneration || this.running) throw new Error("These results are no longer available for export");
            // saveFile supplies the native Save As dialog; the worker replaces the empty file with the export.
            const path = await window.toolboxAPI.fileSystem.saveFile(
                `${this.title.replace(/\.sql$/i, "")}-results-${summary.batchId + 1}-${summary.id + 1}.${exportFormat.extension}`,
                "",
                [{ name: exportFormat.label, extensions: [exportFormat.extension] }],
            );
            if (!path) return;
            if (generation !== this.resultsGeneration || this.running) throw new Error("The query results changed while choosing the export filename. Please export again.");
            this.gridStatus = `Exporting ${exportFormat.label} results...`;
            this.gridStatusIsError = false;
            this.host.onStatusChanged(this);
            await this.host.client.saveResults(this.uri, summary.batchId, summary.id, path, format);
            if (generation !== this.resultsGeneration) return;
            this.gridStatus = `Results exported to ${path}`;
            this.host.onStatusChanged(this);
        } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            if (generation === this.resultsGeneration) {
                this.gridStatus = `Unable to export results: ${message}`;
                this.gridStatusIsError = true;
                this.host.onStatusChanged(this);
            }
            await window.toolboxAPI.utils.showNotification({ title: "Unable to export results", body: message, type: "error" });
        }
    }

    private resultSetKey(summary: ResultSetSummary): string {
        return `${summary.batchId}:${summary.id}`;
    }

    private onResultSetUpdated(params: ResultSetEventParams): void {
        if (this.isPlanResultSet(params.resultSetSummary)) {
            this.planResultSets.add(this.resultSetKey(params.resultSetSummary));
            this.addPlans(params);
            return;
        }

        this.updateResultSet(params.resultSetSummary);
    }

    private addPlans(params: ResultSetEventParams): void {
        if (!params.executionPlans?.length) return;

        for (const plan of params.executionPlans) this.planView.addPlan(plan);
        this.resultsTabButtons.get("plan")!.hidden = false;
    }

    private updateResultSet(summary: ResultSetSummary): void {
        if (this.isPlanResultSet(summary)) return;

        const key = this.resultSetKey(summary);
        this.rowCounts.set(key, summary.rowCount);
        this.grids.get(key)?.setRowCount(summary.rowCount, summary.complete);
        const textResult = this.textResultSets.get(key);
        if (textResult) textResult.summary = summary;
        this.host.onStatusChanged(this);
    }

    private async renderTextResults(generation: number): Promise<void> {
        try {
            for (const { summary, element } of this.textResultSets.values()) {
                for (let start = 0; start < summary.rowCount;) {
                    const result = await this.host.client.subset(this.uri, summary.batchId, summary.id, start, Math.min(200, summary.rowCount - start));
                    if (generation !== this.resultsGeneration) return;
                    const rows = result.resultSubset.rows;
                    if (rows.length === 0) throw new Error("The server returned no rows before the end of the result set");
                    element.appendChild(document.createTextNode(rows.map((row) => row.map((cell) => cell.isNull ? "NULL" : cell.displayValue).join("\t")).join("\n") + "\n"));
                    start += rows.length;
                }
            }
        } catch (err) {
            if (generation !== this.resultsGeneration) return;
            this.addMessage(`Unable to load text results: ${err instanceof Error ? err.message : String(err)}`, true);
            this.showResultsView("messages");
        }
    }

    private showResultsView(view: ResultsView): void {
        const changed = this.view !== view;
        this.view = view;
        this.resultsPane.hidden = view !== "results" || this.executionResultsMode !== "grid";
        this.textPane.hidden = view !== "results" || this.executionResultsMode !== "text";
        this.messagesPane.hidden = view !== "messages";
        this.planPane.hidden = view !== "plan";
        for (const [key, button] of this.resultsTabButtons) {
            button.classList.toggle("active", key === view);
            button.setAttribute("aria-pressed", String(key === view));
        }
        if (changed) this.host.onSelectedPlanNodeChanged(this);
    }

    private async clearResults(): Promise<void> {
        this.resultsGeneration++;
        this.textResultSets.clear();
        this.textPane.replaceChildren();
        for (const grid of this.grids.values()) grid.dispose();
        this.grids.clear();
        this.rowCounts.clear();
        this.planResultSets.clear();
        this.planView.clear();
        this.resultsTabButtons.get("plan")!.hidden = true;
        if (this.view === "plan") this.showResultsView("results");
        this.gridStatus = "";
        this.gridStatusIsError = false;
        this.resultsPane.dataset.count = "0";
        this.messagesPane.replaceChildren();
        monaco.editor.setModelMarkers(this.model, "sql4cds", []);

        try {
            await this.host.client.disposeQuery(this.uri);
        } catch {
            // There may not be a previous query to dispose
        }
    }

    async dispose(): Promise<void> {
        this.resultsGeneration++;
        this.changeListener.dispose();
        for (const grid of this.grids.values()) grid.dispose();
        this.grids.clear();
        if (!this.readOnly)
            this.host.client.removeListener(this.uri);

        if (!this.readOnly && this.host.client.isRunning) {
            try {
                if (this.running) await this.host.client.cancelQuery(this.uri);
                await this.host.client.disposeQuery(this.uri);
                await this.disconnect();
                await this.host.client.closeDocument(this.uri);
            } catch {
                // Ignore errors while closing the tab
            }
        }

        this.model.dispose();
        this.resultsElement.remove();
    }
}
