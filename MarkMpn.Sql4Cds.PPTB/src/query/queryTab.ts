import { monaco, applyDiagnostics } from "../editor/monacoSetup";
import { ConnectionRegistry } from "../connections";
import type { Sql4CdsClient } from "../lsp/client";
import type { ConfirmationParams, ConnectionCompleteParams, EntityReferenceValue, PptbConnectionOptions, ResultSetEventParams, ResultSetSummary, SelectionData } from "../lsp/protocol";
import { ResultsGrid } from "./resultsGrid";

export interface QueryTabHost {
    readonly client: Sql4CdsClient;
    /** Gets the connection options to use for a PPTB connection slot */
    getConnectionOptions(slot: number): PptbConnectionOptions | null;
    confirm(params: ConfirmationParams): void;
    onStateChanged(tab: QueryTab): void;
    onStatusChanged(tab: QueryTab): void;
    copyToClipboard(text: string): void;
    openRecord(reference: EntityReferenceValue): void;
}

type ResultsView = "results" | "messages";

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

    private readonly resultsPane: HTMLElement;
    private readonly messagesPane: HTMLElement;
    private readonly resultsTabButtons = new Map<ResultsView, HTMLButtonElement>();
    private readonly grids = new Map<string, ResultsGrid>();
    private readonly rowCounts = new Map<string, number>();
    private readonly changeListener: monaco.IDisposable;
    private pendingConnect: ((params: ConnectionCompleteParams) => void) | null = null;
    private hasErrors = false;
    private cancelled = false;
    private endTime = 0;

    get elapsedMilliseconds(): number {
        return this.startTime === 0 ? 0 : (this.running ? Date.now() : this.endTime) - this.startTime;
    }

    get totalRowCount(): number {
        return Array.from(this.rowCounts.values()).reduce((total, count) => total + count, 0);
    }

    constructor(private readonly host: QueryTabHost, initialText = "") {
        this.id = nextTabId++;
        this.uri = `pptb://query/${this.id}`;
        this.title = `Query ${this.id}`;

        this.model = monaco.editor.createModel(initialText, "sql", monaco.Uri.parse(this.uri));
        // The language server splits documents on \n, so keep the model line endings consistent
        this.model.setEOL(monaco.editor.EndOfLineSequence.LF);

        this.resultsElement = document.createElement("div");
        this.resultsElement.className = "query-results";

        const resultsTabs = document.createElement("div");
        resultsTabs.className = "results-tabs";
        for (const [view, label] of [["results", "Results"], ["messages", "Messages"]] as const) {
            const button = document.createElement("button");
            button.textContent = label;
            button.addEventListener("click", () => this.showResultsView(view));
            this.resultsTabButtons.set(view, button);
            resultsTabs.appendChild(button);
        }

        this.resultsPane = document.createElement("div");
        this.resultsPane.className = "results-pane";

        this.messagesPane = document.createElement("div");
        this.messagesPane.className = "messages-pane";

        this.resultsElement.append(resultsTabs, this.resultsPane, this.messagesPane);
        this.showResultsView("results");

        host.client.addListener(this.uri, {
            connectionComplete: (p) => this.onConnectionComplete(p),
            batchStart: () => {},
            message: (p) => this.addMessage(p.message.message, p.message.isError),
            resultSetAvailable: (p) => this.onResultSetAvailable(p),
            resultSetUpdated: (p) => this.updateResultSet(p.resultSetSummary),
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
        });
    }

    /** Connects this tab to one of the PPTB connection slots */
    async connect(slot: number): Promise<void> {
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
            if (result.errorMessage) throw new Error(result.errorMessage);

            this.connected = true;
            this.connectionSummary = result.connectionSummary;
            this.statusMessage = "Connected";
            this.statusKind = "success";
        } catch (err) {
            this.statusMessage = `Connection failed: ${err instanceof Error ? err.message : String(err)}`;
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

    /** Executes the selected text, or the entire document if nothing is selected */
    async execute(selection: monaco.Selection | null): Promise<void> {
        if (this.running) return;
        if (!this.connected) throw new Error("Select a connection before running a query");

        await this.clearResults();

        this.running = true;
        this.hasErrors = false;
        this.cancelled = false;
        this.startTime = Date.now();
        this.endTime = 0;
        this.statusMessage = "Executing query…";
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
            await this.host.client.executeDocumentSelection(this.uri, querySelection);
        } catch (err) {
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
            this.cancelled = false;
            this.addMessage(`Unable to cancel query: ${err instanceof Error ? err.message : String(err)}`, true);
        }
    }

    private onQueryComplete(): void {
        if (!this.running) return;
        this.running = false;
        this.endTime = Date.now();

        const elapsed = (this.elapsedMilliseconds / 1000).toFixed(1);
        this.statusMessage = this.cancelled ? "Query cancelled" : this.hasErrors ? "Query completed with errors" : "Query completed successfully";
        this.statusKind = this.cancelled ? "warning" : this.hasErrors ? "error" : "success";
        this.addMessage(`${this.statusMessage} in ${elapsed}s`, false, true);

        // Switch to the messages if there's nothing else to show
        if (this.grids.size === 0 || this.hasErrors) this.showResultsView("messages");
        this.host.onStateChanged(this);
    }

    private onResultSetAvailable(params: ResultSetEventParams): void {
        const summary = params.resultSetSummary;
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
        );
        this.grids.set(this.resultSetKey(summary), grid);
        this.updateResultSet(summary);
        this.resultsPane.appendChild(grid.element);
        this.resultsPane.dataset.count = String(this.grids.size);
        this.showResultsView("results");
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

    private resultSetKey(summary: ResultSetSummary): string {
        return `${summary.batchId}:${summary.id}`;
    }

    private updateResultSet(summary: ResultSetSummary): void {
        const key = this.resultSetKey(summary);
        this.rowCounts.set(key, summary.rowCount);
        this.grids.get(key)?.setRowCount(summary.rowCount, summary.complete);
        this.host.onStatusChanged(this);
    }

    private showResultsView(view: ResultsView): void {
        this.resultsPane.hidden = view !== "results";
        this.messagesPane.hidden = view !== "messages";
        for (const [key, button] of this.resultsTabButtons) button.classList.toggle("active", key === view);
    }

    private async clearResults(): Promise<void> {
        for (const grid of this.grids.values()) grid.dispose();
        this.grids.clear();
        this.rowCounts.clear();
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
        this.changeListener.dispose();
        for (const grid of this.grids.values()) grid.dispose();
        this.grids.clear();
        this.host.client.removeListener(this.uri);

        if (this.host.client.isRunning) {
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
