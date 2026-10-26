import type {
    BatchEventParams,
    BatchSummary,
    CompletionItem,
    CompletionList,
    Hover,
    FormattingOptions,
    TextEdit,
    SignatureHelp,
    ConfirmationParams,
    ConfirmationResult,
    ConnectionCompleteParams,
    DataverseChunkParams,
    DataverseMessageParams,
    DataverseResponse,
    DbCellValue,
    DocumentListener,
    ExecuteBatchParams,
    ExecutionPlanOptions,
    ExportFormat,
    LogMessageParams,
    MessageParams,
    ObjectExplorerSessionCreated,
    ObjectExplorerExpandComplete,
    ObjectExplorerSessionDisconnected,
    PptbConnectionOptions,
    PublishDiagnosticsParams,
    QueryCompleteParams,
    QueryEventsParams,
    ResultSetEventParams,
    ResultSetSummary,
    SelectionData,
    SubsetResult,
    WebApiRequest,
    WebApiResponse,
} from "./protocol";

/** The worker ID declared in pptb.config.json */
const WORKER_ID = "sql4cds";
const MAX_RPC_MESSAGE_SIZE = 32 * 1024 * 1024;
const INITIAL_ROW_COUNT = 200;
const DATAVERSE_CHUNK_METHOD = "dataverse/executeBatch/chunk";
const DATAVERSE_MESSAGE_METHOD = "dataverse/executeBatch/message";

export interface ClientEvents {
    objectExplorerSessionCreated?(params: ObjectExplorerSessionCreated): void;
    objectExplorerExpandComplete?(params: ObjectExplorerExpandComplete): void;
    objectExplorerSessionDisconnected?(params: ObjectExplorerSessionDisconnected): void;
    /** Status messages such as metadata cache loading progress */
    progress?(message: string): void;
    log?(params: LogMessageParams): void;
    exit?(snapshot: ToolBoxAPI.WorkerSnapshot): void;
    failed?(message: string): void;
}

function logRpc(level: "debug" | "error", message: string, payload: unknown): void {
    console[level](`[SQL 4 CDS RPC] [${new Date().toISOString()}] ${message}`, payload);
}

function logRpcRequest(
    method: string,
    handler: (params: unknown, context: ToolBoxAPI.WorkerRequestContext) => unknown | Promise<unknown>,
) {
    return async (params: unknown, context: ToolBoxAPI.WorkerRequestContext): Promise<unknown> => {
        logRpc("debug", `<- ${method} request`, params);

        try {
            const result = await handler(params, context);
            logRpc("debug", `-> ${method} response`, result);
            return result;
        } catch (error) {
            logRpc("error", `-> ${method} error`, error);
            throw error;
        }
    };
}

function logRpcNotification(method: string, handler: (params: unknown) => void) {
    return (params: unknown): void => {
        logRpc("debug", `<- ${method} notification`, params);

        try {
            handler(params);
        } catch (error) {
            logRpc("error", `<- ${method} notification error`, error);
            throw error;
        }
    };
}

interface ChunkAssembly {
    nextSequence: number;
    chunks: string[];
}

class ChunkProtocolError extends Error {}

function rpcMessageSize(method: string, params: DataverseChunkParams | DataverseMessageParams): number {
    return new TextEncoder().encode(JSON.stringify({ jsonrpc: "2.0", method, params })).byteLength;
}

function splitRpcPayload(payload: string, id: string): DataverseChunkParams[] {
    const codePoints = Array.from(payload);
    const chunks: DataverseChunkParams[] = [];
    let offset = 0;

    while (offset < codePoints.length) {
        const sequence = chunks.length;
        let low = 1;
        let high = codePoints.length - offset;
        let best = 0;

        while (low <= high) {
            const count = Math.floor((low + high) / 2);
            const data = codePoints.slice(offset, offset + count).join("");
            const candidate: DataverseChunkParams = { id, sequence, complete: false, data };

            if (rpcMessageSize(DATAVERSE_CHUNK_METHOD, candidate) <= MAX_RPC_MESSAGE_SIZE) {
                best = count;
                low = count + 1;
            } else {
                high = count - 1;
            }
        }

        if (best === 0) throw new Error("Unable to fit a Dataverse RPC chunk within the PPTB message size limit");
        chunks.push({
            id,
            sequence,
            complete: false,
            data: codePoints.slice(offset, offset + best).join(""),
        });
        offset += best;
    }

    if (chunks.length === 0) chunks.push({ id, sequence: 0, complete: true, data: "" });
    else chunks[chunks.length - 1].complete = true;

    if (chunks.some((chunk) => rpcMessageSize(DATAVERSE_CHUNK_METHOD, chunk) > MAX_RPC_MESSAGE_SIZE)) {
        throw new Error("A Dataverse RPC chunk exceeds the PPTB message size limit");
    }

    return chunks;
}

/**
 * Wraps the PPTB worker session that hosts the SQL 4 CDS language server.
 *
 * Worker handlers are fixed when the session is created, so notifications that relate to a single editor are
 * routed to the listener registered for that editor's URI.
 */
export class Sql4CdsClient {
    private session: ToolBoxAPI.WorkerSession | null = null;
    private failure: string | null = null;
    private stopping = false;
    private readonly listeners = new Map<string, DocumentListener>();
    private readonly versions = new Map<string, number>();
    private readonly chunkAssemblies = new Map<string, ChunkAssembly>();
    private compactQueryEvents = false;
    private initialRowCount = 0;
    private readonly initialRows = new Map<string, Map<string, { rows: DbCellValue[][]; rowCount: number }>>();
    private executionSequence = 0;
    private readonly executions = new Map<string, {
        id: string;
        batches: Map<number, BatchSummary>;
        results: Map<string, ResultSetSummary>;
    }>();

    constructor(private readonly events: ClientEvents) {}

    async start(settings: Record<string, unknown>): Promise<void> {
        if (this.failure) throw new Error(this.failure);
        this.stopping = false;
        const session = await window.toolboxAPI.workers.connect(WORKER_ID, {
            requests: {
                "dataverse/executeBatch": logRpcRequest("dataverse/executeBatch", (params) => executeBatch(params as ExecuteBatchParams)),
            },
            notifications: {
                "sql4cds/queryEvents": logRpcNotification("sql4cds/queryEvents", (p) => this.receiveQueryEvents(p as QueryEventsParams)),
                [DATAVERSE_CHUNK_METHOD]: (p) => this.receiveDataverseChunk(p as DataverseChunkParams),
                [DATAVERSE_MESSAGE_METHOD]: (p) => this.receiveDataverseMessage(p as DataverseMessageParams),
                "connection/complete": logRpcNotification("connection/complete", (p) => this.route(p as ConnectionCompleteParams, "connectionComplete")),
                "objectexplorer/sessioncreated": logRpcNotification("objectexplorer/sessioncreated", (p) => this.events.objectExplorerSessionCreated?.(p as ObjectExplorerSessionCreated)),
                "objectexplorer/expandCompleted": logRpcNotification("objectexplorer/expandCompleted", (p) => this.events.objectExplorerExpandComplete?.(p as ObjectExplorerExpandComplete)),
                "objectexplorer/sessiondisconnected": logRpcNotification("objectexplorer/sessiondisconnected", (p) => this.events.objectExplorerSessionDisconnected?.(p as ObjectExplorerSessionDisconnected)),
                "query/batchStart": logRpcNotification("query/batchStart", (p) => this.route(p as BatchEventParams, "batchStart")),
                "query/batchComplete": logRpcNotification("query/batchComplete", (p) => this.route(p as BatchEventParams, "batchComplete")),
                "query/message": logRpcNotification("query/message", (p) => this.route(p as MessageParams, "message")),
                "query/resultSetAvailable": logRpcNotification("query/resultSetAvailable", (p) => this.route(p as ResultSetEventParams, "resultSetAvailable")),
                "query/resultSetUpdated": logRpcNotification("query/resultSetUpdated", (p) => this.route(p as ResultSetEventParams, "resultSetUpdated")),
                "query/resultSetComplete": logRpcNotification("query/resultSetComplete", (p) => this.route(p as ResultSetEventParams, "resultSetComplete")),
                "query/complete": logRpcNotification("query/complete", (p) => this.route(p as QueryCompleteParams, "queryComplete")),
                "sql4cds/confirmation": logRpcNotification("sql4cds/confirmation", (p) => this.route(p as ConfirmationParams, "confirmation")),
                "sql4cds/progress": logRpcNotification("sql4cds/progress", (p) => this.events.progress?.(String(Array.isArray(p) ? p[0] : p))),
                "textDocument/publishDiagnostics": logRpcNotification("textDocument/publishDiagnostics", (p) => {
                    const params = p as PublishDiagnosticsParams;
                    this.listeners.get(params.uri)?.diagnostics?.(params);
                }),
                "window/logMessage": logRpcNotification("window/logMessage", (p) => this.events.log?.(p as LogMessageParams)),
            },
            onExit: (snapshot) => {
                this.executions.clear();
                this.initialRows.clear();
                this.compactQueryEvents = false;
                this.initialRowCount = 0;
                this.session = null;
                if (!this.stopping)
                    this.failWorker(`SQL 4 CDS engine stopped${snapshot.failure ? `: ${snapshot.failure}` : ""}`);
                this.events.exit?.(snapshot);
            },
        });

        await session.ready;
        if (this.failure) throw new Error(this.failure);
        this.session = session;

        const initialized = await this.request<{ capabilities?: { experimental?: { sql4cds?: { queryEvents?: { version?: number; initialRowCount?: number } } } } }>("initialize", {
            processId: null,
            clientInfo: { name: "Power Platform ToolBox" },
            rootUri: null,
            capabilities: {
                textDocument: {
                    synchronization: { dynamicRegistration: false, didSave: false },
                    completion: { completionItem: { documentationFormat: ["plaintext"] } },
                    hover: { contentFormat: ["markdown", "plaintext"] },
                    signatureHelp: { signatureInformation: { documentationFormat: ["plaintext"] } },
                    publishDiagnostics: {},
                    formatting: { dynamicRegistration: false },
                },
                workspace: { configuration: false, didChangeConfiguration: { dynamicRegistration: false } },
                experimental: {
                    pptb: { maxMessageSizeBytes: MAX_RPC_MESSAGE_SIZE },
                    sql4cds: { queryEvents: { version: 1, initialRowCount: INITIAL_ROW_COUNT } },
                },
            },
        });
        this.compactQueryEvents = initialized.capabilities?.experimental?.sql4cds?.queryEvents?.version === 1;
        const initialRowCount = initialized.capabilities?.experimental?.sql4cds?.queryEvents?.initialRowCount ?? 0;
        if (!Number.isInteger(initialRowCount) || initialRowCount < 0 || initialRowCount > INITIAL_ROW_COUNT)
            throw new Error("The server acknowledged an invalid initial row count");
        this.initialRowCount = this.compactQueryEvents ? initialRowCount : 0;
        await this.notify("initialized", {});
        await this.changeConfiguration(settings);
    }

    async stop(): Promise<void> {
        const session = this.session;
        if (!session) return;

        this.stopping = true;
        try {
            await session.stop();
        } finally {
            this.executions.clear();
            this.initialRows.clear();
            this.chunkAssemblies.clear();
            this.compactQueryEvents = false;
            this.initialRowCount = 0;
            this.session = null;
        }
    }

    get isRunning(): boolean {
        return this.session !== null && this.failure === null;
    }

    private get rpc(): ToolBoxAPI.WorkerSession {
        if (this.failure) throw new Error(this.failure);
        if (!this.session) throw new Error("The SQL 4 CDS worker is not running");
        return this.session;
    }

    private failWorker(message: string): void {
        if (this.failure || this.stopping) return;
        this.failure = `${message}. Close and reopen the SQL 4 CDS tool to restart the engine.`;
        this.executions.clear();
        this.initialRows.clear();
        this.chunkAssemblies.clear();
        this.compactQueryEvents = false;
        this.initialRowCount = 0;
        this.events.failed?.(this.failure);
    }

    private checkWorkerFailure(error: unknown): void {
        const code = typeof error === "object" && error !== null && "code" in error ? error.code : undefined;
        const message = typeof error === "object" && error !== null && "message" in error ? String(error.message) : String(error);
        if ((typeof code === "string" && /^WORKER_(FAILED|EXITED|STOPPED|EXITED_BEFORE_READY)$/.test(code)) ||
            /\bWorker API: WORKER_(FAILED|EXITED|STOPPED|EXITED_BEFORE_READY)\b/.test(message))
            this.failWorker(`SQL 4 CDS engine failed: ${message}`);
    }

    private async request<Result>(method: string, params?: unknown): Promise<Result> {
        logRpc("debug", `-> ${method} request`, params);

        try {
            const result = await this.rpc.request<Result>(method, params);
            logRpc("debug", `<- ${method} response`, result);
            return result;
        } catch (error) {
            logRpc("error", `<- ${method} error`, error);
            this.checkWorkerFailure(error);
            throw error;
        }
    }

    private async notify(method: string, params?: unknown): Promise<void> {
        logRpc("debug", `-> ${method} notification`, params);

        try {
            await this.rpc.notify(method, params);
        } catch (error) {
            logRpc("error", `-> ${method} notification error`, error);
            this.checkWorkerFailure(error);
            throw error;
        }
    }

    private async notifyChunk(chunk: DataverseChunkParams): Promise<void> {
        logRpc("debug", `-> ${DATAVERSE_CHUNK_METHOD} response chunk`, {
            id: chunk.id,
            sequence: chunk.sequence,
            complete: chunk.complete,
            bytes: rpcMessageSize(DATAVERSE_CHUNK_METHOD, chunk),
        });
        try {
            await this.rpc.notify(DATAVERSE_CHUNK_METHOD, chunk);
        } catch (error) {
            this.checkWorkerFailure(error);
            throw error;
        }
    }

    private async sendDataverseResponse(id: string, data: DataverseResponse): Promise<void> {
        const message: DataverseMessageParams = { id, data };
        if (rpcMessageSize(DATAVERSE_MESSAGE_METHOD, message) <= MAX_RPC_MESSAGE_SIZE) {
            await this.notify(DATAVERSE_MESSAGE_METHOD, message);
        } else {
            for (const chunk of splitRpcPayload(JSON.stringify(data), id)) await this.notifyChunk(chunk);
        }
    }

    private receiveDataverseMessage(message: DataverseMessageParams): void {
        logRpc("debug", `<- ${DATAVERSE_MESSAGE_METHOD} notification`, message);
        void this.processDataverseMessage(message).catch((error) => this.handleDataverseError(error, message));
    }

    private async processDataverseMessage(message: DataverseMessageParams): Promise<void> {
        if (!message.id || !message.data || typeof message.data !== "object" || !("requests" in message.data)) {
            throw new ChunkProtocolError("Invalid Dataverse RPC message");
        }
        if (rpcMessageSize(DATAVERSE_MESSAGE_METHOD, message) > MAX_RPC_MESSAGE_SIZE) {
            throw new ChunkProtocolError("Received a Dataverse RPC message larger than the PPTB message size limit");
        }
        if (this.chunkAssemblies.delete(message.id)) {
            throw new ChunkProtocolError("Received a non-chunked Dataverse RPC request during a chunked transfer");
        }
        await this.executeDataverseRequest(message.id, message.data);
    }

    private async handleDataverseError(error: unknown, message: Pick<DataverseMessageParams, "id">): Promise<void> {
        logRpc("error", "Dataverse message processing failed", error);
        this.checkWorkerFailure(error);
        if (!(error instanceof ChunkProtocolError) || !message.id) return;

        try {
            await this.sendDataverseResponse(message.id, { error: { message: error.message } });
        } catch (responseError) {
            logRpc("error", "Failed to send Dataverse error response", responseError);
        }
    }

    private receiveDataverseChunk(chunk: DataverseChunkParams): void {
        logRpc("debug", `<- ${DATAVERSE_CHUNK_METHOD} request chunk`, {
            id: chunk.id,
            sequence: chunk.sequence,
            complete: chunk.complete,
            bytes: rpcMessageSize(DATAVERSE_CHUNK_METHOD, chunk),
        });

        void this.processDataverseChunk(chunk).catch((error) => this.handleDataverseError(error, chunk));
    }

    private async processDataverseChunk(chunk: DataverseChunkParams): Promise<void> {
        if (!chunk.id || !Number.isInteger(chunk.sequence) || chunk.sequence < 0 || typeof chunk.complete !== "boolean" || typeof chunk.data !== "string") {
            throw new ChunkProtocolError("Invalid Dataverse RPC chunk");
        }

        if (rpcMessageSize(DATAVERSE_CHUNK_METHOD, chunk) > MAX_RPC_MESSAGE_SIZE) {
            throw new ChunkProtocolError("Received a Dataverse RPC chunk larger than the PPTB message size limit");
        }

        let assembly = this.chunkAssemblies.get(chunk.id);
        if (!assembly) {
            if (chunk.sequence !== 0) throw new ChunkProtocolError(`Dataverse RPC chunk sequence starts at ${chunk.sequence} instead of 0`);
            assembly = { nextSequence: 0, chunks: [] };
            this.chunkAssemblies.set(chunk.id, assembly);
        }

        if (chunk.sequence !== assembly.nextSequence) {
            this.chunkAssemblies.delete(chunk.id);
            throw new ChunkProtocolError(`Unexpected Dataverse RPC chunk sequence ${chunk.sequence}; expected ${assembly.nextSequence}`);
        }

        assembly.chunks.push(chunk.data);
        assembly.nextSequence++;
        if (!chunk.complete) return;

        this.chunkAssemblies.delete(chunk.id);

        let request: ExecuteBatchParams;
        try {
            request = JSON.parse(assembly.chunks.join("")) as ExecuteBatchParams;
        } catch (error) {
            await this.sendDataverseResponse(chunk.id, { error: { message: error instanceof Error ? error.message : String(error) } });
            return;
        }
        await this.executeDataverseRequest(chunk.id, request);
    }

    private async executeDataverseRequest(id: string, request: ExecuteBatchParams): Promise<void> {
        let response: DataverseResponse;
        try {
            const result = await executeBatch(request);
            response = { result };
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            response = { error: { message } };
        }

        await this.sendDataverseResponse(id, response);
    }

    private route<K extends keyof DocumentListener, P extends { ownerUri: string }>(params: P, event: K): void {
        if (this.failure) return;
        const handler = this.listeners.get(params.ownerUri)?.[event] as ((p: P) => void) | undefined;
        handler?.(params);
    }

    private receiveQueryEvents(params: QueryEventsParams): void {
        const execution = this.executions.get(params.ownerUri);
        if (!this.compactQueryEvents || !execution || execution.id !== params.executionId) return;
        const ownerUri = params.ownerUri;
        for (const event of params.events) {
            if (this.executions.get(ownerUri) !== execution) return;
            switch (event.type) {
                case "resultSet": {
                    const key = `${event.data.batchId}:${event.data.id}`;
                    const previous = execution.results.get(key);
                    const { executionPlans, rows, rowsStartIndex, ...delta } = event.data;
                    const summary = { ...previous, ...delta };
                    if (!summary.columnInfo) throw new Error("Query result metadata was not introduced before its update");
                    const result: ResultSetSummary = { ...summary, columnInfo: summary.columnInfo };
                    const cache = this.initialRows.get(ownerUri);
                    if (cache) {
                        const cached = cache.get(key) ?? { rows: [], rowCount: 0 };
                        if (rows !== undefined) {
                            if (rowsStartIndex !== cached.rows.length || rowsStartIndex + rows.length > this.initialRowCount ||
                                rowsStartIndex + rows.length > result.rowCount)
                                throw new Error("Initial query rows are not a contiguous prefix of the result");
                            cached.rows.push(...rows);
                        }
                        cached.rowCount = result.rowCount;
                        cache.set(key, cached);
                    }
                    execution.results.set(key, result);
                    if (!previous) this.route({ ownerUri, resultSetSummary: result }, "resultSetAvailable");
                    this.route({ ownerUri, resultSetSummary: result, executionPlans }, "resultSetUpdated");
                    if (result.complete && !previous?.complete)
                        this.route({ ownerUri, resultSetSummary: result }, "resultSetComplete");
                    break;
                }
                case "query/batchStart":
                case "query/batchComplete": {
                    const batchSummary = {
                        ...event.data,
                        resultSetSummaries: [...execution.results.values()].filter(result => result.batchId === event.data.id),
                    };
                    execution.batches.set(batchSummary.id, batchSummary);
                    this.route({ ownerUri, batchSummary }, event.type === "query/batchStart" ? "batchStart" : "batchComplete");
                    break;
                }
                case "query/message":
                    this.route({ ownerUri, message: event.data }, "message");
                    break;
                case "sql4cds/progress":
                    this.events.progress?.(event.data);
                    break;
                case "query/complete": {
                    const batchSummaries = [...execution.batches.values()].map(batch => ({
                        ...batch,
                        resultSetSummaries: [...execution.results.values()].filter(result => result.batchId === batch.id),
                    }));
                    this.executions.delete(ownerUri);
                    this.route({ ownerUri, batchSummaries }, "queryComplete");
                    break;
                }
                default:
                    throw new Error("Unsupported compact query event");
            }
        }
    }

    addListener(uri: string, listener: DocumentListener): void {
        this.listeners.set(uri, listener);
    }

    removeListener(uri: string): void {
        this.listeners.delete(uri);
        this.executions.delete(uri);
        this.initialRows.delete(uri);
    }

    changeConfiguration(settings: Record<string, unknown>): Promise<void> {
        return this.notify("workspace/didChangeConfiguration", { settings: { SQL4CDS: settings } });
    }

    transformQuery(ownerUri: string, query: string, target: "fetchxml" | "m"): Promise<string[]> {
        return this.request("sql4cds/transform", { ownerUri, query, target });
    }

    formatDocument(uri: string, options: FormattingOptions): Promise<TextEdit[] | null> {
        return this.request("textDocument/formatting", { textDocument: { uri }, options });
    }

    // Text document synchronization. The server only supports full document sync.

    openDocument(uri: string, text: string): Promise<void> {
        this.versions.set(uri, 1);
        return this.notify("textDocument/didOpen", { textDocument: { uri, languageId: "sql", version: 1, text } });
    }

    changeDocument(uri: string, text: string): Promise<void> {
        const version = (this.versions.get(uri) ?? 1) + 1;
        this.versions.set(uri, version);
        return this.notify("textDocument/didChange", { textDocument: { uri, version }, contentChanges: [{ text }] });
    }

    closeDocument(uri: string): Promise<void> {
        this.versions.delete(uri);
        this.executions.delete(uri);
        this.initialRows.delete(uri);
        return this.notify("textDocument/didClose", { textDocument: { uri } });
    }

    // Language features

    completion(uri: string, line: number, character: number, triggerCharacter?: string) {
        return this.request<CompletionItem[] | CompletionList | null>("textDocument/completion", {
            textDocument: { uri },
            position: { line, character },
            context: triggerCharacter ? { triggerKind: 2, triggerCharacter } : { triggerKind: 1 },
        });
    }

    hover(uri: string, line: number, character: number) {
        return this.request<Hover | null>("textDocument/hover", { textDocument: { uri }, position: { line, character } });
    }

    signatureHelp(uri: string, line: number, character: number) {
        return this.request<SignatureHelp | null>("textDocument/signatureHelp", { textDocument: { uri }, position: { line, character } });
    }

    // Connections

    connect(ownerUri: string, options: PptbConnectionOptions): Promise<boolean> {
        return this.request<boolean>("connection/connect", { ownerUri, connection: { options } });
    }

    associateConnection(ownerUri: string, sourceOwnerUri: string): Promise<boolean> {
        return this.request<boolean>("connection/associate", { ownerUri, sourceOwnerUri });
    }

    disconnect(ownerUri: string): Promise<boolean> {
        return this.request<boolean>("connection/disconnect", { ownerUri });
    }

    // Object Explorer

    createObjectExplorerSession(options: PptbConnectionOptions): Promise<{ sessionId: string }> {
        return this.request("objectexplorer/createsession", { options });
    }

    expandObjectExplorerNode(sessionId: string, nodePath: string): Promise<boolean> {
        return this.request("objectexplorer/expand", { sessionId, nodePath });
    }

    refreshObjectExplorerNode(sessionId: string, nodePath: string): Promise<boolean> {
        return this.request("objectexplorer/refresh", { sessionId, nodePath });
    }

    closeObjectExplorerSession(sessionId: string): Promise<ObjectExplorerSessionDisconnected> {
        return this.request("objectexplorer/closesession", { sessionId });
    }

    // Query execution

    executeDocumentSelection(
        ownerUri: string,
        querySelection?: SelectionData,
        executionPlanOptions: ExecutionPlanOptions = { includeActualExecutionPlanXml: false, includeEstimatedExecutionPlanXml: false },
    ): Promise<unknown> {
        const executionId = this.compactQueryEvents ? String(++this.executionSequence) : undefined;
        this.initialRows.delete(ownerUri);
        if (this.initialRowCount > 0) this.initialRows.set(ownerUri, new Map());
        if (executionId) this.executions.set(ownerUri, { id: executionId, batches: new Map(), results: new Map() });
        return this.request("query/executeDocumentSelection", {
            ownerUri,
            ...(executionId ? { executionId } : {}),
            ...(querySelection === undefined ? {} : { querySelection }),
            executionPlanOptions,
        });
    }

    cancelQuery(ownerUri: string): Promise<{ messages?: string }> {
        return this.request("query/cancel", { ownerUri });
    }

    disposeQuery(ownerUri: string): Promise<unknown> {
        this.executions.delete(ownerUri);
        this.initialRows.delete(ownerUri);
        return this.request("query/dispose", { ownerUri });
    }

    async subset(ownerUri: string, batchIndex: number, resultSetIndex: number, rowsStartIndex: number, rowsCount: number): Promise<SubsetResult> {
        if (this.failure) throw new Error(this.failure);
        const cached = this.initialRows.get(ownerUri)?.get(`${batchIndex}:${resultSetIndex}`);
        if (cached && Number.isInteger(rowsStartIndex) && Number.isInteger(rowsCount) && rowsStartIndex >= 0 && rowsCount > 0 &&
            rowsStartIndex < cached.rows.length) {
            const rows = cached.rows.slice(rowsStartIndex, rowsStartIndex + rowsCount);
            if (rows.length === rowsCount || rowsStartIndex + rows.length >= cached.rowCount)
                return { resultSubset: { rows, rowCount: rows.length, totalRowCount: cached.rowCount } };
            const remaining = await this.request<SubsetResult>("query/subset", {
                ownerUri, batchIndex, resultSetIndex, rowsStartIndex: rowsStartIndex + rows.length, rowsCount: rowsCount - rows.length,
            });
            return { resultSubset: { ...remaining.resultSubset, rows: [...rows, ...remaining.resultSubset.rows], rowCount: rows.length + remaining.resultSubset.rowCount } };
        }
        return this.request<SubsetResult>("query/subset", { ownerUri, batchIndex, resultSetIndex, rowsStartIndex, rowsCount });
    }

    async saveResults(ownerUri: string, batchIndex: number, resultSetIndex: number, filePath: string, format: ExportFormat): Promise<void> {
        const result = await this.request<{ messages?: string }>(`query/save${format}`, {
            ownerUri, batchIndex, resultSetIndex, filePath,
            includeHeaders: true,
            ...(format === "Csv" ? { delimiter: ",", lineSeperator: "\r\n", textIdentifier: "\"", encoding: "utf-8" } : {}),
            ...(format === "Excel" ? { freezeHeaderRow: true, boldHeaderRow: true, autoFilterHeaderRow: true, autoSizeColumns: true } : {}),
            ...(format === "Markdown" ? { encoding: "utf-8", lineSeparator: "\r\n" } : {}),
            ...(format === "Xml" ? { formatted: true, encoding: "utf-8" } : {}),
        });
        if (result.messages) throw new Error(result.messages);
    }

    confirm(ownerUri: string, result: ConfirmationResult): Promise<void> {
        return this.notify("sql4cds/confirm", { ownerUri, result });
    }
}

/**
 * Executes the Web API requests generated by the worker's IOrganizationService proxy. The worker has already
 * translated the SDK requests into Web API requests, so they are passed straight through to PPTB.
 */
async function executeBatch(params: ExecuteBatchParams): Promise<WebApiResponse[]> {
    const requests: DataverseAPI.BatchRequest[] = params.requests.map((r: WebApiRequest) => ({
        method: r.method,
        url: r.url,
        headers: r.headers ?? undefined,
        body: r.body,
        contentId: r.contentId,
        ...(r.changesetId ? { changesetId: r.changesetId } : {}),
    }));

    const headers = { ...params.headers };
    if (params.continueOnError && requests.length > 1) {
        const preferKey = Object.keys(headers).find((key) => key.toLowerCase() === "prefer") ?? "Prefer";
        headers[preferKey] = headers[preferKey] ? `${headers[preferKey]},odata.continue-on-error` : "odata.continue-on-error";
    }

    try {
        const results = await executeBatchWithRetry(requests, params.target, headers);

        return results.map((r: DataverseAPI.BatchResult & { batchHeaders?: Record<string, string> }) => ({
            contentId: r.contentId,
            status: r.status,
            statusText: r.statusText,
            headers: r.headers ?? {},
            batchHeaders: r.batchHeaders ?? {},
            body: r.body === undefined ? undefined : JSON.stringify(r.body),
        }));
    } catch (err) {
        // Report failures of the batch as a whole in the same shape as an individual Web API error so the
        // worker can convert it to the appropriate OrganizationServiceFault.
        const message = err instanceof Error ? err.message : String(err);
        const error = { error: { code: "", message } };
        return params.requests.map((r) => ({
            contentId: r.contentId,
            status: 500,
            statusText: "Internal Server Error",
            headers: {},
            batchHeaders: {},
            body: JSON.stringify(error),
        }));
    }
}

const MAX_BATCH_RETRY_COUNT = 10;

async function executeBatchWithRetry(
    requests: DataverseAPI.BatchRequest[],
    target: DataverseAPI.ConnectionTarget,
    headers: Record<string, string>,
): Promise<(DataverseAPI.BatchResult & { batchHeaders?: Record<string, string> })[]> {
    let pendingRequests = requests;
    let completedResults: (DataverseAPI.BatchResult & { batchHeaders?: Record<string, string> })[] = [];
    let retryCount = 0;

    while (true) {
        const results = await window.dataverseAPI.executeBatch(pendingRequests, target, headers);

        if (results.length !== pendingRequests.length || retryCount >= MAX_BATCH_RETRY_COUNT) {
            return completedResults.concat(results);
        }

        let retryStart = results.length;
        while (retryStart > 0 && results[retryStart - 1].status === 429)
            retryStart--;

        if (retryStart === results.length)
            return completedResults.concat(results);

        // Never replay only part of an atomic changeset.
        const changesetId = pendingRequests[retryStart].changesetId;
        if (changesetId && pendingRequests.slice(0, retryStart).some((request) => request.changesetId === changesetId))
            return completedResults.concat(results);

        const retryAfter = results
            .map((result) => getHeaderValue(result.batchHeaders, "Retry-After"))
            .find((value) => value !== undefined);
        const retryDelay = retryAfter?.trim();
        if (!retryDelay || !/^\d+$/.test(retryDelay))
            return completedResults.concat(results);

        const retryAfterSeconds = Number(retryDelay);
        if (!Number.isSafeInteger(retryAfterSeconds))
            return completedResults.concat(results);

        if (retryStart > 0) {
            completedResults = completedResults.concat(results.slice(0, retryStart));
            pendingRequests = pendingRequests.slice(retryStart);
        }

        await delay(retryAfterSeconds * 1000);
        retryCount++;
    }
}

function getHeaderValue(headers: Record<string, string> | undefined, name: string): string | undefined {
    if (!headers)
        return undefined;

    const key = Object.keys(headers).find((header) => header.toLowerCase() === name.toLowerCase());
    return key === undefined ? undefined : headers[key];
}

function delay(milliseconds: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
