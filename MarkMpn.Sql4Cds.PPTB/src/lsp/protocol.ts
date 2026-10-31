/**
 * Type definitions for the subset of the SQL 4 CDS language server protocol used by this tool.
 *
 * The server uses the standard LSP messages for editor features, plus the SQL Tools Service style
 * `connection/*` and `query/*` messages for connection management and query execution.
 */
import type { DbColumn, DbCellValue, ExecutionPlanGraph } from "../resultsProtocol";
export * from "../resultsProtocol";

export interface Position {
    line: number;
    character: number;
}

export interface Range {
    start: Position;
    end: Position;
}

export interface TextEdit {
    range: Range;
    newText: string;
}

export interface FormattingOptions {
    tabSize: number;
    insertSpaces: boolean;
    addDisplayNames?: boolean;
}

export interface MarkupContent {
    kind: "plaintext" | "markdown";
    value: string;
}

export interface CompletionItem {
    label: string;
    kind?: number;
    detail?: string;
    documentation?: string | MarkupContent;
    insertText?: string;
    sortText?: string;
    filterText?: string;
}

export interface CompletionList {
    isIncomplete: boolean;
    items: CompletionItem[];
}

export interface Hover {
    contents: MarkupContent | string | Array<string | { language: string; value: string }>;
    range?: Range;
}

export interface ParameterInformation {
    label: string | [number, number];
    documentation?: string | MarkupContent;
}

export interface SignatureInformation {
    label: string;
    documentation?: string | MarkupContent;
    parameters?: ParameterInformation[];
}

export interface SignatureHelp {
    signatures: SignatureInformation[];
    activeSignature?: number;
    activeParameter?: number;
}

export interface Diagnostic {
    range: Range;
    severity?: 1 | 2 | 3 | 4;
    message: string;
    source?: string;
}

export interface PublishDiagnosticsParams {
    uri: string;
    diagnostics: Diagnostic[];
}

export interface LogMessageParams {
    type: 1 | 2 | 3 | 4;
    message: string;
}

/** Options passed in `connection/connect` for a PPTB-proxied connection */
export interface PptbConnectionOptions {
    authenticationType: "PPTB";
    url: string;
    connectionName: string;
    connectionId: string;
    connectionTarget: number;
}

export interface ConnectionCompleteParams {
    ownerUri: string;
    connectionId?: string;
    errorMessage?: string;
    messages?: string;
    errorNumber?: number;
    connectionSummary?: {
        serverName: string;
        databaseName: string;
        userName: string;
    };
}

export interface ObjectExplorerNode {
    nodePath: string;
    nodeType: string;
    label: string;
    isLeaf: boolean;
    nodeSubType?: string;
    errorMessage?: string;
    metadata?: {
        schema?: string;
        name?: string;
        metadataTypeName?: string;
    };
}

export interface ObjectExplorerSessionCreated {
    sessionId: string;
    success: boolean;
    rootNode?: ObjectExplorerNode;
    errorMessage?: string;
}

export interface ObjectExplorerExpandComplete {
    sessionId: string;
    nodePath: string;
    nodes?: ObjectExplorerNode[];
    errorMessage?: string;
}

export interface ObjectExplorerSessionDisconnected {
    sessionId: string;
    success: boolean;
    errorMessage?: string;
}

export interface SelectionData {
    startLine: number;
    startColumn: number;
    endLine: number;
    endColumn: number;
}

export interface ResultMessage {
    batchId?: number;
    isError: boolean;
    time?: string;
    message: string;
    range?: Range;
}

export interface MessageParams {
    ownerUri: string;
    message: ResultMessage;
}

export interface ResultSetSummary {
    id: number;
    batchId: number;
    rowCount: number;
    complete: boolean;
    columnInfo: DbColumn[];
    specialAction?: {
        /** Set when the result set holds execution plans rather than data */
        expectYukonXMLShowPlan?: boolean;
    };
}

export interface ResultSetEventParams {
    ownerUri: string;
    resultSetSummary: ResultSetSummary;
    /** Supplied with `query/resultSetUpdated` for execution plan result sets */
    executionPlans?: ExecutionPlanGraph[];
}

export interface ExecutionPlanOptions {
    includeActualExecutionPlanXml: boolean;
    includeEstimatedExecutionPlanXml: boolean;
}

export interface BatchSummary {
    id: number;
    hasError: boolean;
    executionElapsed?: string;
    selection?: SelectionData;
    resultSetSummaries?: ResultSetSummary[];
}

export interface BatchEventParams {
    ownerUri: string;
    batchSummary: BatchSummary;
}

export interface QueryCompleteParams {
    ownerUri: string;
    batchSummaries: BatchSummary[];
}

export interface QueryEventsParams {
    ownerUri: string;
    executionId: string;
    events: QueryEvent[];
}

export type QueryEvent =
    | { type: "resultSet"; data: Pick<ResultSetSummary, "id" | "batchId" | "rowCount" | "complete"> & Partial<ResultSetSummary> & Pick<ResultSetEventParams, "executionPlans"> & { rowsStartIndex?: number; rows?: DbCellValue[][] } }
    | { type: "query/batchStart" | "query/batchComplete"; data: BatchSummary }
    | { type: "query/message"; data: MessageParams["message"] }
    | { type: "sql4cds/progress"; data: string }
    | { type: "query/complete" };

export interface SubsetResult {
    resultSubset: {
        rowCount: number;
        rows: DbCellValue[][];
    };
}

export interface ConfirmationParams {
    ownerUri: string;
    msg: string;
}

export const ConfirmationResult = {
    No: 0,
    Yes: 1,
    All: 2,
} as const;

export type ConfirmationResult = (typeof ConfirmationResult)[keyof typeof ConfirmationResult];

/** A single Web API request generated by the worker's IOrganizationService proxy */
export interface WebApiRequest {
    method: DataverseAPI.BatchMethod;
    url: string;
    /** The worker serializes an unset header record as null. */
    headers?: Record<string, string> | null;
    /** Pre-serialized JSON body */
    body?: string;
    contentId: string;
    changesetId?: string;
}

export interface ExecuteBatchParams {
    target: DataverseAPI.ConnectionTarget;
    requests: WebApiRequest[];
    continueOnError: boolean;
    headers: Record<string, string>;
}

export interface DataverseChunkParams {
    id: string;
    sequence: number;
    complete: boolean;
    data: string;
}

export interface DataverseMessageParams {
    id: string;
    data: ExecuteBatchParams | DataverseResponse;
}

export type DataverseResponse = { result: WebApiResponse[] } | { error: { message: string } };

export interface WebApiResponse {
    contentId?: string;
    status: number;
    statusText?: string;
    headers: Record<string, string>;
    batchHeaders: Record<string, string>;
    /** Response body serialized as JSON text */
    body?: string;
}

/** Notifications from the server that are addressed to a specific query editor */
export interface DocumentListener {
    connectionComplete?(params: ConnectionCompleteParams): void;
    batchStart?(params: BatchEventParams): void;
    batchComplete?(params: BatchEventParams): void;
    message?(params: MessageParams): void;
    resultSetAvailable?(params: ResultSetEventParams): void;
    resultSetUpdated?(params: ResultSetEventParams): void;
    resultSetComplete?(params: ResultSetEventParams): void;
    queryComplete?(params: QueryCompleteParams): void;
    confirmation?(params: ConfirmationParams): void;
    diagnostics?(params: PublishDiagnosticsParams): void;
}
