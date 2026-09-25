export type AuthenticationType = "AzureMFA" | "SqlLogin" | "Integrated" | "None" | "ConnectionString";

export interface ConnectionProfile {
  id: string;
  name: string;
  url?: string;
  authenticationType: AuthenticationType;
  user?: string;
  clientId?: string;
  redirectUrl?: string;
}

export interface ConnectionDetails { options: Record<string, unknown>; }

export interface ConnectionCompleteParams {
  ownerUri: string;
  requestId?: string;
  connectionId?: string;
  messages?: string;
  errorMessage?: string;
  connectionSummary?: { serverName?: string; databaseName?: string; userName?: string };
  serverInfo?: { machineName?: string; options?: Record<string, unknown> };
  type?: string;
}

export interface ObjectMetadata {
  metadataType?: number;
  metadataTypeName?: string;
  schema?: string;
  name?: string;
  parentName?: string;
  parentTypeName?: string;
  urn?: string;
}

export interface NodeInfo {
  nodePath: string;
  nodeType: string;
  label: string;
  nodeSubType?: string;
  nodeStatus?: string;
  isLeaf: boolean;
  metadata?: ObjectMetadata;
  errorMessage?: string;
}

export interface SessionCreatedParams {
  success: boolean;
  sessionId: string;
  rootNode?: NodeInfo;
  errorMessage?: string;
}

export interface ExpandResponse {
  sessionId: string;
  nodePath: string;
  nodes?: NodeInfo[];
  errorMessage?: string;
}

export interface SelectionData {
  startLine: number;
  startColumn: number;
  endLine: number;
  endColumn: number;
}

export interface ColumnInfo {
  columnName?: string;
  name?: string;
  dataTypeName?: string;
  numericScale?: number;
}

export interface ResultSetSummary {
  id: number;
  batchId: number;
  rowCount: number;
  complete: boolean;
  columnInfo: ColumnInfo[];
}

export interface BatchSummary {
  executionElapsed?: string;
  executionEnd?: string;
  executionStart?: string;
  hasError?: boolean;
  id: number;
  resultSetSummaries?: ResultSetSummary[];
}

export interface ResultSetEventParams {
  ownerUri: string;
  resultSetSummary: ResultSetSummary;
}

export interface ResultMessage {
  batchId?: number;
  isError: boolean;
  time?: string;
  message: string;
}

export interface MessageParams { ownerUri: string; message: ResultMessage; }
export interface QueryCompleteParams { ownerUri: string; batchSummaries?: BatchSummary[]; }

export interface QueryCancelResult { messages?: string; }
export interface QueryDisposeResult { messages?: string; }

export interface SaveResultsParams {
  ownerUri: string;
  filePath: string;
  batchIndex: number;
  resultSetIndex: number;
}

export interface SaveResultsAsCsvParams extends SaveResultsParams {
  includeHeaders: boolean;
  delimiter: string;
  lineSeperator: string;
  textIdentifier: string;
  encoding: string;
  maxCharsToStore: number;
}

export interface SaveResultsAsExcelParams extends SaveResultsParams {
  includeHeaders: boolean;
  freezeHeaderRow: boolean;
  boldHeaderRow: boolean;
  autoFilterHeaderRow: boolean;
  autoSizeColumns: boolean;
}

export interface SaveResultsAsMarkdownParams extends SaveResultsParams {
  encoding: string;
  includeHeaders: boolean;
  lineSeparator: string;
}

export interface SaveResultsAsXmlParams extends SaveResultsParams {
  formatted: boolean;
  encoding: string;
}

export interface SaveResultRequestResult { messages?: string; }

export interface CellValue {
  displayValue?: string;
  invariantCultureDisplayValue?: string;
  isNull: boolean;
  rawObject?: unknown;
}

export type ResultFilterOperator =
  | "contains"
  | "notContains"
  | "equals"
  | "notEquals"
  | "startsWith"
  | "endsWith"
  | "isEmpty"
  | "isNotEmpty"
  | "greaterThan"
  | "greaterThanOrEqual"
  | "lessThan"
  | "lessThanOrEqual";

export interface ResultSetFilter {
  columnIndex: number;
  operator: ResultFilterOperator;
  value?: string;
}

export interface ResultSetSort {
  columnIndex: number;
  direction: "asc" | "desc";
}

export interface SubsetParams {
  ownerUri: string;
  batchIndex: number;
  resultSetIndex: number;
  rowsStartIndex: number;
  rowsCount: number;
  searchText?: string;
  filters?: ResultSetFilter[];
  sort?: ResultSetSort;
  viewVersion?: number;
}

export interface SubsetResult {
  resultSubset?: {
    rowCount: number;
    totalRowCount?: number;
    viewVersion?: number;
    rows: CellValue[][];
  };
}

export const Methods = {
  connect: "connection/connect",
  cancelConnect: "connection/cancelconnect",
  disconnect: "connection/disconnect",
  connectionComplete: "connection/complete",
  createObjectExplorerSession: "objectexplorer/createsession",
  objectExplorerSessionCreated: "objectexplorer/sessioncreated",
  expandObjectExplorer: "objectexplorer/expand",
  objectExplorerExpanded: "objectexplorer/expandCompleted",
  closeObjectExplorerSession: "objectexplorer/closesession",
  executeString: "query/executeString",
  cancelQuery: "query/cancel",
  disposeQuery: "query/dispose",
  queryMessage: "query/message",
  resultSetAvailable: "query/resultSetAvailable",
  resultSetUpdated: "query/resultSetUpdated",
  resultSetComplete: "query/resultSetComplete",
  queryComplete: "query/complete",
  subset: "query/subset",
  saveCsv: "query/saveCsv",
  saveExcel: "query/saveExcel",
  saveJson: "query/saveJson",
  saveMarkdown: "query/saveMarkdown",
  saveXml: "query/saveXml",
  progress: "sql4cds/progress",
  confirmation: "sql4cds/confirmation",
  confirm: "sql4cds/confirm"
} as const;
