import type { ExecutionPlanGraph } from "../../../MarkMpn.Sql4Cds.PPTB/src/resultsProtocol";

export interface ResultsState {
  type: "state";
  ownerUri: string;
  runId: number;
  status: "running" | "cancelling" | "completed" | "cancelled" | "failed";
  estimatedPlan: boolean;
  planVersion: number;
  autoSizeColumns: boolean;
  returnedRows: number;
  batchElapsed?: string;
  results: {
    key: string;
    rowCount: number;
    displayRowCount: number;
    complete: boolean;
    columns: string[];
    truncated: boolean;
  }[];
  plans: { key: string; graphs: ExecutionPlanGraph[] }[];
  messages: { message: string; isError: boolean }[];
}

export interface ResultResponse {
  type: "response";
  requestId: number;
  value?: unknown;
  error?: string;
}
