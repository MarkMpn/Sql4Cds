import type { Sql4CdsClient } from "./lsp/client";
import type { ResultMessage, ResultSetSummary } from "./lsp/protocol";

export const defaultResponseLimitBytes = 64 * 1024;

export interface InvocationInput {
    query: string;
    fetchXml: boolean;
    execute: boolean;
    expectsResponse: boolean;
    responseLimitBytes: number;
}

export function parseInvocation(context: Record<string, unknown> | null): InvocationInput | null {
    if (!context || (!("sql" in context) && !("fetchXml" in context))) return null;
    if ("sql" in context && "fetchXml" in context) throw new Error("Provide either sql or fetchXml, not both.");
    const fetchXml = "fetchXml" in context;
    const query = fetchXml ? context.fetchXml : context.sql;
    if (typeof query !== "string" || !query.trim()) throw new Error(`The calling tool must provide a non-empty ${fetchXml ? "fetchXml" : "sql"} string.`);
    if (context.execute !== undefined && typeof context.execute !== "boolean") throw new Error("execute must be a boolean.");
    const metadata = context.__pptb;
    const expectsResponse = typeof metadata === "object" && metadata !== null &&
        "expectsResponse" in metadata && metadata.expectsResponse === true;
    const responseLimitBytes = context.maxResponseBytes ?? defaultResponseLimitBytes;
    if (typeof responseLimitBytes !== "number" || !Number.isSafeInteger(responseLimitBytes) ||
        responseLimitBytes < 1024 || responseLimitBytes > 1024 * 1024)
        throw new Error("maxResponseBytes must be an integer between 1024 and 1048576.");
    return { query, fetchXml, execute: context.execute === true, expectsResponse, responseLimitBytes };
}

export function responseSize(value: unknown): number {
    return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

export function invocationError(error: string, responseLimitBytes: number) {
    const result = { status: "error", error, truncated: false, responseLimitBytes };
    if (responseSize(result) <= responseLimitBytes) return result;
    result.truncated = true;
    let low = 0;
    let high = error.length;
    while (low < high) {
        const middle = Math.ceil((low + high) / 2);
        result.error = error.slice(0, middle);
        if (responseSize(result) <= responseLimitBytes) low = middle;
        else high = middle - 1;
    }
    result.error = error.slice(0, low);
    return result;
}

export interface InvocationResultSet {
    batchId: number;
    id: number;
    columns: ResultSetSummary["columnInfo"];
    rowCount: number;
    returnedRowCount: number;
    rows: (string | null)[][];
    truncated: boolean;
}

export interface InvocationResult {
    status: "success" | "error" | "cancelled";
    resultSets: InvocationResultSet[];
    messages: { message: string; isError: boolean }[];
    truncated: boolean;
    responseLimitBytes: number;
}

export async function collectInvocationResults(
    client: Sql4CdsClient, ownerUri: string, summaries: ResultSetSummary[], messages: ResultMessage[],
    status: InvocationResult["status"], responseLimitBytes: number,
): Promise<InvocationResult> {
    const result: InvocationResult = {
        status,
        resultSets: summaries.filter(summary => !summary.specialAction?.expectYukonXMLShowPlan).map(summary => ({
            batchId: summary.batchId, id: summary.id, columns: summary.columnInfo,
            rowCount: summary.rowCount, returnedRowCount: 0, rows: [], truncated: false,
        })),
        messages: [], truncated: false, responseLimitBytes,
    };
    let size = responseSize(result);
    if (size > responseLimitBytes) throw new Error("Result set metadata exceeds maxResponseBytes. Increase the response budget or select fewer columns.");
    let messagesTruncated = false;
    // Preserve errors ahead of informational messages when the response budget is tight.
    for (const message of [...messages.filter(message => message.isError), ...messages.filter(message => !message.isError)]) {
        const value = { message: message.message, isError: message.isError };
        const bytes = responseSize(value) + (result.messages.length ? 1 : 0);
        if (size + bytes > responseLimitBytes) messagesTruncated = true;
        else {
            result.messages.push(value);
            size += bytes;
        }
    }
    for (const summary of result.resultSets) {
        let full = false;
        for (let start = 0; start < summary.rowCount && !full;) {
            const subset = await client.subset(ownerUri, summary.batchId, summary.id, start, Math.min(100, summary.rowCount - start));
            const rows = subset.resultSubset.rows;
            if (!rows.length) throw new Error("The language server returned no rows before the result set was fully read.");
            for (const row of rows) {
                const values = row.map(cell => cell.isNull ? null : cell.displayValue);
                const bytes = responseSize(values) + (summary.rows.length ? 1 : 0) +
                    String(summary.returnedRowCount + 1).length - String(summary.returnedRowCount).length;
                if (size + bytes > responseLimitBytes) {
                    full = true;
                    break;
                }
                summary.rows.push(values);
                summary.returnedRowCount++;
                size += bytes;
            }
            start += rows.length;
        }
        summary.truncated = summary.returnedRowCount < summary.rowCount;
        if (summary.truncated) size--;
    }
    result.truncated = messagesTruncated || result.resultSets.some(summary => summary.truncated);
    return result;
}
