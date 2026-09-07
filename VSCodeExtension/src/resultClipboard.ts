export type ClipboardFormat = "tsv" | "csv" | "json" | "xml" | "markdown" | "sqlIn";

export type ClipboardValue = string | number | boolean | null | undefined | Date | bigint | object;

export interface ClipboardTable {
  headers: readonly string[];
  rows: readonly (readonly ClipboardValue[])[];
}

export interface ClipboardOptions {
  /** Applies to TSV, CSV, and Markdown. Object formats always use field names. */
  includeHeaders?: boolean;
}

export interface SelectionRange {
  rowStart: number;
  rowEnd: number;
  columnStart: number;
  columnEnd: number;
}

/** Keep only selected rows; holes within a selected row remain empty cells. */
export function projectClipboardRows(
  rows: readonly (readonly ClipboardValue[])[], start: number,
  ranges: readonly SelectionRange[], columnOrder: readonly number[]
): ClipboardValue[][] {
  const columns = columnOrder.map((original, visual) => ({ original, visual }))
    .filter(({ visual }) => ranges.some(range => visual >= range.columnStart && visual <= range.columnEnd));
  return rows.flatMap((row, offset) => {
    const selected = ranges.filter(range => start + offset >= range.rowStart && start + offset <= range.rowEnd);
    if (!selected.length) { return []; }
    return [columns.map(({ original, visual }) => selected.some(range => visual >= range.columnStart && visual <= range.columnEnd)
      ? row[original] ?? null : "")];
  });
}

/**
 * Serializes a logical selection. Callers can obtain the rows in server-side chunks;
 * this function deliberately has no dependency on rendered/page DOM state.
 */
export function serializeClipboard(
  format: ClipboardFormat,
  table: ClipboardTable,
  options: ClipboardOptions = {}
): string {
  validateTable(table);
  switch (format) {
    case "tsv": return serializeDelimited(table, "\t", options.includeHeaders ?? false);
    case "csv": return serializeDelimited(table, ",", options.includeHeaders ?? false);
    case "json": return serializeJson(table);
    case "xml": return serializeXml(table);
    case "markdown": return serializeMarkdown(table, options.includeHeaders ?? true);
    case "sqlIn": return serializeSqlIn(table);
  }
}

/** Collects asynchronously retrieved row chunks before serializing them. */
export async function serializeClipboardChunks(
  format: ClipboardFormat,
  headers: readonly string[],
  chunks: AsyncIterable<readonly (readonly ClipboardValue[])[]>,
  options: ClipboardOptions = {}
): Promise<string> {
  const rows: (readonly ClipboardValue[])[] = [];
  for await (const chunk of chunks) { rows.push(...chunk); }
  return serializeClipboard(format, { headers, rows }, options);
}

export function makeUniqueHeaders(headers: readonly string[]): string[] {
  const used = new Set<string>();
  const nextSuffix = new Map<string, number>();
  return headers.map((header, index) => {
    const base = header.trim() || `Column${index + 1}`;
    let candidate = base;
    let suffix = nextSuffix.get(base) ?? 2;
    while (used.has(candidate)) { candidate = `${base}_${suffix++}`; }
    nextSuffix.set(base, suffix);
    used.add(candidate);
    return candidate;
  });
}

export function estimateClipboardCharacters(table: ClipboardTable): number {
  let size = table.headers.reduce((total, header) => total + header.length + 1, 0);
  for (const row of table.rows) {
    for (const value of row) { size += displayValue(value).length + 1; }
  }
  return size;
}

function validateTable(table: ClipboardTable): void {
  if (table.rows.some(row => row.length > table.headers.length)) {
    throw new Error("A clipboard row contains more values than there are selected columns.");
  }
}

function serializeDelimited(table: ClipboardTable, delimiter: string, includeHeaders: boolean): string {
  const rows: readonly (readonly ClipboardValue[])[] = includeHeaders ? [table.headers, ...table.rows] : table.rows;
  return rows.map(row => table.headers.map((_, index) => quoteDelimited(displayValue(row[index]), delimiter)).join(delimiter)).join("\r\n");
}

function quoteDelimited(value: string, delimiter: string): string {
  return value.includes(delimiter) || /[\r\n"]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;
}

function serializeJson(table: ClipboardTable): string {
  const headers = makeUniqueHeaders(table.headers);
  const objects = table.rows.map(row => Object.fromEntries(headers.map((header, index) => [header, jsonValue(row[index])] as const)));
  return JSON.stringify(objects, undefined, 2);
}

function jsonValue(value: ClipboardValue): unknown {
  if (value === undefined) { return null; }
  if (value instanceof Date) { return value.toISOString(); }
  if (typeof value === "bigint") { return value.toString(); }
  return value;
}

function serializeXml(table: ClipboardTable): string {
  const headers = makeUniqueHeaders(table.headers.map((header, index) => xmlName(header.trim() || `Column${index + 1}`)));
  const rows = table.rows.map(row => {
    const fields = headers.map((header, index) => {
      const value = row[index];
      return value === null || value === undefined
        ? `    <${header} xsi:nil="true" />`
        : `    <${header}>${escapeXml(displayValue(value))}</${header}>`;
    });
    return ["  <row>", ...fields, "  </row>"].join("\n");
  });
  return [`<?xml version="1.0" encoding="utf-8"?>`, `<results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">`, ...rows, "</results>"].join("\n");
}

function xmlName(value: string): string {
  let name = value.replace(/[^A-Za-z0-9_.-]/g, "_");
  if (!/^[A-Za-z_]/.test(name)) { name = `_${name}`; }
  return name || "Column";
}

function escapeXml(value: string): string {
  return value.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, "\uFFFD")
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&apos;");
}

function serializeMarkdown(table: ClipboardTable, includeHeaders: boolean): string {
  const lines: string[] = [];
  if (includeHeaders) {
    lines.push(markdownRow(table.headers), markdownRow(table.headers.map(() => "---")));
  }
  lines.push(...table.rows.map(row => markdownRow(table.headers.map((_, index) => displayValue(row[index])))));
  return lines.join("\n");
}

function markdownRow(row: readonly ClipboardValue[]): string {
  return `| ${row.map(value => displayValue(value).replace(/\\/g, "\\\\").replace(/\|/g, "\\|").replace(/\r?\n/g, "<br>")).join(" | ")} |`;
}

function serializeSqlIn(table: ClipboardTable): string {
  if (table.headers.length !== 1) { throw new Error("SQL IN format requires a single selected column."); }
  return `IN (${table.rows.map(row => sqlLiteral(row[0])).join(", ")})`;
}

function sqlLiteral(value: ClipboardValue): string {
  if (value === null || value === undefined) { return "NULL"; }
  if (typeof value === "number") { return Number.isFinite(value) ? String(value) : `'${String(value)}'`; }
  if (typeof value === "bigint") { return value.toString(); }
  if (typeof value === "boolean") { return value ? "1" : "0"; }
  const text = value instanceof Date ? value.toISOString() : displayValue(value);
  return `N'${text.replace(/'/g, "''")}'`;
}

function displayValue(value: ClipboardValue): string {
  if (value === null || value === undefined) { return "NULL"; }
  if (value instanceof Date) { return value.toISOString(); }
  if (typeof value === "object") {
    try { return JSON.stringify(value); } catch { return String(value); }
  }
  return String(value);
}
