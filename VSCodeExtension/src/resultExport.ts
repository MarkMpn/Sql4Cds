import {
  Methods,
  SaveResultsAsCsvParams,
  SaveResultsAsExcelParams,
  SaveResultsAsMarkdownParams,
  SaveResultsAsXmlParams,
  SaveResultsParams
} from "./protocol";

export type ResultExportFormat = "csv" | "xlsx" | "json" | "md" | "xml";
export type ResultExportParams = SaveResultsParams | SaveResultsAsCsvParams | SaveResultsAsExcelParams | SaveResultsAsMarkdownParams | SaveResultsAsXmlParams;

export interface ResultExportChoice {
  format: ResultExportFormat;
  label: string;
  description: string;
  extension: string;
  filterName: string;
}

export const resultExportChoices: readonly ResultExportChoice[] = [
  { format: "csv", label: "CSV", description: "Comma-separated values", extension: "csv", filterName: "CSV files" },
  { format: "xlsx", label: "Excel", description: "Formatted Excel workbook", extension: "xlsx", filterName: "Excel workbooks" },
  { format: "json", label: "JSON", description: "JSON array of row objects", extension: "json", filterName: "JSON files" },
  { format: "md", label: "Markdown", description: "Markdown table", extension: "md", filterName: "Markdown files" },
  { format: "xml", label: "XML", description: "Formatted XML document", extension: "xml", filterName: "XML files" }
];

export function exportMethod(format: ResultExportFormat): string {
  switch (format) {
    case "csv": return Methods.saveCsv;
    case "xlsx": return Methods.saveExcel;
    case "json": return Methods.saveJson;
    case "md": return Methods.saveMarkdown;
    case "xml": return Methods.saveXml;
  }
}

export function opensInTextEditor(format: ResultExportFormat): boolean {
  return format !== "xlsx";
}

export function createExportParams(format: ResultExportFormat, base: SaveResultsParams): ResultExportParams {
  switch (format) {
    case "csv": return {
      ...base,
      includeHeaders: true,
      delimiter: ",",
      lineSeperator: "\r\n",
      textIdentifier: "\"",
      encoding: "utf-8",
      maxCharsToStore: 0
    } satisfies SaveResultsAsCsvParams;
    case "xlsx": return {
      ...base,
      includeHeaders: true,
      freezeHeaderRow: true,
      boldHeaderRow: true,
      autoFilterHeaderRow: true,
      autoSizeColumns: true
    } satisfies SaveResultsAsExcelParams;
    case "json": return base;
    case "md": return {
      ...base,
      encoding: "utf-8",
      includeHeaders: true,
      lineSeparator: "\r\n"
    } satisfies SaveResultsAsMarkdownParams;
    case "xml": return {
      ...base,
      formatted: true,
      encoding: "utf-8"
    } satisfies SaveResultsAsXmlParams;
  }
}
