import assert from "node:assert/strict";
import test from "node:test";
import { createExportParams, exportMethod, opensInTextEditor, resultExportChoices } from "../src/resultExport";

const base = {
  ownerUri: "untitled:query.sql4cds",
  filePath: "/tmp/result",
  batchIndex: 2,
  resultSetIndex: 3
};

test("all advertised result formats have distinct extensions and service methods", () => {
  assert.deepEqual(resultExportChoices.map(choice => choice.extension), ["csv", "xlsx", "json", "md", "xml"]);
  assert.deepEqual(resultExportChoices.map(choice => exportMethod(choice.format)), [
    "query/saveCsv",
    "query/saveExcel",
    "query/saveJson",
    "query/saveMarkdown",
    "query/saveXml"
  ]);
});

test("text formats open in VS Code while Excel uses the system application", () => {
  assert.equal(opensInTextEditor("csv"), true);
  assert.equal(opensInTextEditor("json"), true);
  assert.equal(opensInTextEditor("md"), true);
  assert.equal(opensInTextEditor("xml"), true);
  assert.equal(opensInTextEditor("xlsx"), false);
});

test("CSV export uses interoperable UTF-8 defaults", () => {
  assert.deepEqual(createExportParams("csv", base), {
    ...base,
    includeHeaders: true,
    delimiter: ",",
    lineSeperator: "\r\n",
    textIdentifier: "\"",
    encoding: "utf-8",
    maxCharsToStore: 0
  });
});

test("Excel export creates a readable, filterable workbook", () => {
  assert.deepEqual(createExportParams("xlsx", base), {
    ...base,
    includeHeaders: true,
    freezeHeaderRow: true,
    boldHeaderRow: true,
    autoFilterHeaderRow: true,
    autoSizeColumns: true
  });
});

test("text exports use formatted defaults and JSON requires only common fields", () => {
  assert.deepEqual(createExportParams("json", base), base);
  assert.deepEqual(createExportParams("md", base), { ...base, encoding: "utf-8", includeHeaders: true, lineSeparator: "\r\n" });
  assert.deepEqual(createExportParams("xml", base), { ...base, formatted: true, encoding: "utf-8" });
});
