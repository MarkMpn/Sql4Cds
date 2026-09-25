import assert from "node:assert/strict";
import test from "node:test";
import { makeUniqueHeaders, projectClipboardRows, serializeClipboard, serializeClipboardChunks } from "../src/resultClipboard";

const table = {
  headers: ["name", "name", "", "active"],
  rows: [["O'Brien", "line 1\nline 2", null, true], ["a,b", "<tag>", 42, false]]
};

test("sparse selections never introduce unselected rows into SQL IN or JSON", () => {
  const rows = projectClipboardRows([["first"], ["unselected"], ["third"]], 0, [
    { rowStart: 0, rowEnd: 0, columnStart: 0, columnEnd: 0 },
    { rowStart: 2, rowEnd: 2, columnStart: 0, columnEnd: 0 }
  ], [0]);
  assert.deepEqual(rows, [["first"], ["third"]]);
  assert.equal(serializeClipboard("sqlIn", { headers: ["name"], rows }), "IN (N'first', N'third')");
  assert.deepEqual(JSON.parse(serializeClipboard("json", { headers: ["name"], rows })), [{ name: "first" }, { name: "third" }]);
});

test("chunked selection respects reordered columns, holes and SQL nulls", () => {
  const rows = projectClipboardRows([["a", null], ["b", "c"]], 200, [
    { rowStart: 200, rowEnd: 200, columnStart: 0, columnEnd: 0 },
    { rowStart: 201, rowEnd: 201, columnStart: 1, columnEnd: 1 }
  ], [1, 0]);
  assert.deepEqual(rows, [[null, ""], ["", "b"]]);
});

test("duplicate and blank headers are made deterministic", () => {
  assert.deepEqual(makeUniqueHeaders(["name", "name", "name_2", "", ""]), ["name", "name_2", "name_2_2", "Column4", "Column5"]);
});

test("TSV and CSV quote delimiters, quotes, and multiline values", () => {
  assert.equal(serializeClipboard("tsv", { headers: ["a", "b"], rows: [["x\ty", 'say "hi"\nnext']] }, { includeHeaders: true }),
    'a\tb\r\n"x\ty"\t"say ""hi""\nnext"');
  assert.equal(serializeClipboard("csv", { headers: ["a", "b"], rows: [["x,y", null]] }), '"x,y",NULL');
});

test("JSON uses typed values, unique object keys, and null for missing cells", () => {
  assert.deepEqual(JSON.parse(serializeClipboard("json", table)), [
    { name: "O'Brien", name_2: "line 1\nline 2", Column3: null, active: true },
    { name: "a,b", name_2: "<tag>", Column3: 42, active: false }
  ]);
});

test("XML is parseable-shaped, escaped, uniquely named, and explicit about null", () => {
  const xml = serializeClipboard("xml", table);
  assert.match(xml, /<name>O&apos;Brien<\/name>/);
  assert.match(xml, /<name_2>line 1\nline 2<\/name_2>/);
  assert.match(xml, /<Column3 xsi:nil="true" \/>/);
  assert.match(xml, /<name_2>&lt;tag&gt;<\/name_2>/);
  const sanitized = serializeClipboard("xml", { headers: ["a b", "a_b"], rows: [["x\u0000y", "z"]] });
  assert.match(sanitized, /<a_b>x�y<\/a_b>/);
  assert.match(sanitized, /<a_b_2>z<\/a_b_2>/);
});

test("Markdown escapes pipes and normalizes multiline cells", () => {
  assert.equal(serializeClipboard("markdown", { headers: ["A"], rows: [["x|y\nz"]] }), "| A |\n| --- |\n| x\\|y<br>z |");
});

test("SQL IN is restricted to one column and escapes SQL string literals", () => {
  assert.equal(serializeClipboard("sqlIn", { headers: ["id"], rows: [["O'Brien"], [42], [true], [null]] }), "IN (N'O''Brien', 42, 1, NULL)");
  assert.throws(() => serializeClipboard("sqlIn", { headers: ["a", "b"], rows: [] }), /single selected column/);
});

test("row chunks can be retrieved asynchronously before serialization", async () => {
  async function* chunks() { yield [[1], [2]]; yield [[3]]; }
  assert.equal(await serializeClipboardChunks("sqlIn", ["id"], chunks()), "IN (1, 2, 3)");
});
