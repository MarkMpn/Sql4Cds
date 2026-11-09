const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");
const ts = require("typescript");

const source = fs.readFileSync(path.join(__dirname, "../src/mcpInvocation.ts"), "utf8");
const script = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS } }).outputText;
const moduleObject = { exports: {} };
vm.runInNewContext(script, { module: moduleObject, exports: moduleObject.exports, TextEncoder });
const { parseInvocation, collectInvocationResults, responseSize, invocationError } = moduleObject.exports;

test("MCP invocation validates SQL/FetchXML, execution intent and byte budgets", () => {
    assert.equal(parseInvocation(null), null);
    assert.equal(parseInvocation({ unrelated: true }), null);
    const handoff = parseInvocation({ fetchXml: "<fetch />" });
    assert.equal(handoff.execute, false);
    assert.equal(handoff.expectsResponse, false);
    assert.equal(handoff.responseLimitBytes, 65536);
    const execution = parseInvocation({ sql: "DELETE FROM account WHERE name = 'test'", execute: true,
        maxResponseBytes: 4096, __pptb: { expectsResponse: true } });
    assert.equal(execution.execute, true);
    assert.equal(execution.expectsResponse, true);
    assert.equal(execution.fetchXml, false);
    for (const input of [
        { sql: "" }, { sql: 123 }, { sql: "SELECT 1", fetchXml: "<fetch />" },
        { sql: "SELECT 1", execute: "true" }, { sql: "SELECT 1", maxResponseBytes: 0 },
        { sql: "SELECT 1", maxResponseBytes: 1048577 }, { sql: "SELECT 1", maxResponseBytes: 1024.5 },
    ]) assert.throws(() => parseInvocation(input));
});

function summary(id, rows, columns = [{ columnName: "name", dataTypeName: "nvarchar" }]) {
    return { batchId: 0, id, rowCount: rows.length, complete: true, columnInfo: columns };
}

function clientFor(resultRows) {
    return { subset: async (_uri, _batch, id, start, count) => ({
        resultSubset: { rows: resultRows[id].slice(start, start + count), rowCount: Math.min(count, resultRows[id].length - start) }
    }) };
}

const cell = value => ({ displayValue: value ?? "NULL", isNull: value === null });

test("MCP row counts adapt to serialized column sizes, escaping and UTF-8", async () => {
    const counts = [];
    for (const value of ["x", "界".repeat(100), '"\\\n'.repeat(100), "x".repeat(20000)]) {
        const rows = Array.from({ length: 1000 }, () => [cell(value)]);
        const result = await collectInvocationResults(clientFor([rows]), "query", [summary(0, rows)], [], "success", 4096);
        assert.ok(responseSize(result) <= 4096);
        assert.equal(result.resultSets[0].rowCount, 1000);
        assert.equal(result.resultSets[0].returnedRowCount, result.resultSets[0].rows.length);
        assert.equal(result.resultSets[0].truncated, true);
        assert.equal(result.truncated, true);
        counts.push(result.resultSets[0].returnedRowCount);
    }
    assert.ok(counts[0] > counts[1]);
    assert.ok(counts[0] > counts[2]);
    assert.equal(counts[3], 0, "An oversized cell is omitted rather than silently shortened");
});

test("MCP budgets include all result sets, metadata, messages and NULLs", async () => {
    const rows = [[cell(null), cell("NULL")], [cell("value"), cell("another")]];
    const columns = [{ columnName: "a" }, { columnName: "b" }];
    const summaries = [summary(0, rows, columns), summary(1, [], columns),
        { ...summary(2, []), specialAction: { expectYukonXMLShowPlan: true } }];
    const messages = [{ isError: false, message: "(2 rows affected)" }];
    const result = await collectInvocationResults(clientFor([rows, []]), "query", summaries, messages, "success", 4096);
    assert.ok(responseSize(result) <= 4096);
    assert.equal(result.resultSets.length, 2);
    assert.equal(result.truncated, false);
    assert.equal(result.resultSets[0].returnedRowCount, 2);
    assert.equal(result.resultSets[0].rows[0][0], null);
    assert.equal(result.resultSets[0].rows[0][1], "NULL");
    assert.equal(result.resultSets[1].truncated, false);
    assert.equal(result.messages[0].message, "(2 rows affected)");
});

test("MCP preserves failure status even when error text is too large", async () => {
    const result = await collectInvocationResults(clientFor([]), "query", [], [
        { isError: false, message: "info" },
        { isError: true, message: "error".repeat(10000) },
    ], "error", 1024);
    assert.equal(result.status, "error");
    assert.equal(result.truncated, true);
    assert.ok(responseSize(result) <= 1024);
});

test("MCP reports oversized metadata and premature empty subsets explicitly", async () => {
    const rows = [[cell("x")]];
    await assert.rejects(collectInvocationResults(clientFor([rows]), "query",
        [summary(0, rows, [{ columnName: "x".repeat(2000) }])], [], "success", 1024), /metadata exceeds/);
    await assert.rejects(collectInvocationResults(clientFor([[]]), "query",
        [summary(0, rows)], [], "success", 1024), /returned no rows/);
});

test("MCP error responses fit the configured UTF-8 budget", () => {
    const result = invocationError('界"\\\n'.repeat(10000), 1024);
    assert.equal(result.status, "error");
    assert.equal(result.truncated, true);
    assert.ok(responseSize(result) <= 1024);
    assert.equal(invocationError("Connection failed", 1024).truncated, false);
});
