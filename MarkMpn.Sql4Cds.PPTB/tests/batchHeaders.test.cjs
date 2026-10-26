const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");
const ts = require("typescript");

async function createClient(responseHeaders = {}, options = {}) {
    let handlers;
    const calls = [];
    const notifications = [];
    const notificationMethods = [];
    const logs = [];
    const rpcRequests = [];
    let complete;
    const response = new Promise((resolve) => { complete = resolve; });
    const execute = (transaction) => async (...args) => {
        calls.push({ transaction, args });
        if (typeof responseHeaders === "function")
            return responseHeaders(args[0], transaction, calls.length);
        return args[0].map((r) => ({ contentId: r.contentId, status: 204, ...responseHeaders }));
    };
    const module = { exports: {} };
    const filename = path.resolve(__dirname, "..", "src", "lsp", "client.ts");
    const source = fs.readFileSync(filename, "utf8").replace("const MAX_RPC_MESSAGE_SIZE = 32 * 1024 * 1024;", `const MAX_RPC_MESSAGE_SIZE = ${options.maxMessageSize ?? 32 * 1024 * 1024};`);
    const code = ts.transpileModule(source, {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText;
    vm.runInNewContext(code, {
        module,
        exports: module.exports,
        TextEncoder,
        __SQL4CDS_VERSION__: JSON.parse(fs.readFileSync(path.resolve(__dirname, "..", "package.json"), "utf8")).version,
        Date: options.Date ?? Date,
        setTimeout: options.setTimeout ?? setTimeout,
        window: {
            dataverseAPI: { executeBatch: execute(false), executeTransaction: execute(true) },
            toolboxAPI: { workers: { connect: async (_id, connectOptions) => {
                handlers = connectOptions;
                return {
                    ready: Promise.resolve(),
                    stop: async () => {},
                    request: async (method, params) => {
                        rpcRequests.push({ method, params });
                        if (options.request && method !== "initialize") return options.request(method, params);
                        return method === "initialize" && options.queryEventsVersion
                            ? { capabilities: { experimental: { sql4cds: { queryEvents: { version: options.queryEventsVersion, initialRowCount: options.initialRowCount } } } } }
                            : {};
                    },
                    notify: async (method, params) => {
                        if (options.notify) await options.notify(method, params);
                        if (method !== "dataverse/executeBatch/chunk" && method !== "dataverse/executeBatch/message") return;
                        notifications.push(params);
                        notificationMethods.push(method);
                        if (method === "dataverse/executeBatch/message" || params.complete) complete();
                    },
                };
            } } },
        },
        console: {
            debug: (...args) => logs.push({ level: "debug", args }),
            error: (...args) => logs.push({ level: "error", args }),
        },
    }, { filename });
    const client = new module.exports.Sql4CdsClient(options.events ?? {});
    await client.start({});
    return { client, handlers, calls, notifications, notificationMethods, response, logs, rpcRequests };
}

test("production build embeds the release package version", async () => {
    const { loadConfigFromFile } = await import("vite");
    const result = await loadConfigFromFile(
        { command: "build", mode: "production" }, path.resolve(__dirname, "..", "vite.config.ts"));
    assert.equal(result.config.define.__SQL4CDS_VERSION__,
        JSON.stringify(JSON.parse(fs.readFileSync(path.resolve(__dirname, "..", "package.json"), "utf8")).version));
});

test("initialize advertises the PPTB client name and release package version", async () => {
    const { rpcRequests } = await createClient();
    const initialize = rpcRequests.find(request => request.method === "initialize");
    assert.equal(initialize.params.clientInfo.name, "Power Platform ToolBox");
    assert.equal(initialize.params.clientInfo.version,
        JSON.parse(fs.readFileSync(path.resolve(__dirname, "..", "package.json"), "utf8")).version);
});

function batch(transaction = false, continueOnError = false) {
    return {
        target: 2,
        continueOnError,
        headers: { MSCRMCallerID: "00000000-0000-0000-0000-000000000123", prefer: "return=representation" },
        requests: [
            { method: "PATCH", url: "accounts(123)", contentId: "1", changesetId: transaction ? "tx-1" : undefined, headers: { "If-Match": "*" }, body: "{}" },
            { method: "DELETE", url: "accounts(456)", contentId: "2", changesetId: transaction ? "tx-1" : undefined, headers: null },
        ],
    };
}

test("terminal worker errors from requests and both Dataverse response transports fail the client once", async () => {
    for (const source of ["request", "direct", "chunk"]) {
        const failures = [];
        let resolveFailure;
        const failed = new Promise(resolve => { resolveFailure = resolve; });
        const fatal = Object.assign(new Error("Worker API: WORKER_FAILED"), { code: "WORKER_FAILED" });
        const { client, handlers, rpcRequests, logs } = await createClient(source === "chunk" ? { body: { text: "x".repeat(2000) } } : {}, {
            maxMessageSize: source === "chunk" ? 512 : undefined,
            queryEventsVersion: 1,
            initialRowCount: 200,
            events: { failed: message => { failures.push(message); resolveFailure(); } },
            request: method => { if (method === "query/subset") throw fatal; return {}; },
            notify: method => {
                if (method === "dataverse/executeBatch/message" || method === "dataverse/executeBatch/chunk") throw fatal;
            },
        });
        await client.executeDocumentSelection("query");
        const executionId = rpcRequests.at(-1).params.executionId;
        handlers.notifications["sql4cds/queryEvents"]({ ownerUri: "query", executionId, events: [{
            type: "resultSet", data: { id: 0, batchId: 0, rowCount: 1, complete: false, columnInfo: [],
                rowsStartIndex: 0, rows: [[{ displayValue: "cached" }]] },
        }] });
        const messages = [];
        client.addListener("query", { message: params => messages.push(params.message.message) });
        if (source === "request") {
            await assert.rejects(client.subset("query", 0, 1, 0, 1), /WORKER_FAILED/);
        } else {
            handlers.notifications["dataverse/executeBatch/message"]({ id: "fatal", data: batch() });
        }
        await failed;
        if (source !== "request")
            assert.ok(logs.some(log => String(log.args[0]).includes(source === "chunk" ? "response chunk" : "notification error")));
        assert.equal(client.isRunning, false);
        assert.match(failures[0], /Close and reopen.*restart/);
        await assert.rejects(client.subset("query", 0, 0, 0, 1), /restart the engine/);
        handlers.onExit({ state: "failed", failure: "WORKER_FAILED" });
        handlers.notifications["query/message"]({ ownerUri: "query", message: { message: "late message" } });
        assert.equal(failures.length, 1);
        assert.deepEqual(messages, []);
        await assert.rejects(client.start({}), /restart the engine/);
    }
});

test("unexpected worker exits fail the client but intentional stops and normal SQL errors do not", async () => {
    const failures = [];
    const { client, handlers } = await createClient({}, {
        events: { failed: message => failures.push(message) },
        request: () => { throw new Error("Invalid column name"); },
    });
    await assert.rejects(client.executeDocumentSelection("query"), /Invalid column name/);
    assert.equal(client.isRunning, true);
    assert.deepEqual(failures, []);
    handlers.onExit({ state: "exited" });
    assert.equal(client.isRunning, false);
    assert.equal(failures.length, 1);
    const stoppedFailures = [];
    const stopped = await createClient({}, { events: { failed: message => stoppedFailures.push(message) } });
    await stopped.client.stop();
    stopped.handlers.onExit({ state: "exited" });
    assert.deepEqual(stoppedFailures, []);
});

test("compact query events negotiate version 1 and reconstruct metadata, plans, errors and completion", async () => {
    const progress = [];
    const { client, handlers, rpcRequests } = await createClient({}, { queryEventsVersion: 1, events: { progress: message => progress.push(message) } });
    const received = [];
    const uri = "pptb://query/compact";
    const listener = {};
    for (const name of ["batchStart", "resultSetAvailable", "resultSetUpdated", "resultSetComplete", "batchComplete", "message", "queryComplete"])
        listener[name] = params => received.push({ name, params });
    client.addListener(uri, listener);
    await client.executeDocumentSelection(uri);
    const executionId = rpcRequests.at(-1).params.executionId;
    assert.equal(rpcRequests[0].params.capabilities.experimental.sql4cds.queryEvents.version, 1);
    const notify = events => handlers.notifications["sql4cds/queryEvents"]({ ownerUri: uri, executionId, events });
    const columns = [{ columnName: "name", dataTypeName: "nvarchar" }];
    notify([
        { type: "query/batchStart", data: { id: 0, hasError: false } },
        { type: "resultSet", data: { batchId: 0, id: 0, rowCount: 0, complete: false, columnInfo: columns } },
        { type: "sql4cds/progress", data: "Reading..." },
    ]);
    notify([
        { type: "resultSet", data: { batchId: 0, id: 0, rowCount: 5, complete: true } },
        { type: "resultSet", data: { batchId: 0, id: 1, rowCount: 0, complete: true, columnInfo: columns, specialAction: { expectYukonXMLShowPlan: true }, executionPlans: [{ graphFile: "plan" }] } },
        { type: "query/message", data: { batchId: 0, message: "Failed", isError: true } },
        { type: "query/batchComplete", data: { id: 0, hasError: true } },
        { type: "query/complete" },
    ]);
    assert.equal(received.filter(event => event.name === "resultSetAvailable").length, 2);
    assert.equal(received.filter(event => event.name === "resultSetComplete").length, 2);
    const result = received.at(-1);
    assert.equal(result.name, "queryComplete");
    assert.equal(result.params.batchSummaries[0].hasError, true);
    assert.equal(result.params.batchSummaries[0].resultSetSummaries[0].rowCount, 5);
    assert.deepEqual(result.params.batchSummaries[0].resultSetSummaries[0].columnInfo, columns);
    assert.equal(received.find(event => event.params.executionPlans)?.params.executionPlans[0].graphFile, "plan");
    assert.equal(received.find(event => event.name === "message").params.message.isError, true);
    assert.deepEqual(progress, ["Reading..."]);
});

test("initial result rows avoid subset RPCs during execution and after completion, paging only the missing suffix", async () => {
    const cell = value => ({ displayValue: value, isNull: false, rawObject: value });
    const { client, handlers, rpcRequests } = await createClient({}, {
        queryEventsVersion: 1, initialRowCount: 200,
        request: (_method, params) => ({
            resultSubset: { rowCount: params.rowsCount, rows: Array.from({ length: params.rowsCount }, (_, i) => [cell(String(params.rowsStartIndex + i))]) },
        }),
    });
    assert.equal(rpcRequests[0].params.capabilities.experimental.sql4cds.queryEvents.initialRowCount, 200);
    const uri = "rows";
    await client.executeDocumentSelection(uri);
    const executionId = rpcRequests.at(-1).params.executionId;
    const send = events => handlers.notifications["sql4cds/queryEvents"]({ ownerUri: uri, executionId, events });
    const callbackRows = [];
    client.addListener(uri, { resultSetUpdated: params => {
        if (params.resultSetSummary.rowCount > 0)
            callbackRows.push(client.subset(uri, 0, 0, 0, params.resultSetSummary.rowCount));
    } });
    send([{ type: "resultSet", data: {
        id: 0, batchId: 0, rowCount: 1, complete: false, columnInfo: [{ columnName: "name" }],
        rowsStartIndex: 0, rows: [[cell("0")]],
    } }]);
    send([{ type: "resultSet", data: {
        id: 0, batchId: 0, rowCount: 2, complete: false, rowsStartIndex: 1, rows: [[cell("1")]],
    } }]);
    assert.equal((await callbackRows[0]).resultSubset.rows[0][0].displayValue, "0");
    assert.equal((await callbackRows[1]).resultSubset.rows[1][0].displayValue, "1");
    assert.equal((await client.subset(uri, 0, 0, 0, 200)).resultSubset.rowCount, 2);
    assert.equal(rpcRequests.filter(request => request.method === "query/subset").length, 0);
    client.removeListener(uri);
    // Removing a listener invalidates a stream; start a fresh execution to fill the whole first page.
    await client.executeDocumentSelection(uri);
    const current = rpcRequests.at(-1).params.executionId;
    handlers.notifications["sql4cds/queryEvents"]({ ownerUri: uri, executionId: current, events: [
        { type: "resultSet", data: { id: 0, batchId: 0, rowCount: 205, complete: true, columnInfo: [],
            rowsStartIndex: 0, rows: Array.from({ length: 200 }, (_, i) => [cell(String(i))]) } },
        { type: "query/complete" },
    ] });
    const cached = await client.subset(uri, 0, 0, 0, 200);
    assert.equal(cached.resultSubset.rows.length, 200);
    assert.equal(cached.resultSubset.rowCount, 200);
    assert.equal("totalRowCount" in cached.resultSubset, false);
    assert.equal(rpcRequests.filter(request => request.method === "query/subset").length, 0);
    const mixed = await client.subset(uri, 0, 0, 198, 7);
    assert.deepEqual(Array.from(mixed.resultSubset.rows, row => row[0].displayValue), ["198", "199", "200", "201", "202", "203", "204"]);
    assert.equal(mixed.resultSubset.rowCount, 7);
    assert.deepEqual({ ...rpcRequests.at(-1).params }, { ownerUri: uri, batchIndex: 0, resultSetIndex: 0, rowsStartIndex: 200, rowsCount: 5 });
});

test("initial row caches are isolated and removed on disposal, listener removal and reexecution", async () => {
    const { client, handlers, rpcRequests } = await createClient({}, { queryEventsVersion: 1, initialRowCount: 200 });
    const push = async (uri, id = 0) => {
        await client.executeDocumentSelection(uri);
        const executionId = rpcRequests.at(-1).params.executionId;
        handlers.notifications["sql4cds/queryEvents"]({ ownerUri: uri, executionId, events: [{
            type: "resultSet", data: { id, batchId: 0, rowCount: 1, complete: false, columnInfo: [],
                rowsStartIndex: 0, rows: [[{ displayValue: uri, isNull: false }]] },
        }] });
        return executionId;
    };
    const stale = await push("first");
    await push("second", 1);
    assert.equal((await client.subset("first", 0, 0, 0, 1)).resultSubset.rows[0][0].displayValue, "first");
    assert.equal((await client.subset("second", 0, 1, 0, 1)).resultSubset.rows[0][0].displayValue, "second");
    assert.equal(rpcRequests.filter(request => request.method === "query/subset").length, 0);
    await client.executeDocumentSelection("first");
    handlers.notifications["sql4cds/queryEvents"]({ ownerUri: "first", executionId: stale, events: [{
        type: "resultSet", data: { id: 0, batchId: 0, rowCount: 1, complete: false, rowsStartIndex: 0, rows: [[{ displayValue: "stale" }]] },
    }] });
    await client.subset("first", 0, 0, 0, 1);
    assert.equal(rpcRequests.at(-1).method, "query/subset");
    await client.disposeQuery("second");
    await client.subset("second", 0, 1, 0, 1);
    assert.equal(rpcRequests.at(-1).method, "query/subset");
    await push("third");
    client.removeListener("third");
    await client.subset("third", 0, 0, 0, 1);
    assert.equal(rpcRequests.at(-1).method, "query/subset");
});

test("initial rows require acknowledgement and invalid row offsets are rejected", async () => {
    for (const initialRowCount of [undefined, 0, 200]) {
        const { client, handlers, rpcRequests } = await createClient({}, { queryEventsVersion: 1, initialRowCount });
        await client.executeDocumentSelection("query");
        const executionId = rpcRequests.at(-1).params.executionId;
        const send = data => handlers.notifications["sql4cds/queryEvents"]({ ownerUri: "query", executionId, events: [{
            type: "resultSet", data: { id: 0, batchId: 0, rowCount: 1, complete: false, columnInfo: [], ...data },
        }] });
        if (initialRowCount) {
            assert.throws(() => send({ rowsStartIndex: 1, rows: [[{ displayValue: "gap" }]] }), /contiguous prefix/);
            send({ rowsStartIndex: 0, rows: [[{ displayValue: "first" }]] });
            assert.throws(() => send({ rowsStartIndex: 0, rows: [[{ displayValue: "duplicate" }]] }), /contiguous prefix/);
        } else {
            send({});
            await client.subset("query", 0, 0, 0, 1);
            assert.equal(rpcRequests.at(-1).method, "query/subset");
        }
    }
});

test("initial row caches are cleared by document closure, worker exit and stop", async () => {
    for (const cleanup of ["close", "exit", "stop"]) {
        const { client, handlers, rpcRequests } = await createClient({}, { queryEventsVersion: 1, initialRowCount: 200 });
        await client.executeDocumentSelection("query");
        const executionId = rpcRequests.at(-1).params.executionId;
        const send = () => handlers.notifications["sql4cds/queryEvents"]({ ownerUri: "query", executionId, events: [{
            type: "resultSet", data: { id: 0, batchId: 0, rowCount: 1, complete: false, columnInfo: [],
                rowsStartIndex: 0, rows: [[{ displayValue: "cached" }]] },
        }] });
        send();
        if (cleanup === "close") {
            await client.closeDocument("query");
            send();
            await client.subset("query", 0, 0, 0, 1);
            assert.equal(rpcRequests.at(-1).method, "query/subset");
        } else {
            if (cleanup === "exit") handlers.onExit({});
            else await client.stop();
            await assert.rejects(client.subset("query", 0, 0, 0, 1), cleanup === "exit" ? /restart the engine/ : /worker is not running/);
        }
    }
});

test("compact query streams reject stale executions and remain isolated by editor", async () => {
    const { client, handlers, rpcRequests } = await createClient({}, { queryEventsVersion: 1 });
    const received = [];
    for (const uri of ["first", "second"])
        client.addListener(uri, { message: params => received.push(params.message.message) });
    await client.executeDocumentSelection("first");
    const stale = rpcRequests.at(-1).params.executionId;
    await client.executeDocumentSelection("first");
    const current = rpcRequests.at(-1).params.executionId;
    await client.executeDocumentSelection("second");
    const second = rpcRequests.at(-1).params.executionId;
    const send = (ownerUri, executionId, message) => handlers.notifications["sql4cds/queryEvents"]({
        ownerUri, executionId, events: [{ type: "query/message", data: { message } }],
    });
    send("first", stale, "stale");
    send("first", current, "current");
    send("second", second, "second");
    await client.disposeQuery("first");
    send("first", current, "disposed");
    client.removeListener("second");
    send("second", second, "closed");
    assert.deepEqual(received, ["current", "second"]);
});

test("compact streams require acknowledgement and preserve legacy notifications without it", async () => {
    for (const queryEventsVersion of [undefined, 2]) {
        const { client, handlers, rpcRequests } = await createClient({}, { queryEventsVersion });
        const received = [];
        client.addListener("query", { message: params => received.push(params.message.message) });
        await client.executeDocumentSelection("query");
        assert.equal("executionId" in rpcRequests.at(-1).params, false);
        handlers.notifications["query/message"]({ ownerUri: "query", message: { message: "legacy" } });
        assert.deepEqual(received, ["legacy"]);
    }
});

for (const changesetId of [undefined, null, ""]) {
    test(`absent changeset ID (${String(changesetId)}) is omitted from host requests`, async () => {
        const { handlers, calls } = await createClient();
        const params = batch();
        for (const request of params.requests)
            request.changesetId = changesetId;
        await handlers.requests["dataverse/executeBatch"](params, {});
        assert.equal(calls.length, 1);
        for (const request of calls[0].args[0]) {
            assert.equal(request.changesetId, undefined);
            assert.equal(Object.hasOwn(request, "changesetId"), false);
        }
    });
}

test("mixed batch omits null changeset IDs and preserves transaction IDs", async () => {
    const { handlers, calls } = await createClient();
    const params = batch();
    params.requests[0].changesetId = null;
    params.requests[1].changesetId = "tx-1";
    await handlers.requests["dataverse/executeBatch"](params, {});
    const requests = calls[0].args[0];
    assert.equal(Object.hasOwn(requests[0], "changesetId"), false);
    assert.equal(requests[1].changesetId, "tx-1");
    assert.equal(params.requests[0].changesetId, null);
});

test("RPC logs timestamp every request, response, notification, chunk and error at emission time", async () => {
    let clock = Date.parse("2026-10-25T17:01:06.088Z");
    const initialClock = clock;
    const failure = new Error("RPC failed");
    const { client, handlers, logs } = await createClient({}, {
        Date: class extends Date { constructor() { super(clock++); } },
        events: { progress: () => { throw failure; } },
    });
    await handlers.requests["dataverse/executeBatch"](batch(), {});
    assert.throws(() => handlers.notifications["sql4cds/progress"]("loading"), error => error === failure);
    await client.notifyChunk({ id: "response", sequence: 0, complete: true, data: "{}" });
    client.receiveDataverseChunk({ id: "request", sequence: 0, complete: false, data: "{}" });
    client.session.request = async () => { throw failure; };
    await assert.rejects(client.request("test/failure", {}), error => error === failure);
    client.session.notify = async () => { throw failure; };
    await assert.rejects(client.notify("test/notification", {}), error => error === failure);
    await assert.rejects(handlers.requests["dataverse/executeBatch"](null, {}));

    for (const [index, { args }] of logs.entries()) {
        const prefix = `[SQL 4 CDS RPC] [${new Date(initialClock + index).toISOString()}] `;
        assert.ok(args[0].startsWith(prefix), `Missing emission timestamp: ${args[0]}`);
        assert.equal(args.length, 2, "Payloads remain separate console arguments");
    }
    const messages = logs.map(log => log.args[0]);
    for (const message of [
        "-> initialize request", "<- initialize response",
        "<- dataverse/executeBatch request", "-> dataverse/executeBatch response",
        "<- sql4cds/progress notification", "<- sql4cds/progress notification error",
        "-> dataverse/executeBatch/chunk response chunk", "<- dataverse/executeBatch/chunk request chunk",
        "<- test/failure error", "-> test/notification notification error", "-> dataverse/executeBatch error",
    ]) {
        assert.ok(messages.some(log => log.endsWith(message)), `Missing log: ${message}`);
    }
    assert.ok(logs.some(log => log.level === "error" && log.args[1] === failure));
});

for (const transaction of [false, true]) {
    test(`batch response headers reach the worker (transaction=${transaction})`, async () => {
        const { handlers } = await createClient({
            headers: { "x-ms-dop-hint": "2" },
            batchHeaders: { "x-ms-dop-hint": "8", "Retry-After": "12" },
        });
        const result = await handlers.requests["dataverse/executeBatch"](batch(transaction), {});
        const serialized = JSON.parse(JSON.stringify(result));
        assert.equal(serialized[0].headers["x-ms-dop-hint"], "2");
        assert.equal(serialized[0].batchHeaders["x-ms-dop-hint"], "8");
        assert.equal(serialized[0].batchHeaders["Retry-After"], "12");
    });

    for (const continueOnError of [false, true]) {
        test(`batch headers reach the third host argument (transaction=${transaction}, continueOnError=${continueOnError})`, async () => {
            const { handlers, calls } = await createClient();
            const params = batch(transaction, continueOnError);
            await handlers.requests["dataverse/executeBatch"](params, {});
            const call = calls[0];
            assert.equal(call.transaction, false);
            const [requests, target, headers] = call.args;
            assert.equal(target, 2);
            assert.equal(headers.MSCRMCallerID, params.headers.MSCRMCallerID);
            assert.equal(headers.prefer, continueOnError
                ? "return=representation,odata.continue-on-error" : "return=representation");
            assert.equal(headers.Prefer, undefined);
            assert.equal(params.headers.prefer, "return=representation");
            assert.equal(requests[0].headers["If-Match"], "*");
            assert.equal(requests[0].headers.MSCRMCallerID, undefined);
            assert.equal(requests[0].headers.Prefer, undefined);
            assert.equal(requests[0].headers.prefer, undefined);
            assert.equal(requests[1].headers, undefined);
            assert.equal(requests[0].changesetId, transaction ? "tx-1" : undefined);
            assert.equal(requests[1].changesetId, requests[0].changesetId);
        });
    }
}

test("stop-on-error batches preserve partial responses without replaying completed requests", async () => {
    const { handlers, calls } = await createClient(() => [
        { contentId: "1", status: 400, body: { error: { message: "Write failed" } } },
    ]);
    const params = batch(false, false);
    const result = await handlers.requests["dataverse/executeBatch"](params, {});
    assert.equal(calls.length, 1);
    assert.equal(calls[0].args[0].length, 2);
    assert.equal(calls[0].args[2].prefer, "return=representation");
    assert.equal(result.length, 1);
    assert.equal(result[0].status, 400);
    assert.equal(result[0].contentId, "1");
    assert.equal(JSON.parse(result[0].body).error.message, "Write failed");
});

test("batch headers survive chunked RPC reassembly", { timeout: 5000 }, async () => {
    const { handlers, calls, notifications, response } = await createClient({
        batchHeaders: { "x-ms-dop-hint": "8", "Retry-After": "12" },
    });
    const params = batch(true, true);
    const payload = JSON.stringify(params);
    const split = payload.indexOf("MSCRMCallerID") + 5;
    const handler = handlers.notifications["dataverse/executeBatch/chunk"];
    handler({ id: "batch-test", sequence: 0, complete: false, data: payload.slice(0, split) });
    assert.equal(calls.length, 0);
    handler({ id: "batch-test", sequence: 1, complete: true, data: payload.slice(split) });
    await response;
    assert.equal(calls[0].args[2].MSCRMCallerID, params.headers.MSCRMCallerID);
    assert.equal(calls[0].args[2].prefer, "return=representation,odata.continue-on-error");
    assert.equal(calls[0].transaction, false);
    assert.equal(calls[0].args[0][0].changesetId, "tx-1");
    assert.equal(calls[0].args[0][1].changesetId, "tx-1");
    assert.equal(notifications.length, 1);
    assert.equal(notifications[0].id, "batch-test");
    assert.equal(typeof notifications[0].data, "object");
    const result = notifications[0].data;
    assert.equal(result.result.length, 2);
    assert.equal(result.result[0].status, 204);
    assert.equal(result.result[0].batchHeaders["x-ms-dop-hint"], "8");
    assert.equal(result.result[0].batchHeaders["Retry-After"], "12");
});

test("204 responses omit undefined properties before PPTB validates direct notifications", async () => {
    const hostResult = {
        contentId: "1", status: 204, statusText: undefined, body: undefined,
        headers: { ETag: "version", absent: undefined },
        batchHeaders: { absent: undefined },
    };
    const assertJsonValues = value => {
        assert.notEqual(value, undefined);
        if (value !== null && typeof value === "object")
            for (const child of Object.values(value)) assertJsonValues(child);
    };
    const { handlers, notifications, response } = await createClient(() => [hostResult], {
        notify: (method, params) => {
            if (method === "dataverse/executeBatch/message") assertJsonValues(params);
        },
    });
    handlers.notifications["dataverse/executeBatch/message"]({ id: "update", data: batch() });
    await response;
    const result = notifications[0].data.result[0];
    assert.equal(result.status, 204);
    assert.equal(Object.hasOwn(result, "body"), false);
    assert.equal(Object.hasOwn(result, "statusText"), false);
    assert.deepEqual({ ...result.headers }, { ETag: "version" });
    assert.deepEqual({ ...result.batchHeaders }, {});
    assert.equal(Object.hasOwn(hostResult.headers, "absent"), true, "Do not mutate the host response");
});

test("outgoing RPC objects and legacy responses recursively omit undefined while preserving JSON values", async () => {
    const { client, handlers, rpcRequests } = await createClient({
        statusText: undefined, body: undefined, headers: { missing: undefined, present: "value" },
    });
    await client.request("test", {
        omitted: undefined, nested: { omitted: undefined, nullable: null },
        array: [undefined, { omitted: undefined, zero: 0, flag: false, text: "" }],
    });
    assert.equal(JSON.stringify(rpcRequests.at(-1).params), '{"nested":{"nullable":null},"array":[null,{"zero":0,"flag":false,"text":""}]}');
    assert.equal(Object.hasOwn(rpcRequests.at(-1).params, "omitted"), false);
    assert.equal(Object.hasOwn(rpcRequests.at(-1).params.nested, "omitted"), false);
    assert.equal(rpcRequests.at(-1).params.array[0], null);
    const result = await handlers.requests["dataverse/executeBatch"](batch(), {});
    for (const item of result) {
        assert.equal(Object.hasOwn(item, "body"), false);
        assert.equal(Object.hasOwn(item, "statusText"), false);
        assert.equal(Object.hasOwn(item.headers, "missing"), false);
    }
});

test("small notifications preserve request objects and return an unescaped response object", async () => {
    const { handlers, calls, notifications, notificationMethods, response } = await createClient({
        body: { name: "Quotes \"\\\r\n and Unicode \u00e9 \ud83d\ude00" },
    });
    const params = batch();
    handlers.notifications["dataverse/executeBatch/message"]({ id: "direct", data: params });
    await response;
    assert.equal(calls.length, 1);
    assert.equal(calls[0].args[0][0].url, params.requests[0].url);
    assert.deepEqual(notificationMethods, ["dataverse/executeBatch/message"]);
    assert.equal(notifications[0].id, "direct");
    assert.equal("direction" in notifications[0], false);
    assert.equal(typeof notifications[0].data, "object");
    assert.equal(JSON.parse(notifications[0].data.result[0].body).name, "Quotes \"\\\r\n and Unicode \u00e9 \ud83d\ude00");
});

for (const chunkedRequest of [false, true]) {
    test(`large responses use bounded chunks independently of request framing (chunkedRequest=${chunkedRequest})`, async () => {
        const text = "Quotes \"\\\r\n \u00e9 \ud83d\ude00".repeat(100);
        const { handlers, notifications, notificationMethods, response } = await createClient({ body: { text } }, { maxMessageSize: 512 });
        const data = batch();
        if (chunkedRequest) {
            const payload = JSON.stringify(data);
            const handler = handlers.notifications["dataverse/executeBatch/chunk"];
            let sequence = 0;
            for (let offset = 0; offset < payload.length; offset += 64)
                handler({ id: "large", sequence: sequence++, complete: offset + 64 >= payload.length, data: payload.slice(offset, offset + 64) });
        } else {
            data.requests = [{ method: "GET", url: "accounts", contentId: "1" }];
            handlers.notifications["dataverse/executeBatch/message"]({ id: "large", data });
        }
        await response;
        assert.ok(notifications.length > 1);
        for (const [sequence, chunk] of notifications.entries()) {
            assert.equal(notificationMethods[sequence], "dataverse/executeBatch/chunk");
            assert.equal(chunk.id, "large");
            assert.equal("direction" in chunk, false);
            assert.equal(chunk.sequence, sequence);
            assert.equal(chunk.complete, sequence === notifications.length - 1);
            assert.ok(Buffer.byteLength(JSON.stringify({ jsonrpc: "2.0", method: notificationMethods[sequence], params: chunk })) <= 512);
        }
        const result = JSON.parse(notifications.map(chunk => chunk.data).join(""));
        assert.equal(JSON.parse(result.result[0].body).text, text);
    });
}

test("direct response size includes UTF-8 bytes and the complete notification envelope at the exact boundary", async () => {
    const id = "boundary";
    const data = { error: { message: "\u00e9\ud83d\ude00\"\\\r\n".repeat(20) } };
    const size = Buffer.byteLength(JSON.stringify({ jsonrpc: "2.0", method: "dataverse/executeBatch/message", params: { id, data } }));
    for (const limit of [size, size - 1]) {
        const { client, notifications, notificationMethods } = await createClient({}, { maxMessageSize: limit });
        await client.sendDataverseResponse(id, data);
        assert.equal(notificationMethods[0], limit === size ? "dataverse/executeBatch/message" : "dataverse/executeBatch/chunk");
        for (const [index, params] of notifications.entries())
            assert.ok(Buffer.byteLength(JSON.stringify({ jsonrpc: "2.0", method: notificationMethods[index], params })) <= limit);
    }
});

test("malformed direct requests and mixed partial transfers return correlated errors", async () => {
    for (const mixed of [false, true]) {
        const { handlers, calls, notifications, response } = await createClient();
        if (mixed)
            handlers.notifications["dataverse/executeBatch/chunk"]({ id: "invalid", sequence: 0, complete: false, data: "{" });
        handlers.notifications["dataverse/executeBatch/message"]({ id: "invalid", data: mixed ? batch() : null });
        await response;
        assert.equal(calls.length, 0);
        assert.equal(notifications[0].id, "invalid");
        assert.equal(typeof notifications[0].data.error.message, "string");
    }
});

test("missing batch response headers are serialized as an empty collection", async () => {
    const { handlers } = await createClient();
    const result = await handlers.requests["dataverse/executeBatch"](batch(), {});
    assert.equal(JSON.stringify(result[0].batchHeaders), "{}");
});

test("ordinary batches have no impersonation header", async () => {
    const { handlers, calls } = await createClient();
    const params = batch();
    params.headers = {};
    await handlers.requests["dataverse/executeBatch"](params, {});
    assert.equal(Object.keys(calls[0].args[2]).length, 0);
});

test("retries only the trailing 429 requests using batch Retry-After", async () => {
    const delays = [];
    const { handlers, calls } = await createClient((_requests, _transaction, callNumber) => {
        if (callNumber === 1)
            return [
                { contentId: "1", status: 204, batchHeaders: { "Retry-After": "1" } },
                { contentId: "2", status: 429 },
                { contentId: "3", status: 429 },
            ];
        if (callNumber === 2)
            return [
                { contentId: "2", status: 204 },
                { contentId: "3", status: 429, batchHeaders: { "Retry-After": "2" } },
            ];
        return [{ contentId: "3", status: 204 }];
    }, {
        setTimeout: (callback, milliseconds) => {
            delays.push(milliseconds);
            callback();
        },
    });
    const params = batch();
    params.requests.push({ method: "POST", url: "accounts", contentId: "3", body: "{}" });

    const results = await handlers.requests["dataverse/executeBatch"](params, {});

    assert.deepEqual(calls.map((call) => call.args[0].map((request) => request.contentId)), [["1", "2", "3"], ["2", "3"], ["3"]]);
    assert.deepEqual(delays, [1000, 2000]);
    assert.deepEqual(JSON.parse(JSON.stringify(results)).map((result) => result.status), [204, 204, 204]);
});

test("does not retry a 429 unless it is in the trailing result segment", async () => {
    const { handlers, calls } = await createClient(() => [
        { contentId: "1", status: 204 },
        { contentId: "2", status: 429, batchHeaders: { "Retry-After": "1" } },
        { contentId: "3", status: 204 },
    ], { setTimeout: (callback) => callback() });
    const params = batch();
    params.requests.push({ method: "POST", url: "accounts", contentId: "3", body: "{}" });

    const results = await handlers.requests["dataverse/executeBatch"](params, {});

    assert.equal(calls.length, 1);
    assert.deepEqual(JSON.parse(JSON.stringify(results)).map((result) => result.status), [204, 429, 204]);
});

test("retries transactional batches only as a whole", async () => {
    const delays = [];
    const { handlers, calls } = await createClient((requests, transaction, callNumber) => {
        assert.equal(transaction, false);
        if (callNumber === 1)
            return requests.map((request, index) => ({
                contentId: request.contentId,
                status: index === 0 ? 204 : 429,
                batchHeaders: { "Retry-After": "1" },
            }));
        return requests.map((request) => ({
            contentId: request.contentId,
            status: 204,
        }));
    }, { setTimeout: (callback, milliseconds) => { delays.push(milliseconds); callback(); } });
    const params = batch(true);

    const results = await handlers.requests["dataverse/executeBatch"](params, {});

    assert.equal(calls.length, 1);
    assert.deepEqual(delays, []);
    assert.deepEqual(JSON.parse(JSON.stringify(results)).map((result) => result.status), [204, 429]);
});

test("retries a wholly throttled transaction as a whole", async () => {
    const { handlers, calls } = await createClient((requests, transaction, callNumber) => {
        assert.equal(transaction, false);
        return requests.map((request) => ({
            contentId: request.contentId,
            status: callNumber === 1 ? 429 : 204,
            batchHeaders: { "Retry-After": "0" },
        }));
    }, { setTimeout: (callback) => callback() });
    const params = batch(true);

    const results = await handlers.requests["dataverse/executeBatch"](params, {});

    assert.equal(calls.length, 2);
    assert.deepEqual(calls[1].args[0].map((request) => request.contentId), ["1", "2"]);
    assert.deepEqual(JSON.parse(JSON.stringify(results)).map((result) => result.status), [204, 204]);
});

test("leaves the final throttling responses after the retry limit", async () => {
    const { handlers, calls } = await createClient((requests) => requests.map((request) => ({
        contentId: request.contentId,
        status: 429,
        batchHeaders: { "Retry-After": "0" },
    })), { setTimeout: (callback) => callback() });

    const results = await handlers.requests["dataverse/executeBatch"](batch(), {});

    assert.equal(calls.length, 11);
    assert.deepEqual(JSON.parse(JSON.stringify(results)).map((result) => result.status), [429, 429]);
});

test("mixed batch forwards changesets and retries only whole trailing changesets", async () => {
    const { handlers, calls } = await createClient((requests, transaction, callNumber) => {
        assert.equal(transaction, false);
        return requests.map((request, index) => ({
            contentId: request.contentId,
            status: callNumber === 1 && index >= 3 ? 429 : 204,
            batchHeaders: { "Retry-After": "0" },
        }));
    }, { setTimeout: (callback) => callback() });
    const params = batch(false, true);
    params.requests = [
        { method: "GET", url: "WhoAmI", contentId: "1" },
        { method: "PATCH", url: "accounts(1)", contentId: "2", changesetId: "tx-a" },
        { method: "DELETE", url: "accounts(2)", contentId: "3", changesetId: "tx-a" },
        { method: "PATCH", url: "accounts(3)", contentId: "4", changesetId: "tx-b" },
        { method: "DELETE", url: "accounts(4)", contentId: "5", changesetId: "tx-b" },
    ];
    const results = await handlers.requests["dataverse/executeBatch"](params, {});
    assert.equal(calls.length, 2);
    assert.deepEqual(calls[0].args[0].map((r) => r.changesetId), [undefined, "tx-a", "tx-a", "tx-b", "tx-b"]);
    assert.deepEqual(calls[1].args[0].map((r) => r.contentId), ["4", "5"]);
    assert.deepEqual(calls[1].args[0].map((r) => r.changesetId), ["tx-b", "tx-b"]);
    assert.equal(calls[1].args[2].prefer, "return=representation,odata.continue-on-error");
    assert.equal(results.length, 5);
});

test("does not retry throttling responses without a valid batch Retry-After", async () => {
    const { handlers, calls } = await createClient(() => [
        { contentId: "1", status: 429, batchHeaders: { "Retry-After": "invalid" } },
        { contentId: "2", status: 429, batchHeaders: { "Retry-After": "1" } },
    ]);

    await handlers.requests["dataverse/executeBatch"](batch(), {});

    assert.equal(calls.length, 1);
});
