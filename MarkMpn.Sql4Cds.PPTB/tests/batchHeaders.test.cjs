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
        Date: options.Date ?? Date,
        setTimeout: options.setTimeout ?? setTimeout,
        window: {
            dataverseAPI: { executeBatch: execute(false), executeTransaction: execute(true) },
            toolboxAPI: { workers: { connect: async (_id, options) => {
                handlers = options;
                return {
                    ready: Promise.resolve(),
                    request: async () => ({}),
                    notify: async (method, params) => {
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
    return { client, handlers, calls, notifications, notificationMethods, response, logs };
}

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
