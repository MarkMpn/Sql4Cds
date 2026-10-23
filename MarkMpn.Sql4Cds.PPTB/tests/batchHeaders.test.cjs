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
    const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText;
    vm.runInNewContext(code, {
        module,
        exports: module.exports,
        TextEncoder,
        setTimeout: options.setTimeout ?? setTimeout,
        window: {
            dataverseAPI: { executeBatch: execute(false), executeTransaction: execute(true) },
            toolboxAPI: { workers: { connect: async (_id, options) => {
                handlers = options;
                return {
                    ready: Promise.resolve(),
                    request: async () => ({}),
                    notify: async (method, params) => {
                        if (method !== "dataverse/executeBatch/chunk") return;
                        notifications.push(params);
                        if (params.complete) complete();
                    },
                };
            } } },
        },
        console: { debug() {}, error() {} },
    }, { filename });
    await new module.exports.Sql4CdsClient({}).start({});
    return { handlers, calls, notifications, response };
}

function batch(transaction = false, continueOnError = false) {
    return {
        target: 2,
        transaction,
        continueOnError,
        headers: { MSCRMCallerID: "00000000-0000-0000-0000-000000000123", prefer: "return=representation" },
        requests: [
            { method: "PATCH", url: "accounts(123)", contentId: "1", headers: { "If-Match": "*" }, body: "{}" },
            { method: "DELETE", url: "accounts(456)", contentId: "2", headers: null },
        ],
    };
}

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
            assert.equal(call.transaction, transaction);
            const [requests, target, headers] = call.args;
            assert.equal(target, 2);
            assert.equal(headers.MSCRMCallerID, params.headers.MSCRMCallerID);
            assert.equal(headers.prefer, !transaction && continueOnError
                ? "return=representation,odata.continue-on-error" : "return=representation");
            assert.equal(headers.Prefer, undefined);
            assert.equal(params.headers.prefer, "return=representation");
            assert.equal(requests[0].headers["If-Match"], "*");
            assert.equal(requests[0].headers.MSCRMCallerID, undefined);
            assert.equal(requests[1].headers, undefined);
        });
    }
}

test("batch headers survive chunked RPC reassembly", { timeout: 5000 }, async () => {
    const { handlers, calls, notifications, response } = await createClient({
        batchHeaders: { "x-ms-dop-hint": "8", "Retry-After": "12" },
    });
    const params = batch(false, true);
    const payload = JSON.stringify(params);
    const split = payload.indexOf("MSCRMCallerID") + 5;
    const handler = handlers.notifications["dataverse/executeBatch/chunk"];
    handler({ id: "batch-test", direction: "request", sequence: 0, complete: false, data: payload.slice(0, split) });
    assert.equal(calls.length, 0);
    handler({ id: "batch-test", direction: "request", sequence: 1, complete: true, data: payload.slice(split) });
    await response;
    assert.equal(calls[0].args[2].MSCRMCallerID, params.headers.MSCRMCallerID);
    assert.equal(calls[0].args[2].prefer, "return=representation,odata.continue-on-error");
    const result = JSON.parse(notifications.map((n) => n.data).join(""));
    assert.equal(result.result.length, 2);
    assert.equal(result.result[0].status, 204);
    assert.equal(result.result[0].batchHeaders["x-ms-dop-hint"], "8");
    assert.equal(result.result[0].batchHeaders["Retry-After"], "12");
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
        assert.equal(transaction, true);
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
        assert.equal(transaction, true);
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

test("does not retry throttling responses without a valid batch Retry-After", async () => {
    const { handlers, calls } = await createClient(() => [
        { contentId: "1", status: 429, batchHeaders: { "Retry-After": "invalid" } },
        { contentId: "2", status: 429, batchHeaders: { "Retry-After": "1" } },
    ]);

    await handlers.requests["dataverse/executeBatch"](batch(), {});

    assert.equal(calls.length, 1);
});
