const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");
const ts = require("typescript");

async function createClient() {
    let handlers;
    const calls = [];
    const notifications = [];
    let complete;
    const response = new Promise((resolve) => { complete = resolve; });
    const execute = (transaction) => async (...args) => {
        calls.push({ transaction, args });
        return args[0].map((r) => ({ contentId: r.contentId, status: 204 }));
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
    const { handlers, calls, notifications, response } = await createClient();
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
});

test("ordinary batches have no impersonation header", async () => {
    const { handlers, calls } = await createClient();
    const params = batch();
    params.headers = {};
    await handlers.requests["dataverse/executeBatch"](params, {});
    assert.equal(Object.keys(calls[0].args[2]).length, 0);
});
