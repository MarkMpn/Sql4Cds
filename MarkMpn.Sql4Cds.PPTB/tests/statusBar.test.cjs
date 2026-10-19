const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");
const ts = require("typescript");

class Element {
    constructor() {
        this.children = [];
        this.dataset = {};
        this.textContent = "";
        this.classList = { toggle() {} };
    }
    append(...children) { this.children.push(...children); }
    appendChild(child) { this.children.push(child); }
    setAttribute() {}
    contains() { return false; }
    replaceChildren(...children) { this.children = children; }
    addEventListener() {}
    remove() {}
}

function load(relativePath, dependencies, globals = {}, extra = "") {
    const filename = path.resolve(__dirname, "..", "src", relativePath);
    const module = { exports: {} };
    const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText;
    vm.runInNewContext(code + extra, {
        module, exports: module.exports, require: (name) => {
            assert.ok(name in dependencies, `Unexpected dependency: ${name}`);
            return dependencies[name];
        },
        URL, console, ...globals,
    }, { filename });
    return module.exports;
}

function setup() {
    let now = 1000;
    const elements = new Map();
    const document = {
        createElement: () => new Element(),
        getElementById: (id) => {
            if (!elements.has(id)) elements.set(id, new Element());
            return elements.get(id);
        },
    };
    const listeners = new Map();
    const client = {
        addListener: (uri, listener) => listeners.set(uri, listener),
        openDocument: async () => {},
        changeDocument: async () => {},
        disposeQuery: async () => {},
        executeDocumentSelection: async () => {},
        cancelQuery: async () => ({}),
        associateConnection: async (uri) => {
            listeners.get(uri).connectionComplete({
                ownerUri: uri,
                connectionSummary: { serverName: "contoso.crm.dynamics.com", userName: "test.user@contoso.com", databaseName: "contoso_2" },
            });
            return true;
        },
    };
    const registry = {
        get: () => ({ options: { url: "https://contoso.crm.dynamics.com", connectionName: "contoso_2" } }),
    };
    const timers = new Map();
    let nextTimer = 0;
    const { App } = load("app.ts", {
        "./styles.css": {},
        "./connections": { ConnectionRegistry: class { constructor() { return registry; } } },
        "./editor/monacoSetup": {},
        "./lsp/client": { Sql4CdsClient: class { constructor() { return client; } } },
        "./lsp/protocol": {},
        "./query/queryTab": {},
        "./objectExplorer": { ObjectExplorer: class {} },
        "./settings": {},
    }, {
        document, window: {},
        setInterval: (callback) => { timers.set(++nextTimer, callback); return nextTimer; },
        clearInterval: (id) => timers.delete(id),
    }, "\nexports.App = App;");
    const app = new App();
    app.ready = true;
    app.renderTabs = () => {};
    app.renderConnectionList = () => {};
    const grids = [];
    const { QueryTab } = load("query/queryTab.ts", {
        "../editor/monacoSetup": {
            monaco: {
                editor: {
                    createModel: () => ({
                        setEOL() {}, getValue: () => "", onDidChangeContent: () => ({ dispose() {} }),
                    }),
                    EndOfLineSequence: { LF: 1 },
                    setModelMarkers() {},
                },
                Uri: { parse: (uri) => uri },
            },
        },
        "../connections": { ConnectionRegistry: { ownerUri: (slot) => `connection:${slot}` } },
        "./resultsGrid": { ResultsGrid: class {
            constructor(_columns, _fetch, _copy, _open, status) {
                this.element = new Element();
                this.status = status;
                grids.push(this);
            }
            setRowCount() {}
            dispose() {}
        } },
    }, { document, Date: { now: () => now } });
    function createTab() {
        const tab = new QueryTab(app);
        app.activeTab = tab;
        return { tab, events: listeners.get(tab.uri) };
    }
    return { app, client, registry, createTab, elements, timers, grids, setNow: (value) => { now = value; } };
}

function summary(batchId, id, rowCount) {
    return { batchId, id, rowCount, columnInfo: [], complete: false };
}

test("Object Explorer insertion replaces the current editor selection as a single undoable edit and restores focus", () => {
    const { app, createTab } = setup();
    createTab();
    const selection = { startLineNumber: 2, startColumn: 8, endLineNumber: 2, endColumn: 12 };
    const actions = [];
    app.editor = {
        getSelection: () => selection,
        pushUndoStop: () => actions.push("undo"),
        executeEdits: (source, edits) => {
            assert.equal(source, "object-explorer");
            assert.equal(edits[0].range, selection);
            assert.equal(edits[0].text, "dbo.account");
            assert.equal(edits[0].forceMoveMarkers, true);
            actions.push("edit");
        },
        focus: () => actions.push("focus"),
    };
    app.insertObjectName("dbo.account");
    assert.deepEqual(actions, ["undo", "edit", "undo", "focus"]);
    app.activeTab = null;
    app.insertObjectName("ignored");
    assert.equal(actions.length, 4);
});

test("one global status bar shows connection details and accurately ticks elapsed time and all result rows", async () => {
    const { app, createTab, elements, timers, setNow } = setup();
    const { tab, events } = createTab();
    await tab.connect(0);
    assert.equal(elements.get("status-domain").textContent, "contoso.crm.dynamics.com");
    assert.equal(elements.get("status-lock").textContent, "\uD83D\uDD12");
    assert.equal(elements.get("status-user").textContent, "test.user@contoso.com");
    assert.equal(elements.get("status-database").textContent, "contoso_2");
    assert.equal(elements.get("status-icon").className, "status-icon success");

    await tab.execute(null);
    assert.equal(timers.size, 1);
    events.resultSetAvailable({ resultSetSummary: summary(0, 0, 2) });
    events.resultSetAvailable({ resultSetSummary: summary(1, 0, 3) });
    events.resultSetUpdated({ resultSetSummary: summary(0, 0, 10) });
    events.resultSetUpdated({ resultSetSummary: summary(0, 0, 10) });
    assert.equal(elements.get("status-rows").textContent, "13 rows", "Updates replace rather than double-count rows");
    assert.equal(tab.grids.size, 2, "Result IDs are scoped to their batch");
    setNow(65000);
    timers.values().next().value();
    assert.equal(elements.get("status-elapsed").textContent, "00:01:04");
    events.queryComplete({ batchSummaries: [{ resultSetSummaries: [summary(1, 0, 7)] }] });
    assert.equal(elements.get("status-rows").textContent, "17 rows", "Completion summaries reconcile final totals");
    assert.equal(elements.get("status-text").textContent, "Query completed successfully");
    assert.equal(timers.size, 0);
    setNow(90000);
    app.updateStatusBar();
    assert.equal(elements.get("status-elapsed").textContent, "00:01:04", "Elapsed time freezes on completion");

    await tab.execute(null);
    assert.equal(elements.get("status-rows").textContent, "0 rows");
    assert.equal(elements.get("status-elapsed").textContent, "00:00:00");
    events.queryComplete({ batchSummaries: [] });
});

test("query errors, execution failures, cancellation and connection failures have distinct persistent statuses", async () => {
    const { createTab, client, elements } = setup();
    const { tab, events } = createTab();
    await tab.connect(0);
    await tab.execute(null);
    events.message({ message: { message: "SQL error", isError: true } });
    events.queryComplete({ batchSummaries: [] });
    assert.equal(elements.get("status-text").textContent, "Query completed with errors");
    assert.equal(elements.get("status-icon").className, "status-icon error");

    client.executeDocumentSelection = async () => { throw new Error("Execution rejected"); };
    await tab.execute(null);
    assert.equal(tab.running, false);
    assert.equal(elements.get("status-icon").className, "status-icon error");

    client.executeDocumentSelection = async () => {};
    await tab.execute(null);
    await tab.cancel();
    events.queryComplete({ batchSummaries: [] });
    assert.equal(elements.get("status-text").textContent, "Query cancelled");
    assert.equal(elements.get("status-icon").className, "status-icon warning");

    tab.connected = false;
    client.associateConnection = async () => { throw new Error("Connection rejected"); };
    await assert.rejects(tab.connect(0), /Connection rejected/);
    assert.match(elements.get("status-text").textContent, /Connection failed/);
    assert.equal(elements.get("status-icon").className, "status-icon error");
    assert.equal(elements.get("status-domain").textContent, "");
    assert.equal(elements.get("status-user").textContent, "");
});

test("background tabs do not overwrite the active tab and switching restores its state", async () => {
    const { app, createTab, elements, timers, setNow, grids } = setup();
    const first = createTab();
    await first.tab.connect(0);
    await first.tab.execute(null);
    const second = createTab();
    await second.tab.connect(0);
    first.events.resultSetAvailable({ resultSetSummary: summary(0, 0, 42) });
    assert.equal(elements.get("status-rows").textContent, "0 rows");
    assert.equal(elements.get("status-text").textContent, "Connected");
    assert.equal(timers.size, 0);

    app.activeTab = first.tab;
    app.updateToolbar();
    setNow(4000);
    timers.values().next().value();
    assert.equal(elements.get("status-rows").textContent, "42 rows");
    assert.equal(elements.get("status-elapsed").textContent, "00:00:03");
    grids[0].status("Copying…", false);
    assert.equal(elements.get("status-progress").textContent, "Copying…");
    grids[0].status("Copy failed: Fetch error", true);
    assert.match(elements.get("status-text").textContent, /Copy failed/);
    assert.equal(elements.get("status-icon").className, "status-icon error");
    first.events.queryComplete({ batchSummaries: [] });
    assert.equal(timers.size, 0);
});

test("insecure connections are not shown as secure and disconnect clears connection details", async () => {
    const { registry, client, createTab, elements } = setup();
    registry.get = () => ({ options: { url: "http://onprem.example.test/org", connectionName: "onprem" } });
    client.disconnect = async () => {};
    const { tab } = createTab();
    await tab.connect(0);
    assert.equal(elements.get("status-lock").textContent, "\uD83D\uDD13");
    assert.match(elements.get("status-server").title, /Unencrypted connection/);
    await tab.disconnect();
    assert.equal(elements.get("status-user").textContent, "");
    assert.equal(elements.get("status-database").textContent, "");
    assert.equal(elements.get("status-lock").textContent, "");
    assert.equal(elements.get("status-icon").className, "status-icon warning");
});

test("grid elements no longer contain their own status bars", () => {
    const source = fs.readFileSync(path.resolve(__dirname, "..", "src", "query", "resultsGrid.ts"), "utf8");
    assert.doesNotMatch(source, /grid-status|statusBar/);
    const html = fs.readFileSync(path.resolve(__dirname, "..", "src", "index.html"), "utf8");
    assert.equal((html.match(/class="status-bar"/g) ?? []).length, 1);
});
