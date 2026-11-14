const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test, afterEach } = require("node:test");
const ts = require("typescript");
const { JSDOM } = require("jsdom");
const mobx = require("mobx");
const { observable, runInAction } = mobx;
const dom = new JSDOM("<!doctype html><html><body></body></html>");
global.window = dom.window;
global.document = dom.window.document;
global.Node = dom.window.Node;
const { flushSync } = require("react-dom");
const disposables = [];

afterEach(() => {
    flushSync(() => {
        for (const disposable of disposables.splice(0)) disposable.dispose();
    });
    document.body.replaceChildren();
});

function load(relativePath, dependencies = {}, globals = {}) {
    const filename = path.resolve(__dirname, "..", "src", relativePath);
    const module = { exports: {} };
    const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX },
    }).outputText;
    vm.runInNewContext(code, {
        module, exports: module.exports, document, window, Node, console, URL,
        require: (name) => {
            if (name in dependencies) return dependencies[name];
            if (!name.startsWith(".")) return require(name);
            assert.equal(name, "./statusBarView", `Unexpected dependency: ${name}`);
            return load("statusBarView.tsx");
        },
        ...globals,
    }, { filename });
    return module.exports;
}

function fixture() {
    let now = 1000;
    let timerId = 0;
    const timers = new Map();
    const { StatusBar, StatusBarState } = load("statusBar.ts", {}, {
        setInterval: (callback, interval) => {
            assert.equal(interval, 100);
            timers.set(++timerId, callback);
            return timerId;
        },
        clearInterval: (id) => timers.delete(id),
    });
    const { QueryTab } = load("query\\queryTab.ts", {
        "mobx": mobx,
        "../editor/monacoSetup": {
            monaco: {
                editor: {
                    createModel: (text) => ({
                        setEOL() {}, getValue: () => text, onDidChangeContent: () => ({ dispose() {} }),
                    }),
                    EndOfLineSequence: { LF: 1 },
                    setModelMarkers() {},
                },
                Uri: { parse: (uri) => uri },
            },
        },
        "../connections": { ConnectionRegistry: { ownerUri: (slot) => `connection:${slot}` } },
        "../lsp/protocol": { exportFormats: [] },
        "../executionPlan/executionPlanView": { ExecutionPlanView: class {
            element = document.createElement("div");
            selectedNode = null;
            count = 0;
            clear() {}
        } },
        "../executionPlan/nodeImages": { getNodeImageUrl() {} },
        "./resultsGrid": { ResultsGrid: class {
            element = document.createElement("div");
            setExportEnabled() {}
            setRowCount() {}
            dispose() {}
        } },
        "../mcpInvocation": {},
    }, { Date: { now: () => now } });
    const state = new StatusBarState();
    const slots = observable.map([
        [0, { options: { url: "https://contoso.crm.dynamics.com", connectionName: "Contoso" } }],
        [1, { options: { url: "http://onprem.example.test/org", connectionName: "On-premises" } }],
    ], { deep: false });
    const connections = { get: (slot) => slots.get(slot) ?? null };
    const listeners = new Map();
    const client = {
        addListener: (uri, listener) => listeners.set(uri, listener),
        openDocument: async () => {},
        associateConnection: async (uri) => {
            listeners.get(uri).connectionComplete({ ownerUri: uri, connectionSummary: {
                serverName: "contoso.crm.dynamics.com", userName: "Test User", databaseName: "Contoso DB",
            } });
            return true;
        },
        disposeQuery: async () => {},
        executeDocumentSelection: async () => {},
        disconnect: async () => {},
        cancelQuery: async () => ({}),
    };
    const host = {
        client, resultsToText: false, showMessageLineNumbers: false, includeActualPlan: false,
        getConnectionOptions: (slot) => connections.get(slot)?.options,
        onStateChanged() {}, onSelectedPlanNodeChanged() {},
    };
    const element = document.createElement("div");
    element.className = "status-bar";
    document.body.appendChild(element);
    let statusBar;
    flushSync(() => { statusBar = new StatusBar(element, state, connections); });
    disposables.push(statusBar);
    const change = (callback) => flushSync(() => runInAction(callback));
    const text = (id) => element.querySelector(`#status-${id}`).textContent;
    const send = (tab, name, params) => flushSync(() => listeners.get(tab.uri)[name](params));
    const createTab = (options) => new QueryTab(host, "", options);
    async function connect(tab, slot = 0) {
        await tab.connect(slot);
        flushSync(() => {});
    }
    async function execute(tab) {
        await tab.execute(null);
        flushSync(() => {});
    }
    function tick(time) {
        now = time;
        assert.equal(timers.size, 1);
        flushSync(() => timers.values().next().value());
    }
    return { state, statusBar, slots, client, element, timers, change, text, send, createTab, connect, execute, tick };
}

const summary = (batchId, id, rowCount) => ({ batchId, id, rowCount, columnInfo: [] });

test("React status bar observes startup, connection and execution changes without host status callbacks", async () => {
    const { state, change, text, createTab, connect, execute, element, send } = fixture();
    assert.equal(text("text"), "Loading\u2026");
    change(() => state.setStatus("Starting SQL 4 CDS engine\u2026"));
    assert.equal(text("text"), "Starting SQL 4 CDS engine\u2026");
    const tab = createTab();
    change(() => { state.ready = true; state.activeTab = tab; });
    assert.equal(text("text"), "Not connected");
    await connect(tab);
    assert.equal(text("domain"), "contoso.crm.dynamics.com");
    assert.equal(text("lock"), "\uD83D\uDD12");
    assert.equal(text("user"), "Test User");
    assert.equal(text("database"), "Contoso DB");
    assert.equal(element.querySelector("#status-user").title, "Connected user: Test User");
    await execute(tab);
    assert.equal(text("text"), "Executing query\u2026");
    change(() => { state.progressMessage = "Retrieving rows"; });
    assert.equal(text("progress"), "Retrieving rows");
    send(tab, "resultSetAvailable", { resultSetSummary: summary(0, 0, 4) });
    send(tab, "resultSetAvailable", { resultSetSummary: summary(1, 0, 3) });
    send(tab, "resultSetUpdated", { resultSetSummary: summary(0, 0, 10) });
    assert.equal(text("rows"), "13 rows");
    send(tab, "message", { message: { message: "Text results", isError: false, rowCount: 2 } });
    assert.equal(text("rows"), "15 rows");
    send(tab, "queryComplete", { batchSummaries: [] });
    assert.equal(text("text"), "Query completed successfully");
    assert.equal(text("progress"), "");
    assert.equal(element.querySelector("#status-message").getAttribute("role"), "status");
    assert.equal(element.querySelector("#status-icon").className, "status-icon success");
});

test("tab switching automatically restores each query's status, rows and timer without toolbar updates", async () => {
    const { state, change, text, createTab, connect, execute, send, timers, tick } = fixture();
    const first = createTab();
    const second = createTab();
    change(() => { state.ready = true; state.activeTab = first; });
    await connect(first);
    await execute(first);
    change(() => { state.activeTab = second; });
    await connect(second, 1);
    send(first, "resultSetAvailable", { resultSetSummary: summary(0, 0, 42) });
    assert.equal(text("rows"), "0 rows");
    assert.equal(text("text"), "Connected");
    assert.equal(text("lock"), "\uD83D\uDD13");
    assert.equal(timers.size, 0);
    change(() => { state.activeTab = first; });
    assert.equal(text("rows"), "42 rows");
    tick(5000);
    assert.equal(text("elapsed"), "00:00:04");
    change(() => { state.activeTab = null; });
    assert.equal(text("rows"), "0 rows");
    assert.equal(text("domain"), "");
    assert.equal(text("elapsed"), "00:00:00");
    assert.equal(timers.size, 0);
});

test("elapsed time ticks only for a running foreground query and freezes at its final duration", async () => {
    const { state, change, text, createTab, connect, execute, send, timers, tick } = fixture();
    const tab = createTab();
    change(() => { state.ready = true; state.activeTab = tab; });
    await connect(tab);
    await execute(tab);
    tick(65000);
    assert.equal(text("elapsed"), "00:01:04");
    send(tab, "queryComplete", { batchSummaries: [] });
    assert.equal(timers.size, 0);
    change(() => { state.progressMessage = "Unrelated progress"; });
    assert.equal(text("elapsed"), "00:01:04");
    await execute(tab);
    assert.equal(text("elapsed"), "00:00:00");
    assert.equal(timers.size, 1);
    change(() => { state.ready = false; state.setStatus("Engine failed", "error"); });
    assert.equal(timers.size, 0);
    assert.equal(text("text"), "Engine failed");
});

test("grid errors take precedence over progress and connection changes update tooltips automatically", async () => {
    const { state, change, text, createTab, connect, execute, slots, element } = fixture();
    const tab = createTab();
    change(() => { state.ready = true; state.activeTab = tab; });
    await connect(tab);
    await execute(tab);
    change(() => {
        state.progressMessage = "Retrieving rows";
        tab.gridStatus = "Copy failed";
        tab.gridStatusIsError = true;
    });
    assert.equal(text("text"), "Copy failed");
    assert.equal(text("progress"), "Retrieving rows");
    assert.equal(element.querySelector("#status-icon").className, "status-icon error");
    change(() => {
        tab.gridStatus = "Copying";
        tab.gridStatusIsError = false;
        tab.connectionSummary = undefined;
        slots.set(0, { options: { url: "http://new.example.test/org", connectionName: "New database" } });
    });
    assert.equal(text("progress"), "Copying | Retrieving rows");
    assert.equal(text("domain"), "new.example.test");
    assert.equal(text("database"), "New database");
    assert.equal(text("user"), "");
    assert.match(element.querySelector("#status-server").title, /Unencrypted connection/);
    change(() => { tab.connected = false; });
    assert.equal(text("lock"), "");
    assert.equal(text("database"), "");
    assert.equal(element.querySelector("#status-server").title, "Not connected");
});

test("disposal stops the timer and reactions, unmounts React and ignores later state changes", async () => {
    const { state, statusBar, change, element, createTab, connect, execute, timers } = fixture();
    const tab = createTab();
    change(() => { state.ready = true; state.activeTab = tab; });
    await connect(tab);
    await execute(tab);
    assert.equal(timers.size, 1);
    flushSync(() => statusBar.dispose());
    assert.equal(timers.size, 0);
    assert.equal(element.childElementCount, 0);
    change(() => { tab.running = false; });
    change(() => { tab.running = true; state.progressMessage = "After disposal"; });
    assert.equal(timers.size, 0);
    assert.equal(element.childElementCount, 0);
});
