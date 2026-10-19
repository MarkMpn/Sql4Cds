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
        this.attributes = {};
        this.events = {};
        this.classes = new Set();
        this.classList = { toggle: (name, value) => value ? this.classes.add(name) : this.classes.delete(name) };
    }
    append(...children) { this.children.push(...children); }
    appendChild(child) { this.children.push(child); }
    setAttribute(name, value) { this.attributes[name] = value; }
    contains() { return false; }
    replaceChildren(...children) { this.children = children; }
    addEventListener(name, handler) { this.events[name] = handler; }
    focus() { this.focused = true; }
    querySelector() { return this.children[0]; }
    closest() { return null; }
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
        URL, Error, console, ...globals,
    }, { filename });
    return module.exports;
}

function setup(pinned = true) {
    let now = 1000;
    const elements = new Map();
    const document = {
        events: {},
        addEventListener(name, handler) { this.events[name] = handler; },
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
    const savedSettings = [];
    const settingsApi = {
        saveSettings: async (settings) => { savedSettings.push({ ...settings }); },
    };
    const window = { addEventListener() {} };
    const { App } = load("app.ts", {
        "./styles.css": {},
        "./connections": { ConnectionRegistry: class { constructor() { return registry; } } },
        "./editor/monacoSetup": {},
        "./lsp/client": { Sql4CdsClient: class { constructor() { return client; } } },
        "./lsp/protocol": {},
        "./query/queryTab": {},
        "./objectExplorer": { ObjectExplorer: class {} },
        "./settings": settingsApi,
    }, {
        document, window, Node: Element, Element,
        setInterval: (callback) => { timers.set(++nextTimer, callback); return nextTimer; },
        clearInterval: (id) => timers.delete(id),
    }, "\nexports.App = App;");
    const app = new App();
    app.settings = { objectExplorerPinned: pinned, batchSize: 100 };
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
    return { app, client, registry, createTab, elements, timers, grids, document, savedSettings, settingsApi, window, setNow: (value) => { now = value; } };
}

function summary(batchId, id, rowCount) {
    return { batchId, id, rowCount, columnInfo: [], complete: false };
}

test("connection dropdown shows only the registered Object Explorer name", () => {
    const { app, registry, elements } = setup();
    registry.all = [
        { slot: 0, connection: { name: "Old alternate", url: "https://org.crm.dynamics.com" }, options: { connectionName: "Contoso Dev" } },
        { slot: 1, connection: { name: "Old alternate", url: "https://org2.crm.dynamics.com" }, options: { connectionName: "Contoso Dev_2" }, error: "Connection failed" },
    ];
    Object.getPrototypeOf(app).renderConnectionList.call(app);
    const options = elements.get("connection-select").children;
    assert.equal(options[1].textContent, "Contoso Dev");
    assert.equal(options[2].textContent, "Contoso Dev_2 ⚠");
    assert.equal(options[2].title, "Connection failed");
});

test("Object Explorer pin collapses to a side tab, opens as a flyout and pins back without losing width or tree", async () => {
    const { app, elements, document } = setup();
    app.bindObjectExplorer();
    const pane = elements.get("object-explorer");
    const pin = elements.get("explorer-pin");
    const tab = elements.get("explorer-tab");
    const splitter = elements.get("explorer-splitter");
    const tree = elements.get("explorer-tree");
    const item = new Element();
    tree.appendChild(item);
    pane.style = { width: "320px" };
    await pin.events.click();
    assert.equal(pane.hidden, true);
    assert.equal(splitter.hidden, true);
    assert.equal(tab.hidden, false);
    assert.equal(tab.focused, true);
    assert.equal(pin.attributes["aria-label"], "Pin Object Explorer");
    tab.events.click();
    assert.equal(pane.hidden, false);
    assert.equal(pane.classes.has("auto-hide"), true);
    assert.equal(tab.hidden, false, "The side tab keeps its layout space while the flyout covers it");
    assert.equal(tab.attributes["aria-expanded"], "true");
    assert.equal(item.focused, true);
    const menuItem = new Element();
    menuItem.closest = () => new Element();
    document.events.focusin({ target: menuItem });
    assert.equal(pane.hidden, false, "Object Explorer context menu does not collapse the flyout");
    document.events.pointerdown({ target: new Element() });
    assert.equal(pane.hidden, true);
    assert.equal(tab.hidden, false, "The side tab returns when the flyout collapses");
    tab.events.click();
    document.events.keydown({ key: "Escape", preventDefault() {} });
    assert.equal(pane.hidden, true);
    assert.equal(tab.hidden, false);
    tab.events.click();
    document.events.focusin({ target: new Element() });
    assert.equal(pane.hidden, true);
    tab.events.click();
    await pin.events.click();
    assert.equal(pane.hidden, false);
    assert.equal(pane.classes.has("auto-hide"), false);
    assert.equal(splitter.hidden, false);
    assert.equal(tab.hidden, true);
    assert.equal(pin.attributes["aria-label"], "Unpin Object Explorer");
    document.events.pointerdown({ target: new Element() });
    assert.equal(pane.hidden, false, "Pinned pane remains docked");
    assert.equal(pane.style.width, "320px");
    assert.equal(tree.children[0], item);
});

test("pin state is saved with other user settings and an unpinned launch stays collapsed until opened", async () => {
    const { app, elements, savedSettings } = setup(false);
    app.bindObjectExplorer();
    const pane = elements.get("object-explorer");
    const tab = elements.get("explorer-tab");
    const pin = elements.get("explorer-pin");
    assert.equal(pane.hidden, true);
    assert.equal(tab.hidden, false);
    assert.equal(savedSettings.length, 0, "Restoring settings does not write them again");
    tab.events.click();
    assert.equal(pane.hidden, false);
    assert.equal(savedSettings.length, 0, "Opening the flyout does not change the pin preference");
    await pin.events.click();
    assert.deepEqual(savedSettings[0], { objectExplorerPinned: true, batchSize: 100 });
    await pin.events.click();
    assert.deepEqual(savedSettings[1], { objectExplorerPinned: false, batchSize: 100 });
});

test("pin setting save errors are notified and the pin is re-enabled", async () => {
    const { app, elements, settingsApi, window } = setup();
    const notifications = [];
    window.toolboxAPI = { utils: { showNotification: async (message) => notifications.push(message) } };
    settingsApi.saveSettings = async () => { throw new Error("Settings storage unavailable"); };
    app.bindObjectExplorer();
    await elements.get("explorer-pin").events.click();
    assert.equal(notifications[0].title, "Unable to save settings");
    assert.equal(notifications[0].body, "Settings storage unavailable");
    assert.equal(notifications[0].type, "error");
    assert.equal(elements.get("explorer-pin").disabled, false);
});

test("stored pin preference loads from the user settings API, with pinned as the default for older settings", async () => {
    let stored;
    let saved;
    const { loadSettings, saveSettings } = load("settings.ts", {}, {
        window: { toolboxAPI: { settings: {
            get: async (key) => { assert.equal(key, "settings"); return stored; },
            set: async (key, value) => { assert.equal(key, "settings"); saved = value; },
        } } },
    });
    assert.equal((await loadSettings()).objectExplorerPinned, true);
    stored = { batchSize: 250 };
    assert.equal((await loadSettings()).objectExplorerPinned, true);
    stored = { ...stored, objectExplorerPinned: false };
    const settings = await loadSettings();
    assert.equal(settings.objectExplorerPinned, false);
    assert.equal(settings.batchSize, 250);
    await saveSettings(settings);
    assert.equal(saved.objectExplorerPinned, false);
    stored = { objectExplorerPinned: "false" };
    assert.equal((await loadSettings()).objectExplorerPinned, true);
});

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
