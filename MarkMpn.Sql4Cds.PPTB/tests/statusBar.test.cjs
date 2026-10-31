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
    querySelectorAll() { return this.children.filter(child => !child.disabled); }
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
    const logs = [];
    let now = 1000;
    const elements = new Map();
    const document = {
        events: {},
        addEventListener(name, handler) { this.events[name] = handler; },
        createElement: () => new Element(),
        createTextNode: (text) => ({ textContent: text }),
        getElementById: (id) => {
            if (!elements.has(id)) elements.set(id, new Element());
            return elements.get(id);
        },
    };
    const listeners = new Map();
    const executions = [];
    let clientEvents;
    const client = {
        addListener: (uri, listener) => listeners.set(uri, listener),
        openDocument: async () => {},
        changeDocument: async () => {},
        disposeQuery: async () => {},
        executeDocumentSelection: async (uri, selection, planOptions) => { executions.push(planOptions); },
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
    const modelLanguages = [];
    const savedSettings = [];
    const settingsApi = {
        saveSettings: async (settings) => { savedSettings.push({ ...settings }); },
    };
    const window = { addEventListener() {} };
    const pinnablePane = load("pinnablePane.ts", {}, { document, window, Node: Element, Element });
    const propertiesPanels = [];
    const { App } = load("app.ts", {
        "./styles.css": {},
        "./connections": { ConnectionRegistry: class { constructor() { return registry; } } },
        "./editor/monacoSetup": { monaco: { Range: class {
            constructor(startLineNumber, startColumn, endLineNumber, endColumn) {
                Object.assign(this, { startLineNumber, startColumn, endLineNumber, endColumn });
            }
        } } },
        "./lsp/client": { Sql4CdsClient: class { constructor(events) { clientEvents = events; return client; } } },
        "./lsp/protocol": {},
        "./query/queryTab": {},
        "./objectExplorer": { ObjectExplorer: class { engineStopped() { this.stopped = true; } } },
        "./pinnablePane": pinnablePane,
        "./propertiesPanel": { PropertiesPanel: class {
            constructor() { this.shown = []; propertiesPanels.push(this); }
            show(name, properties) { this.shown.push({ name, properties }); }
        } },
        "./executionPlan/executionPlanView": { getFetchXmlQuery: (node) =>
            node.nodeType === "FetchXmlScan" ? node.properties?.find(property => property.name === "FetchXML")?.value ?? null : null },
        "./settings": settingsApi,
    }, {
        document, window, Node: Element, Element,
        console: {
            error: (message) => logs.push({ type: "error", message }),
            warn: (message) => logs.push({ type: "warning", message }),
            log: (message) => logs.push({ type: "info", message }),
        },
        setInterval: (callback) => { timers.set(++nextTimer, callback); return nextTimer; },
        clearInterval: (id) => timers.delete(id),
    }, "\nexports.App = App;");
    const app = new App();
    app.settings = { objectExplorerPinned: pinned, batchSize: 100, includeActualPlan: false };
    app.ready = true;
    app.renderTabs = () => {};
    app.renderConnectionList = () => {};
    const grids = [];
    const planViews = [];
    const { QueryTab } = load("query/queryTab.ts", {
        "../lsp/protocol": load("lsp/protocol.ts", { "../resultsProtocol": load("resultsProtocol.ts", {}) }),
        "../editor/monacoSetup": {
            monaco: {
                Range: class {
                    constructor(startLineNumber, startColumn, endLineNumber, endColumn) {
                        Object.assign(this, { startLineNumber, startColumn, endLineNumber, endColumn });
                    }
                },
                editor: {
                    createModel: (initialText, language) => {
                        modelLanguages.push(language);
                        let value = initialText;
                        let version = 1;
                        let onChange;
                        return {
                            setEOL() {},
                            getValue: () => value,
                            getLineCount: () => value.split("\n").length,
                            getLineMaxColumn: (line) => value.split("\n")[line - 1].length + 1,
                            onDidChangeContent: (callback) => { onChange = callback; return { dispose() {} }; },
                            setValue: (text) => { value = text; version++; onChange?.(); },
                            getVersionId: () => version,
                            getOptions: () => ({ tabSize: 4, insertSpaces: true }),
                            isDisposed: () => false,
                            getFullModelRange: () => ({ fullModel: true }),
                            getValueInRange: () => value,
                            pushStackElement() {},
                            pushEditOperations: (_selection, edits) => { value = edits[0].text; version++; onChange?.(); },
                        };
                    },
                    EndOfLineSequence: { LF: 1 },
                    setModelMarkers() {},
                },
                Uri: { parse: (uri) => uri },
            },
        },
        "../connections": { ConnectionRegistry: { ownerUri: (slot) => `connection:${slot}` } },
        "../executionPlan/executionPlanView": { ExecutionPlanView: class {
            constructor(getImageUrl, onSelectionChanged) {
                this.element = new Element();
                this.plans = [];
                this.selectedNode = null;
                this.onSelectionChanged = onSelectionChanged;
                planViews.push(this);
            }
            get count() { return this.plans.length; }
            addPlan(plan) { this.plans.push(plan); }
            clear() { this.plans = []; this.selectedNode = null; }
            select(node) { this.selectedNode = node; this.onSelectionChanged(node); }
        } },
        "../executionPlan/nodeImages": { getNodeImageUrl: () => "" },
        "./resultsGrid": { ResultsGrid: class {
            constructor(_columns, _fetch, _copy, _open, status, onExport, recordActions) {
                this.element = new Element();
                this.status = status;
                this.onExport = onExport;
                this.recordActions = recordActions;
                grids.push(this);
            }
            setExportEnabled(enabled) { this.exportEnabled = enabled; }
            setRowCount() {}
            dispose() {}
        } },
    }, { document, window, Date: { now: () => now } });
    function createTab(options) {
        const tab = new QueryTab(app, "", options);
        app.activeTab = tab;
        return { tab, events: listeners.get(tab.uri) };
    }
    return { app, client, clientEvents, registry, createTab, elements, timers, grids, planViews, propertiesPanels, executions, document, savedSettings, settingsApi, window, modelLanguages, listeners, logs, setNow: (value) => { now = value; } };
}

test("server warnings and errors are logged and shown as PPTB notifications", async () => {
    const { clientEvents, window, logs } = setup();
    const notifications = [];
    window.toolboxAPI = { utils: { showNotification: async (notification) => notifications.push(notification) } };
    for (const type of [1, 2, 3, 4]) clientEvents.log({ type, message: `message ${type}` });
    await Promise.resolve();
    assert.deepEqual(notifications.map(notification => ({ ...notification })), [
        { title: "SQL 4 CDS", body: "message 1", type: "error" },
        { title: "SQL 4 CDS", body: "message 2", type: "warning" },
    ]);
    assert.deepEqual(logs, [
        { type: "error", message: "message 1" }, { type: "warning", message: "message 2" },
        { type: "info", message: "message 3" }, { type: "info", message: "message 4" },
    ]);
});

test("Messages tab line numbers follow the showMessageLineNumbers preference and default to off", async () => {
    const { app, createTab } = setup();
    app.settings.showMessageLineNumbers = false;
    const { tab: tabOff } = createTab();
    const paneOff = tabOff.resultsElement.children.find(child => child.className === "messages-pane");
    assert.ok(paneOff);
    assert.equal(paneOff.classes.has("show-line-numbers"), false);

    app.settings.showMessageLineNumbers = true;
    const { tab: tabOn } = createTab();
    const paneOn = tabOn.resultsElement.children.find(child => child.className === "messages-pane");
    assert.equal(paneOn.classes.has("show-line-numbers"), true);
});

function summary(batchId, id, rowCount) {
    return { batchId, id, rowCount, columnInfo: [], complete: false };
}

test("results area stays hidden for new tabs until the active tab runs a query", async () => {
    const { app, createTab, elements } = setup();
    const first = createTab();
    const second = new (first.tab.constructor)(app, "");
    app.tabs.push(first.tab, second);
    app.editor = {
        saveViewState: () => null,
        restoreViewState: () => {},
        setModel: () => {},
        updateOptions: () => {},
        focus: () => {},
    };
    app.activeTab = undefined;
    app.activateTab(first.tab);
    assert.equal(elements.get("results-host").hidden, true);
    assert.equal(elements.get("splitter").hidden, true);
    first.tab.connected = true;
    await first.tab.execute(null);
    assert.equal(elements.get("results-host").hidden, false);
    assert.equal(elements.get("splitter").hidden, false);
    app.activateTab(second);
    assert.equal(elements.get("results-host").hidden, true);
    app.activateTab(first.tab);
    assert.equal(elements.get("results-host").hidden, false);
});

test("fatal worker failure stops all tabs, preserves editors and shows a restart notification", async () => {
    const { app, clientEvents, createTab, grids, elements, timers, window, setNow } = setup();
    const notifications = [];
    window.toolboxAPI = {
        preventClose: async () => {},
        releasePreventClose: async () => {},
        utils: { showNotification: async notification => notifications.push(notification) },
    };
    const first = createTab();
    const second = createTab();
    const idle = createTab();
    app.tabs.push(first.tab, second.tab, idle.tab);
    for (const { tab, events } of [first, second]) {
        tab.connected = true;
        tab.model.setValue("SELECT 1; -- unsaved");
        await tab.execute(null);
        events.resultSetAvailable({ resultSetSummary: summary(0, 0, 1) });
    }
    app.activeTab = first.tab;
    app.updateStatusBar();
    assert.equal(timers.size, 1);
    app.progressMessage = "Retrieving...";
    setNow(2500);
    const message = "SQL 4 CDS engine failed: WORKER_FAILED. Close and reopen the SQL 4 CDS tool to restart the engine.";
    clientEvents.failed(message);
    assert.equal(app.ready, false);
    assert.equal(timers.size, 0);
    assert.equal(elements.get("btn-run").disabled, true);
    assert.equal(elements.get("btn-cancel").disabled, true);
    assert.equal(elements.get("status-text").textContent, message);
    assert.equal(elements.get("status-progress").textContent, "");
    for (const { tab } of [first, second, idle]) {
        assert.equal(tab.running, false);
        assert.equal(tab.connected, false);
        assert.equal(tab.statusKind, "error");
        assert.equal(tab.statusMessage, message);
        assert.equal(tab.view, "messages");
        assert.equal(tab.hasResults, true);
        }
    assert.equal(first.tab.model.getValue(), "SELECT 1; -- unsaved");
    assert.equal(first.tab.elapsedMilliseconds, 1500);
    assert.ok(grids.every(grid => grid.exportEnabled === false));
    first.events.queryComplete({ batchSummaries: [] });
    assert.equal(first.tab.statusMessage, message);
    await assert.rejects(first.tab.execute(null), /restart the engine/);
    assert.equal(notifications.length, 1);
    assert.equal(notifications[0].type, "error");
    assert.match(notifications[0].body, /restart the engine/);
});

test("worker failure releases a tab waiting for connection completion", async () => {
    const { client, createTab } = setup();
    client.associateConnection = async () => true;
    const { tab } = createTab();
    const connecting = tab.connect(0);
    await Promise.resolve();
    tab.engineStopped("Engine failed. Restart required.");
    await assert.rejects(connecting, /Restart required/);
    assert.equal(tab.connecting, false);
    assert.equal(tab.connected, false);
    assert.equal(tab.statusKind, "error");
    assert.equal(tab.statusMessage, "Engine failed. Restart required.");
});

test("record menu wiring copies the shared URL and appends SELECT SQL at the latest document end as an undoable edit", async () => {
    const { app, registry, createTab, grids, window } = setup();
    const reference = { dataSource: "Other", logicalName: "email", id: "123", isNull: false };
    const sql = "SELECT * FROM [Other].dbo.[email] WHERE [activityid] = '123';";
    const copied = [];
    const requests = [];
    window.toolboxAPI = { utils: { copyToClipboard: async text => copied.push(text) } };
    registry.getRecordUrl = ref => { assert.equal(ref, reference); return "https://org/main.aspx?id=123"; };
    let resolveSql;
    registry.getRecordSelectStatement = (ref, source) => {
        requests.push([ref, source]);
        return new Promise(resolve => { resolveSql = resolve; });
    };
    const { tab, events } = createTab();
    tab.connectionSlot = 0;
    events.resultSetAvailable({ resultSetSummary: summary(0, 0, 1) });
    grids[0].recordActions.copyUrl(reference);
    await new Promise(resolve => setImmediate(resolve));
    assert.deepEqual(copied, ["https://org/main.aspx?id=123"]);
    tab.model.setValue("SELECT 1;");
    let undoStops = 0;
    const edits = [];
    tab.model.pushStackElement = () => undoStops++;
    tab.model.pushEditOperations = (_selection, operations) => {
        edits.push(operations[0]);
        tab.model.setValue(tab.model.getValue() + operations[0].text);
    };
    grids[0].recordActions.createSelect(reference);
    tab.model.setValue("SELECT 1;\n-- added while metadata loads");
    resolveSql(sql);
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(requests[0][0], reference);
    assert.equal(requests[0][1], "contoso_2");
    assert.equal(undoStops, 2);
    assert.equal(edits.length, 1);
    assert.equal(edits[0].range.startLineNumber, 2);
    assert.equal(edits[0].range.startColumn, 30);
    assert.equal(edits[0].range.endLineNumber, 2);
    assert.equal(edits[0].range.endColumn, 30);
    assert.equal(tab.model.getValue(), `SELECT 1;\n-- added while metadata loads\n\n${sql}`);
});

test("record SELECT failures notify the user without changing the editor", async () => {
    const { registry, createTab, window } = setup();
    const notifications = [];
    window.toolboxAPI = { utils: { showNotification: async message => notifications.push(message) } };
    registry.getRecordSelectStatement = async () => { throw new Error("Metadata denied"); };
    const { tab } = createTab();
    tab.model.setValue("SELECT 1;");
    await tab.appendRecordSelectStatement({ dataSource: "Other", logicalName: "account", id: "123", isNull: false });
    assert.equal(tab.model.getValue(), "SELECT 1;");
    assert.equal(tab.gridStatusIsError, true);
    assert.match(tab.gridStatus, /Metadata denied/);
    assert.equal(notifications[0].type, "error");
});

test("record SELECT insertion handles empty, disposed, and read-only editors", async () => {
    const { registry, createTab, window } = setup();
    const notifications = [];
    window.toolboxAPI = { utils: { showNotification: async message => notifications.push(message) } };
    const sql = "SELECT * FROM [account] WHERE [accountid] = '123';";
    registry.getRecordSelectStatement = async () => sql;
    const reference = { dataSource: "contoso_2", logicalName: "account", id: "123", isNull: false };
    const { tab } = createTab();
    await tab.appendRecordSelectStatement(reference);
    assert.equal(tab.model.getValue(), sql, "Empty editors do not receive leading blank lines");
    tab.model.isDisposed = () => true;
    await tab.appendRecordSelectStatement(reference);
    assert.equal(tab.model.getValue(), sql);
    const { tab: readOnly } = createTab({ readOnly: true });
    await readOnly.appendRecordSelectStatement(reference);
    assert.equal(readOnly.model.getValue(), "");
    assert.match(notifications[0].body, /read-only/);
});

test("record URL copying reports missing connections and clipboard failures", async () => {
    const { app, registry, window } = setup();
    const notifications = [];
    window.toolboxAPI = { utils: {
        showNotification: async message => notifications.push(message),
        copyToClipboard: async () => { throw new Error("Clipboard denied"); },
    } };
    registry.getRecordUrl = () => { throw new Error("Missing connection"); };
    app.copyRecordUrl({});
    await new Promise(resolve => setImmediate(resolve));
    assert.match(notifications[0].body, /Missing connection/);
    registry.getRecordUrl = () => "https://org/main.aspx";
    app.copyRecordUrl({});
    await new Promise(resolve => setImmediate(resolve));
    assert.match(notifications[1].body, /Clipboard denied/);
});

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
    assert.deepEqual(savedSettings[0], { objectExplorerPinned: true, batchSize: 100, includeActualPlan: false });
    await pin.events.click();
    assert.deepEqual(savedSettings[1], { objectExplorerPinned: false, batchSize: 100, includeActualPlan: false });
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
    assert.equal(tab.messagesPane.children.length, 0, "Completion status is shown in the status bar rather than as a message");
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

function planSummary(id, rowCount = 0) {
    return { ...summary(0, id, rowCount), specialAction: { expectYukonXMLShowPlan: true } };
}

const samplePlan = { query: "SELECT name FROM account", root: { name: "SELECT", type: "result", properties: [{ name: "Node Type", value: "SelectNode", displayOrder: 0, dataType: 1 }], children: [], edges: [] } };

test("estimated plans are requested without running the query and are shown in the Execution Plan tab", async () => {
    const { createTab, executions, planViews, grids, elements } = setup();
    const { tab, events } = createTab();
    await tab.connect(0);
    const planButton = tab.resultsTabButtons.get("plan");
    assert.equal(planButton.hidden, true, "Execution Plan tab is hidden until there is a plan");

    await tab.execute(null, true);
    assert.deepEqual({ ...executions.at(-1) }, { includeEstimatedExecutionPlanXml: true, includeActualExecutionPlanXml: false });
    assert.equal(elements.get("status-text").textContent, "Generating estimated execution plan…");
    events.resultSetAvailable({ resultSetSummary: planSummary(0) });
    events.resultSetUpdated({ resultSetSummary: planSummary(0), executionPlans: [samplePlan] });
    events.resultSetComplete({ resultSetSummary: { ...planSummary(0), complete: true } });
    events.queryComplete({ batchSummaries: [] });

    assert.equal(grids.length, 0, "Plan result sets do not create a grid");
    assert.equal(elements.get("status-rows").textContent, "0 rows");
    assert.deepEqual(planViews[0].plans, [samplePlan]);
    assert.equal(planButton.hidden, false);
    assert.equal(tab.showingPlan, true);
    assert.equal(planButton.attributes["aria-pressed"], "true");
    assert.equal(elements.get("status-text").textContent, "Estimated execution plan generated");

    await tab.execute(null, true);
    assert.equal(planViews[0].plans.length, 0, "Previous plans are cleared");
    assert.equal(planButton.hidden, true);
    assert.equal(tab.showingPlan, false);
    events.queryComplete({ batchSummaries: [] });
});

test("actual plans are only requested when enabled and are shown alongside the results without hiding them", async () => {
    const { app, createTab, executions, planViews, grids, elements } = setup();
    const { tab, events } = createTab();
    await tab.connect(0);
    await tab.execute(null);
    assert.deepEqual({ ...executions.at(-1) }, { includeEstimatedExecutionPlanXml: false, includeActualExecutionPlanXml: false });
    events.queryComplete({ batchSummaries: [] });

    app.settings.includeActualPlan = true;
    await tab.execute(null);
    assert.deepEqual({ ...executions.at(-1) }, { includeEstimatedExecutionPlanXml: false, includeActualExecutionPlanXml: true });
    events.resultSetAvailable({ resultSetSummary: summary(0, 0, 5) });
    events.resultSetAvailable({ resultSetSummary: planSummary(1) });
    // Later updates for the plan result set are still recognised even without the special action
    events.resultSetUpdated({ resultSetSummary: summary(0, 1, 0), executionPlans: [samplePlan] });
    events.queryComplete({ batchSummaries: [] });

    assert.equal(grids.length, 1);
    assert.equal(elements.get("status-rows").textContent, "5 rows");
    assert.equal(planViews[0].plans.length, 1);
    assert.equal(tab.resultsTabButtons.get("plan").hidden, false);
    assert.equal(tab.showingPlan, false, "Results stay visible when there are grids");

    await tab.execute(null, true);
    assert.equal(executions.at(-1).includeActualExecutionPlanXml, false, "Estimated plans never execute the query");
    events.queryComplete({ batchSummaries: [] });
});

for (const mode of ["grid", "text"]) {
    for (const [name, estimated, hasResult, error, expected] of [
        ["actual plan without results", false, false, false, "messages"],
        ["actual plan with results", false, true, false, "results"],
        ["actual plan with results and error", false, true, true, "messages"],
        ["actual plan without results and error", false, false, true, "messages"],
        ["estimated plan", true, false, false, "plan"],
        ["estimated plan with error", true, false, true, "messages"],
    ]) {
        test(`${name} defaults to ${expected} in ${mode} mode`, async () => {
            const { app, createTab } = setup();
            app.settings.includeActualPlan = true;
            const { tab, events } = createTab();
            await tab.connect(0);
            tab.resultsMode = mode;
            await tab.execute(null, estimated);
            assert.equal(tab.resultsTabButtons.get("results").hidden, true, "Results tab is hidden until a result set arrives");
            assert.equal(tab.view, "messages");
            if (hasResult) events.resultSetAvailable({ resultSetSummary: summary(0, 0, 0) });
            const plan = planSummary(hasResult ? 1 : 0);
            events.resultSetAvailable({ resultSetSummary: plan });
            events.resultSetUpdated({ resultSetSummary: plan, executionPlans: [samplePlan] });
            if (error) events.message({ message: { message: "Query failed", isError: true } });
            if (!estimated && hasResult) tab.showResultsView("plan");
            events.queryComplete({ batchSummaries: [] });
            assert.equal(tab.view, expected);
            assert.equal(tab.resultsTabButtons.get("results").hidden, !hasResult);
            assert.equal(tab.resultsTabButtons.get("plan").hidden, false, "Actual plans remain available for manual selection");
        });
    }
}

test("the selected plan node is shown in the Properties panel only while the active tab shows its plan", async () => {
    const { app, createTab, planViews, propertiesPanels } = setup();
    app.bindProperties();
    const panel = propertiesPanels[0];
    assert.deepEqual(panel.shown.at(-1), { name: null, properties: null });

    const { tab, events } = createTab();
    await tab.connect(0);
    await tab.execute(null, true);
    events.resultSetAvailable({ resultSetSummary: planSummary(0) });
    events.resultSetUpdated({ resultSetSummary: planSummary(0), executionPlans: [samplePlan] });
    events.queryComplete({ batchSummaries: [] });

    planViews[0].select({ ...samplePlan.root, name: "SELECT\r\naccount" });
    assert.equal(panel.shown.at(-1).name, "SELECT");
    assert.equal(panel.shown.at(-1).properties, samplePlan.root.properties);

    tab.resultsTabButtons.get("messages").events.click();
    assert.deepEqual(panel.shown.at(-1), { name: null, properties: null });
    tab.resultsTabButtons.get("plan").events.click();
    assert.equal(panel.shown.at(-1).name, "SELECT");

    const other = createTab();
    app.activeTab = tab;
    planViews[1].select(samplePlan.root);
    assert.equal(panel.shown.at(-1).name, "SELECT", "Background tabs don't update the panel");
});

test("Properties pane pins like Object Explorer and saves its own setting", async () => {
    const { app, elements, savedSettings } = setup();
    app.settings = { ...app.settings, propertiesPinned: false };
    app.bindProperties();
    const pane = elements.get("properties");
    const tab = elements.get("properties-tab");
    const pin = elements.get("properties-pin");
    assert.equal(pane.hidden, true);
    assert.equal(tab.hidden, false);
    tab.events.click();
    assert.equal(pane.hidden, false);
    assert.equal(pane.classes.has("auto-hide"), true);
    await pin.events.click();
    assert.equal(pane.classes.has("auto-hide"), false);
    assert.deepEqual(savedSettings[0], { objectExplorerPinned: true, batchSize: 100, includeActualPlan: false, propertiesPinned: true });
    assert.equal(pin.attributes["aria-label"], "Unpin Properties");
});

test("the Properties pin preference defaults to unpinned", async () => {
    let stored;
    const { loadSettings } = load("settings.ts", {}, {
        window: { toolboxAPI: { settings: { get: async () => stored, set: async () => {} } } },
    });
    assert.equal((await loadSettings()).propertiesPinned, false);
    stored = { propertiesPinned: true };
    assert.equal((await loadSettings()).propertiesPinned, true);
    stored = { propertiesPinned: "yes" };
    assert.equal((await loadSettings()).propertiesPinned, false);
});

test("Include Actual Plan is saved in user settings and applies to every query tab", async () => {
    const { app, createTab, elements, executions, savedSettings } = setup();
    app.bindToolbar();
    const button = elements.get("btn-actual-plan");
    app.updateToolbar();
    assert.equal(button.attributes["aria-pressed"], "false");

    await button.events.click();
    assert.equal(button.attributes["aria-pressed"], "true");
    assert.deepEqual(savedSettings.at(-1), { objectExplorerPinned: true, batchSize: 100, includeActualPlan: true });

    const { tab } = createTab();
    await tab.connect(0);
    await tab.execute(null);
    assert.equal(executions.at(-1).includeActualExecutionPlanXml, true);

    await button.events.click();
    assert.equal(button.attributes["aria-pressed"], "false");
    assert.equal(savedSettings.at(-1).includeActualPlan, false);
});

test("query tabs track dirty state, saved content, undo-to-saved text, and file names", () => {
    const { app, createTab } = setup();
    app.onStateChanged = () => {};
    const { tab } = createTab();

    assert.equal(tab.isDirty, false);
    tab.model.setValue("SELECT 1");
    assert.equal(tab.isDirty, true);
    tab.markSaved("SELECT 1");
    assert.equal(tab.isDirty, false);
    tab.model.setValue("SELECT 2");
    assert.equal(tab.isDirty, true);
    tab.model.setValue("SELECT 1");
    assert.equal(tab.isDirty, false);

    tab.setFile("C:\\queries\\accounts.sql");
    assert.equal(tab.title, "accounts.sql");
    assert.equal(tab.filePath, "C:\\queries\\accounts.sql");
});

test("FetchXML preview tabs use XML models, are read-only, and do not register SQL documents", () => {
    const { createTab, modelLanguages, listeners } = setup();
    const { tab, events } = createTab({ language: "xml", readOnly: true, isFetchXmlPreview: true });

    assert.equal(modelLanguages.at(-1), "xml");
    assert.equal(tab.readOnly, true);
    assert.equal(tab.isFetchXmlPreview, true);
    assert.equal(events, undefined);
    assert.equal(listeners.size, 0);
});

test("the Include Actual Plan preference defaults to off", async () => {
    let stored;
    const { loadSettings } = load("settings.ts", {}, {
        window: { toolboxAPI: { settings: { get: async () => stored, set: async () => {} } } },
    });
    assert.equal((await loadSettings()).includeActualPlan, false);
    stored = { includeActualPlan: true };
    assert.equal((await loadSettings()).includeActualPlan, true);
    stored = { includeActualPlan: 1 };
    assert.equal((await loadSettings()).includeActualPlan, false);
});

test("toolbar commands have the requested order, accessible icon buttons, split menus and credit link", () => {
    const html = fs.readFileSync(path.resolve(__dirname, "..", "src", "index.html"), "utf8");
    const toolbar = html.slice(html.indexOf('<div class="toolbar"'), html.indexOf('<div class="tool-content"'));
    const commands = ["connection-select", "btn-new", "btn-open", "btn-save", "btn-run", "btn-cancel",
        "btn-estimated-plan", "btn-edit-fetchxml", "btn-convert", "btn-actual-plan", "btn-results-text",
        "btn-results-grid", "btn-format", "btn-settings"];
    const actual = [...toolbar.matchAll(/\bid="([^"]+)"/g)].map(match => match[1]).filter(id => commands.includes(id));
    assert.deepEqual(actual, commands);
    for (const id of ["btn-open", "btn-save", "btn-cancel", "btn-estimated-plan", "btn-edit-fetchxml",
        "btn-actual-plan", "btn-results-text", "btn-results-grid"]) {
        const button = new RegExp(`<button id="${id}"([^>]*)>([\\s\\S]*?)<\\/button>`).exec(toolbar);
        assert.match(button[1], /aria-label="/);
        assert.match(button[1], /icon-only/);
        assert.equal(button[2].replace(/<svg[\s\S]*?<\/svg>/, "").trim(), "");
    }
    for (const id of ["save-menu", "convert-menu", "format-menu"])
        assert.match(toolbar, new RegExp(`id="${id}"[^>]*role="menu"[^>]*hidden`));
    assert.match(toolbar, /href="https:\/\/markcarrington.dev\/sql-4-cds\/"[^>]*>SQL 4 CDS by Mark Carrington<\/a>/);
});

test("toolbar enables transformations only for suitable tabs and keeps FetchXML Studio in place", () => {
    const { app, createTab, elements } = setup();
    const { tab } = createTab();
    app.updateToolbar();
    assert.equal(elements.get("btn-format").disabled, false);
    assert.equal(elements.get("btn-convert").disabled, true);
    assert.equal(elements.get("btn-display-names").disabled, true);
    assert.equal(elements.get("btn-edit-fetchxml").disabled, true);
    assert.notEqual(elements.get("btn-edit-fetchxml").hidden, true);
    tab.connected = true;
    app.updateToolbar();
    assert.equal(elements.get("btn-convert").disabled, false);
    assert.equal(elements.get("btn-display-names").disabled, false);
    tab.running = true;
    app.updateToolbar();
    assert.equal(elements.get("btn-convert").disabled, true);
    assert.equal(elements.get("btn-format").disabled, true);
    createTab({ language: "xml", readOnly: true, isFetchXmlPreview: true });
    app.updateToolbar();
    assert.equal(elements.get("btn-edit-fetchxml").disabled, false);
    assert.equal(elements.get("btn-results-text").disabled, true);
    assert.equal(elements.get("btn-format").disabled, true);
    assert.equal(elements.get("btn-save-menu").disabled, true);
});

test("FetchXML Studio can edit the selected FetchXML plan node", () => {
    const { app, createTab, elements, planViews } = setup();
    createTab();
    const fetchXml = '<fetch><entity name="account" /></fetch>';
    planViews[0].selectedNode = { name: "FetchXML Scan", nodeType: "FetchXmlScan", properties: [{ name: "FetchXML", value: fetchXml }] };
    app.activeTab.view = "plan";
    app.updateProperties();
    assert.equal(app.fetchXmlForEditing, fetchXml);
    assert.equal(elements.get("btn-edit-fetchxml").disabled, false);
    planViews[0].selectedNode = { name: "Sort", nodeType: "Sort" };
    app.updateProperties();
    assert.equal(elements.get("btn-edit-fetchxml").disabled, true);
});

test("split menus support keyboard navigation, Escape and outside dismissal", () => {
    const { app, elements, document } = setup();
    const toggle = elements.get("btn-save-menu");
    const menu = document.getElementById("save-menu");
    const saveAs = elements.get("btn-save-as");
    menu.appendChild(saveAs);
    menu.hidden = true;
    app.bindToolbarMenu(toggle, menu);
    toggle.events.keydown({ key: "ArrowDown", preventDefault() {} });
    assert.equal(menu.hidden, false);
    assert.equal(toggle.attributes["aria-expanded"], "true");
    assert.equal(saveAs.focused, true);
    menu.events.keydown({ key: "Escape", preventDefault() {} });
    assert.equal(menu.hidden, true);
    assert.equal(toggle.focused, true);
    toggle.events.click();
    document.events.pointerdown({ target: new Element() });
    assert.equal(menu.hidden, true);
    assert.equal(toggle.attributes["aria-expanded"], "false");
});

test("Save and Save As retain their distinct handlers", () => {
    const { app, createTab, elements } = setup();
    const { tab } = createTab();
    const calls = [];
    app.saveTab = (target, saveAs = false) => calls.push({ target, saveAs });
    app.bindToolbar();
    elements.get("btn-save").events.click();
    elements.get("btn-save-as").events.click();
    assert.equal(calls[0].target, tab);
    assert.equal(calls[0].saveAs, false);
    assert.equal(calls[1].saveAs, true);
});

test("format requests document formatting options, applies LSP edits with undo stops, and preserves concurrent edits", async () => {
    const { app, client, createTab } = setup();
    const { tab } = createTab();
    tab.model.setValue("select 1");
    const range = { isEmpty: () => false, selected: true };
    app.editor = { getSelection: () => range };
    let undoStops = 0;
    const edits = [];
    tab.model.pushStackElement = () => undoStops++;
    tab.model.pushEditOperations = (_selection, operations) => edits.push(operations);
    const calls = [];
    client.formatDocument = async (...args) => {
        calls.push(args);
        return [{ range: { start: { line: 0, character: 0 }, end: { line: 0, character: 8 } }, newText: "SELECT 1;" }];
    };
    await app.transformQuery("format");
    assert.equal(calls[0][0], tab.uri);
    assert.equal(calls[0][1].tabSize, 4);
    assert.equal(calls[0][1].insertSpaces, true);
    assert.equal(calls[0][1].addDisplayNames, false);
    assert.equal(undoStops, 2);
    assert.equal(edits[0][0].range.startLineNumber, 1);
    assert.equal(edits[0][0].range.startColumn, 1);
    assert.equal(edits[0][0].range.endColumn, 9);
    assert.equal(edits[0][0].text, "SELECT 1;");
    tab.connected = true;
    await app.transformQuery("format", true);
    assert.equal(calls[1][1].addDisplayNames, true);

    const notifications = [];
    const state = setup();
    const second = state.createTab().tab;
    state.app.editor = { getSelection: () => null };
    state.window.toolboxAPI = { utils: { showNotification: async (notification) => notifications.push(notification) } };
    state.client.formatDocument = async () => {
        second.model.setValue("select 2");
        return [{ range: { start: { line: 0, character: 0 }, end: { line: 0, character: 0 } }, newText: "SELECT 1;" }];
    };
    await state.app.transformQuery("format");
    assert.equal(second.model.getValue(), "select 2");
    assert.match(notifications[0].body, /query changed/);
    assert.equal(state.app.transforming, false);
});

test("conversion remembers the selected target and opens read-only preview content", async () => {
    const { app, client, createTab, elements } = setup();
    const { tab } = createTab();
    tab.connected = true;
    app.editor = { getSelection: () => null };
    const previews = [];
    app.openPreview = (...args) => previews.push(args);
    const calls = [];
    client.transformQuery = async (...args) => { calls.push(args); return ["let Source = 1 in Source"]; };
    app.convertTarget = "m";
    app.updateToolbar();
    await app.transformQuery("convert");
    assert.equal(calls[0][2], "m");
    assert.deepEqual(previews[0], ["let Source = 1 in Source", "powerquery", "Power BI (M)"]);
    assert.equal(elements.get("btn-convert-m").attributes["aria-checked"], "true");
    assert.equal(elements.get("btn-convert").title, "Convert to Power BI (M)");
});

test("Power BI previews do not register SQL documents", () => {
    const { createTab, listeners } = setup();
    const { tab } = createTab({ language: "powerquery", readOnly: true });
    assert.equal(tab.readOnly, true);
    assert.equal(tab.isFetchXmlPreview, false);
    assert.equal(listeners.size, 0);
});

test("Results to Text is per-tab and snapshots the mode at execution with paged rows and NULLs", async () => {
    const { app, client, createTab, grids, elements } = setup();
    const { tab, events } = createTab();
    app.setResultsMode("text");
    assert.equal(elements.get("btn-results-text").attributes["aria-pressed"], "true");
    await tab.connect(0);
    await tab.execute(null);
    app.setResultsMode("grid");
    const result = { ...summary(0, 0, 201), complete: true, columnInfo: [{ columnName: "Name" }, { columnName: "Value" }] };
    const pages = [];
    client.subset = async (_uri, _batch, _result, start, count) => {
        pages.push({ start, count });
        return { resultSubset: { rows: Array.from({ length: count }, () => [
            { displayValue: "Contoso", isNull: false }, { displayValue: "", isNull: true },
        ]) } };
    };
    events.resultSetAvailable({ resultSetSummary: result });
    events.queryComplete({ batchSummaries: [{ hasError: false, resultSetSummaries: [result] }] });
    await new Promise(resolve => setImmediate(resolve));
    assert.deepEqual(pages, [{ start: 0, count: 200 }, { start: 200, count: 1 }]);
    assert.equal(grids.length, 0);
    assert.equal(tab.textPane.hidden, false);
    assert.equal(tab.resultsPane.hidden, true);
    assert.equal(tab.textPane.children[0].textContent, "Name\tValue\n");
    assert.match(tab.textPane.children[0].children[0].textContent, /^Contoso\tNULL\n/);
    const second = createTab().tab;
    assert.equal(second.resultsMode, "grid");
});

test("text result loading errors are visible and stale responses do not append to a new execution", async () => {
    const { client, createTab } = setup();
    const { tab, events } = createTab();
    tab.resultsMode = "text";
    await tab.connect(0);
    await tab.execute(null);
    const result = { ...summary(0, 0, 1), complete: true, columnInfo: [{ columnName: "Name" }] };
    events.resultSetAvailable({ resultSetSummary: result });
    client.subset = async () => ({ resultSubset: { rows: [] } });
    await tab.renderTextResults(tab.resultsGeneration);
    assert.equal(tab.messagesPane.hidden, false);
    assert.match(tab.messagesPane.children.at(-1).textContent, /Unable to load text results/);
    let respond;
    client.subset = () => new Promise(resolve => { respond = resolve; });
    const pending = tab.renderTextResults(tab.resultsGeneration);
    await tab.clearResults();
    respond({ resultSubset: { rows: [[{ displayValue: "stale", isNull: false }]] } });
    await pending;
    assert.equal(tab.textPane.children.length, 0);
});

test("each completed grid exports its own entire result set through the native Save As dialog", async () => {
    const { app, client, createTab, grids, window } = setup();
    const dialogs = [];
    const saved = [];
    window.toolboxAPI = {
        fileSystem: { saveFile: async (...args) => { dialogs.push(args); return "C:\\exports\\chosen.file"; } },
        utils: { showNotification: async () => { assert.fail("Unexpected export error"); } },
    };
    client.saveResults = async (...args) => saved.push(args);
    const { tab, events } = createTab();
    tab.title = "Accounts.sql";
    await tab.connect(0);
    await tab.execute(null);
    for (const id of [0, 1]) events.resultSetAvailable({ resultSetSummary: summary(2, id, 10000) });
    assert.equal(grids[0].exportEnabled, false);
    assert.equal(grids[1].exportEnabled, false);
    events.queryComplete({ batchSummaries: [{ hasError: false, resultSetSummaries: [] }] });
    assert.equal(grids[0].exportEnabled, true);
    assert.equal(grids[1].exportEnabled, true);
    const formats = [["Csv", "CSV", "csv"], ["Excel", "Excel", "xlsx"], ["Json", "JSON", "json"],
        ["Markdown", "Markdown", "md"], ["Xml", "XML", "xml"]];
    app.activeTab = null;
    for (const [format, label, extension] of formats) {
        await grids[1].onExport(format);
        assert.equal(dialogs.at(-1)[0], `Accounts-results-3-2.${extension}`);
        assert.equal(dialogs.at(-1)[1], "");
        assert.equal(dialogs.at(-1)[2][0].name, label);
        assert.equal(dialogs.at(-1)[2][0].extensions[0], extension);
        assert.deepEqual(saved.at(-1), [tab.uri, 2, 1, "C:\\exports\\chosen.file", format]);
    }
    await grids[0].onExport("Csv");
    assert.equal(saved.at(-1)[2], 0);
    assert.equal(tab.gridStatus, "Results exported to C:\\exports\\chosen.file");
    assert.equal(tab.gridStatusIsError, false);
});

test("export cancellation sends no LSP request and filename or worker errors are visible", async () => {
    const { client, createTab, grids, window } = setup();
    const notifications = [];
    let mode = "cancel";
    const saves = [];
    window.toolboxAPI = {
        fileSystem: { saveFile: async () => {
            if (mode === "dialog-error") throw new Error("Dialog failed");
            return mode === "cancel" ? null : "C:\\results.xlsx";
        } },
        utils: { showNotification: async (notification) => notifications.push(notification) },
    };
    client.saveResults = async (...args) => { saves.push(args); throw new Error("Disk full"); };
    const { tab, events } = createTab();
    events.resultSetAvailable({ resultSetSummary: summary(0, 0, 1) });
    await grids[0].onExport("Excel");
    assert.equal(saves.length, 0);
    assert.equal(notifications.length, 0);
    mode = "dialog-error";
    await grids[0].onExport("Excel");
    assert.match(tab.gridStatus, /Dialog failed/);
    assert.equal(tab.gridStatusIsError, true);
    assert.equal(notifications.at(-1).type, "error");
    mode = "worker-error";
    await grids[0].onExport("Excel");
    assert.match(tab.gridStatus, /Disk full/);
    assert.match(notifications.at(-1).body, /Disk full/);
});

test("exports never save replacement query results after the filename dialog or tab disposal", async () => {
    const { client, createTab, grids, window } = setup();
    const notifications = [];
    let choose;
    window.toolboxAPI = {
        fileSystem: { saveFile: () => new Promise(resolve => { choose = resolve; }) },
        utils: { showNotification: async (notification) => notifications.push(notification) },
    };
    client.saveResults = async () => assert.fail("Stale results must never be exported");
    const { tab, events } = createTab();
    events.resultSetAvailable({ resultSetSummary: summary(0, 0, 1) });
    const pending = grids[0].onExport("Json");
    await tab.clearResults();
    choose("C:\\results.json");
    await pending;
    assert.match(notifications[0].body, /query results changed/);
    await grids[0].onExport("Json");
    assert.match(notifications[1].body, /no longer available/);
});
