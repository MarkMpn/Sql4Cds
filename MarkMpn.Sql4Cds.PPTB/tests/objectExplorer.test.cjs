const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test, afterEach } = require("node:test");
const ts = require("typescript");
const { JSDOM } = require("jsdom");
const { observable, runInAction } = require("mobx");
const dom = new JSDOM("<!doctype html><html><body></body></html>");
global.window = dom.window;
global.document = dom.window.document;
global.Node = dom.window.Node;
const { flushSync } = require("react-dom");
const explorers = [];
afterEach(async () => {
    for (const explorer of explorers.splice(0)) {
        let disposed;
        flushSync(() => { disposed = explorer.dispose(); });
        await disposed;
    }
    document.body.replaceChildren();
    document.querySelectorAll("main").forEach(host => host.remove());
});

function load(relativePath, globals) {
    const filename = path.resolve(__dirname, "..", "src", relativePath);
    const module = { exports: {} };
    const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX },
    }).outputText;
    vm.runInNewContext(code, {
        module, exports: module.exports, console, Error, window, document, Node, ...globals,
        require: (name) => name.startsWith(".")
            ? load(`${path.join(path.dirname(relativePath), name)}${name === "./objectExplorerView" ? ".tsx" : ".ts"}`, globals)
            : require(name),
    }, { filename });
    return module.exports;
}

function walk(element) {
    return [element, ...Array.from(element.children).flatMap(walk)];
}

function findButton(element, text) {
    return walk(element).find((candidate) => candidate.tagName === "BUTTON" && candidate.textContent === text);
}

// Exercise the real React DOM event path while keeping event invocations concise.
Object.defineProperty(window.EventTarget.prototype, "events", { get() {
    return new Proxy({}, { get: (_target, name) => (init = {}) => {
        const target = init.target ?? this;
        const Event = name === "keydown" ? window.KeyboardEvent : window.MouseEvent;
        const event = new Event(name, { bubbles: true, cancelable: true, ...init });
        for (const method of ["preventDefault", "stopPropagation"]) {
            const original = event[method].bind(event);
            event[method] = () => { init[method]?.(); original(); };
        }
        flushSync(() => target.dispatchEvent(event));
    } });
} });
window.HTMLElement.prototype.getBoundingClientRect = () => ({ left: 10, top: 20, width: 140, height: 32 });

function setup() {
    window.innerWidth = 800;
    window.innerHeight = 600;
    const calls = [];
    const errors = [];
    const inserted = [];
    const newQueries = [];
    let nextSession = 0;
    const client = {
        isRunning: true,
        createObjectExplorerSession: async (options) => {
            const sessionId = `s${++nextSession}`;
            calls.push(["create", options, sessionId]);
            return { sessionId };
        },
        expandObjectExplorerNode: async (...args) => { calls.push(["expand", ...args]); return true; },
        refreshObjectExplorerNode: async (...args) => { calls.push(["refresh", ...args]); return true; },
        generateObjectExplorerScript: async (sessionId, nodePath, action) => {
            calls.push(["script", sessionId, nodePath, action]);
            if (action === "SELECT_TOP_1000") return "SELECT TOP 1000 [accountid], [name]\nFROM dbo.account";
            if (nodePath.endsWith("/tvf")) return "SELECT *\nFROM GetAccounts(<@owner, uniqueidentifier>)";
            if (nodePath.endsWith("/proc")) return "DECLARE @RC int";
            return "SELECT [accountid], [name]\nFROM dbo.account";
        },
        closeObjectExplorerSession: async (sessionId) => { calls.push(["close", sessionId]); return { sessionId, success: true }; },
    };
    const tree = document.createElement("div");
    const host = document.createElement("main");
    document.documentElement.appendChild(host);
    host.appendChild(tree);
    const { ObjectExplorer } = load("objectExplorer.ts", {});
    const registry = observable({ all: [] }, { all: observable.shallow });
    let explorer;
    flushSync(() => {
        explorer = new ObjectExplorer(client, tree, (message) => errors.push(message), (text) => inserted.push(text),
            (connection, query) => newQueries.push({ connection, query }), registry);
    });
    // RPC notifications are external events; flush React commits before inspecting their DOM.
    const methods = new Set(["setConnections", "sessionCreated", "expandComplete", "sessionDisconnected",
        "toggle", "refresh", "removeConnection", "engineStopped", "dispose"]);
    explorer = new Proxy(explorer, { get(target, name) {
        const value = Reflect.get(target, name, target);
        if (!methods.has(name)) return value;
        return (...args) => {
            let result;
            flushSync(() => { result = value.apply(target, args); });
            return result;
        };
    } });
    explorers.push(explorer);
    function session(sessionId = "s1") {
        explorer.sessionCreated({ sessionId, success: true, rootNode: node(`objectexplorer://${sessionId}`, "SERVERNODE", "Server") });
    }
    function complete(nodePath, nodes, errorMessage, sessionId = "s1") {
        explorer.expandComplete({ sessionId, nodePath, nodes, errorMessage });
    }
    function key(key, shiftKey = false) {
        let prevented = false;
        let stopped = false;
        tree.events.keydown({ key, shiftKey, preventDefault() { prevented = true; }, stopPropagation() { stopped = true; } });
        return { prevented, stopped };
    }
    return { explorer, registry, client, tree, calls, errors, inserted, newQueries, session, complete, key, document, window };
}

function connection(slot = 0) {
    return observable.object({
        slot,
        status: "connected",
        connection: { name: `Contoso ${slot}`, id: `c${slot}`, url: "https://contoso.crm.dynamics.com" },
        options: { authenticationType: "PPTB", connectionName: `Contoso ${slot}`, connectionId: `c${slot}`, connectionTarget: slot, url: "https://contoso.crm.dynamics.com" },
    }, { options: observable.ref, connection: observable.ref }, { deep: false });
}

function node(nodePath, label, nodeType = "Folder", isLeaf = false) {
    return { nodePath, label, nodeType, isLeaf };
}

async function settle() {
    await new Promise((resolve) => setTimeout(resolve, 10));
    flushSync(() => {});
}

test("connecting names are visible but expansion, refresh and context actions wait for registration", async () => {
    const { explorer, tree, calls, key, newQueries, document } = setup();
    const slot = connection();
    slot.status = "connecting";
    await explorer.setConnections([slot]);
    const root = tree.children[0];
    assert.equal(root.getAttribute("aria-busy"), "true");
    assert.equal(root.getAttribute("aria-disabled"), "true");
    assert.ok(walk(root).some(element => element.textContent === "Contoso 0"));
    assert.ok(walk(root).some(element => element.textContent === "(Connecting\u2026)"));
    for (const action of ["ArrowRight", "Enter", " ", "F5", "ContextMenu"]) key(action);
    const row = tree.children[0].children[0];
    row.events.dblclick();
    row.children[0].events.click({ stopPropagation() {} });
    row.events.contextmenu({ preventDefault() {}, stopPropagation() {}, clientX: 0, clientY: 0 });
    await settle();
    assert.deepEqual(calls, []);
    assert.deepEqual(newQueries, []);
    assert.equal(document.body.children.length, 0);
    assert.equal(explorer.selected.expanded, false);

    flushSync(() => runInAction(() => { slot.status = "connected"; }));
    assert.equal(tree.children[0].getAttribute("aria-disabled"), "false");
    assert.equal(tree.children[0].getAttribute("aria-busy"), "false");
    assert.ok(!walk(tree).some(element => element.textContent === "(Connecting\u2026)"));
    key("ArrowRight");
    await settle();
    assert.equal(calls[0][0], "create");
});

test("new connections appear before an old explorer session finishes closing", async () => {
    const { explorer, client, tree, key, session } = setup();
    await explorer.setConnections([connection()]);
    key("ArrowRight");
    await settle();
    session();
    let finishClose;
    client.closeObjectExplorerSession = () => new Promise(resolve => { finishClose = resolve; });
    const replacement = connection();
    replacement.options.connectionName = "Replacement";
    replacement.status = "connecting";
    const updating = explorer.setConnections([replacement]);
    assert.ok(walk(tree).some(element => element.textContent === "Replacement"));
    assert.ok(walk(tree).some(element => element.textContent === "(Connecting\u2026)"));
    await settle();
    finishClose({ success: true });
    await updating;
});

test("registry completion updates each explorer connection independently and failures leave no connecting indicator", async () => {
    const { explorer, registry: source, client, tree, window, errors } = setup();
    const listeners = new Map();
    client.addListener = (uri, listener) => listeners.set(uri, listener);
    client.removeListener = (uri) => listeners.delete(uri);
    client.connect = async () => true;
    window.toolboxAPI = { connections: { getConnections: async () => [connection(0).connection, connection(1).connection] } };
    const { ConnectionRegistry } = load("connections.ts", { window });
    const registry = new ConnectionRegistry(client);
    await registry.refresh();
    flushSync(() => runInAction(() => { source.all = registry.all; }));
    assert.equal(registry.get(0).status, "connecting");
    const first = registry.register(0);
    const duplicate = registry.register(0);
    assert.equal(first, duplicate, "Concurrent registration shares the same attempt");
    const second = registry.register(1);
    const failed = assert.rejects(second, /Access denied/);
    await settle();
    assert.equal(listeners.size, 2);
    listeners.get("pptb://connection/1").connectionComplete({ errorMessage: "Access denied" });
    await failed;
    await settle();
    assert.equal(registry.get(1).status, "error");
    assert.equal(tree.children[1].getAttribute("aria-busy"), "false");
    assert.ok(walk(tree.children[1]).some(element => element.textContent === "(Connection failed)"));
    assert.equal(tree.children[1].children[0].title, "Access denied");
    assert.equal(tree.children[0].getAttribute("aria-disabled"), "true", "The slower connection stays disabled");
    listeners.get("pptb://connection/0").connectionComplete({});
    await first;
    await settle();
    assert.equal(registry.get(0).status, "connected");
    assert.equal(tree.children[0].getAttribute("aria-disabled"), "false");
    let retry;
    flushSync(() => { retry = registry.register(1); });
    assert.equal(registry.get(1).status, "connecting");
    assert.equal(registry.get(1).error, undefined);
    assert.equal(tree.children[1].getAttribute("aria-disabled"), "true");
    listeners.get("pptb://connection/1").connectionComplete({});
    await retry;
    await settle();
    assert.equal(registry.get(1).status, "connected");
    assert.equal(tree.children[1].getAttribute("aria-disabled"), "false");
    assert.equal(listeners.size, 0);
    assert.deepEqual(errors, []);
});

test("observable connection membership updates the tree without setConnections or change notifications", async () => {
    const { registry, tree, explorer, key, session, calls } = setup();
    assert.equal(tree.textContent, "No connections available");
    const first = connection();
    flushSync(() => runInAction(() => { registry.all = [first]; }));
    assert.equal(tree.querySelectorAll('[role="treeitem"]').length, 1);
    key("ArrowRight");
    await settle();
    session();
    const originalRoot = explorer.roots[0];
    const originalElement = tree.children[0];
    flushSync(() => runInAction(() => { registry.all = [first, connection(1)]; }));
    assert.equal(explorer.roots[0], originalRoot);
    assert.equal(tree.children[0], originalElement, "Adding a connection does not remount existing nodes");
    flushSync(() => runInAction(() => { registry.all = [registry.all[1]]; }));
    await settle();
    assert.equal(tree.querySelectorAll('[role="treeitem"]').length, 1);
    assert.ok(tree.textContent.includes("Contoso 1"));
    assert.ok(calls.some(call => call[0] === "close" && call[1] === "s1"));
});

test("nested node loading, children and errors update automatically without replacing focused rows", async () => {
    const { registry, explorer, tree, key, session, complete } = setup();
    flushSync(() => runInAction(() => { registry.all = [connection()]; }));
    key("ArrowRight");
    await settle();
    session();
    complete("objectexplorer://s1", [node("objectexplorer://s1/Tables", "Tables")]);
    key("ArrowRight");
    const folder = tree.querySelector('[data-path="objectexplorer://s1/Tables"]');
    assert.equal(document.activeElement, folder);
    key("ArrowRight");
    assert.equal(explorer.selected.state, "loading");
    assert.equal(folder.getAttribute("aria-busy"), "true");
    assert.ok(folder.textContent.includes("Loading\u2026"));
    await settle();
    complete("objectexplorer://s1/Tables", [node("objectexplorer://s1/Tables/account", "account", "Table", true)]);
    assert.equal(explorer.selected.state, "loaded");
    assert.equal(tree.querySelector('[data-path="objectexplorer://s1/Tables"]'), folder);
    assert.equal(document.activeElement, folder);
    assert.equal(folder.getAttribute("aria-busy"), "false");
    assert.ok(folder.textContent.includes("account"));
    explorer.refresh();
    await settle();
    complete("objectexplorer://s1/Tables", undefined, "Metadata unavailable");
    assert.equal(explorer.selected.state, "error");
    assert.ok(folder.querySelector('[role="alert"]').textContent.includes("Metadata unavailable"));
    assert.equal(document.activeElement, folder);
});

test("collapsing a pending node keeps the completed children cached without reopening it", async () => {
    const { explorer, registry, tree, key, session, complete, calls } = setup();
    flushSync(() => runInAction(() => { registry.all = [connection()]; }));
    key("ArrowRight");
    await settle();
    session();
    complete("objectexplorer://s1", [node("objectexplorer://s1/Tables", "Tables")]);
    key("ArrowRight");
    key("ArrowRight");
    await settle();
    key("ArrowLeft");
    assert.equal(explorer.selected.expanded, false);
    complete("objectexplorer://s1/Tables", [node("objectexplorer://s1/Tables/account", "account", "Table")]);
    assert.equal(explorer.selected.state, "loaded");
    assert.equal(explorer.selected.expanded, false);
    assert.equal(tree.querySelector('[data-path="objectexplorer://s1/Tables/account"]'), null);
    const requests = calls.length;
    key("ArrowRight");
    assert.ok(tree.querySelector('[data-path="objectexplorer://s1/Tables/account"]'));
    assert.equal(calls.length, requests);
});

test("observable background changes do not steal focus from an editor or an open context menu", async () => {
    const { registry, tree, key, document } = setup();
    const first = connection();
    const second = connection(1);
    flushSync(() => runInAction(() => { registry.all = [first, second]; }));
    key("ArrowDown");
    key("ContextMenu");
    const menuButton = document.activeElement;
    flushSync(() => runInAction(() => { first.status = "connecting"; }));
    assert.equal(document.activeElement, menuButton);
    assert.ok(document.querySelector(".explorer-context-menu"));
    document.events.pointerdown({ target: tree });
    const editor = document.createElement("textarea");
    document.body.appendChild(editor);
    editor.focus();
    flushSync(() => runInAction(() => { registry.all = [first]; }));
    assert.equal(document.activeElement, editor);
});

test("a context menu closes automatically if its own connection starts reconnecting", () => {
    const { registry, key, document } = setup();
    const slot = connection();
    flushSync(() => runInAction(() => { registry.all = [slot]; }));
    key("ContextMenu");
    assert.ok(document.querySelector(".explorer-context-menu"));
    flushSync(() => runInAction(() => { slot.status = "connecting"; }));
    assert.equal(document.querySelector(".explorer-context-menu"), null);
});

test("disposal unmounts React and stops connection reconciliation and stale notifications", async () => {
    const { explorer, registry, tree, key, session, calls } = setup();
    flushSync(() => runInAction(() => { registry.all = [connection()]; }));
    key("ArrowRight");
    await settle();
    session();
    await explorer.dispose();
    assert.equal(tree.childElementCount, 0);
    const before = calls.length;
    flushSync(() => runInAction(() => { registry.all = [connection(1)]; }));
    explorer.expandComplete({ sessionId: "s1", nodePath: "objectexplorer://s1", nodes: [node("stale", "stale")] });
    key("ArrowRight");
    await settle();
    assert.equal(tree.childElementCount, 0);
    assert.equal(explorer.roots.length, 0);
    assert.equal(calls.length, before);
});

test("columns, table-valued functions and stored procedures have distinct icons while existing icons are preserved", async () => {
    const { explorer, tree, session, complete, key } = setup();
    await explorer.setConnections([connection()]);
    key("ArrowRight");
    await settle();
    session();
    const types = ["Folder", "Table", "Column", "TableValuedFunction", "StoredProcedure", "UnknownType"];
    complete("objectexplorer://s1", types.map((type) => node(`objectexplorer://s1/${type}`, type, type, true)));
    const icons = walk(tree).filter((element) => element.className?.startsWith("explorer-icon "));
    assert.deepEqual(icons.map((icon) => icon.textContent),
        ["\u25A4", "\u25B0", "\u25A6", "\u25A5", "\u0192", "\u2699", "\u25A6"]);
    assert.equal(new Set(icons.slice(2, 6).map((icon) => icon.textContent)).size, 4);
    assert.ok(icons.every((icon) => icon.getAttribute("aria-hidden") === "true"));
});

test("root labels use the exact registered connection name, including duplicate suffixes and session notifications", async () => {
    const { explorer, tree, key, session } = setup();
    const slot = connection();
    slot.options.connectionName = "Contoso 0_2";
    await explorer.setConnections([slot]);
    assert.equal(explorer.selected.info.label, "Contoso 0_2");
    key("ArrowRight");
    await settle();
    session();
    assert.equal(explorer.selected.info.label, "Contoso 0_2");
    await explorer.setConnections([{ ...slot, connection: { ...slot.connection, name: "Ignored alternate name" } }]);
    assert.equal(explorer.selected.info.label, "Contoso 0_2");
    assert.ok(walk(tree).some((element) => element.textContent === "Contoso 0_2"));
});

test("double-click inserts object names without expanding them, while arrows still expand tables", async () => {
    const { explorer, tree, inserted, calls, session, complete, key } = setup();
    await explorer.setConnections([connection()]);
    key("ArrowRight");
    await settle();
    session();
    const objects = [
        { ...node("objectexplorer://s1/account", "account", "Table"), metadata: { schema: "dbo", name: "account" } },
        { ...node("objectexplorer://s1/column", "name", "Column", true), metadata: { name: "account.name" } },
        { ...node("objectexplorer://s1/function", "SomeFunction", "TableValuedFunction", true), metadata: { schema: "dbo", name: "SomeFunction" } },
        { ...node("objectexplorer://s1/procedure", "SomeProcedure", "StoredProcedure", true), metadata: { schema: "dbo", name: "SomeProcedure" } },
        node("objectexplorer://s1/fallback", "other", "Table", true),
    ];
    complete("objectexplorer://s1", objects);
    const before = calls.length;
    for (const info of objects) {
        const item = walk(tree).find((element) => element.dataset.path === info.nodePath);
        item.children[0].events.click();
        item.children[0].events.dblclick();
    }
    assert.deepEqual(inserted, ["dbo.account", "name", "dbo.SomeFunction", "dbo.SomeProcedure", "other"]);
    assert.equal(calls.length, before);
    const table = walk(tree).find((element) => element.dataset.path === objects[0].nodePath);
    assert.equal(table.getAttribute("aria-expanded"), "false");
    table.children[0].children[0].events.click({ stopPropagation() {} });
    await settle();
    assert.deepEqual(calls.at(-1), ["expand", "s1", objects[0].nodePath]);
});

test("connections render immediately but sessions and children are lazy, cached and accessible", async () => {
    const { explorer, tree, calls, session, complete, key } = setup();
    const slot = connection();
    await explorer.setConnections([slot]);
    assert.equal(tree.getAttribute("role"), "tree");
    assert.equal(calls.length, 0);
    assert.equal(walk(tree).filter((item) => item.getAttribute("role") === "treeitem").length, 1);
    assert.deepEqual(key("ArrowRight"), { prevented: true, stopped: true });
    await settle();
    assert.equal(calls[0][0], "create");
    assert.equal(calls[0][1], slot.options);
    assert.equal(calls.length, 1, "Wait for sessioncreated before expanding");
    session();
    await settle();
    assert.deepEqual(calls[1], ["expand", "s1", "objectexplorer://s1"]);
    complete("objectexplorer://s1", [node("objectexplorer://s1/Tables", "Tables")]);
    key("ArrowRight");
    assert.equal(explorer.selected.info.label, "Tables");
    key("ArrowRight");
    await settle();
    assert.deepEqual(calls[2], ["expand", "s1", "objectexplorer://s1/Tables"]);
    complete("objectexplorer://s1/Tables", [node("objectexplorer://s1/Tables/account", "account", "Table")]);
    const before = calls.length;
    key("ArrowLeft");
    key("ArrowRight");
    await settle();
    assert.equal(calls.length, before, "Reopening a loaded node uses cached children");
    assert.equal(walk(tree).filter((item) => item.getAttribute("role") === "treeitem").length, 3);
    assert.equal(tree.querySelectorAll('[role="treeitem"][tabindex="0"]').length, 1);
    assert.equal(explorer.connections.get(0).root.info.label, "Contoso 0", "Do not show the server's placeholder label");
});

test("sessioncreated arriving before the createsession response is not lost", async () => {
    const { explorer, client, calls, session } = setup();
    client.createObjectExplorerSession = async () => {
        session("early");
        return { sessionId: "early" };
    };
    await explorer.setConnections([connection()]);
    explorer.toggle(explorer.connections.get(0).root);
    await settle();
    assert.deepEqual(calls, [["expand", "early", "objectexplorer://early"]]);
    assert.equal(explorer.earlySessions.size, 0);
});

test("click selection preserves the row for double-click expansion and concurrent expansion is deduplicated", async () => {
    const { explorer, tree, calls } = setup();
    await explorer.setConnections([connection()]);
    const item = tree.children[0];
    const row = item.children[0];
    row.events.click();
    assert.equal(tree.children[0], item, "Selecting must not replace the element before dblclick");
    row.events.dblclick();
    explorer.refresh();
    await settle();
    assert.equal(calls.filter((call) => call[0] === "create").length, 1);
    assert.equal(explorer.connections.get(0).root.expanded, true);
});

test("refresh uses the selected node, replaces children and F5 is handled by the tree", async () => {
    const { explorer, calls, session, complete, key } = setup();
    await explorer.setConnections([connection()]);
    key("ArrowRight");
    await settle();
    session();
    complete("objectexplorer://s1", [node("objectexplorer://s1/Tables", "Tables")]);
    key("ArrowRight");
    key("ArrowRight");
    await settle();
    complete("objectexplorer://s1/Tables", [node("objectexplorer://s1/Tables/account", "account", "Table", true)]);
    key("ArrowRight");
    const before = calls.length;
    key("Enter");
    await settle();
    assert.equal(calls.length, before, "Leaf nodes do not expand");
    assert.deepEqual(key("F5"), { prevented: true, stopped: true });
    await settle();
    assert.deepEqual(calls.at(-1), ["refresh", "s1", "objectexplorer://s1/Tables"], "Refresh a leaf's parent");
    complete("objectexplorer://s1/Tables", [node("objectexplorer://s1/Tables/contact", "contact", "Table")]);
    assert.equal(explorer.selected.info.label, "Tables");
    assert.equal(explorer.selected.children[0].info.label, "contact");
    key("Home");
    assert.equal(explorer.selected.info.label, "Contoso 0");
    key("End");
    assert.equal(explorer.selected.info.label, "contact");
    key("ArrowUp");
    assert.equal(explorer.selected.info.label, "Tables");
});

test("right-click selects the clicked node and Refresh targets its session, with leaf refresh reloading its parent", async () => {
    const { explorer, tree, calls, session, complete, key, document } = setup();
    await explorer.setConnections([connection()]);
    key("ArrowRight");
    await settle();
    session();
    complete("objectexplorer://s1", [node("objectexplorer://s1/Tables", "Tables")]);
    const rightClick = (path) => {
        const item = walk(tree).find((element) => element.dataset.path === path);
        let prevented = false;
        let stopped = false;
        item.children[0].events.contextmenu({
            clientX: 795, clientY: 595,
            preventDefault() { prevented = true; }, stopPropagation() { stopped = true; },
        });
        assert.ok(prevented && stopped);
        assert.equal(explorer.selected.info.nodePath, path);
        const menu = document.body.children[0];
        assert.equal(menu.getAttribute("role"), "menu");
        assert.equal(menu.style.left, "660px");
        assert.equal(menu.style.top, "568px");
        return menu;
    };
    let menu = rightClick("objectexplorer://s1/Tables");
    menu.children[0].events.click();
    await settle();
    assert.deepEqual(calls.at(-1), ["refresh", "s1", "objectexplorer://s1/Tables"]);
    assert.equal(document.body.children.length, 0);
    complete("objectexplorer://s1/Tables", [node("objectexplorer://s1/Tables/name", "name", "Column", true)]);
    key("Home");
    menu = rightClick("objectexplorer://s1/Tables/name");
    menu.children[0].events.click();
    await settle();
    assert.deepEqual(calls.at(-1), ["refresh", "s1", "objectexplorer://s1/Tables"]);
});

test("object scripts expose type-gated actions and editor, file and clipboard destinations", async () => {
    const { explorer, tree, newQueries, session, complete, key, document, window } = setup();
    const objects = [
        { ...node("objectexplorer://s1/account", "account", "Table", true), metadata: { schema: "dbo", name: "account" } },
        { ...node("objectexplorer://s1/tvf", "GetAccounts", "TableValuedFunction", true), metadata: { schema: "dbo", name: "GetAccounts" } },
        { ...node("objectexplorer://s1/proc", "Reassign", "StoredProcedure", true), metadata: { schema: "dbo", name: "Reassign" } },
    ];
    await explorer.setConnections([connection()]);
    key("ArrowRight");
    await settle();
    session();
    complete("objectexplorer://s1", objects);

    const table = walk(tree).find((element) => element.dataset.path === objects[0].nodePath);
    const play = table.querySelector(".explorer-select-top");
    assert.ok(play);
    play.events.click({ stopPropagation() {} });
    await settle();
    assert.equal(newQueries.at(-1).query, "SELECT TOP 1000 [accountid], [name]\nFROM dbo.account");

    window.toolboxAPI = {
        fileSystem: { saveFile: async (...args) => { window.savedFile = args; return "C:\\scripts\\account.sql"; } },
        utils: { copyToClipboard: async (text) => { window.copiedText = text; } },
    };
    const openTableMenu = () => {
        table.children[0].events.contextmenu({ clientX: 10, clientY: 20, preventDefault() {}, stopPropagation() {} });
        const menu = document.body.children[0];
        findButton(menu, "Script Table as").events.pointerenter();
        return menu;
    };
    let menu = openTableMenu();
    findButton(menu, "SELECT To").events.pointerenter();
    findButton(menu, "New Query Editor Window").events.click();
    await settle();
    assert.equal(newQueries.at(-1).query, "SELECT [accountid], [name]\nFROM dbo.account");

    menu = openTableMenu();
    findButton(menu, "SELECT To").events.pointerenter();
    findButton(menu, "File...").events.click();
    await settle();
    assert.equal(window.savedFile[0], "account.sql");
    assert.equal(window.savedFile[1], "SELECT [accountid], [name]\nFROM dbo.account");

    menu = openTableMenu();
    findButton(menu, "SELECT To").events.pointerenter();
    findButton(menu, "Clipboard").events.click();
    await settle();
    assert.equal(window.copiedText, "SELECT [accountid], [name]\nFROM dbo.account");

    menu = openTableMenu();
    findButton(menu, "EXECUTE To").events.pointerenter();
    assert.equal(findButton(menu, "EXECUTE To").disabled, true);

    const procedure = walk(tree).find((element) => element.dataset.path === objects[2].nodePath);
    procedure.children[0].events.contextmenu({ clientX: 10, clientY: 20, preventDefault() {}, stopPropagation() {} });
    menu = document.body.children[0];
    findButton(menu, "Script Stored Procedure as").events.pointerenter();
    assert.equal(findButton(menu, "SELECT To").disabled, true);
    assert.equal(findButton(menu, "INSERT To").disabled, true);
    assert.equal(findButton(menu, "EXECUTE To").disabled, false);

    const functionItem = walk(tree).find((element) => element.dataset.path === objects[1].nodePath);
    functionItem.children[0].events.contextmenu({ clientX: 10, clientY: 20, preventDefault() {}, stopPropagation() {} });
    menu = document.body.children[0];
    findButton(menu, "Script Table-Valued Function as").events.pointerenter();
    findButton(menu, "SELECT To").events.pointerenter();
    findButton(menu, "New Query Editor Window").events.click();
    await settle();
    assert.equal(newQueries.at(-1).query, "SELECT *\nFROM GetAccounts(<@owner, uniqueidentifier>)");
});

test("context menus support keyboard access, Escape, outside clicks, scrolling and lifecycle cleanup", async () => {
    const { explorer, key, tree, document, window, calls } = setup();
    await explorer.setConnections([connection()]);
    for (const open of [() => key("F10", true), () => key("ContextMenu")]) {
        assert.deepEqual(open(), { prevented: true, stopped: true });
        const menu = document.body.children[0];
        assert.equal(document.activeElement, menu.children[0]);
        menu.events.keydown({ key: "Escape", preventDefault() {}, stopPropagation() {} });
        assert.equal(document.body.children.length, 0);
        assert.equal(document.activeElement, tree.children[0]);
    }
    key("ContextMenu");
    document.events.pointerdown({ target: document.body.children[0].children[0] });
    assert.equal(document.body.children.length, 1, "Inside clicks do not dismiss before activation");
    document.events.pointerdown({ target: tree });
    assert.equal(document.body.children.length, 0);
    assert.equal(calls.length, 0, "Dismissing does not refresh");
    key("ContextMenu");
    tree.events.scroll();
    assert.equal(document.body.children.length, 0);
    key("ContextMenu");
    window.events.resize();
    assert.equal(document.body.children.length, 0);
    key("ContextMenu");
    await explorer.removeConnection(0);
    assert.equal(document.body.children.length, 0);
    document.events.pointerdown({ target: tree });
    window.events.resize();
    assert.equal(document.body.children.length, 0, "Dismissal listeners are removed with the menu");
});

test("RPC and expand errors are visible and refresh retries instead of leaving a loading node", async () => {
    const { explorer, client, calls, session, complete, errors, key, tree } = setup();
    await explorer.setConnections([connection()]);
    client.createObjectExplorerSession = async () => { throw new Error("Authentication failed"); };
    key("ArrowRight");
    await settle();
    assert.equal(errors.at(-1), "Authentication failed");
    const root = explorer.connections.get(0).root;
    assert.equal(root.loading, false);
    assert.ok(walk(tree).some((element) => element.getAttribute("role") === "alert"));
    client.createObjectExplorerSession = async () => ({ sessionId: "s1" });
    explorer.refresh();
    await settle();
    session();
    await settle();
    complete("objectexplorer://s1", undefined, "Metadata unavailable");
    assert.equal(root.loading, false);
    assert.equal(errors.at(-1), "Metadata unavailable");
    explorer.refresh();
    await settle();
    assert.deepEqual(calls.at(-1), ["refresh", "s1", "objectexplorer://s1"]);
    complete("objectexplorer://s1", []);
    assert.equal(root.error, undefined);
    assert.ok(walk(tree).some((element) => element.textContent === "No objects"));
    client.refreshObjectExplorerNode = async () => false;
    explorer.refresh();
    await settle();
    assert.match(errors.at(-1), /rejected/);
    assert.equal(root.loading, false);
    assert.equal(explorer.connections.get(0).sessionId, undefined);
});

test("different connections route by session and replacement/removal closes only their explorer sessions", async () => {
    const { explorer, calls, session, complete } = setup();
    const first = connection(0);
    const second = connection(2);
    await explorer.setConnections([first, second]);
    const root1 = explorer.connections.get(0).root;
    const root2 = explorer.connections.get(2).root;
    explorer.toggle(root1);
    explorer.toggle(root2);
    await settle();
    session("s2");
    session("s1");
    complete("objectexplorer://s2", [node("objectexplorer://s2/Metadata", "Metadata")], undefined, "s2");
    assert.equal(root2.children[0].info.label, "Metadata");
    assert.equal(root1.children.length, 0);
    const before = calls.length;
    await explorer.setConnections([first, second]);
    assert.equal(calls.length, before, "Unchanged connections preserve their sessions");
    await explorer.setConnections([connection(0), second]);
    assert.ok(calls.some((call) => call[0] === "close" && call[1] === "s1"));
    complete("objectexplorer://s1", [node("stale", "stale")]);
    assert.equal(explorer.connections.get(0).root.children.length, 0);
    await explorer.dispose();
    assert.ok(calls.some((call) => call[0] === "close" && call[1] === "s2"));
    assert.equal(explorer.connections.size, 0);
});

test("removing a connection while createsession is pending still closes the eventual session", async () => {
    const { explorer, client, calls, session } = setup();
    let resolve;
    client.createObjectExplorerSession = () => new Promise((done) => { resolve = done; });
    await explorer.setConnections([connection()]);
    explorer.toggle(explorer.connections.get(0).root);
    const removed = explorer.removeConnection(0);
    session("pending");
    resolve({ sessionId: "pending" });
    await removed;
    assert.deepEqual(calls, [["close", "pending"]]);
    assert.equal(explorer.connections.size, 0);
    assert.equal(explorer.earlySessions.size, 0);
});

test("unexpected session disconnect and engine exit clear stale state and surface errors", async () => {
    const { explorer, errors, session, key, tree } = setup();
    await explorer.setConnections([connection()]);
    key("ArrowRight");
    await settle();
    session();
    explorer.sessionDisconnected({ sessionId: "s1", success: true });
    assert.match(errors.at(-1), /disconnected/);
    const entry = explorer.connections.get(0);
    assert.equal(entry.sessionId, undefined);
    assert.equal(entry.root.loading, false);
    explorer.engineStopped();
    assert.equal(explorer.connections.size, 0);
    assert.ok(walk(tree).some((element) => element.textContent === "SQL 4 CDS engine stopped"));
});

test("failed session notifications can be retried and close failures are surfaced", async () => {
    const { explorer, client, errors, calls, key, session } = setup();
    await explorer.setConnections([connection()]);
    key("ArrowRight");
    await settle();
    explorer.sessionCreated({ sessionId: "s1", success: false, errorMessage: "Connection unavailable" });
    assert.equal(errors.at(-1), "Connection unavailable");
    explorer.refresh();
    await settle();
    assert.ok(calls.some((call) => call[0] === "close" && call[1] === "s1"));
    session("s2");
    assert.equal(explorer.connections.get(0).ready, true);
    client.closeObjectExplorerSession = async () => ({ success: false, errorMessage: "Close failed" });
    await explorer.dispose();
    assert.equal(errors.at(-1), "Close failed");
});

test("Object Explorer client sends exact RPC methods/payloads and routes completion notifications", async () => {
    const requests = [];
    const notifications = [];
    let handlers;
    const { Sql4CdsClient } = load("lsp/client.ts", {
        TextEncoder,
        console: { debug() {}, error() {} },
        window: { toolboxAPI: { workers: { connect: async (_worker, options) => {
            handlers = options.notifications;
            return {
                ready: Promise.resolve(),
                request: async (method, params) => { requests.push([method, params]); return {}; },
                notify: async () => {},
            };
        } } } },
    });
    const client = new Sql4CdsClient({
        objectExplorerSessionCreated: (params) => notifications.push(["created", params]),
        objectExplorerExpandComplete: (params) => notifications.push(["expanded", params]),
        objectExplorerSessionDisconnected: (params) => notifications.push(["disconnected", params]),
    });
    await client.start({});
    requests.length = 0;
    const options = connection().options;
    await client.createObjectExplorerSession(options);
    await client.expandObjectExplorerNode("s1", "path");
    await client.refreshObjectExplorerNode("s1", "path");
    await client.closeObjectExplorerSession("s1");
    assert.deepEqual(JSON.parse(JSON.stringify(requests)), [
        ["objectexplorer/createsession", { options }],
        ["objectexplorer/expand", { sessionId: "s1", nodePath: "path" }],
        ["objectexplorer/refresh", { sessionId: "s1", nodePath: "path" }],
        ["objectexplorer/closesession", { sessionId: "s1" }],
    ]);
    const params = { sessionId: "s1" };
    handlers["objectexplorer/sessioncreated"](params);
    handlers["objectexplorer/expandCompleted"](params);
    handlers["objectexplorer/sessiondisconnected"](params);
    assert.deepEqual(notifications, [["created", params], ["expanded", params], ["disconnected", params]]);
});
