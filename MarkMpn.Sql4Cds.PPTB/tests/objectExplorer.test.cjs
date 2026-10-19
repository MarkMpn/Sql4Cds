const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");
const ts = require("typescript");

function load(relativePath, globals) {
    const filename = path.resolve(__dirname, "..", "src", relativePath);
    const module = { exports: {} };
    const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText;
    vm.runInNewContext(code, { module, exports: module.exports, console, Error, ...globals }, { filename });
    return module.exports;
}

function walk(element) {
    return [element, ...element.children.flatMap(walk)];
}

function setup() {
    const document = { activeElement: null };
    class Element {
        constructor() {
            this.children = [];
            this.attributes = {};
            this.events = {};
            this.style = {};
            this.dataset = {};
        }
        setAttribute(name, value) { this.attributes[name] = value; }
        addEventListener(name, callback) { this.events[name] = callback; }
        removeEventListener(name) { delete this.events[name]; }
        appendChild(child) { this.children.push(child); child.parent = this; }
        append(...children) { this.children.push(...children); }
        replaceChildren(...children) { this.children = children.flatMap((child) => child.fragment ? child.children : [child]); }
        contains(element) { return walk(this).includes(element); }
        focus() { document.activeElement = this; }
        querySelector() { return walk(this).find((element) => element.tabIndex === 0); }
        getBoundingClientRect() { return { left: 10, top: 20, width: 140, height: 32 }; }
        remove() { this.parent.children = this.parent.children.filter((child) => child !== this); }
    }
    document.body = new Element();
    document.events = {};
    document.addEventListener = (name, callback) => { document.events[name] = callback; };
    document.removeEventListener = (name) => { delete document.events[name]; };
    const window = new Element();
    window.innerWidth = 800;
    window.innerHeight = 600;
    document.createElement = () => new Element();
    document.createDocumentFragment = () => Object.assign(new Element(), { fragment: true });
    const calls = [];
    const errors = [];
    const inserted = [];
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
        closeObjectExplorerSession: async (sessionId) => { calls.push(["close", sessionId]); return { sessionId, success: true }; },
    };
    const tree = new Element();
    const { ObjectExplorer } = load("objectExplorer.ts", { document, window, Node: Element });
    const explorer = new ObjectExplorer(client, tree, (message) => errors.push(message), (text) => inserted.push(text));
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
    return { explorer, client, tree, calls, errors, inserted, session, complete, key, document, window };
}

function connection(slot = 0) {
    return {
        slot,
        connection: { name: `Contoso ${slot}`, id: `c${slot}`, url: "https://contoso.crm.dynamics.com" },
        options: { authenticationType: "PPTB", connectionName: `contoso_${slot}`, connectionId: `c${slot}`, connectionTarget: slot, url: "https://contoso.crm.dynamics.com" },
    };
}

function node(nodePath, label, nodeType = "Folder", isLeaf = false) {
    return { nodePath, label, nodeType, isLeaf };
}

async function settle() {
    await new Promise((resolve) => setImmediate(resolve));
}

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
    assert.ok(icons.every((icon) => icon.attributes["aria-hidden"] === "true"));
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
    assert.equal(table.attributes["aria-expanded"], "false");
    table.children[0].children[0].events.click({ stopPropagation() {} });
    await settle();
    assert.deepEqual(calls.at(-1), ["expand", "s1", objects[0].nodePath]);
});

test("connections render immediately but sessions and children are lazy, cached and accessible", async () => {
    const { explorer, tree, calls, session, complete, key } = setup();
    const slot = connection();
    await explorer.setConnections([slot]);
    assert.equal(tree.attributes.role, "tree");
    assert.equal(calls.length, 0);
    assert.equal(walk(tree).filter((item) => item.attributes.role === "treeitem").length, 1);
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
    assert.equal(walk(tree).filter((item) => item.attributes.role === "treeitem").length, 3);
    assert.equal(walk(tree).filter((item) => item.tabIndex === 0).length, 1);
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
        assert.equal(menu.attributes.role, "menu");
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
    assert.equal(Object.keys(document.events).length, 0);
    assert.equal(Object.keys(window.events).length, 0);
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
    assert.ok(walk(tree).some((element) => element.attributes.role === "alert"));
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
