const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");
const ts = require("typescript");

function loadModule(relativePath, globals = {}) {
    const cache = new Map();
    function load(filename) {
        if (cache.has(filename)) return cache.get(filename).exports;
        const module = { exports: {} };
        cache.set(filename, module);
        const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
            compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
        }).outputText;
        vm.runInNewContext(code, {
            module,
            exports: module.exports,
            require: (name) => load(path.resolve(path.dirname(filename), `${name}.ts`)),
            URL,
            ...globals,
        }, { filename });
        return module.exports;
    }
    return load(path.resolve(__dirname, "..", "src", relativePath));
}

const reference = { dataSource: "contoso_2", id: "00000000-0000-0000-0000-000000000123", isNull: false, logicalName: "account" };

test("formatting sends standard DocumentFormattingParams and returns TextEdits", async () => {
    const { Sql4CdsClient } = loadModule("lsp/client.ts", { console });
    const client = new Sql4CdsClient({});
    const calls = [];
    const edits = [{ range: { start: { line: 0, character: 0 }, end: { line: 0, character: 8 } }, newText: "SELECT 1;" }];
    client.session = { request: async (method, params) => { calls.push({ method, params }); return edits; } };
    for (const addDisplayNames of [false, true]) {
        const options = { tabSize: 4, insertSpaces: true, addDisplayNames };
        assert.equal(await client.formatDocument("pptb://query/1", options), edits);
        const call = calls.at(-1);
        assert.equal(call.method, "textDocument/formatting");
        assert.equal(call.params.textDocument.uri, "pptb://query/1");
        assert.equal(call.params.options, options);
        assert.deepEqual(Object.keys(call.params).sort(), ["options", "textDocument"]);
    }
});

test("recognizes only non-null entity-reference objects", () => {
    const { isEntityReferenceValue } = loadModule("lsp/protocol.ts");
    assert.equal(isEntityReferenceValue(reference), true);
    for (const value of [null, undefined, "guid", {}, { ...reference, isNull: true },
        { ...reference, dataSource: 2 }, { ...reference, id: "" }, { ...reference, logicalName: undefined }]) {
        assert.equal(isEntityReferenceValue(value), false);
    }
});

test("opens the named source connection's record URL using its actual slot", async () => {
    const opened = [];
    const connections = [
        { id: "first", name: "First", url: "https://contoso.crm.dynamics.com" },
        null,
        { id: "second", name: "Second", url: "https://contoso.crm.dynamics.com/org/" },
    ];
    const { ConnectionRegistry } = loadModule("connections.ts", {
        window: { toolboxAPI: {
            connections: { getConnections: async () => connections },
            utils: { openInConnectionBrowser: async (...args) => opened.push(args) },
        } },
    });
    const registry = new ConnectionRegistry({});
    await registry.refresh();
    await registry.openRecord({ ...reference, dataSource: "SECOND", logicalName: "account & test" });
    const [url, slot] = opened[0];
    const parsed = new URL(url);
    assert.equal(slot, 2);
    assert.equal(parsed.origin, "https://contoso.crm.dynamics.com");
    assert.equal(parsed.pathname, "/org/main.aspx");
    assert.equal(parsed.searchParams.get("pagetype"), "entityrecord");
    assert.equal(parsed.searchParams.get("etn"), "account & test");
    assert.equal(parsed.searchParams.get("id"), reference.id);
    await assert.rejects(registry.openRecord({ ...reference, dataSource: "missing" }), /no longer available/);
    assert.equal(opened.length, 1, "Unknown sources must not fall back to the active connection");
});

test("propagates connection-browser failures", async () => {
    const { ConnectionRegistry } = loadModule("connections.ts", {
        window: { toolboxAPI: {
            connections: { getConnections: async () => [{ id: "first", name: "First", url: "contoso.crm.dynamics.com" }] },
            utils: { openInConnectionBrowser: async () => { throw new Error("Browser failed"); } },
        } },
    });
    const registry = new ConnectionRegistry({});
    await registry.refresh();
    await assert.rejects(registry.openRecord({ ...reference, dataSource: "First" }), /Browser failed/);
});

test("registers display names unchanged, disambiguates case-insensitive duplicates and reconnects renamed connections", async () => {
    let connections = [
        { id: "first", name: "Contoso Dev", url: "https://org123.crm.dynamics.com" },
        { id: "second", name: "contoso dev", url: "https://org456.crm.dynamics.com" },
        { id: "third", name: "Contoso Dev_2", url: "https://org789.crm.dynamics.com" },
    ];
    const listeners = new Map();
    const calls = [];
    const client = {
        addListener: (uri, listener) => listeners.set(uri, listener),
        removeListener: (uri) => listeners.delete(uri),
        connect: async (uri, options) => {
            calls.push(["connect", uri, options.connectionName]);
            listeners.get(uri).connectionComplete({ ownerUri: uri });
            return true;
        },
        disconnect: async (uri) => { calls.push(["disconnect", uri]); return true; },
    };
    const { ConnectionRegistry } = loadModule("connections.ts", {
        window: { toolboxAPI: { connections: { getConnections: async () => connections } } },
    });
    const registry = new ConnectionRegistry(client);
    await registry.refresh();
    assert.deepEqual(Array.from(registry.all, (entry) => entry.options.connectionName),
        ["Contoso Dev", "contoso dev_2", "Contoso Dev_2_2"]);
    await Promise.all(registry.all.map((entry) => registry.register(entry.slot)));
    assert.deepEqual(calls, [
        ["connect", "pptb://connection/0", "Contoso Dev"],
        ["connect", "pptb://connection/1", "contoso dev_2"],
        ["connect", "pptb://connection/2", "Contoso Dev_2_2"],
    ]);
    assert.deepEqual(Array.from(await registry.refresh()), []);
    connections = [{ ...connections[0], name: "Production (EU)" }];
    assert.deepEqual(Array.from(await registry.refresh()), [0, 1, 2]);
    await registry.register(0);
    assert.deepEqual(calls.at(-1), ["connect", "pptb://connection/0", "Production (EU)"]);
});

class Element {
    constructor(tag) {
        this.tag = tag;
        this.children = [];
        this.style = {};
        this.dataset = {};
        this.events = {};
        this.classList = { add() {} };
    }
    appendChild(child) { this.children.push(child); child.parentElement = this; }
    append(...children) { children.forEach((child) => this.appendChild(child)); }
    replaceChildren(...children) { this.children = []; this.append(...children); }
    addEventListener(name, handler) { this.events[name] = handler; }
    focus() {}
    closest(selector) {
        if (selector === ".grid-record-link" && this.className === "grid-record-link") return this;
        if (selector === ".grid-cell" && this.className === "grid-cell") return this;
        return this.parentElement?.closest(selector) ?? null;
    }
}

test("renders references as clickable links without breaking text, NULLs, or copying", () => {
    const opened = [];
    const copied = [];
    const { ResultsGrid } = loadModule("query/resultsGrid.ts", {
        document: { createElement: (tag) => new Element(tag), addEventListener() {}, removeEventListener() {} },
        ResizeObserver: class { observe() {} },
        requestAnimationFrame() {},
    });
    const values = [
        { displayValue: "Account <name>", isNull: false, rawObject: reference },
        { displayValue: "plain GUID", isNull: false, rawObject: "guid" },
        { displayValue: "", isNull: true, rawObject: reference },
        { displayValue: "null reference", isNull: false, rawObject: { ...reference, isNull: true } },
    ];
    const grid = new ResultsGrid(values.map((_, i) => ({ columnName: `col${i}` })), async () => [],
        (text) => copied.push(text), (value) => opened.push(value));
    grid.pages.set(0, { rows: [values] });
    const row = grid.renderRow(0);
    const link = row.children[1].children[0];
    assert.equal(link.tag, "a");
    assert.equal(link.textContent, "Account <name>");
    assert.equal(row.children[2].textContent, "plain GUID");
    assert.equal(row.children[3].textContent, "NULL");
    assert.equal(row.children[4].textContent, "null reference");
    grid.render = () => { throw new Error("Link must not be removed before click"); };
    grid.onCellClick({ target: link, button: 0, preventDefault() {} });
    let prevented = false;
    link.events.click({ preventDefault: () => { prevented = true; } });
    assert.equal(prevented, true);
    assert.equal(opened[0], reference);
    grid.selected = { row: 0, col: 0 };
    grid.onKeyDown({ ctrlKey: true, key: "c", preventDefault() {} });
    assert.deepEqual(copied, ["Account <name>"]);
});
