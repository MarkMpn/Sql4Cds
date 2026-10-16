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
    await registry.openRecord({ ...reference, dataSource: "CONTOSO_2", logicalName: "account & test" });
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
    await assert.rejects(registry.openRecord({ ...reference, dataSource: "contoso" }), /Browser failed/);
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
