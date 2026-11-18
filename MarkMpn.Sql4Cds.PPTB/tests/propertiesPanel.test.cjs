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
const React = require("react");
const { createRoot } = require("react-dom/client");
const { flushSync } = require("react-dom");
let unmount;
afterEach(() => {
    flushSync(() => unmount?.());
    document.body.replaceChildren();
});

function load(file, dependencies = {}) {
    const module = { exports: {} };
    const filename = path.resolve(__dirname, "..", "src", file);
    const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX },
    }).outputText;
    vm.runInNewContext(code, {
        module, exports: module.exports, document, window, Node,
        require: name => dependencies[name] ?? require(name),
    }, { filename });
    return module.exports;
}

function fixture() {
    const context = load("appContext.tsx");
    const { PropertiesPanel } = load("propertiesPanel.tsx", {
        "./appContext": context,
        "./executionPlan/propertyRows": load("executionPlan/propertyRows.ts"),
    });
    const app = observable({ activeTab: null, settings: { propertiesPinned: true } }, {
        activeTab: observable.ref, settings: observable.ref,
    });
    const element = document.createElement("main");
    document.body.appendChild(element);
    const root = createRoot(element);
    unmount = () => root.unmount();
    flushSync(() => root.render(React.createElement(context.AppContext.Provider, { value: app },
        React.createElement(PropertiesPanel))));
    const change = callback => flushSync(() => runInAction(callback));
    const show = node => change(() => { app.activeTab = { selectedPlanNode: node }; });
    const rows = () => Array.from(element.querySelectorAll(".property-row"));
    const key = value => {
        const event = new window.KeyboardEvent("keydown", { key: value, bubbles: true, cancelable: true });
        flushSync(() => document.activeElement.dispatchEvent(event));
        return event;
    };
    const click = element => flushSync(() => element.click());
    const focus = element => flushSync(() => element.focus());
    return { app, element, change, show, rows, key, click, focus };
}

const properties = () => [
    { name: "Last", value: "last", displayOrder: 2 },
    { name: "Parent", displayValue: "(Collection)", displayOrder: 1, value: [
        { name: "Second", value: "second", displayOrder: 2 },
        { name: "Child", displayOrder: 1, value: [
            { name: "Leaf", value: "raw", displayValue: "Formatted", displayOrder: 0 },
        ] },
    ] },
    { name: "First", value: "<fetch />", displayOrder: 0 },
];

test("PropertiesPanel reads context, renders placeholders and safely sorts/displays plan properties", () => {
    const { element, show, rows } = fixture();
    assert.match(element.textContent, /Select an execution plan node/);
    const tree = element.querySelector('[role="tree"]');
    assert.equal(tree.tabIndex, 0, "Empty property trees must be reachable by keyboard");
    flushSync(() => tree.focus());
    assert.equal(document.activeElement, tree);
    assert.equal(element.querySelector('[role="tree"]').getAttribute("aria-label"), "Properties of the selected item");
    show({ name: "SELECT\r\naccount", properties: [] });
    assert.equal(element.querySelector("#properties-title").textContent, "SELECT");
    assert.match(element.textContent, /No properties/);
    const list = properties();
    show({ name: "<FetchXML>", properties: list });
    assert.equal(element.querySelector("#properties-title").title, "<FetchXML>");
    assert.deepEqual(rows().map(row => row.querySelector(".property-name").textContent), ["First", "\u25B8Parent", "Last"]);
    assert.deepEqual(list.map(property => property.name), ["Last", "Parent", "First"], "Rendering must not sort the controller data in place");
    assert.equal(rows()[0].querySelector(".property-value").textContent, "<fetch />");
    assert.equal(element.querySelector("fetch"), null);
    assert.equal(rows()[1].querySelector(".property-value").title, "(Collection)");
    assert.equal(rows()[1].getAttribute("aria-expanded"), "false");
    assert.equal(rows()[0].hasAttribute("aria-expanded"), false);
    assert.equal(rows()[0].getAttribute("aria-level"), "1");
    assert.deepEqual(rows().map(row => row.getAttribute("aria-selected")), ["true", "false", "false"]);
    assert.equal(rows()[0].getAttribute("aria-setsize"), "3");
    assert.equal(rows()[2].getAttribute("aria-posinset"), "3");
    assert.equal(rows().filter(row => row.tabIndex === 0).length, 1);
    assert.equal(tree.tabIndex, -1, "Populated trees use their selected row as the single tab stop");
    assert.equal(tree.querySelectorAll('[tabindex="0"]').length, 1);
});

test("PropertiesPanel supports nested keyboard navigation, toggling and roving focus", () => {
    const { show, rows, key, focus } = fixture();
    show({ name: "Plan", properties: properties() });
    focus(rows()[0]);
    assert.equal(key("ArrowDown").defaultPrevented, true);
    assert.equal(document.activeElement, rows()[1]);
    key("ArrowRight");
    assert.equal(rows().length, 5);
    assert.equal(rows()[1].getAttribute("aria-expanded"), "true");
    assert.equal(document.activeElement, rows()[1], "Expanding retains focus on its parent");
    key("ArrowRight");
    assert.equal(document.activeElement, rows()[2]);
    assert.equal(rows()[2].getAttribute("aria-level"), "2");
    assert.equal(rows()[2].getAttribute("aria-selected"), "true");
    assert.equal(rows()[1].getAttribute("aria-selected"), "false");
    key("Enter");
    assert.equal(rows().length, 6);
    key("ArrowRight");
    assert.equal(document.activeElement, rows()[3]);
    assert.equal(rows()[3].querySelector(".property-value").textContent, "Formatted");
    assert.equal(rows()[3].getAttribute("aria-level"), "3");
    key("ArrowLeft");
    assert.equal(document.activeElement, rows()[2]);
    key(" ");
    assert.equal(rows().length, 5);
    key("ArrowLeft");
    assert.equal(document.activeElement, rows()[1]);
    key("ArrowLeft");
    assert.equal(rows().length, 3);
    key("End");
    assert.equal(document.activeElement, rows()[2]);
    key("Home");
    assert.equal(document.activeElement, rows()[0]);
    key("ArrowUp");
    assert.equal(document.activeElement, rows()[0], "Navigation does not wrap at the boundary");
    assert.equal(key("x").defaultPrevented, false);
    assert.equal(rows().filter(row => row.tabIndex === 0).length, 1);
});

test("PropertiesPanel preserves row identity and expansion for the same data and resets for another node", () => {
    const { app, element, show, rows, click, change, focus } = fixture();
    const list = properties();
    show({ name: "Plan", properties: list });
    const first = rows()[0];
    const parent = rows()[1];
    click(parent);
    assert.equal(document.activeElement, parent);
    assert.equal(parent.tabIndex, 0);
    assert.equal(parent.getAttribute("aria-selected"), "true");
    assert.equal(first.getAttribute("aria-selected"), "false");
    assert.equal(first.tabIndex, -1, "Mouse selection updates the roving tab stop");
    assert.equal(rows().length, 5);
    show({ name: "Renamed", properties: list });
    assert.equal(rows()[1], parent);
    assert.equal(rows().length, 5);
    const outside = document.createElement("button");
    document.body.appendChild(outside);
    focus(outside);
    show({ name: "Another", properties: properties() });
    assert.equal(document.activeElement, outside, "Background property changes must not steal focus");
    assert.equal(rows().length, 3);
    assert.equal(rows()[1].getAttribute("aria-expanded"), "false");
    click(rows()[1]);
    const child = rows()[2];
    focus(child);
    show({ name: "Replacement", properties: [{ name: "Only", value: "value" }] });
    assert.equal(rows().length, 1);
    assert.equal(rows()[0].tabIndex, 0);
    assert.equal(document.activeElement, rows()[0], "Replacing focused properties restores focus within the panel");
    change(() => { app.settings = { propertiesPinned: false }; });
    assert.equal(element.querySelector("#properties-grid").contains(document.activeElement), true);
    show(null);
    assert.match(element.textContent, /Select an execution plan node/);
});
