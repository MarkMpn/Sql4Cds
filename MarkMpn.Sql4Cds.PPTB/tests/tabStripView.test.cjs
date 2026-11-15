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
dom.window.HTMLElement.prototype.scrollIntoView = function() {};
let cleanup;
afterEach(() => {
    flushSync(() => cleanup?.());
    document.body.replaceChildren();
});

function fixture() {
    const observers = [];
    const filename = path.resolve(__dirname, "..", "src", "tabStripView.tsx");
    const module = { exports: {} };
    const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX },
    }).outputText;
    vm.runInNewContext(code, {
        module, exports: module.exports, require, document, window, Node,
        ResizeObserver: class {
            constructor(callback) { observers.push(this); this.callback = callback; }
            observe() {}
            disconnect() { this.disconnected = true; }
        },
    });
    const tabs = [
        observable({ id: 1, title: "First.sql", connectionSlot: 0, connected: true, connecting: false, running: false, isDirty: true }),
        observable({ id: 2, title: "Second.sql", connectionSlot: 1, connected: false, connecting: true, running: true, isDirty: false }),
    ];
    const activated = [];
    const closed = [];
    let editorFocused = false;
    const app = observable({
        tabs, activeTab: tabs[1],
        connections: { get: slot => ({ options: { connectionName: ["Dev", "Test"][slot] } }) },
        activateTab(tab) { runInAction(() => { this.activeTab = tab; }); activated.push(tab); },
        closeTab: async tab => { closed.push(tab); },
        focusEditor: () => { editorFocused = true; },
    }, { tabs: observable.shallow, activeTab: observable.ref, connections: false });
    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);
    cleanup?.();
    cleanup = () => root.unmount();
    flushSync(() => root.render(React.createElement(module.exports.TabStripView, { app })));
    const change = callback => flushSync(() => runInAction(callback));
    const event = (element, name, options = {}) => {
        const type = name.startsWith("key") ? dom.window.KeyboardEvent : name === "wheel" ? dom.window.WheelEvent : dom.window.MouseEvent;
        const event = new type(name, { bubbles: true, cancelable: true, ...options });
        flushSync(() => element.dispatchEvent(event));
        return event;
    };
    return {
        app, tabs, activated, closed, observers, change, event,
        strip: container.querySelector("#tab-strip"),
        listButton: container.querySelector("#btn-tab-list"),
        menu: () => document.querySelector(".tab-list-menu"),
        editorFocused: () => editorFocused,
    };
}

test("tab strip observes labels, connection, dirty, running and active state while preserving DOM identity", () => {
    const { app, tabs, strip, change, event, activated, closed } = fixture();
    const [first, second] = strip.children;
    assert.equal(first.title, "First.sql - Dev (unsaved changes)");
    assert.equal(first.querySelector(".tab-label").title, "Unsaved changes");
    assert.equal(first.classList.contains("modified"), true);
    assert.equal(first.querySelector(".tab-indicator").classList.contains("connected"), true);
    assert.equal(second.classList.contains("active"), true);
    assert.equal(second.classList.contains("running"), true);
    assert.equal(second.querySelector(".tab-indicator").classList.contains("connecting"), true);
    event(first, "click");
    assert.equal(activated[0], tabs[0]);
    assert.equal(first.getAttribute("aria-selected"), "true");
    event(first, "auxclick", { button: 0 });
    assert.equal(closed.length, 0);
    event(first, "auxclick", { button: 1 });
    event(second.querySelector(".tab-close"), "click");
    assert.deepEqual(closed, [tabs[0], tabs[1]]);
    assert.equal(activated.length, 1, "Closing must not activate the tab");
    change(() => { tabs[0].isDirty = false; tabs[0].title = "Renamed.sql"; app.activeTab = tabs[1]; });
    assert.equal(strip.children[0], first);
    assert.equal(first.title, "Renamed.sql - Dev");
    assert.equal(first.classList.contains("modified"), false);
    assert.equal(second.classList.contains("active"), true);
});

test("tab strip scrolls on resize, accepts either wheel direction and disconnects its observer", () => {
    const { strip, observers, event } = fixture();
    const active = strip.children[1];
    Object.defineProperties(strip, { clientWidth: { value: 100 } });
    Object.defineProperties(active, { offsetLeft: { value: 150, configurable: true }, offsetWidth: { value: 60 } });
    observers[0].callback();
    assert.equal(strip.scrollLeft, 110);
    Object.defineProperty(active, "offsetLeft", { value: 20 });
    observers[0].callback();
    assert.equal(strip.scrollLeft, 20);
    assert.equal(event(strip, "wheel", { deltaX: 2, deltaY: 30 }).defaultPrevented, true);
    assert.equal(strip.scrollLeft, 50);
    event(strip, "wheel", { deltaX: -40, deltaY: 2 });
    assert.equal(strip.scrollLeft, 10);
    flushSync(cleanup);
    cleanup = null;
    assert.equal(observers[0].disconnected, true);
});

test("open queries supports keyboard selection and observes changes while open", () => {
    const { tabs, listButton, menu, event, activated, editorFocused, change } = fixture();
    event(listButton, "keydown", { key: "ArrowDown" });
    const popup = menu();
    assert.equal(popup.getAttribute("role"), "menu");
    assert.equal(popup.children[0].title, "First.sql - Dev (unsaved changes)");
    assert.equal(document.activeElement, popup.children[1]);
    assert.equal(listButton.getAttribute("aria-expanded"), "true");
    for (const [key, index] of [["Home", 0], ["ArrowUp", 1], ["ArrowDown", 0], ["End", 1]]) {
        event(popup, "keydown", { key });
        assert.equal(document.activeElement, popup.children[index]);
    }
    change(() => { tabs[0].title = "Updated.sql"; tabs[0].isDirty = false; });
    assert.equal(popup.children[0].title, "Updated.sql - Dev");
    event(popup.children[1], "click");
    assert.equal(activated[0], tabs[1]);
    assert.equal(editorFocused(), true);
    assert.equal(popup.isConnected, false);
    assert.equal(listButton.getAttribute("aria-expanded"), "false");
});

test("open queries dismisses for Escape, Tab, toggle, outside focus, scrolling, resize and blur", () => {
    for (const reason of ["Escape", "Tab", "toggle", "pointerdown", "focusin", "scroll", "resize", "blur"]) {
        const { listButton, menu, event } = fixture();
        event(listButton, "click");
        const popup = menu();
        for (const name of ["pointerdown", "focusin", "scroll"]) event(popup.children[0], name);
        assert.equal(popup.isConnected, true);
        if (reason === "Escape" || reason === "Tab") {
            event(popup, "keydown", { key: reason });
            assert.equal(document.activeElement, listButton);
        } else if (reason === "toggle") event(listButton, "click");
        else event(reason === "resize" || reason === "blur" ? window : document, reason);
        assert.equal(popup.isConnected, false, reason);
        flushSync(cleanup);
        cleanup = null;
        document.body.replaceChildren();
    }
});
