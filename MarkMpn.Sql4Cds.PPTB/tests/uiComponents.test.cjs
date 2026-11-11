const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");
const ts = require("typescript");

function load(relativePath, dependencies, globals = {}) {
    const filename = path.resolve(__dirname, "..", "src", relativePath);
    const module = { exports: {} };
    const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText;
    vm.runInNewContext(code, {
        module, exports: module.exports,
        require: (name) => {
            assert.ok(name in dependencies, `Unexpected dependency: ${name}`);
            return dependencies[name];
        },
        ...globals,
    }, { filename });
    return module.exports;
}

function dom() {
    class EventTarget {
        events = new Map();
        addEventListener(name, callback) {
            if (!this.events.has(name)) this.events.set(name, new Set());
            this.events.get(name).add(callback);
        }
        removeEventListener(name, callback) { this.events.get(name)?.delete(callback); }
        dispatch(name, event = {}) {
            for (const callback of [...this.events.get(name) ?? []])
                callback({ preventDefault() {}, ...event });
        }
        get listenerCount() {
            return [...this.events.values()].reduce((count, listeners) => count + listeners.size, 0);
        }
    }
    const document = new EventTarget();
    class Element extends EventTarget {
        children = [];
        style = {};
        attributes = {};
        classes = new Set();
        classList = {
            toggle: (name, value) => value ? this.classes.add(name) : this.classes.delete(name),
            contains: (name) => this.classes.has(name),
        };
        offsetLeft = 0;
        offsetWidth = 0;
        clientWidth = 100;
        scrollLeft = 0;
        append(...children) { children.forEach(child => this.appendChild(child)); }
        appendChild(child) {
            if (child.fragment) {
                this.append(...child.children);
            } else {
                this.children.push(child);
                child.parentElement = this;
            }
        }
        replaceChildren(...children) { this.children = []; this.append(...children); }
        setAttribute(name, value) { this.attributes[name] = value; }
        focus() { document.activeElement = this; }
        scrollIntoView() { this.scrolledIntoView = true; }
        getBoundingClientRect() { return { right: 200, bottom: 30, width: 100, height: 100 }; }
        contains(target) { return this === target || this.children.some(child => child.contains(target)); }
        remove() {
            this.removed = true;
            if (this.parentElement)
                this.parentElement.children = this.parentElement.children.filter(child => child !== this);
        }
        showModal() { this.modal = true; }
    }
    document.createElement = () => new Element();
    document.createDocumentFragment = () => Object.assign(new Element(), { fragment: true });
    document.body = new Element();
    const window = Object.assign(new EventTarget(), { innerWidth: 800, innerHeight: 600 });
    const observers = [];
    class ResizeObserver {
        constructor(callback) { observers.push(callback); }
        observe(element) { this.element = element; }
    }
    return { document, window, Node: Element, Element, ResizeObserver, observers };
}

function tabStripFixture() {
    const globals = dom();
    const { TabStrip } = load("tabStrip.ts", {}, globals);
    const strip = new globals.Element();
    const listButton = new globals.Element();
    const tabs = [
        { title: "First.sql", connectionSlot: 0, connected: true, isDirty: true },
        { title: "Second.sql", connectionSlot: 1, connecting: true, running: true },
    ];
    let activeTab = tabs[1];
    const activated = [];
    const closed = [];
    let editorFocused = false;
    const view = new TabStrip(strip, listButton, {
        getTabs: () => tabs,
        getActiveTab: () => activeTab,
        getConnectionName: (slot) => ["Dev", "Test"][slot],
        activateTab: (tab) => { activeTab = tab; activated.push(tab); },
        closeTab: async (tab) => { closed.push(tab); },
        focusEditor: () => { editorFocused = true; },
    });
    view.bind();
    return { ...globals, view, strip, listButton, tabs, activated, closed, editorFocused: () => editorFocused };
}

test("tab strip preserves labels, connection and dirty indicators, activation and closing", () => {
    const { view, strip, tabs, activated, closed } = tabStripFixture();
    view.render();
    const [first, second] = strip.children;
    assert.equal(first.title, "First.sql - Dev (unsaved changes)");
    assert.equal(first.children[1].textContent, "First.sql");
    assert.equal(first.children[1].title, "Unsaved changes");
    assert.equal(first.classes.has("modified"), true);
    assert.equal(first.children[0].classes.has("connected"), true);
    assert.equal(second.classes.has("active"), true);
    assert.equal(second.classes.has("running"), true);
    assert.equal(second.children[0].classes.has("connecting"), true);
    first.dispatch("click");
    assert.equal(activated[0], tabs[0]);
    first.dispatch("auxclick", { button: 0 });
    assert.equal(closed.length, 0);
    first.dispatch("auxclick", { button: 1 });
    let stopped = false;
    second.children[2].dispatch("click", { stopPropagation: () => { stopped = true; } });
    assert.deepEqual(closed, [tabs[0], tabs[1]]);
    assert.equal(stopped, true);
    view.render();
    assert.equal(strip.children[0].classes.has("active"), true, "Rendering reads the latest active tab");
});

test("tab strip scrolls the active tab into view on resize and accepts either wheel direction", () => {
    const { view, strip, observers } = tabStripFixture();
    view.render();
    const active = strip.children[1];
    active.offsetLeft = 150;
    active.offsetWidth = 60;
    observers[0]();
    assert.equal(strip.scrollLeft, 110);
    active.offsetLeft = 20;
    observers[0]();
    assert.equal(strip.scrollLeft, 20);
    let prevented = false;
    strip.dispatch("wheel", { deltaX: 2, deltaY: 30, preventDefault: () => { prevented = true; } });
    assert.equal(strip.scrollLeft, 50);
    assert.equal(prevented, true);
    strip.dispatch("wheel", { deltaX: -40, deltaY: 2 });
    assert.equal(strip.scrollLeft, 10);
});

test("open-query menu supports keyboard selection, restores editor focus and removes dismissal listeners", () => {
    const state = tabStripFixture();
    const { listButton, document, window, tabs, activated } = state;
    listButton.dispatch("keydown", { key: "ArrowDown" });
    const menu = document.body.children[0];
    assert.equal(menu.attributes.role, "menu");
    assert.equal(menu.children[0].title, "First.sql - Dev (unsaved changes)");
    assert.equal(document.activeElement, menu.children[1]);
    assert.equal(listButton.attributes["aria-expanded"], "true");
    menu.dispatch("keydown", { key: "Home" });
    assert.equal(document.activeElement, menu.children[0]);
    menu.dispatch("keydown", { key: "ArrowUp" });
    assert.equal(document.activeElement, menu.children[1]);
    menu.dispatch("keydown", { key: "ArrowDown" });
    assert.equal(document.activeElement, menu.children[0]);
    menu.dispatch("keydown", { key: "End" });
    assert.equal(document.activeElement, menu.children[1]);
    menu.children[1].dispatch("click");
    assert.equal(activated[0], tabs[1]);
    assert.equal(state.editorFocused(), true, "Even selecting the already active tab focuses its editor");
    assert.equal(menu.removed, true);
    assert.equal(listButton.attributes["aria-expanded"], "false");
    assert.equal(document.listenerCount, 0);
    assert.equal(window.listenerCount, 0);
});

test("open-query menu dismisses on Escape, Tab, toggle, outside events, resize and blur", () => {
    for (const reason of ["Escape", "Tab", "toggle", "pointerdown", "focusin", "scroll", "resize", "blur"]) {
        const { listButton, document, window, Element } = tabStripFixture();
        listButton.dispatch("click");
        const menu = document.body.children[0];
        for (const name of ["pointerdown", "focusin", "scroll"])
            document.dispatch(name, { target: menu.children[0] });
        assert.equal(menu.removed, undefined, "Events inside the menu do not dismiss it");
        if (reason === "Escape" || reason === "Tab") {
            menu.dispatch("keydown", { key: reason });
            assert.equal(document.activeElement, listButton);
        } else if (reason === "toggle") {
            listButton.dispatch("click");
        } else if (reason === "resize" || reason === "blur") {
            window.dispatch(reason);
        } else {
            document.dispatch(reason, { target: new Element() });
        }
        assert.equal(menu.removed, true, reason);
        assert.equal(document.listenerCount, 0, reason);
        assert.equal(window.listenerCount, 0, reason);
    }
});

test("execution confirmation preserves Yes, Yes to all and safe cancellation results", async () => {
    const protocol = load("lsp/protocol.ts", { "../resultsProtocol": load("resultsProtocol.ts", {}) });
    const { ConfirmationResult } = protocol;
    for (const [value, expected] of [
        [String(ConfirmationResult.Yes), ConfirmationResult.Yes],
        [String(ConfirmationResult.All), ConfirmationResult.All],
        [String(ConfirmationResult.No), ConfirmationResult.No],
        ["", ConfirmationResult.No],
        ["unexpected", ConfirmationResult.No],
    ]) {
        const globals = dom();
        const { showConfirmation } = load("dialogs.ts", { "./lsp/protocol": protocol }, globals);
        const result = showConfirmation("<delete all records?>");
        const dialog = globals.document.body.children[0];
        assert.equal(dialog.modal, true);
        assert.equal(dialog.children[0].children[1].textContent, "<delete all records?>");
        const buttons = dialog.children[0].children[2].children;
        assert.deepEqual(buttons.map(button => button.textContent), ["Yes", "Yes to all", "No"]);
        assert.equal(buttons[0].className, "primary");
        dialog.returnValue = value;
        dialog.dispatch("close");
        assert.equal(await result, expected);
        assert.equal(dialog.removed, true);
    }
});

test("unsaved-query dialog preserves save, discard and cancel choices and removes itself", async () => {
    for (const [value, expected] of [["save", "save"], ["discard", "discard"], ["cancel", "cancel"], ["", "cancel"]]) {
        const globals = dom();
        const { showUnsavedChangesDialog } = load("dialogs.ts", { "./lsp/protocol": {} }, globals);
        const result = showUnsavedChangesDialog("Query.sql");
        const dialog = globals.document.body.children[0];
        assert.equal(dialog.modal, true);
        assert.equal(dialog.children[0].children[1].textContent, "Save changes to Query.sql before closing?");
        dialog.returnValue = value;
        dialog.dispatch("close");
        assert.equal(await result, expected);
        assert.equal(dialog.removed, true);
    }
});
