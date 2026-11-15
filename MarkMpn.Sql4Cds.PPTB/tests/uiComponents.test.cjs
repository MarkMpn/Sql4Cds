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
