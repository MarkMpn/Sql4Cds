const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");
const ts = require("typescript");
const gridStatuses = new WeakMap();

function gridStatus(grid) {
    return gridStatuses.get(grid).text;
}

class Element {
    constructor(tag) {
        this.tag = tag;
        this.children = [];
        this.style = {};
        this.dataset = {};
        this.events = {};
        const classes = new Set();
        this.classList = { add: (...names) => names.forEach((name) => classes.add(name)), contains: (name) => classes.has(name) };
        this.scrollTop = 0;
        this.scrollLeft = 0;
        this.clientHeight = 240;
        this.clientWidth = 300;
        this.clientLeft = 0;
        this.clientTop = 0;
    }
    appendChild(child) { this.children.push(child); child.parentElement = this; }
    append(...children) { children.forEach((child) => this.appendChild(child)); }
    replaceChildren(...children) { this.children = []; this.append(...children); }
    addEventListener(name, handler) { this.events[name] = handler; }
    setAttribute(name, value) { this[name] = value; }
    contains() { return true; }
    focus() { this.focused = true; }
    closest(selector) {
        if (selector === ".grid-cell" && this.dataset.col !== undefined) return this;
        return this.parentElement?.closest(selector) ?? null;
    }
    remove() {}
    getBoundingClientRect() { return { left: 0, top: 0, width: 160, height: 150 }; }
}

function createGrid({ platform = "Win32", rowCount = 1000, columns = 5, fetchRows = async () => [], onExport, recordActions, onOpenRecord = () => {} } = {}) {
    const filename = path.resolve(__dirname, "..", "src", "query", "resultsGrid.ts");
    const module = { exports: {} };
    const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText;
    const documentListeners = {};
    const frames = new Map();
    const errors = [];
    let nextFrame = 0;
    vm.runInNewContext(code, {
        module,
        exports: module.exports,
        require: () => ({
            isEntityReferenceValue: value => value?.isNull === false && !!value.logicalName && !!value.id && !!value.dataSource,
            exportFormats: [
                { format: "Csv", label: "CSV" }, { format: "Excel", label: "Excel" },
                { format: "Json", label: "JSON" }, { format: "Markdown", label: "Markdown" }, { format: "Xml", label: "XML" },
            ],
        }),
        document: {
            body: new Element("body"),
            createElement: (tag) => new Element(tag),
            createDocumentFragment: () => new Element("fragment"),
            addEventListener: (name, handler) => { documentListeners[name] = handler; },
            removeEventListener: (name) => { delete documentListeners[name]; },
            getSelection: () => ({ isCollapsed: true }),
            activeElement: {},
        },
        window: { innerWidth: 800, innerHeight: 600, addEventListener() {}, removeEventListener() {} },
        Node: Element,
        Element,
        navigator: { platform, userAgent: "" },
        ResizeObserver: class { observe() {} },
        requestAnimationFrame: (callback) => { frames.set(++nextFrame, callback); return nextFrame; },
        cancelAnimationFrame: (id) => frames.delete(id),
        console: { error: (...args) => errors.push(args) },
    }, { filename });

    const copied = [];
    const status = { text: "" };
    const grid = new module.exports.ResultsGrid(
        Array.from({ length: columns }, (_, i) => ({ columnName: `col${i}` })),
        fetchRows,
        (text) => copied.push(text),
        onOpenRecord,
        (text) => { status.text = text; },
        onExport,
        recordActions,
    );
    gridStatuses.set(grid, status);
    grid.render = () => {};
    grid.rowCount = rowCount;
    return { grid, copied, documentListeners, frames, errors };
}

function key(grid, key, modifiers = {}) {
    let prevented = false;
    grid.onKeyDown({ key, code: "", ctrlKey: false, metaKey: false, shiftKey: false, altKey: false, ...modifiers, preventDefault() { prevented = true; } });
    return prevented;
}

test("grid context menu offers all export formats and disables export until enabled", async () => {
    const calls = [];
    const { grid } = createGrid({ onExport: async (format) => calls.push(format) });
    assert.equal(grid.element.children.length, 2, "No persistent export toolbar");
    let prevented = false;
    grid.element.events.contextmenu({ clientX: 790, clientY: 590, preventDefault() { prevented = true; } });
    assert.equal(prevented, true);
    assert.equal(grid.exportMenu.role, "menu");
    assert.deepEqual(Array.from(grid.exportButtons, button => button.textContent), [
        "Export to CSV...", "Export to Excel...", "Export to JSON...", "Export to Markdown...", "Export to XML...",
    ]);
    assert.equal(grid.exportMenu.style.left, "640px");
    assert.equal(grid.exportMenu.style.top, "450px");
    assert.ok(grid.exportButtons.every(button => button.disabled));
    await grid.exportResults("Csv");
    assert.equal(calls.length, 0);
    grid.setExportEnabled(true);
    for (const format of ["Csv", "Excel", "Json", "Markdown", "Xml"]) {
        await grid.exportResults(format);
    }
    assert.deepEqual(calls, ["Csv", "Excel", "Json", "Markdown", "Xml"]);
    assert.ok(grid.exportButtons.every(button => !button.disabled));
    grid.exportButtons[1].events.click();
    assert.equal(grid.exportMenu, null);
    assert.equal(calls.at(-1), "Excel");
});

test("export prevents duplicate requests and restores controls when it finishes", async () => {
    let finish;
    const { grid } = createGrid({ onExport: () => new Promise(resolve => { finish = resolve; }) });
    grid.setExportEnabled(true);
    const pending = grid.exportResults("Excel");
    grid.showContextMenu(10, 10);
    assert.ok(grid.exportButtons.every(button => button.disabled));
    await grid.exportResults("Excel");
    finish();
    await pending;
    assert.ok(grid.exportButtons.every(button => !button.disabled));
    grid.dispose();
    assert.equal(grid.exportMenu, null);
    await grid.exportResults("Excel");
    assert.equal(grid.exporting, false);
});

test("grid context menu supports keyboard navigation, Escape, outside click and listener cleanup", () => {
    const { grid, documentListeners } = createGrid({ onExport: async () => {} });
    grid.setExportEnabled(true);
    assert.equal(key(grid, "F10", { shiftKey: true }), true);
    assert.equal(grid.exportButtons[0].focused, true);
    grid.exportMenu.events.keydown({ key: "End", preventDefault() {} });
    assert.equal(grid.exportButtons[4].focused, true);
    grid.exportMenu.events.keydown({ key: "Escape", preventDefault() {} });
    assert.equal(grid.exportMenu, null);
    assert.equal(grid.element.focused, true);
    assert.equal(documentListeners.pointerdown, undefined);
    assert.equal(key(grid, "ContextMenu"), true);
    grid.exportMenu.contains = () => false;
    documentListeners.pointerdown({ target: new Element("outside") });
    assert.equal(grid.exportMenu, null);
    assert.equal(documentListeners.focusin, undefined);
    grid.showContextMenu(0, 0);
    documentListeners.scroll();
    assert.equal(grid.exportMenu, null);
});

test("context menu copies selections with or without headers and selects a right-clicked cell outside the selection", async () => {
    const { grid, copied } = createGrid({ rowCount: 1, columns: 2 });
    grid.pages.set(0, { rows: [[{ displayValue: "A", isNull: false }, { displayValue: "B", isNull: false }]] });
    const row = new Element("row");
    row.dataset.row = "0";
    const cell = new Element("cell");
    cell.dataset.col = "1";
    row.appendChild(cell);
    grid.element.events.contextmenu({ target: cell, clientX: 0, clientY: 0, preventDefault() {} });
    assert.deepEqual({ ...grid.selected }, { row: 0, col: 1 });
    grid.exportMenu.children[0].events.click();
    assert.equal(copied.at(-1), "B");
    grid.selectRectangle({ row: 0, col: 0 }, { row: 0, col: 1 });
    grid.element.events.contextmenu({ target: cell, clientX: 0, clientY: 0, preventDefault() {} });
    assert.equal(grid.selectionRange.firstCol, 0, "Right-click inside a selection preserves it");
    grid.exportMenu.children[1].events.click();
    assert.equal(copied.at(-1), "col0\tcol1\r\nA\tB");
    assert.ok(grid.exportButtons.length === 0, "Copy menu does not require export support");
});

test("record actions target only the right-clicked reference, including when another cell is selected", () => {
    const reference = { dataSource: "Other", logicalName: "account", id: "123", isNull: false };
    const calls = [];
    const { grid } = createGrid({
        rowCount: 1, columns: 4,
        onOpenRecord: ref => calls.push(["open", ref]),
        recordActions: { copyUrl: ref => calls.push(["url", ref]), createSelect: ref => calls.push(["select", ref]) },
    });
    grid.pages.set(0, { rows: [[
        { displayValue: "record", isNull: false, rawObject: reference },
        { displayValue: "plain", isNull: false },
        { displayValue: "NULL", isNull: true, rawObject: reference },
        { displayValue: "null reference", isNull: false, rawObject: { ...reference, isNull: true } },
    ]] });
    grid.selected = { row: 0, col: 0 };
    for (const col of [1, 2, 3]) {
        grid.showContextMenu(0, 0, { row: 0, col });
        assert.ok(grid.exportMenu.children.slice(2, 5).every(button => button.disabled));
    }
    grid.showContextMenu(0, 0, null);
    assert.ok(grid.exportMenu.children.slice(2, 5).every(button => button.disabled), "Empty space does not use the selected reference");
    for (const item of [2, 3, 4]) {
        grid.showContextMenu(0, 0, { row: 0, col: 0 });
        assert.equal(grid.exportMenu.children[item].disabled, false);
        grid.exportMenu.children[item].events.click();
    }
    assert.deepEqual(calls, [["open", reference], ["url", reference], ["select", reference]]);
    key(grid, "ContextMenu");
    assert.ok(grid.exportMenu.children.slice(2, 5).every(button => !button.disabled), "Keyboard menu uses the active cell");
});

test("clicking a data cell prevents default mouse focus handling before focusing and rendering", () => {
    const { grid, copied } = createGrid();
    const actions = [];
    const cell = { dataset: { col: "1" }, parentElement: { dataset: { row: "0" } } };
    const target = { closest: (selector) => selector === ".grid-cell" ? cell : null };
    grid.element.focus = () => actions.push("focus");
    grid.render = () => actions.push("render");
    grid.pages.set(0, { rows: [[
        { displayValue: "first", isNull: false },
        { displayValue: "second", isNull: false },
        { displayValue: "third", isNull: false },
    ]] });

    grid.onCellClick({ target, button: 0, preventDefault: () => actions.push("preventDefault") });

    assert.deepEqual(actions, ["preventDefault", "focus", "render"]);
    assert.deepEqual({ ...grid.selected }, { row: 0, col: 1 });
    key(grid, "ArrowRight");
    key(grid, "c", { ctrlKey: true });
    assert.deepEqual(copied, ["third"]);
});

test("arrow keys move the selection and stay within the grid", () => {
    const { grid } = createGrid();
    assert.equal(key(grid, "ArrowDown"), true);
    assert.deepEqual({ ...grid.selected }, { row: 0, col: 0 }, "First navigation key selects the first cell");

    key(grid, "ArrowDown");
    key(grid, "ArrowRight");
    assert.deepEqual({ ...grid.selected }, { row: 1, col: 1 });

    key(grid, "ArrowUp");
    key(grid, "ArrowUp");
    key(grid, "ArrowLeft");
    key(grid, "ArrowLeft");
    assert.deepEqual({ ...grid.selected }, { row: 0, col: 0 });
});

test("SSMS-style navigation keys jump to edges and pages", () => {
    const { grid } = createGrid();
    grid.selected = { row: 10, col: 2 };

    key(grid, "End");
    assert.deepEqual({ ...grid.selected }, { row: 10, col: 4 });
    key(grid, "Home");
    assert.deepEqual({ ...grid.selected }, { row: 10, col: 0 });
    key(grid, "PageDown");
    assert.deepEqual({ ...grid.selected }, { row: 19, col: 0 });
    key(grid, "PageUp");
    assert.deepEqual({ ...grid.selected }, { row: 10, col: 0 });
    key(grid, "End", { ctrlKey: true });
    assert.deepEqual({ ...grid.selected }, { row: 999, col: 4 });
    key(grid, "Home", { ctrlKey: true });
    assert.deepEqual({ ...grid.selected }, { row: 0, col: 0 });
    key(grid, "ArrowDown", { ctrlKey: true });
    key(grid, "ArrowRight", { ctrlKey: true });
    assert.deepEqual({ ...grid.selected }, { row: 999, col: 4 });
});

test("moving the selection scrolls it into view vertically and horizontally", () => {
    const { grid } = createGrid();
    grid.selected = { row: 0, col: 0 };
    for (let i = 0; i < 20; i++) key(grid, "ArrowDown");
    assert.equal(grid.viewport.scrollTop, 21 * 24 - 240);

    key(grid, "ArrowRight", { ctrlKey: true });
    const right = 60 + grid.columnWidths.reduce((a, b) => a + b, 0);
    assert.equal(grid.viewport.scrollLeft, right - 300);
    assert.equal(grid.header.scrollLeft, grid.viewport.scrollLeft);

    key(grid, "Home", { ctrlKey: true });
    assert.equal(grid.viewport.scrollTop, 0);
    assert.equal(grid.viewport.scrollLeft, 0);
});

test("scrolls into view when the scroll range is scaled for very large result sets", () => {
    const { grid } = createGrid({ rowCount: 5_000_000 });
    grid.selected = { row: 0, col: 0 };
    key(grid, "ArrowDown", { ctrlKey: true });
    const first = grid.getFirstVisibleRow();
    assert.ok(first <= 4_999_999 && 4_999_999 < first + 10, `Last row should be visible, first visible row is ${first}`);

    key(grid, "PageUp");
    const first2 = grid.getFirstVisibleRow();
    assert.ok(first2 <= grid.selected.row && grid.selected.row < first2 + 10);
});

test("Ctrl+C and Ctrl+Insert copy on Windows/Linux, but Cmd+C is required on macOS", () => {
    const row = [{ displayValue: "a", isNull: false }, { displayValue: "", isNull: true }];
    const win = createGrid({ columns: 2 });
    win.grid.pages.set(0, { rows: [row] });
    win.grid.selected = { row: 0, col: 0 };
    assert.equal(key(win.grid, "c", { ctrlKey: true }), true);
    assert.equal(key(win.grid, "c", { metaKey: true }), false);
    win.grid.selected = { row: 0, col: 1 };
    key(win.grid, "Insert", { ctrlKey: true });
    key(win.grid, "C", { ctrlKey: true, shiftKey: true });
    assert.deepEqual(win.copied, ["a", "NULL", "col1\r\nNULL"]);

    const mac = createGrid({ platform: "MacIntel", columns: 2 });
    mac.grid.pages.set(0, { rows: [row] });
    mac.grid.selected = { row: 0, col: 0 };
    assert.equal(key(mac.grid, "c", { ctrlKey: true }), false);
    assert.equal(key(mac.grid, "c", { metaKey: true }), true);
    assert.deepEqual(mac.copied, ["a"]);
});

test("copy loads the selected row if it isn't cached, and handles Edit menu copy events", async () => {
    const requested = [];
    const { grid, copied, documentListeners } = createGrid({
        fetchRows: async (start, count) => {
            requested.push(start);
            return Array.from({ length: count }, (_, i) => [{ displayValue: `row${start + i}`, isNull: false }]);
        },
        columns: 1,
    });
    grid.scheduleRender = () => {};
    grid.selected = { row: 450, col: 0 };
    key(grid, "c", { ctrlKey: true });
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.deepEqual(requested, [400]);
    assert.deepEqual(copied, ["row450"]);

    let prevented = false;
    documentListeners.copy({ preventDefault() { prevented = true; } });
    assert.equal(prevented, true);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.deepEqual(copied, ["row450", "row450"]);
});

function clickCell(grid, row, col, shiftKey = false) {
    const cell = { dataset: { col: String(col) }, parentElement: { dataset: { row: String(row) } } };
    grid.onCellClick({
        button: 0, shiftKey,
        target: { closest: (selector) => selector === ".grid-cell" ? cell : null },
        preventDefault() {},
    });
}

function rowValues(row, columns = 5) {
    return Array.from({ length: columns }, (_, col) => ({ displayValue: `${row}:${col}`, isNull: false }));
}

function clickHeader(grid, cell, header = grid.header, button = 0) {
    let prevented = false;
    header.events.mousedown({
        button,
        target: { closest: (selector) => selector === ".grid-cell" ? cell : null },
        preventDefault() { prevented = true; },
    });
    return prevented;
}

test("Ctrl+A and Cmd+A select all cells using the platform shortcut without a prior selection", async () => {
    for (const platform of ["Win32", "Linux", "MacIntel"]) {
        const { grid, copied } = createGrid({
            platform, rowCount: 403, columns: 2,
            fetchRows: async (start, count) => Array.from({ length: count }, (_, i) => rowValues(start + i, 2)),
        });
        const modifiers = platform === "MacIntel" ? { metaKey: true } : { ctrlKey: true };
        assert.equal(key(grid, "a", platform === "MacIntel" ? { ctrlKey: true } : { metaKey: true }), false);
        assert.equal(grid.selected, null);
        assert.equal(key(grid, "A", modifiers), true);
        assert.deepEqual({ ...grid.selectionRange }, { firstRow: 0, lastRow: 402, firstCol: 0, lastCol: 1 });
        assert.equal(gridStatus(grid), "");
        await grid.copySelection(false);
        assert.equal(copied[0].split("\r\n").length, 403);
        assert.equal(copied[0].split("\r\n").at(-1), "402:0\t402:1");
    }
});

test("clicking a row header selects and copies every column in just that row", async () => {
    const { grid, copied } = createGrid({ fetchRows: async (start, count) =>
        Array.from({ length: count }, (_, i) => rowValues(start + i)) });
    clickCell(grid, 10, 2);
    const row = grid.renderRow(450);
    assert.equal(clickHeader(grid, row.children[0], grid.body), true);
    assert.deepEqual({ ...grid.selectionRange }, { firstRow: 450, lastRow: 450, firstCol: 0, lastCol: 4 });
    assert.equal(grid.dragging, false);
    assert.ok(grid.renderRow(450).children.slice(1).every((cell) => cell.classList.contains("selected")));
    assert.ok(grid.renderRow(451).children.slice(1).every((cell) => !cell.classList.contains("selected")));
    await grid.copySelection(false);
    assert.deepEqual(copied, ["450:0\t450:1\t450:2\t450:3\t450:4"]);
});

test("clicking a column header selects and copies that column across unloaded pages", async () => {
    const { grid, copied } = createGrid({
        rowCount: 403,
        fetchRows: async (start, count) => Array.from({ length: count }, (_, i) => rowValues(start + i)),
    });
    const header = grid.header.children[0].children[3];
    assert.equal(header.dataset.col, "2");
    assert.equal(clickHeader(grid, header), true);
    assert.deepEqual({ ...grid.selectionRange }, { firstRow: 0, lastRow: 402, firstCol: 2, lastCol: 2 });
    assert.deepEqual(grid.renderRow(400).children.slice(1).map((cell) => cell.classList.contains("selected")),
        [false, false, true, false, false]);
    await grid.copySelection(true);
    assert.equal(copied[0], ["col2", ...Array.from({ length: 403 }, (_, i) => `${i}:2`)].join("\r\n"));
});

test("the top-left corner selects all cells, retains focus, and replaces any previous selection", () => {
    const { grid } = createGrid();
    clickCell(grid, 2, 3);
    let focused = false;
    grid.element.focus = () => { focused = true; };
    assert.equal(clickHeader(grid, grid.header.children[0].children[0]), true);
    assert.equal(focused, true);
    assert.deepEqual({ ...grid.selectionRange }, { firstRow: 0, lastRow: 999, firstCol: 0, lastCol: 4 });
    clickCell(grid, 4, 1);
    assert.deepEqual({ ...grid.selectionRange }, { firstRow: 4, lastRow: 4, firstCol: 1, lastCol: 1 });
});

test("header right-clicks and resize handles do not change the selection", () => {
    const { grid } = createGrid();
    clickCell(grid, 1, 1);
    assert.equal(clickHeader(grid, grid.header.children[0].children[1], grid.header, 2), false);
    grid.header.events.mousedown({
        button: 0,
        target: { closest: (selector) => selector === ".grid-resizer" ? {} : null },
        preventDefault() { throw new Error("Resize selection handler should not run"); },
    });
    assert.deepEqual({ ...grid.selectionRange }, { firstRow: 1, lastRow: 1, firstCol: 1, lastCol: 1 });
});

test("select-all and header clicks on empty results never select invalid cells", () => {
    for (const options of [{ rowCount: 0 }, { columns: 0 }]) {
        const { grid } = createGrid(options);
        assert.equal(key(grid, "a", { ctrlKey: true }), true);
        assert.equal(grid.selectionRange, null);
        clickHeader(grid, grid.header.children[0].children[0]);
        assert.equal(grid.selectionRange, null);
        assert.doesNotMatch(gridStatus(grid), /cells selected/);
    }
});

test("Shift+click selects a rectangle in either direction and a plain click resets its anchor", () => {
    const { grid } = createGrid();
    clickCell(grid, 450, 3);
    clickCell(grid, 2, 1, true);
    assert.deepEqual({ ...grid.selectionRange }, { firstRow: 2, lastRow: 450, firstCol: 1, lastCol: 3 });
    assert.equal(gridStatus(grid), "");
    clickCell(grid, 700, 4, true);
    assert.deepEqual({ ...grid.selectionRange }, { firstRow: 450, lastRow: 700, firstCol: 3, lastCol: 4 });
    clickCell(grid, 8, 2);
    clickCell(grid, 10, 3, true);
    assert.deepEqual({ ...grid.selectionRange }, { firstRow: 8, lastRow: 10, firstCol: 2, lastCol: 3 });
});

test("Shift+navigation extends a rectangle; ordinary navigation collapses it", () => {
    const { grid } = createGrid();
    clickCell(grid, 1, 1);
    key(grid, "ArrowRight", { shiftKey: true });
    key(grid, "ArrowDown", { shiftKey: true });
    assert.deepEqual({ ...grid.selectionRange }, { firstRow: 1, lastRow: 2, firstCol: 1, lastCol: 2 });
    key(grid, "ArrowLeft");
    assert.deepEqual({ ...grid.selectionRange }, { firstRow: 2, lastRow: 2, firstCol: 1, lastCol: 1 });
});

test("a drag can start on a record link without rerendering it before its click handler", () => {
    const { grid, documentListeners } = createGrid();
    grid.render = () => { throw new Error("The link must survive mousedown"); };
    grid.scheduleRender = () => {};
    const cell = { dataset: { col: "1" }, parentElement: { dataset: { row: "1" } } };
    grid.onCellClick({
        button: 0,
        target: { closest: (selector) => selector === ".grid-cell" ? cell : selector === ".grid-record-link" ? {} : null },
        preventDefault() {},
    });
    documentListeners.mousemove({ buttons: 1, clientX: 250, clientY: 85, preventDefault() {} });
    documentListeners.mouseup();
    assert.deepEqual({ ...grid.selectionRange }, { firstRow: 1, lastRow: 3, firstCol: 1, lastCol: 2 });
});

test("dragging extends the selection, scrolls past the viewport, and stops on mouseup or disposal", () => {
    const { grid, documentListeners, frames } = createGrid();
    grid.scheduleRender = () => {};
    clickCell(grid, 0, 0);
    documentListeners.mousemove({ buttons: 1, clientX: 200, clientY: 60, preventDefault() {} });
    assert.deepEqual({ ...grid.selectionRange }, { firstRow: 0, lastRow: 2, firstCol: 0, lastCol: 1 });
    documentListeners.mousemove({ buttons: 1, clientX: 310, clientY: 250, preventDefault() {} });
    const tick = () => {
        const [id, callback] = frames.entries().next().value;
        frames.delete(id);
        callback();
    };
    tick();
    assert.ok(grid.viewport.scrollTop > 0);
    assert.ok(grid.viewport.scrollLeft > 0);
    const previousRow = grid.selected.row;
    tick();
    assert.ok(grid.selected.row > previousRow);
    documentListeners.mouseup();
    assert.equal(frames.size, 0);
    assert.equal(documentListeners.mousemove, undefined);
    clickCell(grid, 0, 0);
    documentListeners.mousemove({ buttons: 1, clientX: 200, clientY: 60, preventDefault() {} });
    grid.dispose();
    assert.equal(frames.size, 0);
    assert.equal(documentListeners.mouseup, undefined);
});

test("range highlighting is independent of loaded pages and is restored after eviction", () => {
    const { grid } = createGrid();
    clickCell(grid, 198, 3);
    clickCell(grid, 405, 1, true);
    const assertHighlight = (rowIndex, expected) => {
        const row = grid.renderRow(rowIndex);
        assert.deepEqual(row.children.slice(1).map((cell) => cell.classList.contains("selected")), expected);
    };
    assertHighlight(197, [false, false, false, false, false]);
    assertHighlight(198, [false, true, true, true, false]);
    assertHighlight(400, [false, true, true, true, false]);
    grid.pages.set(2, { rows: Array.from({ length: 200 }, (_, i) => rowValues(400 + i)) });
    assertHighlight(405, [false, true, true, true, false]);
    grid.pages.clear();
    assertHighlight(405, [false, true, true, true, false]);
    assertHighlight(406, [false, false, false, false, false]);
    assertHighlight(198, [false, true, true, true, false]);
});

test("copy spans more pages than the cache limit without caching copy-only pages", async () => {
    const requested = [];
    const { grid, copied } = createGrid({
        rowCount: 3000,
        fetchRows: async (start, count) => {
            requested.push([start, count]);
            return Array.from({ length: count }, (_, i) => rowValues(start + i));
        },
    });
    // Keep one selected page and one visible, unselected page in the cache.
    const cached = { rows: Array.from({ length: 200 }, (_, i) => rowValues(400 + i)) };
    grid.pages.set(2, cached);
    grid.pages.set(14, { rows: Array.from({ length: 200 }, (_, i) => rowValues(2800 + i)) });
    clickCell(grid, 2401, 3);
    clickCell(grid, 198, 1, true);
    await grid.copySelection(true);
    const expected = ["col1\tcol2\tcol3"];
    for (let row = 198; row <= 2401; row++) expected.push(`${row}:1\t${row}:2\t${row}:3`);
    assert.deepEqual(copied, [expected.join("\r\n")]);
    assert.deepEqual(requested.map(([start]) => start), [0, 200, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400]);
    assert.ok(requested.every(([, count]) => count <= 200));
    assert.deepEqual([...grid.pages.keys()], [2, 14]);
    assert.equal(grid.pages.get(2), cached);
});

test("copy waits for in-flight pages and snapshots the selection while scrolling evicts them", async () => {
    let finish;
    const { grid, copied } = createGrid({
        fetchRows: async () => { throw new Error("Must reuse the pending page"); },
    });
    const page = { rows: null, pending: new Promise((resolve) => { finish = resolve; }) };
    grid.pages.set(0, page);
    clickCell(grid, 1, 1);
    clickCell(grid, 2, 2, true);
    const copying = grid.copySelection(false);
    assert.match(gridStatus(grid), /Copying/);
    grid.pages.clear();
    clickCell(grid, 20, 4);
    page.rows = Array.from({ length: 200 }, (_, i) => rowValues(i));
    finish();
    await copying;
    assert.deepEqual(copied, ["1:1\t1:2\r\n2:1\t2:2"]);
    assert.doesNotMatch(gridStatus(grid), /Copying/);
});

test("copy refreshes partially loaded pages, preserving empty values and NULLs", async () => {
    const requested = [];
    const { grid, copied } = createGrid({
        rowCount: 3,
        fetchRows: async (start, count) => {
            requested.push([start, count]);
            return [
                rowValues(0),
                [{ displayValue: "", isNull: false }, { displayValue: "", isNull: true }],
                [{ displayValue: "x", isNull: false }, { displayValue: "y", isNull: false }],
            ];
        },
    });
    grid.pages.set(0, { rows: [rowValues(0)], requestedCount: 1 });
    clickCell(grid, 1, 0);
    clickCell(grid, 2, 1, true);
    await grid.copySelection(false);
    assert.deepEqual(requested, [[0, 3]]);
    assert.deepEqual(copied, ["\tNULL\r\nx\ty"]);
});

test("range copying includes a partial final page and only the selected columns", async () => {
    const requested = [];
    const { grid, copied } = createGrid({
        rowCount: 403,
        fetchRows: async (start, count) => {
            requested.push([start, count]);
            return Array.from({ length: count }, (_, i) => rowValues(start + i));
        },
    });
    clickCell(grid, 399, 2);
    clickCell(grid, 402, 3, true);
    await grid.copySelection(false);
    assert.deepEqual(requested, [[200, 200], [400, 3]]);
    assert.deepEqual(copied, ["399:2\t399:3\r\n400:2\t400:3\r\n401:2\t401:3\r\n402:2\t402:3"]);
});

test("repeated copy shortcuts don't start overlapping requests", async () => {
    let finish;
    let requests = 0;
    const { grid, copied } = createGrid({
        fetchRows: () => {
            requests++;
            return new Promise((resolve) => { finish = resolve; });
        },
    });
    clickCell(grid, 1, 0);
    clickCell(grid, 2, 1, true);
    const copying = grid.copySelection(false);
    key(grid, "c", { ctrlKey: true });
    assert.equal(requests, 1);
    finish(Array.from({ length: 200 }, (_, i) => rowValues(i)));
    await copying;
    assert.deepEqual(copied, ["1:0\t1:1\r\n2:0\t2:1"]);
});

test("copy failures or incomplete pages never write a partial selection to the clipboard", async () => {
    for (const fetchRows of [
        async () => { throw new Error("Fetch failed"); },
        async () => [],
    ]) {
        const { grid, copied, errors } = createGrid({ fetchRows });
        grid.pages.set(0, { rows: Array.from({ length: 200 }, (_, i) => rowValues(i)) });
        clickCell(grid, 199, 0);
        clickCell(grid, 200, 1, true);
        await grid.copySelection(false);
        assert.deepEqual(copied, []);
        assert.equal(errors.length, 1);
        assert.match(gridStatus(grid), /Copy failed:/);
        assert.equal(grid.copying, false);
    }
});

test("disposing while copying prevents a stale clipboard write", async () => {
    let finish;
    const { grid, copied } = createGrid({ fetchRows: () => new Promise((resolve) => { finish = resolve; }) });
    clickCell(grid, 1, 0);
    const copying = grid.copySelection(false);
    grid.dispose();
    finish(Array.from({ length: 200 }, (_, i) => rowValues(i)));
    await copying;
    assert.deepEqual(copied, []);
});
