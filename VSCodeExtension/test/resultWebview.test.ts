import assert from "node:assert/strict";
import test from "node:test";
import { JSDOM, VirtualConsole } from "jsdom";
import type * as vscode from "vscode";
import { resultsHtml } from "../src/resultWebview";

function grid(rowCount = 3, options: { autoSizeColumns?: boolean; initialRows?: unknown[][] } = {}) {
  const sent: any[] = [];
  const errors: Error[] = [];
  const console = new VirtualConsole();
  console.on("jsdomError", error => errors.push(error));
  const dom = new JSDOM(resultsHtml({ cspSource: "https://webview.invalid" } as vscode.Webview), {
    runScripts: "dangerously", virtualConsole: console,
    beforeParse(window) {
      (window as any).acquireVsCodeApi = () => ({ postMessage: (message: any) => sent.push(JSON.parse(JSON.stringify(message))) });
      window.HTMLElement.prototype.scrollIntoView = () => {};
    }
  });
  const { window } = dom;
  const emit = (message: unknown) => window.dispatchEvent(new window.MessageEvent("message", { data: message }));
  const result = { key: "0:0", ordinal: 1, rowCount, displayRowCount: rowCount, complete: true, columns: ["name", "value"], pageSize: 200 };
  emit({
    type: "state",
    ownerUri: "untitled:query",
    runId: 1,
    status: "completed",
    autoSizeColumns: options.autoSizeColumns,
    results: [result],
    messages: []
  });
  const page = (index: number, version = 0, rows?: unknown[][]) => emit({ type: "page", runId: 1, key: "0:0", page: index, start: index * 200, rows: rows ?? Array.from({ length: Math.min(200, rowCount - index * 200) }, (_, i) => [`row ${index * 200 + i}`, "value"]), displayRows: rowCount, totalRows: rowCount, viewVersion: version });
  page(0, 0, options.initialRows);
  const key = (value: string, options = {}) => window.document.activeElement!.dispatchEvent(new window.KeyboardEvent("keydown", { key: value, bubbles: true, ...options }));
  return { window, sent, errors, page, key, close: () => { dom.window.close(); assert.deepEqual(errors, []); } };
}

test("select all keeps focus and immediate keyboard copy includes the entire result", () => {
  const ui = grid();
  try {
    (ui.window.document.querySelector(".grid") as HTMLElement).focus();
    ui.key("a", { ctrlKey: true });
    assert.equal(ui.window.document.activeElement?.className, "grid");
    ui.key("c", { ctrlKey: true });
    assert.deepEqual(ui.sent.at(-1).selection.ranges, [{ rowStart: 0, rowEnd: 2, columnStart: 0, columnEnd: 1 }]);
  } finally { ui.close(); }
});

test("arrow navigation keeps keyboard focus across an uncached page boundary", () => {
  const ui = grid(201);
  try {
    ui.window.document.querySelector('td[data-row="199"][data-column="0"]')!.dispatchEvent(new ui.window.MouseEvent("pointerdown", { bubbles: true, button: 0 }));
    ui.key("ArrowDown");
    assert.equal(ui.sent.at(-1).page, 1);
    ui.page(1);
    assert.equal(ui.window.document.activeElement?.className, "grid");
    ui.key("c", { metaKey: true });
    assert.equal(ui.sent.at(-1).selection.ranges[0].rowStart, 200);
  } finally { ui.close(); }
});

test("quick search retains its input while loading and after a response", async () => {
  const ui = grid();
  try {
    const input = ui.window.document.querySelector("input")!;
    input.focus(); input.value = "first";
    input.dispatchEvent(new ui.window.Event("input"));
    await new Promise(resolve => setTimeout(resolve, 280));
    assert.equal(ui.sent.at(-1).searchText, "first");
    assert.equal(ui.window.document.activeElement, input);
    input.value = "first second";
    input.dispatchEvent(new ui.window.Event("input"));
    ui.page(0, 1);
    assert.equal((ui.window.document.activeElement as HTMLInputElement).value, "first second");
    await new Promise(resolve => setTimeout(resolve, 280));
    assert.equal(ui.sent.at(-1).searchText, "first second");
  } finally { ui.close(); }
});

test("quick search preserves grid scroll position after refresh", async () => {
  const ui = grid(1000);
  try {
    const gridElement = ui.window.document.querySelector(".grid") as HTMLElement;
    gridElement.scrollLeft = 137;
    gridElement.scrollTop = 88;

    const input = ui.window.document.querySelector("input")!;
    input.value = "refresh";
    input.dispatchEvent(new ui.window.Event("input"));
    await new Promise(resolve => setTimeout(resolve, 280));

    ui.page(0, 1);

    const refreshedGrid = ui.window.document.querySelector(".grid") as HTMLElement;
    assert.equal(refreshedGrid.scrollLeft, 137);
    assert.equal(refreshedGrid.scrollTop, 88);
  } finally { ui.close(); }
});

test("quick search reuses existing table cell nodes after refresh", async () => {
  const ui = grid(1000);
  try {
    const originalCell = ui.window.document.querySelector('td[data-row="0"][data-column="0"]')!;

    const input = ui.window.document.querySelector("input")!;
    input.value = "reuse";
    input.dispatchEvent(new ui.window.Event("input"));
    await new Promise(resolve => setTimeout(resolve, 280));
    ui.page(0, 1);

    const refreshedCell = ui.window.document.querySelector('td[data-row="0"][data-column="0"]')!;
    assert.equal(refreshedCell, originalCell);
  } finally { ui.close(); }
});

test("sort button toggles from ascending to descending on second click", () => {
  const ui = grid();
  try {
    const sortButton = ui.window.document.querySelector('th[data-column="0"] .sort') as HTMLElement;
    sortButton.click();
    assert.equal(ui.sent.at(-1).sort.direction, "asc");

    ui.page(0, 1);

    const updatedSortButton = ui.window.document.querySelector('th[data-column="0"] .sort') as HTMLElement;
    updatedSortButton.click();
    assert.equal(ui.sent.at(-1).sort.direction, "desc");
  } finally { ui.close(); }
});

test("auto-sized columns use visible content for their initial width", () => {
  const ui = grid(1, { autoSizeColumns: true, initialRows: [["x".repeat(40), "value"]] });
  try {
    const header = ui.window.document.querySelector('th[data-column="0"]') as HTMLElement;
    const cell = ui.window.document.querySelector('td[data-column="0"]') as HTMLElement;

    assert.equal(header.style.width, "344px");
    assert.equal(cell.style.width, "344px");
  } finally { ui.close(); }
});

test("disabling auto-size uses fixed-width columns but keeps resize handles active", () => {
  const ui = grid(3, { autoSizeColumns: false });
  try {
    const header = ui.window.document.querySelector('th[data-column="0"]') as HTMLElement;
    const cell = ui.window.document.querySelector('td[data-column="0"]') as HTMLElement;
    const handle = ui.window.document.querySelector('th[data-column="0"] .resize-handle') as HTMLElement;

    assert.equal(header.style.width, "180px");
    assert.equal(cell.style.width, "180px");
    assert.equal(handle.style.display, "");
  } finally { ui.close(); }
});

test("dragging a resize handle updates the visible column width even when auto-size is disabled", () => {
  const ui = grid(3, { autoSizeColumns: false });
  try {
    const header = ui.window.document.querySelector('th[data-column="0"]') as HTMLElement;
    const cell = ui.window.document.querySelector('td[data-column="0"]') as HTMLElement;
    const table = ui.window.document.querySelector("table") as HTMLElement;
    const handle = ui.window.document.querySelector('th[data-column="0"] .resize-handle') as HTMLElement;

    Object.defineProperty(header, "getBoundingClientRect", {
      configurable: true,
      value: () => ({ width: 180, height: 26, top: 0, left: 0, right: 180, bottom: 26, x: 0, y: 0, toJSON: () => ({}) })
    });

    handle.dispatchEvent(new ui.window.MouseEvent("pointerdown", { bubbles: true, button: 0, clientX: 100 }));
    ui.window.dispatchEvent(new ui.window.MouseEvent("pointermove", { bubbles: true, clientX: 140 }));
    ui.window.dispatchEvent(new ui.window.MouseEvent("pointerup", { bubbles: true, clientX: 140 }));

    assert.equal(header.style.width, "220px");
    assert.equal(cell.style.width, "220px");
    assert.equal(table.style.width, "452px");
  } finally { ui.close(); }
});

test("result values render as text and export messages identify their query run", () => {
  const ui = grid();
  try {
    const value = '<img src=x onerror="alert(1)">';
    ui.page(0, 0, [[value, "value"]]);
    assert.equal(ui.window.document.querySelector("td")!.textContent, value);
    assert.equal(ui.window.document.querySelector("img"), null);
    (ui.window.document.querySelector('[title="Export full result set…"]') as HTMLElement).click();
    assert.equal(ui.sent.at(-1).runId, 1);
  } finally { ui.close(); }
});
