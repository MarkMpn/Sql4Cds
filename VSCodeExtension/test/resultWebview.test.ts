import assert from "node:assert/strict";
import test from "node:test";
import { JSDOM, VirtualConsole } from "jsdom";
import type * as vscode from "vscode";
import { resultsHtml } from "../src/resultWebview";

function grid(rowCount = 3) {
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
  emit({ type: "state", ownerUri: "untitled:query", runId: 1, status: "completed", results: [result], messages: [] });
  const page = (index: number, version = 0, rows?: unknown[][]) => emit({ type: "page", runId: 1, key: "0:0", page: index, start: index * 200, rows: rows ?? Array.from({ length: Math.min(200, rowCount - index * 200) }, (_, i) => [`row ${index * 200 + i}`, "value"]), displayRows: rowCount, totalRows: rowCount, viewVersion: version });
  page(0);
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
