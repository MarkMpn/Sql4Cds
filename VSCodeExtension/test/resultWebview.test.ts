import assert from "node:assert/strict";
import test from "node:test";
import { buildSync } from "esbuild";
import { JSDOM, VirtualConsole } from "jsdom";
import type * as vscode from "vscode";
import { resultsHtml } from "../src/resultWebview";
import type { ResultsState } from "../src/webview/protocol";

const script = buildSync({
  entryPoints: ["src/webview/results.ts"], bundle: true, write: false, platform: "browser",
  format: "iife", define: { PLAN_IMAGE_NAMES: '["FetchXmlScan","FallbackNode"]' }
}).outputFiles[0].text;
const graph = {
  query: "SELECT name FROM account",
  root: {
    id: 1, type: "FetchXmlScan", nodeType: "FetchXmlScan", name: "FetchXML Scan", cost: 1,
    properties: [{ name: "FetchXML", value: "<fetch><entity name='account'/></fetch>" }]
  }
};
const result = {
  key: "0:0", rowCount: 3, displayRowCount: 3, complete: true,
  columns: ["name", "value"], truncated: false
};

function fixture() {
  const sent: Record<string, any>[] = [];
  const errors: Error[] = [];
  const console = new VirtualConsole();
  console.on("jsdomError", error => errors.push(error));
  const html = resultsHtml({ cspSource: "https://webview.invalid" } as vscode.Webview,
    { script: "https://webview.invalid/results.js", styles: "https://webview.invalid/results.css", images: "https://webview.invalid/plan-images" });
  const dom = new JSDOM(html.replace(/<script([^>]*) src="[^"]*"><\/script>/, (_, attrs) => `<script${attrs}>${script}</script>`), {
    url: "https://webview.invalid",
    runScripts: "dangerously", pretendToBeVisual: true, virtualConsole: console,
    beforeParse(window) {
      (window as any).acquireVsCodeApi = () => ({ postMessage: (message: unknown) => sent.push(JSON.parse(JSON.stringify(message))) });
      (window as any).ResizeObserver = class { observe() {} disconnect() {} };
      window.HTMLCanvasElement.prototype.getContext = () => null;
      window.HTMLElement.prototype.scrollIntoView = () => {};
      Object.defineProperty(window.HTMLElement.prototype, "clientHeight", { get: () => 240 });
      Object.defineProperty(window.HTMLElement.prototype, "clientWidth", { get: () => 800 });
    }
  });
  const { window } = dom;
  const emit = (message: unknown, origin = window.location.origin) =>
    window.dispatchEvent(new window.MessageEvent("message", { data: message, origin }));
  const state = (overrides: Partial<ResultsState> = {}, origin = window.location.origin) => emit({
    type: "state", ownerUri: "untitled:query", runId: 1, status: "completed", estimatedPlan: false,
    results: [result], messages: [], plans: [], planVersion: 1, autoSizeColumns: true, ...overrides
  }, origin);
  const respond = (request: Record<string, any>, value?: unknown, error?: string) => emit({
    type: "response", requestId: request.requestId, value, error
  });
  const tick = () => new Promise(resolve => setTimeout(resolve, 40));
  return {
    window, sent, emit, state, respond, tick,
    close() { dom.window.close(); assert.deepEqual(errors, []); }
  };
}

for (const origin of ["https://untrusted.invalid", "", "null"]) {
  test(`messages from untrusted origin ${JSON.stringify(origin)} cannot update, clear or respond`, async () => {
    const ui = fixture();
    try {
      const document = ui.window.document;
      ui.state({}, origin);
      assert.equal(document.getElementById("empty-state")!.hidden, false);
      assert.equal(document.querySelectorAll(".results-grid").length, 0);
      assert.equal(ui.sent.filter(message => message.type === "rows").length, 0);

      ui.state();
      await ui.tick();
      const request = ui.sent.find(message => message.type === "rows")!;
      assert.ok(request);
      assert.equal(document.getElementById("empty-state")!.hidden, true);
      assert.equal(document.querySelectorAll(".results-grid").length, 1);

      ui.emit({ type: "empty" }, origin);
      assert.equal(document.getElementById("empty-state")!.hidden, true);
      assert.equal(document.querySelectorAll(".results-grid").length, 1);

      ui.emit({
        type: "response", requestId: request.requestId,
        value: [[{ displayValue: "untrusted row", isNull: false }]]
      }, origin);
      await ui.tick();
      assert.ok(!document.querySelector(".grid-body")?.textContent?.includes("untrusted row"));
      ui.respond(request, [[{ displayValue: "trusted row", isNull: false }]]);
      await ui.tick();
      assert.ok(document.querySelector(".grid-body")?.textContent?.includes("trusted row"));

      ui.emit({ type: "empty" });
      assert.equal(document.getElementById("empty-state")!.hidden, false);
      assert.equal(document.querySelectorAll(".results-grid").length, 0);
    } finally { ui.close(); }
  });
}

for (const estimatedPlan of [false, true]) {
  for (const hasRows of [false, true]) {
    for (const error of [false, true]) {
      test(`default pane: estimated=${estimatedPlan}, results=${hasRows}, error=${error}`, () => {
        const ui = fixture();
        try {
          ui.state({
            estimatedPlan, results: hasRows ? [result] : [], plans: [{ key: "0:1", graphs: [graph] }],
            status: error ? "failed" : "completed",
            messages: error ? [{ isError: true, message: "Query failed" }] : []
          });
          const expected = error ? "messages" : estimatedPlan ? "plan" : hasRows ? "results" : "messages";
          assert.equal(ui.window.document.querySelector('[role="tab"][aria-selected="true"]')?.id, `${expected}-tab`);
          assert.equal(ui.window.document.querySelectorAll(".plan-node").length, 1);
          assert.equal(ui.window.document.getElementById("plan-tab")!.hidden, false);
          assert.equal(ui.window.document.getElementById("results-tab")!.hidden, !hasRows);
          assert.equal(ui.window.document.querySelectorAll(".results-grid").length, hasRows ? 1 : 0);
        } finally { ui.close(); }
      });
    }
  }
}

test("multiple result sets are stacked in one Results pane with bounded row requests", async () => {
  const ui = fixture();
  try {
    ui.state({ results: [result, { ...result, key: "0:1" }] });
    await ui.tick();
    assert.equal(ui.window.document.querySelectorAll(".results-grid").length, 2);
    assert.equal(ui.window.document.querySelectorAll('[role="tab"]').length, 3);
    const requests = ui.sent.filter(message => message.type === "rows");
    assert.equal(requests.length, 2);
    assert.ok(requests.every(request => request.start === 0 && request.count <= 200 && request.runId === 1));
    for (const request of requests) ui.respond(request, [
      [{ displayValue: "<script>unsafe</script>", isNull: false }, { displayValue: "NULL", isNull: true }]
    ]);
    await ui.tick();
    assert.equal(ui.window.document.querySelector(".grid-cell.null")?.textContent, "NULL");
    assert.ok(ui.window.document.querySelector(".grid-body")?.textContent?.includes("<script>unsafe</script>"));
    assert.equal(ui.window.document.querySelectorAll(".grid-body script").length, 0);
  } finally { ui.close(); }
});

test("grid context menu exports the chosen format directly and keyboard copy uses the host", async () => {
  const ui = fixture();
  try {
    ui.state();
    await ui.tick();
    const request = ui.sent.find(message => message.type === "rows")!;
    ui.respond(request, Array.from({ length: 3 }, (_, index) => [
      { displayValue: `row ${index}`, isNull: false }, { displayValue: "NULL", isNull: true }
    ]));
    await ui.tick();
    const grid = ui.window.document.querySelector(".results-grid") as HTMLElement;
    grid.focus();
    grid.dispatchEvent(new ui.window.KeyboardEvent("keydown", { key: "a", ctrlKey: true, bubbles: true }));
    grid.dispatchEvent(new ui.window.KeyboardEvent("keydown", { key: "c", ctrlKey: true, bubbles: true }));
    await ui.tick();
    const copy = ui.sent.find(message => message.type === "copy")!;
    assert.equal(copy.text, "row 0\tNULL\r\nrow 1\tNULL\r\nrow 2\tNULL");
    ui.respond(copy);
    grid.dispatchEvent(new ui.window.MouseEvent("contextmenu", { bubbles: true }));
    const csv = [...ui.window.document.querySelectorAll<HTMLButtonElement>('[role="menuitem"]')]
      .find(button => button.textContent === "Export to CSV...")!;
    assert.equal(csv.disabled, false);
    csv.click();
    const exported = ui.sent.find(message => message.type === "export")!;
    assert.equal(exported.format, "Csv");
    assert.equal(exported.key, "0:0");
    ui.respond(exported);
  } finally { ui.close(); }
});

test("plan properties, fallback icons and FetchXML inspection use the shared renderer", () => {
  const ui = fixture();
  try {
    ui.state({ estimatedPlan: true, results: [], plans: [{ key: "0:0", graphs: [graph] }] });
    const node = ui.window.document.querySelector(".plan-node") as HTMLElement;
    node.dispatchEvent(new ui.window.MouseEvent("pointerdown", { bubbles: true, button: 0 }));
    assert.ok(ui.window.document.querySelector(".properties-grid")?.textContent?.includes("FetchXML"));
    node.dispatchEvent(new ui.window.MouseEvent("dblclick", { bubbles: true }));
    const request = ui.sent.find(message => message.type === "openFetchXml")!;
    assert.equal(request.text, "<fetch><entity name='account'/></fetch>");
    ui.respond(request);
    assert.ok((ui.window.document.querySelector(".plan-node-icon") as HTMLImageElement).src.endsWith("/FetchXmlScan.ico"));
    ui.state({
      planVersion: 2, estimatedPlan: true, results: [],
      plans: [{ key: "0:0", graphs: [{ ...graph, root: { ...graph.root, nodeType: "UnknownNode" } }] }]
    });
    assert.ok((ui.window.document.querySelector(".plan-node-icon") as HTMLImageElement).src.endsWith("/FallbackNode.ico"));
  } finally { ui.close(); }
});

test("row retrieval failures are visible and do not start a render/request retry loop", async () => {
  const ui = fixture();
  try {
    ui.state();
    await ui.tick();
    ui.respond(ui.sent.find(message => message.type === "rows")!, undefined, "Subset request failed");
    await ui.tick();
    assert.match(ui.window.document.getElementById("status")!.textContent!, /Subset request failed/);
    assert.equal(ui.window.document.querySelector(".grid-body .grid-cell[data-col]")?.textContent, "Error");
    await ui.tick();
    assert.equal(ui.sent.filter(message => message.type === "rows").length, 1);
  } finally { ui.close(); }
});

test("completion selects results after actual plans; subsequent updates preserve manual tab choice", () => {
  const ui = fixture();
  try {
    ui.state({ status: "running", plans: [{ key: "0:1", graphs: [graph] }] });
    ui.state({ status: "completed", plans: [{ key: "0:1", graphs: [graph] }] });
    assert.equal(ui.window.document.getElementById("results")!.hidden, false);
    ui.window.document.getElementById("plan-tab")!.click();
    ui.state({ plans: [{ key: "0:1", graphs: [graph] }] });
    assert.equal(ui.window.document.getElementById("plan")!.hidden, false);
    ui.state({ runId: 2, results: [], plans: [], status: "running" });
    assert.equal(ui.window.document.getElementById("plan-tab")!.hidden, true);
    assert.equal(ui.window.document.querySelectorAll(".results-grid").length, 0);
    assert.equal(ui.window.document.getElementById("messages")!.hidden, false);
  } finally { ui.close(); }
});

test("empty and editor switching discard stale rows, grids and execution plans", async () => {
  const ui = fixture();
  try {
    ui.state({ plans: [{ key: "0:1", graphs: [graph] }] });
    await ui.tick();
    const request = ui.sent.find(message => message.type === "rows")!;
    ui.emit({ type: "empty" });
    ui.respond(request, [[{ displayValue: "stale", isNull: false }]]);
    await ui.tick();
    assert.equal(ui.window.document.querySelectorAll(".results-grid,.plan-node").length, 0);
    const empty = ui.window.document.getElementById("empty-state")!;
    assert.equal(empty.hidden, false);
    assert.equal(ui.window.document.querySelector<HTMLElement>(".query-results")!.hidden, true);
    assert.equal(empty.querySelector("h1")!.textContent, "No results for the active editor");
    ui.state({ ownerUri: "untitled:second", results: [] });
    assert.equal(empty.hidden, true);
    assert.equal(ui.window.document.getElementById("messages")!.hidden, false);
  } finally { ui.close(); }
});

test("new editors show the shared no-results state and can hide the panel", () => {
  const ui = fixture();
  try {
    const document = ui.window.document;
    const empty = document.getElementById("empty-state")!;
    assert.equal(empty.hidden, false);
    assert.equal(document.querySelector<HTMLElement>(".query-results")!.hidden, true);
    assert.equal(empty.querySelector("h1")!.textContent, "No results for the active editor");
    assert.equal(empty.querySelector("p")!.textContent,
      "Run a query in the current editor, or switch to an editor that has results");
    document.getElementById("hide-panel")!.click();
    assert.deepEqual(ui.sent.at(-1), { type: "hidePanel" });
    ui.state();
    assert.equal(empty.hidden, true);
    assert.equal(document.querySelector<HTMLElement>(".query-results")!.hidden, false);
  } finally { ui.close(); }
});

test("Messages pane shows only server messages without a client-generated summary", () => {
  const ui = fixture();
  try {
    ui.state({
      results: [], plans: [],
      messages: [{ isError: false, message: "Commands completed successfully." }, { isError: false, message: "Completion time: 2026-10-26T19:48:24.6811234+00:00" }]
    });
    assert.deepEqual([...ui.window.document.querySelectorAll("#messages .message")].map(node => node.textContent),
      ["Commands completed successfully.", "Completion time: 2026-10-26T19:48:24.6811234+00:00"]);
  } finally { ui.close(); }
});

test("Results tab appears only once a running query returns a result set", () => {
  const ui = fixture();
  try {
    const document = ui.window.document;
    ui.state({ status: "running", results: [], plans: [] });
    assert.equal(document.getElementById("results-tab")!.hidden, true);
    assert.equal(document.querySelector('[role="tab"][aria-selected="true"]')?.id, "messages-tab");
    ui.state({ status: "running", plans: [] });
    assert.equal(document.getElementById("results-tab")!.hidden, false);
    assert.equal(document.querySelector('[role="tab"][aria-selected="true"]')?.id, "results-tab");
  } finally { ui.close(); }
});

test("switching editors cancels pending actions without reporting an error in the new view", async () => {
  const ui = fixture();
  try {
    ui.state({ estimatedPlan: true, results: [], plans: [{ key: "0:0", graphs: [graph] }] });
    ui.window.document.querySelector(".plan-node")!.dispatchEvent(new ui.window.MouseEvent("dblclick", { bubbles: true }));
    assert.ok(ui.sent.some(message => message.type === "openFetchXml"));
    ui.emit({ type: "empty" });
    await ui.tick();
    assert.equal(ui.sent.some(message => message.type === "error"), false);
    assert.equal(ui.window.document.getElementById("status")!.textContent, "");
  } finally { ui.close(); }
});
