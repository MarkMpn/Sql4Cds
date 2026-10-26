import { ResultsGrid } from "../../../MarkMpn.Sql4Cds.PPTB/src/query/resultsGrid";
import { ExecutionPlanView } from "../../../MarkMpn.Sql4Cds.PPTB/src/executionPlan/executionPlanView";
import { getNodeImageName } from "../../../MarkMpn.Sql4Cds.PPTB/src/executionPlan/nodeImageName";
import { PropertiesPanel } from "../../../MarkMpn.Sql4Cds.PPTB/src/propertiesPanel";
import type { DbCellValue, ExportFormat } from "../../../MarkMpn.Sql4Cds.PPTB/src/resultsProtocol";
import type { ResultsState, ResultResponse } from "./protocol";

declare function acquireVsCodeApi(): { postMessage(message: unknown): void };
declare const PLAN_IMAGE_NAMES: string[];
const vscode = acquireVsCodeApi();
const element = (id: string) => document.getElementById(id)!;
const results = element("results");
const messages = element("messages");
const plan = element("plan");
const status = element("status");
const emptyState = element("empty-state");
const queryResults = document.querySelector<HTMLElement>(".query-results")!;
const panes = { results, messages, plan };
type Pane = keyof typeof panes;
let active: Pane = "messages";
let state: ResultsState | undefined;
let selectedByUser = false;
let nextRequest = 0;
const grids = new Map<string, ResultsGrid>();
let planVersion = -1;
const pending = new Map<number, { resolve(value: unknown): void; reject(error: Error): void }>();
class ViewChangedError extends Error {}
const properties = new PropertiesPanel(element("properties"), element("properties-title"));
const planView = new ExecutionPlanView(
  node => `${document.body.dataset.images}/${getNodeImageName(node, name => PLAN_IMAGE_NAMES.includes(name))}.ico`,
  node => properties.show(node?.name ?? null, node?.properties ?? null),
  undefined,
  text => void request("openFetchXml", { text }).catch(showError)
);
element("plan-canvas").append(planView.element);

function select(pane: Pane): void {
  active = pane;
  for (const name of Object.keys(panes) as Pane[]) {
    panes[name].hidden = name !== pane;
    const tab = element(`${name}-tab`);
    tab.classList.toggle("active", name === pane);
    tab.setAttribute("aria-selected", String(name === pane));
    tab.tabIndex = name === pane ? 0 : -1;
  }
}

for (const name of Object.keys(panes) as Pane[]) {
  const tab = element(`${name}-tab`);
  tab.addEventListener("click", () => { selectedByUser = true; select(name); });
  tab.addEventListener("keydown", event => {
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
    event.preventDefault();
    const names = (Object.keys(panes) as Pane[]).filter(key => !element(`${key}-tab`).hidden);
    const index = names.indexOf(name);
    const next = event.key === "Home" ? 0 : event.key === "End" ? names.length - 1
      : (index + (event.key === "ArrowRight" ? 1 : -1) + names.length) % names.length;
    selectedByUser = true;
    select(names[next]);
    element(`${names[next]}-tab`).focus();
  });
}

function request<T = void>(type: string, params: Record<string, unknown>): Promise<T> {
  const requestId = ++nextRequest;
  return new Promise<T>((resolve, reject) => {
    pending.set(requestId, { resolve: value => resolve(value as T), reject });
    vscode.postMessage({ ...params, type, requestId, ownerUri: state?.ownerUri, runId: state?.runId });
  });
}

function showError(error: unknown): void {
  if (error instanceof ViewChangedError) return;
  status.textContent = error instanceof Error ? error.message : String(error);
  status.classList.add("error");
  vscode.postMessage({ type: "error", text: status.textContent });
}

function reset(): void {
  for (const request of pending.values()) request.reject(new ViewChangedError("The query result view changed."));
  pending.clear();
  for (const grid of grids.values()) grid.dispose();
  grids.clear();
  planVersion = -1;
  results.replaceChildren();
  messages.replaceChildren();
  planView.clear();
  element("results-tab").hidden = true;
  element("plan-tab").hidden = true;
  status.textContent = "";
  selectedByUser = false;
  select("messages");
}

function update(next: ResultsState): void {
  const changed = next.ownerUri !== state?.ownerUri || next.runId !== state?.runId;
  if (changed) reset();
  const wasRunning = state?.status === "running" || state?.status === "cancelling";
  state = next;
  const running = next.status === "running" || next.status === "cancelling";
  for (const result of next.results) {
    let grid = grids.get(result.key);
    if (!grid) {
      grid = new ResultsGrid(
        result.columns.map(columnName => ({ columnName })),
        (start, count) => request<DbCellValue[][]>("rows", { key: result.key, start, count }),
        text => void request("copy", { text }).catch(showError),
        reference => void request("openRecord", { reference }).catch(showError),
        (text, isError) => { status.textContent = text; status.classList.toggle("error", isError); },
        (format: ExportFormat) => request("export", { key: result.key, format }),
        undefined,
        next.autoSizeColumns
      );
      grids.set(result.key, grid);
      results.append(grid.element);
    }
    grid.setRowCount(result.displayRowCount, result.complete);
    grid.setExportEnabled(!running && result.complete);
  }
  results.dataset.count = String(grids.size);
  element("results-tab").hidden = grids.size === 0;
  if (planVersion !== next.planVersion) {
    planVersion = next.planVersion;
    planView.clear();
    for (const entry of next.plans) {
      for (const graph of entry.graphs) planView.addPlan(graph);
    }
  }
  element("plan-tab").hidden = planView.count === 0;
  const nodes = next.messages.map(message => {
    const node = document.createElement("div");
    node.className = message.isError ? "message error" : "message";
    node.textContent = message.message;
    return node;
  });
  for (const result of next.results.filter(result => result.truncated)) {
    const limit = document.createElement("div");
    limit.className = "message summary";
    limit.textContent = `Displaying the first ${result.displayRowCount.toLocaleString()} of ${result.rowCount.toLocaleString()} rows. Export includes all rows.`;
    nodes.push(limit);
  }
  messages.replaceChildren(...nodes);
  const hasErrors = next.status === "failed" || next.messages.some(message => message.isError);
  if (hasErrors) select("messages");
  else if (!running && (changed || wasRunning)) {
    select(next.estimatedPlan && planView.count ? "plan" : grids.size ? "results" : "messages");
  } else if (!selectedByUser && running && grids.size) select("results");
  else select(active);
}

function showEmpty(): void {
  reset();
  state = undefined;
  emptyState.hidden = false;
  queryResults.hidden = true;
}

window.addEventListener("message", event => {
  const message = event.data as ResultsState | ResultResponse | { type: "empty" };
  if (message.type === "response") {
    const request = pending.get(message.requestId);
    if (!request) return;
    pending.delete(message.requestId);
    if (message.error) request.reject(new Error(message.error));
    else request.resolve(message.value);
  } else if (message.type === "state") {
    emptyState.hidden = true;
    queryResults.hidden = false;
    update(message);
  } else if (message.type === "empty") showEmpty();
});
element("hide-panel").addEventListener("click", () => vscode.postMessage({ type: "hidePanel" }));
showEmpty();
vscode.postMessage({ type: "ready" });
