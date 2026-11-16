import assert from "node:assert/strict";
import test from "node:test";
import { JSDOM } from "jsdom";
import { PropertiesPanel } from "../src/webview/propertiesPanel";

function fixture() {
  const dom = new JSDOM('<!doctype html><body><button>Outside</button><div id="title"></div><div id="grid"></div>');
  const { document } = dom.window;
  const grid = document.getElementById("grid")!;
  const title = document.getElementById("title")!;
  const panel = new PropertiesPanel(grid, title);
  const rows = () => [...grid.querySelectorAll<HTMLElement>(".property-row")];
  const key = (key: string) => {
    const event = new dom.window.KeyboardEvent("keydown", { key, bubbles: true, cancelable: true });
    document.activeElement!.dispatchEvent(event);
    return event;
  };
  return { dom, document, grid, title, panel, rows, key };
}

const properties = () => [
  { name: "Last", value: "last", displayOrder: 2 },
  { name: "Parent", displayValue: "(Collection)", displayOrder: 1, value: [
    { name: "Child", value: [{ name: "Leaf", value: "raw", displayValue: "Formatted" }] },
  ] },
  { name: "First", value: "<fetch />", displayOrder: 0 },
];

test("the VS Code properties view safely renders shared sorted rows with a single tab stop", () => {
  const ui = fixture();
  try {
    const { grid, title, panel, rows } = ui;
    assert.equal(grid.getAttribute("role"), "tree");
    assert.equal(grid.tabIndex, 0);
    assert.match(grid.textContent!, /Select an execution plan node/);
    panel.show("Plan\nsecond line", []);
    assert.equal(title.textContent, "Plan");
    assert.equal(title.title, "Plan");
    assert.equal(grid.textContent, "No properties");
    const list = properties();
    panel.show("<Plan>", list);
    assert.equal(title.textContent, "<Plan>");
    assert.deepEqual(rows().map(row => row.querySelector(".property-name")!.textContent), ["First", "\u25B8Parent", "Last"]);
    assert.deepEqual(list.map(property => property.name), ["Last", "Parent", "First"]);
    assert.equal(rows()[0].querySelector(".property-value")!.textContent, "<fetch />");
    assert.equal(grid.querySelector("fetch"), null);
    assert.equal(rows()[1].querySelector(".property-value")!.getAttribute("title"), "(Collection)");
    assert.equal(rows()[1].getAttribute("aria-expanded"), "false");
    assert.equal(rows()[0].hasAttribute("aria-expanded"), false);
    assert.equal(rows()[0].getAttribute("aria-setsize"), "3");
    assert.equal(rows()[2].getAttribute("aria-posinset"), "3");
    assert.equal(grid.tabIndex, -1);
    assert.equal(rows().filter(row => row.tabIndex === 0).length, 1);
  } finally { ui.dom.window.close(); }
});

test("the VS Code properties view supports nested keyboard navigation and mouse expansion", () => {
  const ui = fixture();
  try {
    const { panel, rows, key, document } = ui;
    panel.show("Plan", properties());
    rows()[0].focus();
    assert.equal(key("ArrowDown").defaultPrevented, true);
    assert.equal(document.activeElement, rows()[1]);
    key("ArrowRight");
    assert.equal(rows()[1].getAttribute("aria-expanded"), "true");
    assert.equal(document.activeElement, rows()[1]);
    key("ArrowRight");
    assert.equal(document.activeElement, rows()[2]);
    assert.equal(rows()[2].getAttribute("aria-level"), "2");
    key("Enter");
    key("ArrowRight");
    assert.equal(document.activeElement, rows()[3]);
    assert.equal(rows()[3].querySelector(".property-value")!.textContent, "Formatted");
    key("ArrowLeft");
    assert.equal(document.activeElement, rows()[2]);
    key(" ");
    assert.equal(rows()[2].getAttribute("aria-expanded"), "false");
    key("ArrowLeft");
    assert.equal(document.activeElement, rows()[1]);
    key("End");
    assert.equal(document.activeElement, rows().at(-1));
    key("ArrowUp");
    assert.equal(document.activeElement, rows()[2]);
    key("Home");
    assert.equal(document.activeElement, rows()[0]);
    assert.equal(key("Tab").defaultPrevented, false);
    rows()[1].click();
    assert.equal(document.activeElement, rows()[1]);
    assert.equal(rows()[1].getAttribute("aria-expanded"), "false");
    assert.equal(rows().filter(row => row.tabIndex === 0).length, 1);
  } finally { ui.dom.window.close(); }
});

test("the VS Code properties view preserves unchanged data and resets changed data without stealing focus", () => {
  const ui = fixture();
  try {
    const { panel, rows, grid, title, document } = ui;
    const list = properties();
    panel.show("Plan", list);
    rows()[1].click();
    const parent = rows()[1];
    panel.show("Renamed", list);
    assert.equal(title.textContent, "Renamed");
    assert.equal(rows()[1], parent);
    assert.equal(parent.getAttribute("aria-expanded"), "true");
    panel.show("Replacement", properties());
    assert.equal(document.activeElement, rows()[0]);
    assert.equal(rows()[1].getAttribute("aria-expanded"), "false");
    const outside = document.querySelector("button")!;
    outside.focus();
    panel.show("Background", properties());
    assert.equal(document.activeElement, outside);
    rows()[0].focus();
    panel.show(null, null);
    assert.equal(document.activeElement, grid);
    assert.equal(grid.tabIndex, 0);
    assert.equal(title.textContent, "");
    assert.match(grid.textContent!, /Select an execution plan node/);
  } finally { ui.dom.window.close(); }
});
