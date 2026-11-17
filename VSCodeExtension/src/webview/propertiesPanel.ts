import { propertyRows, type PropertyRow } from "../../../MarkMpn.Sql4Cds.PPTB/src/executionPlan/propertyRows";
import type { ExecutionPlanGraphProperty } from "../../../MarkMpn.Sql4Cds.PPTB/src/resultsProtocol";

export class PropertiesPanel {
  private properties: ExecutionPlanGraphProperty[] | null = null;
  private readonly expanded = new Set<string>();
  private selectedKey = "";
  private rows: PropertyRow[] = [];

  constructor(private readonly grid: HTMLElement, private readonly title: HTMLElement) {
    grid.setAttribute("role", "tree");
    grid.setAttribute("aria-label", "Properties of the selected item");
    grid.addEventListener("keydown", event => this.onKeyDown(event));
    this.render();
  }

  show(name: string | null, properties: ExecutionPlanGraphProperty[] | null): void {
    this.title.textContent = name?.split(/\r?\n/)[0] ?? "";
    this.title.title = this.title.textContent;
    if (this.properties === properties) return;
    this.properties = properties;
    this.expanded.clear();
    this.selectedKey = "";
    this.render();
  }

  private render(): void {
    const document = this.grid.ownerDocument;
    const focused = this.grid.contains(document.activeElement);
    this.rows = propertyRows(this.properties ?? [], this.expanded);
    if (!this.rows.some(row => row.key === this.selectedKey)) this.selectedKey = this.rows[0]?.key ?? "";
    this.grid.tabIndex = this.rows.length ? -1 : 0;
    const elements = this.rows.map(row => {
      const element = document.createElement("div");
      element.className = "property-row";
      element.dataset.key = row.key;
      element.setAttribute("role", "treeitem");
      element.setAttribute("aria-level", String(row.level));
      element.setAttribute("aria-setsize", String(row.size));
      element.setAttribute("aria-posinset", String(row.position));
      element.setAttribute("aria-selected", String(row.key === this.selectedKey));
      if (row.expandable) element.setAttribute("aria-expanded", String(row.expanded));
      element.tabIndex = row.key === this.selectedKey ? 0 : -1;
      element.addEventListener("focus", () => this.select(row.key, false));
      element.addEventListener("click", () => {
        element.focus();
        if (row.expandable) this.toggle(row.key);
      });
      const name = document.createElement("div");
      name.className = "property-name";
      name.style.paddingLeft = `${(row.level - 1) * 12 + 4}px`;
      name.title = row.property.name;
      const toggle = document.createElement("span");
      toggle.className = "property-toggle";
      toggle.setAttribute("aria-hidden", "true");
      toggle.textContent = row.expandable ? row.expanded ? "\u25BE" : "\u25B8" : "";
      name.append(toggle, row.property.name);
      const value = document.createElement("div");
      value.className = "property-value";
      value.textContent = row.property.displayValue ?? (typeof row.property.value === "string" ? row.property.value : "");
      value.title = value.textContent;
      element.append(name, value);
      return element;
    });
    if (!elements.length) {
      const placeholder = document.createElement("div");
      placeholder.className = "properties-placeholder";
      placeholder.textContent = this.properties
        ? "No properties" : "Select an execution plan node to show its properties";
      elements.push(placeholder);
    }
    this.grid.replaceChildren(...elements);
    if (focused) {
      if (this.rows.length) this.select(this.selectedKey, true);
      else this.grid.focus();
    }
  }

  private select(key: string, focus: boolean): void {
    this.selectedKey = key;
    for (const element of this.grid.querySelectorAll<HTMLElement>(".property-row")) {
      element.tabIndex = element.dataset.key === key ? 0 : -1;
      element.setAttribute("aria-selected", String(element.tabIndex === 0));
      if (focus && element.tabIndex === 0) element.focus();
    }
  }

  private toggle(key: string): void {
    if (!this.expanded.delete(key)) this.expanded.add(key);
    this.render();
  }

  private onKeyDown(event: KeyboardEvent): void {
    const index = this.rows.findIndex(row => row.key === this.selectedKey);
    const row = this.rows[index];
    if (!row) return;
    let target: PropertyRow | undefined;
    switch (event.key) {
      case "ArrowDown": target = this.rows[index + 1]; break;
      case "ArrowUp": target = this.rows[index - 1]; break;
      case "Home": target = this.rows[0]; break;
      case "End": target = this.rows[this.rows.length - 1]; break;
      case "ArrowRight":
        if (row.expandable && !row.expanded) this.toggle(row.key);
        else if (row.expanded) target = this.rows[index + 1];
        break;
      case "ArrowLeft":
        if (row.expanded) this.toggle(row.key);
        else target = this.rows.slice(0, index).reverse().find(parent => row.key.startsWith(`${parent.key}/`));
        break;
      case "Enter":
      case " ":
        if (row.expandable) this.toggle(row.key);
        break;
      default: return;
    }
    event.preventDefault();
    if (target) this.select(target.key, true);
  }
}
