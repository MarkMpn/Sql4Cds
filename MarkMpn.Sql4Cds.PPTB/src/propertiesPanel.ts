import type { ExecutionPlanGraphProperty } from "./lsp/protocol";

/**
 * A read-only property grid for the selected execution plan node. Nested properties can be expanded to show their
 * child properties, in the same way as the WinForms PropertyGrid used by the SSMS / XrmToolBox versions.
 */
export class PropertiesPanel {
    private readonly expanded = new Set<string>();
    private properties: ExecutionPlanGraphProperty[] | null = null;

    constructor(
        private readonly element: HTMLElement,
        private readonly title: HTMLElement,
    ) {
        element.setAttribute("role", "tree");
        element.addEventListener("keydown", (e) => this.onKeyDown(e));
        this.render();
    }

    /** Shows the properties of an object, or a placeholder if nothing is selected */
    show(name: string | null, properties: ExecutionPlanGraphProperty[] | null): void {
        if (properties !== this.properties) this.expanded.clear();
        this.properties = properties;
        this.title.textContent = name ?? "";
        this.title.title = name ?? "";
        this.render();
    }

    private render(): void {
        const focusedKey = (document.activeElement as HTMLElement | null)?.closest?.<HTMLElement>(".property-row")?.dataset.key;

        if (!this.properties) {
            const placeholder = document.createElement("div");
            placeholder.className = "properties-placeholder";
            placeholder.textContent = "Select an execution plan node to show its properties";
            this.element.replaceChildren(placeholder);
            return;
        }

        const rows: HTMLElement[] = [];
        this.renderProperties(this.properties, 1, "", rows);
        if (rows.length === 0) {
            const placeholder = document.createElement("div");
            placeholder.className = "properties-placeholder";
            placeholder.textContent = "No properties";
            rows.push(placeholder);
        }
        this.element.replaceChildren(...rows);

        const focusable = rows.filter((row) => row.classList.contains("property-row"));
        const focusTarget = focusable.find((row) => row.dataset.key === focusedKey) ?? focusable[0];
        if (focusTarget) {
            focusTarget.tabIndex = 0;
            if (focusedKey !== undefined) focusTarget.focus();
        }
    }

    private renderProperties(properties: ExecutionPlanGraphProperty[], level: number, parentKey: string, rows: HTMLElement[]): void {
        const sorted = [...properties].sort((a, b) => (a.displayOrder ?? 0) - (b.displayOrder ?? 0));

        sorted.forEach((property, index) => {
            const key = `${parentKey}/${index}:${property.name}`;
            const nested = Array.isArray(property.value) ? property.value : null;
            const isExpanded = nested !== null && this.expanded.has(key);

            const row = document.createElement("div");
            row.className = "property-row";
            row.tabIndex = -1;
            row.dataset.key = key;
            row.setAttribute("role", "treeitem");
            row.setAttribute("aria-level", String(level));
            row.setAttribute("aria-setsize", String(sorted.length));
            row.setAttribute("aria-posinset", String(index + 1));
            if (nested) row.setAttribute("aria-expanded", String(isExpanded));

            const name = document.createElement("div");
            name.className = "property-name";
            name.style.paddingLeft = `${(level - 1) * 12 + 4}px`;

            const toggle = document.createElement("span");
            toggle.className = "property-toggle";
            toggle.setAttribute("aria-hidden", "true");
            toggle.textContent = nested ? (isExpanded ? "▾" : "▸") : "";
            name.append(toggle, property.name);
            name.title = property.name;

            const value = document.createElement("div");
            value.className = "property-value";
            const display = property.displayValue ?? (typeof property.value === "string" ? property.value : "");
            value.textContent = display;
            value.title = display;

            row.append(name, value);
            row.addEventListener("click", () => {
                row.focus();
                if (nested) this.toggle(key);
            });
            rows.push(row);

            if (isExpanded) this.renderProperties(nested, level + 1, key, rows);
        });
    }

    private toggle(key: string, expand?: boolean): void {
        const shouldExpand = expand ?? !this.expanded.has(key);
        if (shouldExpand === this.expanded.has(key)) return;
        if (shouldExpand) this.expanded.add(key);
        else this.expanded.delete(key);
        this.render();
    }

    private onKeyDown(e: KeyboardEvent): void {
        const row = (e.target as HTMLElement).closest<HTMLElement>(".property-row");
        if (!row) return;

        const rows = Array.from(this.element.querySelectorAll<HTMLElement>(".property-row"));
        const index = rows.indexOf(row);
        const key = row.dataset.key!;
        const expandable = row.hasAttribute("aria-expanded");
        const isExpanded = row.getAttribute("aria-expanded") === "true";
        let target: HTMLElement | undefined;

        switch (e.key) {
            case "ArrowDown": target = rows[index + 1]; break;
            case "ArrowUp": target = rows[index - 1]; break;
            case "Home": target = rows[0]; break;
            case "End": target = rows[rows.length - 1]; break;
            case "ArrowRight":
                if (expandable && !isExpanded) this.toggle(key, true);
                else if (isExpanded) target = rows[index + 1];
                break;
            case "ArrowLeft":
                if (isExpanded) this.toggle(key, false);
                else target = rows.slice(0, index).reverse().find((r) => key.startsWith(`${r.dataset.key}/`));
                break;
            case "Enter":
            case " ":
                if (expandable) this.toggle(key);
                break;
            default:
                return;
        }

        e.preventDefault();
        if (target) {
            row.tabIndex = -1;
            target.tabIndex = 0;
            target.focus();
        }
    }
}
