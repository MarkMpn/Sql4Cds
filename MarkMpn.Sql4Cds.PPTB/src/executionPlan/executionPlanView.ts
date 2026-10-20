import type { ExecutionPlanGraph, ExecutionPlanGraphProperty, ExecutionPlanNode } from "../lsp/protocol";
import { BadgeType } from "../lsp/protocol";
import { getArrowheadPoints, getEdgePath, layoutPlan, type MeasureText, type PlanLayout, type PlanNodeLayout } from "./layout";

const SVG_NS = "http://www.w3.org/2000/svg";
const LABEL_FONT = '12px "Segoe UI", system-ui, sans-serif';
const LABEL_LINE_HEIGHT = 16;

export type GetNodeImageUrl = (node: ExecutionPlanNode) => string | undefined;

let measureContext: CanvasRenderingContext2D | null | undefined;

/** Measures node labels using the same font that is used to display them */
export const measureLabel: MeasureText = (text) => {
    if (measureContext === undefined) {
        measureContext = document.createElement("canvas").getContext("2d");
        if (measureContext) measureContext.font = LABEL_FONT;
    }
    const lines = text.split("\n");
    const width = Math.max(...lines.map((line) => measureContext?.measureText(line).width ?? line.length * 7));
    // Allow a little padding around the text for the selection highlight
    return { width: width + 8, height: lines.length * LABEL_LINE_HEIGHT };
};

/** Gets the text to show in the tooltip for a node - any warning, otherwise the FetchXML for the query */
export function getNodeTooltip(node: ExecutionPlanNode): string {
    if (node.badges?.some((badge) => badge.type === BadgeType.Warning || badge.type === BadgeType.CriticalWarning)) {
        const match = /^<p>([\s\S]*?)<\/p>/.exec(node.description ?? "");
        if (match) return decodeHtml(match[1]);
    }

    const tooltipProperty = findProperty(node.properties, (property) => property.showInTooltip === true);
    if (tooltipProperty && typeof tooltipProperty.value === "string") return tooltipProperty.value;

    return (node.name ?? "").replace(/\r\n/g, "\n");
}

export function hasWarning(node: ExecutionPlanNode): boolean {
    return node.badges?.some((badge) => badge.type === BadgeType.Warning || badge.type === BadgeType.CriticalWarning) ?? false;
}

function findProperty(
    properties: ExecutionPlanGraphProperty[] | undefined,
    predicate: (property: ExecutionPlanGraphProperty) => boolean,
): ExecutionPlanGraphProperty | undefined {
    for (const property of properties ?? []) {
        if (predicate(property)) return property;
        if (Array.isArray(property.value)) {
            const nested = findProperty(property.value, predicate);
            if (nested) return nested;
        }
    }
    return undefined;
}

function decodeHtml(html: string): string {
    const textarea = document.createElement("textarea");
    textarea.innerHTML = html;
    return textarea.value;
}

/**
 * Displays one or more execution plans, laid out in the same way as the ExecutionPlanView control in
 * MarkMpn.Sql4Cds.Controls. A single node can be selected across all the plans.
 */
export class ExecutionPlanView {
    readonly element: HTMLElement;

    private readonly layouts = new Map<PlanNodeLayout, HTMLElement>();
    private selectedLayout: PlanNodeLayout | null = null;
    private planCount = 0;

    constructor(
        private readonly getImageUrl: GetNodeImageUrl,
        private readonly onSelectionChanged: (node: ExecutionPlanNode | null) => void,
        private readonly measure: MeasureText = measureLabel,
    ) {
        this.element = document.createElement("div");
        this.element.className = "execution-plans";
        this.element.addEventListener("pointerdown", (e) => {
            // Clicking on empty space removes the selection
            if (!(e.target as Element).closest(".plan-node")) this.select(null);
        });
    }

    get count(): number {
        return this.planCount;
    }

    get selectedNode(): ExecutionPlanNode | null {
        return this.selectedLayout?.node ?? null;
    }

    clear(): void {
        this.element.replaceChildren();
        this.layouts.clear();
        this.planCount = 0;
        this.select(null);
    }

    addPlan(graph: ExecutionPlanGraph): void {
        if (!graph?.root) return;

        this.planCount++;
        const layout = layoutPlan(graph.root, this.measure);

        const section = document.createElement("section");
        section.className = "execution-plan";

        const header = document.createElement("div");
        header.className = "execution-plan-header";
        const query = (graph.query ?? "").replace(/\s+/g, " ").trim();
        header.textContent = query ? `Query ${this.planCount}: ${query}` : `Query ${this.planCount}`;
        header.title = graph.query ?? "";

        const canvas = document.createElement("div");
        canvas.className = "execution-plan-canvas";
        canvas.setAttribute("role", "tree");
        canvas.setAttribute("aria-label", `Execution plan for query ${this.planCount}`);
        canvas.style.width = `${layout.width}px`;
        canvas.style.height = `${layout.height}px`;

        canvas.appendChild(this.renderEdges(layout));
        for (const nodeLayout of layout.nodes) canvas.appendChild(this.renderNode(nodeLayout));

        section.append(header, canvas);
        this.element.appendChild(section);

        // Keep a single tab stop for the whole view
        if (this.planCount === 1 && !this.selectedLayout) this.layouts.get(layout.root)!.tabIndex = 0;
    }

    /** Moves focus to the selected node, or the first node if nothing is selected */
    focus(): void {
        const element = (this.selectedLayout && this.layouts.get(this.selectedLayout)) ?? this.element.querySelector<HTMLElement>(".plan-node");
        element?.focus();
    }

    private renderEdges(layout: PlanLayout): SVGSVGElement {
        const svg = document.createElementNS(SVG_NS, "svg");
        svg.setAttribute("class", "execution-plan-edges");
        svg.setAttribute("width", String(layout.width));
        svg.setAttribute("height", String(layout.height));
        svg.setAttribute("aria-hidden", "true");

        for (const edge of layout.edges) {
            const path = document.createElementNS(SVG_NS, "path");
            path.setAttribute("d", getEdgePath(edge));
            path.setAttribute("stroke-width", String(edge.width));
            path.setAttribute("class", "plan-edge");
            const title = document.createElementNS(SVG_NS, "title");
            title.textContent = `${edge.rowCount.toLocaleString()} row${edge.rowCount === 1 ? "" : "s"}`;
            path.appendChild(title);

            const arrow = document.createElementNS(SVG_NS, "polygon");
            arrow.setAttribute("points", getArrowheadPoints(edge));
            arrow.setAttribute("class", "plan-arrow");

            svg.append(path, arrow);
        }

        return svg;
    }

    private renderNode(layout: PlanNodeLayout): HTMLElement {
        const { node, bounds, icon } = layout;

        const element = document.createElement("div");
        element.className = "plan-node";
        element.tabIndex = -1;
        element.title = getNodeTooltip(node);
        element.setAttribute("role", "treeitem");
        element.setAttribute("aria-selected", "false");
        element.setAttribute("aria-level", String(depth(layout) + 1));
        const siblings = layout.parent?.children ?? [layout];
        element.setAttribute("aria-setsize", String(siblings.length));
        element.setAttribute("aria-posinset", String(siblings.indexOf(layout) + 1));
        if (layout.children.length) element.setAttribute("aria-expanded", "true");
        element.style.left = `${bounds.x}px`;
        element.style.top = `${bounds.y}px`;
        element.style.width = `${bounds.width}px`;
        element.style.height = `${bounds.height}px`;

        const image = document.createElement("img");
        image.className = "plan-node-icon";
        image.alt = "";
        image.draggable = false;
        const url = this.getImageUrl(node);
        if (url) image.src = url;
        image.style.left = `${icon.x - bounds.x}px`;
        element.appendChild(image);

        if (hasWarning(node)) {
            const warning = document.createElement("span");
            warning.className = "plan-node-warning";
            warning.setAttribute("aria-label", "Warning");
            warning.textContent = "!";
            warning.style.left = `${icon.x - bounds.x}px`;
            element.appendChild(warning);
        }

        const label = document.createElement("div");
        label.className = "plan-node-label";
        label.textContent = layout.label;
        element.appendChild(label);

        element.addEventListener("pointerdown", (e) => {
            if (e.button !== 0) return;
            this.select(layout);
        });
        element.addEventListener("focus", () => {
            if (this.selectedLayout !== layout) this.select(layout);
        });
        element.addEventListener("keydown", (e) => this.onKeyDown(e, layout));

        this.layouts.set(layout, element);
        return element;
    }

    private onKeyDown(e: KeyboardEvent, layout: PlanNodeLayout): void {
        let target: PlanNodeLayout | undefined;
        const siblings = layout.parent?.children ?? [];
        const index = siblings.indexOf(layout);

        switch (e.key) {
            case "ArrowRight": target = layout.children[0]; break;
            case "ArrowLeft": target = layout.parent; break;
            case "ArrowUp": target = index > 0 ? siblings[index - 1] : undefined; break;
            case "ArrowDown": target = index >= 0 ? siblings[index + 1] : undefined; break;
            case "Home": target = rootOf(layout); break;
            default: return;
        }

        e.preventDefault();
        if (target) {
            this.select(target);
            this.layouts.get(target)?.focus();
        }
    }

    private select(layout: PlanNodeLayout | null): void {
        const previous = this.selectedLayout;
        if (previous === layout) return;

        this.selectedLayout = layout;

        const previousElement = previous ? this.layouts.get(previous) : undefined;
        if (previousElement) {
            previousElement.classList.remove("selected");
            previousElement.setAttribute("aria-selected", "false");
        }

        const element = layout ? this.layouts.get(layout) : undefined;
        if (element) {
            element.classList.add("selected");
            element.setAttribute("aria-selected", "true");
            for (const other of this.layouts.values()) other.tabIndex = other === element ? 0 : -1;
            element.scrollIntoView?.({ block: "nearest", inline: "nearest" });
        }

        this.onSelectionChanged(layout?.node ?? null);
    }
}

function depth(layout: PlanNodeLayout): number {
    let result = 0;
    for (let parent = layout.parent; parent; parent = parent.parent) result++;
    return result;
}

function rootOf(layout: PlanNodeLayout): PlanNodeLayout {
    let root = layout;
    while (root.parent) root = root.parent;
    return root;
}
