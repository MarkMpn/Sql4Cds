import type { ExecutionPlanNode } from "../resultsProtocol";

// Layout constants matching MarkMpn.Sql4Cds.Controls.ExecutionPlanView
export const PLAN_OFFSET = 32;
export const ICON_SIZE = 32;
export const ICON_LABEL_GAP = 10;

export interface Rect {
    x: number;
    y: number;
    width: number;
    height: number;
}

export interface Point {
    x: number;
    y: number;
}

export interface PlanNodeLayout {
    node: ExecutionPlanNode;
    parent?: PlanNodeLayout;
    children: PlanNodeLayout[];
    label: string;
    bounds: Rect;
    icon: Rect;
}

export interface PlanEdgeLayout {
    source: PlanNodeLayout;
    target: PlanNodeLayout;
    rowCount: number;
    start: Point;
    end: Point;
    width: number;
}

export interface PlanLayout {
    root: PlanNodeLayout;
    nodes: PlanNodeLayout[];
    edges: PlanEdgeLayout[];
    width: number;
    height: number;
}

export type MeasureText = (text: string) => { width: number; height: number };

const percentFormat = new Intl.NumberFormat(undefined, { style: "percent", maximumFractionDigits: 0 });
const secondsFormat = new Intl.NumberFormat(undefined, { minimumFractionDigits: 3, maximumFractionDigits: 3 });

/** Plans that have been executed include the cost of each node, estimated plans do not */
export function isExecutedPlan(root: ExecutionPlanNode): boolean {
    return !!root.costDisplayString;
}

export function getNodeLabel(node: ExecutionPlanNode, executed: boolean): string {
    let text = (node.name ?? "").replace(/\r\n/g, "\n");

    if (executed) {
        const cost = Math.min(1, Math.max(0, Number.isFinite(node.cost) ? node.cost : 1));
        text += `\nCost: ${percentFormat.format(cost)}`;
        text += `\n${secondsFormat.format((node.elapsedTimeInMs ?? 0) / 1000)}s`;
    }

    return text;
}

export function getIconRect(bounds: Rect): Rect {
    return {
        x: bounds.x + Math.floor((bounds.width - ICON_SIZE) / 2),
        y: bounds.y,
        width: ICON_SIZE,
        height: ICON_SIZE,
    };
}

export function getEdgeWidth(rowCount: number): number {
    return rowCount <= 0 ? 1 : Math.max(1, Math.trunc(Math.log10(rowCount)));
}

/**
 * Lays out the nodes in a plan following the same logic as the WinForms ExecutionPlanView: the root node is in the
 * top-left, the first source of each node is to its right on the same level and subsequent sources move down below
 * everything that has been laid out so far.
 */
export function layoutPlan(root: ExecutionPlanNode, measure: MeasureText): PlanLayout {
    const executed = isExecutedPlan(root);
    const nodes: PlanNodeLayout[] = [];
    const edges: PlanEdgeLayout[] = [];

    const createLayout = (node: ExecutionPlanNode, location: Point, parent?: PlanNodeLayout): PlanNodeLayout => {
        const label = getNodeLabel(node, executed);
        const labelSize = measure(label);
        const bounds = {
            x: location.x,
            y: location.y,
            width: Math.ceil(Math.max(ICON_SIZE, labelSize.width)),
            height: ICON_SIZE + ICON_LABEL_GAP + Math.ceil(labelSize.height),
        };
        const layout = { node, parent, children: [], label, bounds, icon: getIconRect(bounds) };
        nodes.push(layout);
        return layout;
    };

    const rootLayout = createLayout(root, { x: PLAN_OFFSET, y: PLAN_OFFSET });
    let maxBottom = bottom(rootLayout.bounds);

    const layoutChildren = (parent: PlanNodeLayout): void => {
        const sources = parent.node.children ?? [];
        const lineYSpacing = Math.floor(ICON_SIZE / (sources.length + 1));
        const parentIcon = parent.icon;

        sources.forEach((source, i) => {
            const x = right(parent.bounds) + PLAN_OFFSET;
            // First source stays on the same level as the parent, subsequent sources move down
            const child = createLayout(source, { x, y: i === 0 ? parent.bounds.y : maxBottom + PLAN_OFFSET }, parent);
            parent.children.push(child);
            maxBottom = Math.max(maxBottom, bottom(child.bounds));

            const rowCount = parent.node.edges?.[i]?.rowCount ?? 1;
            const endY = parentIcon.y + (i + 1) * lineYSpacing;
            edges.push({
                source: child,
                target: parent,
                rowCount,
                start: {
                    x: child.icon.x,
                    y: parentIcon.y === child.icon.y ? endY : child.icon.y + Math.floor(child.icon.height / 2),
                },
                end: { x: right(parentIcon), y: endY },
                width: getEdgeWidth(rowCount),
            });

            layoutChildren(child);
        });
    };

    layoutChildren(rootLayout);

    return {
        root: rootLayout,
        nodes,
        edges,
        width: Math.max(...nodes.map((n) => right(n.bounds))) + PLAN_OFFSET,
        height: maxBottom + PLAN_OFFSET,
    };
}

/** Gets the SVG path for an edge, including the dogleg when the nodes are at different levels */
export function getEdgePath(edge: PlanEdgeLayout): string {
    const { start, end, width } = edge;

    if (start.y === end.y) return `M ${start.x} ${start.y} H ${end.x + width}`;

    const midX = start.x - Math.trunc((start.x - end.x) / 2);
    return `M ${start.x} ${start.y} H ${midX} V ${end.y} H ${end.x + width}`;
}

export function getArrowheadPoints(edge: PlanEdgeLayout): string {
    const { end, width } = edge;
    const half = Math.trunc(width / 2) + 2;
    return `${end.x},${end.y} ${end.x + width + 2},${end.y - half} ${end.x + width + 2},${end.y + half}`;
}

function right(rect: Rect): number {
    return rect.x + rect.width;
}

function bottom(rect: Rect): number {
    return rect.y + rect.height;
}
