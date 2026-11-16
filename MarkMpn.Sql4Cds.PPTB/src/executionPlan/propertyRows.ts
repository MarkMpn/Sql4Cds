import type { ExecutionPlanGraphProperty } from "../resultsProtocol";

export interface PropertyRow {
    property: ExecutionPlanGraphProperty;
    key: string;
    level: number;
    size: number;
    position: number;
    expanded: boolean;
    expandable: boolean;
}

export function propertyRows(properties: ExecutionPlanGraphProperty[], expanded: ReadonlySet<string>, level = 1, parentKey = ""): PropertyRow[] {
    const sorted = [...properties].sort((a, b) => (a.displayOrder ?? 0) - (b.displayOrder ?? 0));
    return sorted.flatMap((property, index) => {
        const key = `${parentKey}/${index}:${property.name}`;
        const children = Array.isArray(property.value) ? property.value : null;
        const open = children !== null && expanded.has(key);
        const row: PropertyRow = { property, key, level, size: sorted.length, position: index + 1, expandable: children !== null, expanded: open };
        return [row, ...(open ? propertyRows(children!, expanded, level + 1, key) : [])];
    });
}
