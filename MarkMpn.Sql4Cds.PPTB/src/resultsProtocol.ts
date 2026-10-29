/** Host-independent contracts used by the PPTB and VS Code results components. */
export type ExportFormat = "Csv" | "Excel" | "Json" | "Markdown" | "Xml";

export const exportFormats: ReadonlyArray<{ format: ExportFormat; label: string; extension: string }> = [
    { format: "Csv", label: "CSV", extension: "csv" },
    { format: "Excel", label: "Excel", extension: "xlsx" },
    { format: "Json", label: "JSON", extension: "json" },
    { format: "Markdown", label: "Markdown", extension: "md" },
    { format: "Xml", label: "XML", extension: "xml" },
];

export interface DbColumn {
    columnName: string;
    dataTypeName?: string;
}

export const PropertyValueDataType = { Number: 0, String: 1, Boolean: 2, Nested: 3 } as const;
export const BadgeType = { Warning: 0, CriticalWarning: 1, Parallelism: 2 } as const;

export interface ExecutionPlanGraphProperty {
    name: string;
    displayValue?: string | null;
    displayOrder?: number;
    showInTooltip?: boolean;
    dataType?: (typeof PropertyValueDataType)[keyof typeof PropertyValueDataType];
    value?: string | ExecutionPlanGraphProperty[] | null;
}

export interface ExecutionPlanEdge {
    rowCount: number;
    rowSize: number;
}

export interface ExecutionPlanNode {
    id: number;
    type: string;
    nodeType?: string;
    name: string;
    cost: number;
    costDisplayString?: string;
    elapsedTimeInMs?: number | null;
    rowCountDisplayString?: string;
    description?: string | null;
    subtext?: string[];
    properties?: ExecutionPlanGraphProperty[];
    children?: ExecutionPlanNode[];
    edges?: ExecutionPlanEdge[];
    badges?: { type: number; tooltip?: string | null }[];
}

export interface ExecutionPlanGraph {
    root: ExecutionPlanNode;
    query?: string;
}

export interface DbCellValue {
    displayValue: string;
    isNull: boolean;
    rawObject?: unknown;
}

export interface EntityReferenceValue {
    dataSource: string;
    id: string;
    isNull: false;
    logicalName: string;
}

export function isEntityReferenceValue(value: unknown): value is EntityReferenceValue {
    return typeof value === "object" && value !== null &&
        "dataSource" in value && typeof value.dataSource === "string" && value.dataSource.length > 0 &&
        "id" in value && typeof value.id === "string" && value.id.length > 0 &&
        "isNull" in value && value.isNull === false &&
        "logicalName" in value && typeof value.logicalName === "string" && value.logicalName.length > 0;
}
