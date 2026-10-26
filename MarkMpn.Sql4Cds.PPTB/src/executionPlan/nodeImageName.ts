import type { ExecutionPlanNode } from "../resultsProtocol";

/**
 * Gets the name of the image to show for a node, following the same logic as the ExecutionPlanView control
 * in MarkMpn.Sql4Cds.Controls
 */
export function getNodeImageName(node: Pick<ExecutionPlanNode, "nodeType">, available: (name: string) => boolean): string {
    let imageName = node.nodeType ?? "";

    if (imageName === "AdaptiveIndexSpoolNode")
        imageName = "IndexSpoolNode";
    else if (imageName === "OpenJsonNode" || imageName === "SystemFunctionNode")
        imageName = "ExecuteMessageNode";
    else if (imageName === "WindowSpoolNode")
        imageName = "TableSpoolNode";

    if (available(imageName))
        return imageName;

    return imageName.includes("Cursor") ? "CursorFallbackNode" : "FallbackNode";
}
