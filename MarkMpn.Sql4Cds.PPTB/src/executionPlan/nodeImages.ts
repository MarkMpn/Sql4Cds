import type { ExecutionPlanNode } from "../lsp/protocol";
import { getNodeImageName } from "./nodeImageName";

// Share the node images embedded as resources in MarkMpn.Sql4Cds.Controls rather than keeping a separate copy
const images = import.meta.glob<string>("../../../MarkMpn.Sql4Cds.Controls/Images/*.ico", {
    eager: true,
    query: "?url",
    import: "default",
});

const imageUrls = new Map(
    Object.entries(images).map(([path, url]) => [path.slice(path.lastIndexOf("/") + 1, -".ico".length), url]),
);

export function getNodeImageUrl(node: Pick<ExecutionPlanNode, "nodeType">): string | undefined {
    return imageUrls.get(getNodeImageName(node, (name) => imageUrls.has(name)));
}
