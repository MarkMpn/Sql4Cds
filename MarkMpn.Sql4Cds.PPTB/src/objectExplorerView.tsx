import { useCallback, useLayoutEffect } from "react";
import { createRoot } from "react-dom/client";
import { observer } from "mobx-react-lite";
import type { ObjectExplorer, TreeNode } from "./objectExplorer";

const NODE_ICONS: Readonly<Record<string, string>> = {
    Server: "\u25A4",
    Folder: "\u25B0",
    Table: "\u25A6",
    Column: "\u25A5",
    TableValuedFunction: "\u0192",
    StoredProcedure: "\u2699",
};

const ExplorerNode = observer(function ExplorerNodeView({ explorer, node, level }: {
    explorer: ObjectExplorer;
    node: TreeNode;
    level: number;
}) {
    const connecting = node.connection.status === "connecting";
    const selected = explorer.selected === node;
    const bindItem = useCallback((item: HTMLDivElement | null) => explorer.bindItem(node, item), [explorer, node]);
    return <div ref={bindItem} className="explorer-node" role="treeitem" aria-level={level}
        aria-selected={selected} aria-expanded={node.info.isLeaf ? undefined : node.expanded}
        aria-busy={node.loading || connecting} aria-disabled={connecting}
        tabIndex={selected ? 0 : -1} data-path={node.info.nodePath}
        onFocus={(event) => { if (event.target === event.currentTarget) explorer.select(node, false); }}>
        <div className="explorer-row" style={{ paddingLeft: (level - 1) * 16 + 4 }}
            title={node.connection.error || node.error || node.info.errorMessage || node.info.label}
            onClick={() => explorer.select(node)}
            onDoubleClick={() => explorer.activate(node)}
            onContextMenu={(event) => {
                event.preventDefault();
                event.stopPropagation();
                explorer.showContextMenu(node, event.clientX, event.clientY);
            }}>
            <span className="explorer-expander" aria-hidden="true"
                onClick={(event) => {
                    event.stopPropagation();
                    explorer.select(node);
                    explorer.toggle(node);
                }}
                onDoubleClick={(event) => event.stopPropagation()}>
                {node.info.isLeaf || connecting ? "" : node.expanded ? "\u25BE" : "\u25B8"}
            </span>
            <span className={`explorer-icon explorer-icon-${node.info.nodeType.toLowerCase()}`} aria-hidden="true">
                {NODE_ICONS[node.info.nodeType] ?? NODE_ICONS.Table}
            </span>
            <span>{node.info.label}</span>
            {(connecting || node.connection.error) && <span
                className={connecting ? "explorer-connection-status" : "explorer-connection-status error"}
                role={connecting ? "status" : "alert"}>
                {connecting ? "(Connecting\u2026)" : "(Connection failed)"}
            </span>}
            {node.info.nodeType === "Table" && <button className="explorer-select-top" type="button"
                aria-label="Select Top 1000 Rows" title="Select Top 1000 Rows"
                onClick={(event) => {
                    event.stopPropagation();
                    explorer.selectTop(node);
                }}>{"\u25B6"}</button>}
        </div>
        {node.expanded && <div role="group">
            {(node.loading || node.error || (node.loaded && node.children.length === 0)) && <div
                className={node.error ? "explorer-message error" : "explorer-message"}
                style={{ paddingLeft: level * 16 + 24 }} role={node.error ? "alert" : "status"}>
                {node.error ? `${node.error} (Refresh to retry)` : node.loading ? "Loading\u2026" : "No objects"}
            </div>}
            {node.children.map((child) => <ExplorerNode key={child.info.nodePath} explorer={explorer} node={child} level={level + 1} />)}
        </div>}
    </div>;
});

const ExplorerTree = observer(function ExplorerTree({ explorer, tree }: { explorer: ObjectExplorer; tree: HTMLElement }) {
    const focusRequest = explorer.focusRequest;
    useLayoutEffect(() => {
        if (focusRequest > 0) tree.querySelector<HTMLElement>('[tabindex="0"]')?.focus();
    }, [tree, focusRequest]);
    return <>
        {explorer.roots.map((node) => <ExplorerNode key={node.connection.slot} explorer={explorer} node={node} level={1} />)}
        {explorer.roots.length === 0 && <div className="explorer-message">
            {explorer.available ? "No connections available" : "SQL 4 CDS engine stopped"}
        </div>}
    </>;
});

export function mountObjectExplorer(tree: HTMLElement, explorer: ObjectExplorer): () => void {
    const root = createRoot(tree);
    root.render(<ExplorerTree explorer={explorer} tree={tree} />);
    return () => root.unmount();
}
