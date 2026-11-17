import { useCallback, useLayoutEffect, useRef } from "react";
import { observer } from "mobx-react-lite";
import type { TreeNode } from "./objectExplorer";
import { useApp } from "./appContext";

const NODE_ICONS: Readonly<Record<string, string>> = {
    Server: "\u25A4",
    Folder: "\u25B0",
    Table: "\u25A6",
    Column: "\u25A5",
    TableValuedFunction: "\u0192",
    StoredProcedure: "\u2699",
};

const ExplorerNode = observer(function ExplorerNodeView({ node, level }: {
    node: TreeNode;
    level: number;
}) {
    const explorer = useApp().objectExplorer;
    const connecting = node.connection.status === "connecting";
    const selected = explorer.selected === node;
    const bindItem = useCallback((item: HTMLDivElement | null) => explorer.bindItem(node, item), [explorer, node]);
    return <div ref={bindItem} className="explorer-node" role="treeitem" aria-level={level}
        aria-selected={selected} aria-expanded={node.info.isLeaf ? undefined : node.expanded}
        aria-busy={node.loading || connecting} aria-disabled={connecting}
        tabIndex={selected ? 0 : -1} data-path={node.info.nodePath}
        onFocus={(event) => { if (event.target === event.currentTarget) explorer.select(node, false); }}
        onKeyDown={(event) => {
            if (event.target === event.currentTarget) explorer.onKeyDown(event.nativeEvent);
        }}
        onClick={(event) => {
            event.stopPropagation();
            explorer.select(node);
            if ((event.target as Element).closest(".explorer-expander")) explorer.toggle(node);
        }}
        onDoubleClick={(event) => {
            event.stopPropagation();
            if (!(event.target as Element).closest(".explorer-expander")) explorer.activate(node);
        }}
        onContextMenu={(event) => {
            event.preventDefault();
            event.stopPropagation();
            explorer.showContextMenu(node, event.clientX, event.clientY);
        }}>
        <div className="explorer-row" style={{ paddingLeft: (level - 1) * 16 + 4 }}
            title={node.connection.error || node.error || node.info.errorMessage || node.info.label}>
            <span className="explorer-expander" aria-hidden="true">
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
            {node.children.map((child) => <ExplorerNode key={child.info.nodePath} node={child} level={level + 1} />)}
        </div>}
    </div>;
});

export const ExplorerTree = observer(function ExplorerTree() {
    const explorer = useApp().objectExplorer;
    const tree = useRef<HTMLDivElement | null>(null);
    const bindTree = useCallback((element: HTMLDivElement | null) => {
        tree.current = element;
        explorer.bindTree(element);
    }, [explorer]);
    const focusRequest = explorer.focusRequest;
    useLayoutEffect(() => {
        if (focusRequest > 0) tree.current?.querySelector<HTMLElement>('[tabindex="0"]')?.focus();
    }, [explorer, focusRequest]);
    return <div ref={bindTree} id="explorer-tree" className="explorer-tree" role="tree" aria-label="Object Explorer"
        tabIndex={explorer.roots.length === 0 ? 0 : -1}
        onKeyDown={event => { if (event.target === event.currentTarget) explorer.onKeyDown(event.nativeEvent); }}>
        {explorer.roots.map((node) => <ExplorerNode key={node.connection.slot} node={node} level={1} />)}
        {explorer.roots.length === 0 && <div className="explorer-message">
            {explorer.available ? "No connections available" : "SQL 4 CDS engine stopped"}
        </div>}
    </div>;
});
