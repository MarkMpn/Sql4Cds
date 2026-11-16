import { useLayoutEffect, useRef, useState } from "react";
import { observer } from "mobx-react-lite";
import { useApp } from "./appContext";
import { ObjectExplorer as ExplorerController } from "./objectExplorer";
import { ExplorerTree } from "./objectExplorerView";
import { PropertiesPanel } from "./propertiesPanel";

export function ObjectExplorer() {
    const app = useApp();
    const tree = useRef<HTMLDivElement>(null);
    const [explorer, setExplorer] = useState<ExplorerController | null>(null);
    useLayoutEffect(() => {
        const controller = new ExplorerController(app.client, tree.current!,
            message => { void app.reportError("Object Explorer", message); },
            text => app.insertObjectName(text),
            (connection, query) => app.addTab(query, connection.slot), app.connections);
        app.setObjectExplorer(controller);
        setExplorer(controller);
        return () => {
            app.setObjectExplorer(null);
            void controller.dispose().catch(error => app.reportError("Object Explorer", error));
        };
    }, [app]);
    return <div ref={tree} id="explorer-tree" className="explorer-tree">
        {explorer && <ExplorerTree explorer={explorer} tree={tree.current!} />}
    </div>;
}

export const Properties = observer(function Properties() {
    const app = useApp();
    const grid = useRef<HTMLDivElement>(null);
    const title = useRef<HTMLDivElement>(null);
    const panel = useRef<PropertiesPanel | null>(null);
    const node = app.activeTab?.selectedPlanNode ?? null;
    useLayoutEffect(() => {
        panel.current = new PropertiesPanel(grid.current!, title.current!);
        return () => { panel.current?.dispose(); panel.current = null; };
    }, []);
    useLayoutEffect(() => {
        panel.current!.show(node ? node.name.split(/\r?\n/)[0] : null, node ? node.properties ?? [] : null);
    }, [node]);
    return <>
        <div ref={title} id="properties-title" className="properties-title" />
        <div ref={grid} id="properties-grid" className="properties-grid" aria-label="Properties of the selected item" />
    </>;
});
