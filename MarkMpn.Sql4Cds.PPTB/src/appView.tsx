import { useLayoutEffect, useRef } from "react";
import { createRoot } from "react-dom/client";
import { flushSync } from "react-dom";
import { observer } from "mobx-react-lite";
import type { App } from "./app";
import type { QueryTab } from "./query/queryTab";
import { Toolbar } from "./toolbar";
import { TabStripView } from "./tabStripView";
import { ExplorerTree } from "./objectExplorerView";
import { StatusBarView } from "./statusBarView";

const QueryResults = observer(function QueryResults({ tab, active }: { tab: QueryTab; active: boolean }) {
    const host = useRef<HTMLDivElement>(null);
    useLayoutEffect(() => {
        const element = tab.resultsElement;
        host.current!.appendChild(element);
        return () => { element.remove(); };
    }, [tab]);
    return <div ref={host} className="query-result-host" hidden={!active} />;
});

const Workspace = observer(function Workspace({ app }: { app: App }) {
    const visible = app.activeTab?.hasResults === true;
    return <div className="query-area">
        <TabStripView app={app} />
        <div id="workspace" className="workspace">
            <div id="editor" className="editor" />
            <div id="splitter" className="splitter" role="separator" aria-orientation="horizontal" hidden={!visible} />
            <div id="results-host" className="results-host" hidden={!visible}>
                {app.tabs.map(tab => <QueryResults key={tab.id} tab={tab} active={tab === app.activeTab} />)}
            </div>
        </div>
    </div>;
});

function Pin({ id, name }: { id: string; name: string }) {
    return <button id={`${id}-pin`} className="pane-pin" title={`Unpin ${name}`} aria-label={`Unpin ${name}`} aria-pressed="true">
        <svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true"><path d="M5 2h6M6 2v5l-2 3h8l-2-3V2M8 10v5" /></svg>
    </button>;
}

const AppView = observer(function AppView({ app }: { app: App }) {
    return <>
        <Toolbar app={app} />
        <div className="tool-content">
            <button id="explorer-tab" className="pane-tab" aria-label="Show Object Explorer" aria-controls="object-explorer" aria-expanded="false" hidden>Object Explorer</button>
            <aside id="object-explorer" className="side-pane object-explorer" aria-label="Object Explorer">
                <div className="pane-heading"><strong>Object Explorer</strong><Pin id="explorer" name="Object Explorer" /></div>
                <div id="explorer-tree" className="explorer-tree">
                    {app.viewReady && <ExplorerTree explorer={app.objectExplorer} tree={app.explorerTree} />}
                </div>
            </aside>
            <div id="explorer-splitter" className="pane-splitter" role="separator" aria-label="Resize Object Explorer" aria-orientation="vertical" />
            <Workspace app={app} />
            <div id="properties-splitter" className="pane-splitter" role="separator" aria-label="Resize Properties" aria-orientation="vertical" />
            <aside id="properties" className="side-pane properties-pane" aria-label="Properties">
                <div className="pane-heading"><strong>Properties</strong><Pin id="properties" name="Properties" /></div>
                <div id="properties-title" className="properties-title" />
                <div id="properties-grid" className="properties-grid" aria-label="Properties of the selected item" />
            </aside>
            <button id="properties-tab" className="pane-tab" aria-label="Show Properties" aria-controls="properties" aria-expanded="false" hidden>Properties</button>
        </div>
        <div id="status-bar" className="status-bar">
            {app.viewReady && <StatusBarView statusBar={app.statusBar} />}
        </div>
    </>;
});

export function mountApp(element: HTMLElement, app: App): () => void {
    const root = createRoot(element);
    // The existing editor and pane adapters attach to these stable host elements.
    flushSync(() => root.render(<AppView app={app} />));
    return () => root.unmount();
}
