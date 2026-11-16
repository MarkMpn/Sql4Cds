import { useLayoutEffect, useRef } from "react";
import { createRoot } from "react-dom/client";
import { flushSync } from "react-dom";
import { observer } from "mobx-react-lite";
import type { App } from "./app";
import type { QueryTab } from "./query/queryTab";
import { Toolbar } from "./toolbar";
import { TabStripView } from "./tabStripView";
import { StatusBarView } from "./statusBarView";
import { AppContext, useApp } from "./appContext";
import { PinnablePane } from "./pinnablePaneView";
import { ObjectExplorer, Properties } from "./paneViews";

const QueryResults = observer(function QueryResults({ tab, active }: { tab: QueryTab; active: boolean }) {
    const host = useRef<HTMLDivElement>(null);
    useLayoutEffect(() => {
        const element = tab.resultsElement;
        host.current!.appendChild(element);
        return () => { element.remove(); };
    }, [tab]);
    return <div ref={host} className="query-result-host" hidden={!active} />;
});

const Workspace = observer(function Workspace() {
    const app = useApp();
    const visible = app.activeTab?.hasResults === true;
    return <div className="query-area">
        <TabStripView />
        <div id="workspace" className="workspace">
            <div id="editor" className="editor" />
            <div id="splitter" className="splitter" role="separator" aria-orientation="horizontal" hidden={!visible} />
            <div id="results-host" className="results-host" hidden={!visible}>
                {app.tabs.map(tab => <QueryResults key={tab.id} tab={tab} active={tab === app.activeTab} />)}
            </div>
        </div>
    </div>;
});

const AppView = observer(function AppView() {
    return <>
        <Toolbar />
        <div className="tool-content">
            <PinnablePane name="Object Explorer" settingName="objectExplorerPinned">
                <ObjectExplorer />
            </PinnablePane>
            <Workspace />
            <PinnablePane name="Properties" settingName="propertiesPinned">
                <Properties />
            </PinnablePane>
        </div>
        <div id="status-bar" className="status-bar">
            <StatusBarView />
        </div>
    </>;
});

export function mountApp(element: HTMLElement, app: App): () => void {
    const root = createRoot(element);
    // The existing editor and pane adapters attach to these stable host elements.
    flushSync(() => root.render(<AppContext.Provider value={app}><AppView /></AppContext.Provider>));
    return () => root.unmount();
}
