import { useEffect, useLayoutEffect, useRef, useState, type ReactNode } from "react";
import { observer } from "mobx-react-lite";
import type { App } from "./app";

function Icon({ path, className = "" }: { path: string; className?: string }) {
    return <svg className={`icon ${className}`} viewBox="0 0 16 16" aria-hidden="true"><path d={path} /></svg>;
}

function SplitMenu({ id, label, disabled, command, children }: { id: string; label: string; disabled: boolean; command: ReactNode; children: ReactNode }) {
    const [open, setOpen] = useState(false);
    const wrapper = useRef<HTMLDivElement>(null);
    const toggle = useRef<HTMLButtonElement>(null);
    const menu = useRef<HTMLDivElement>(null);
    const visible = open && !disabled;
    useEffect(() => { if (disabled) setOpen(false); }, [disabled]);
    useLayoutEffect(() => {
        if (visible) menu.current?.querySelector<HTMLButtonElement>("button:not(:disabled)")?.focus();
    }, [visible]);
    useEffect(() => {
        if (!visible) return;
        const outside = (event: Event) => {
            if (event.target instanceof Node && !wrapper.current?.contains(event.target)) setOpen(false);
        };
        document.addEventListener("pointerdown", outside);
        document.addEventListener("focusin", outside);
        return () => {
            document.removeEventListener("pointerdown", outside);
            document.removeEventListener("focusin", outside);
        };
    }, [visible]);
    const close = () => { setOpen(false); toggle.current?.focus(); };
    return <div ref={wrapper} className="toolbar-split">
        {command}
        <button ref={toggle} id={`btn-${id}-menu`} className="split-toggle" aria-label={`${label} options`}
            aria-haspopup="menu" aria-expanded={visible} aria-controls={`${id}-menu`} title={`${label} options`}
            disabled={disabled} onClick={() => setOpen(!open)}
            onKeyDown={(event) => {
                if (event.key === "ArrowDown") { event.preventDefault(); setOpen(true); }
            }}>{"\u25BE"}</button>
        <div ref={menu} id={`${id}-menu`} className="toolbar-menu" role="menu" hidden={!visible}
            onClick={(event) => { if (event.target instanceof Element && event.target.closest("button")) close(); }}
            onKeyDown={(event) => {
                if (event.key === "Escape") { event.preventDefault(); close(); }
                else if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
                    event.preventDefault();
                    const buttons = Array.from(menu.current?.querySelectorAll<HTMLButtonElement>("button:not(:disabled)") ?? []);
                    const index = buttons.indexOf(document.activeElement as HTMLButtonElement);
                    const next = event.key === "Home" ? 0 : event.key === "End" ? buttons.length - 1
                        : (index + (event.key === "ArrowDown" ? 1 : -1) + buttons.length) % buttons.length;
                    buttons[next]?.focus();
                }
            }}>{children}</div>
    </div>;
}

export const Toolbar = observer(function Toolbar({ app }: { app: App }) {
    const tab = app.activeTab;
    const state = app.toolbarState;
    const slot = tab?.connectionSlot;
    const selected = slot !== null && slot !== undefined && app.connections.get(slot) ? String(slot) : "";
    return <div className="toolbar" role="toolbar" aria-label="Query commands">
        <select id="connection-select" aria-label="Connection" title="Connection" disabled={state.connectionDisabled}
            value={selected} onChange={(event) => { if (tab) void app.connectTab(tab, Number(event.target.value)); }}>
            <option value="" disabled>{app.connections.all.length ? "Select a connection\u2026" : "No connections available"}</option>
            {app.connections.all.map(entry => <option key={entry.slot} value={entry.slot} disabled={entry.status === "connecting"}
                title={entry.error ?? entry.connection.url}>
                {entry.options.connectionName}{entry.status === "connecting" ? " (Connecting\u2026)" : entry.error ? " \u26A0" : ""}
            </option>)}
        </select>
        <button id="btn-new" title="New query (Ctrl+N)" disabled={!app.editorReady} onClick={() => app.addTab()}><Icon path="M3 1.5h7l3 3V14H3zM10 1.5V5h3M5 8h6M8 5v6" /> New Query</button>
        <button id="btn-open" className="icon-only" aria-label="Open" title="Open a saved SQL query (Ctrl+O)" disabled={!app.editorReady} onClick={() => void app.openQuery()}><Icon path="M1.5 13.5v-10H6l2 2h6v3M2 13.5l2-6h11l-2 6z" /></button>
        <SplitMenu id="save" label="Save" disabled={state.saveAsDisabled} command={
            <button id="btn-save" className="icon-only" aria-label="Save" title="Save the current query (Ctrl+S)" disabled={state.saveDisabled}
                onClick={() => { if (tab) void app.saveTab(tab); }}><Icon path="M2 2h10l2 2v10H2zM5 2v4h6V2M5 14V9h6v5" /></button>}>
            <button id="btn-save-as" role="menuitem" title="Save the current query to a new file (Ctrl+Shift+S)" disabled={state.saveAsDisabled}
                onClick={() => { if (tab) void app.saveTab(tab, true); }}>Save As</button>
        </SplitMenu>
        <span className="toolbar-separator" />
        <button id="btn-run" title="Execute the selected text, or the whole query if nothing is selected (F5 / Ctrl+E)" disabled={state.runDisabled}
            onClick={() => void app.runQuery()}><Icon className="execute-icon" path="M4 2l9 6-9 6z" /> Execute</button>
        <button id="btn-cancel" className="icon-only" aria-label="Stop" title="Stop the running query" disabled={state.cancelDisabled}
            onClick={() => void tab?.cancel()}><Icon className="stop-icon" path="M3 3h10v10H3z" /></button>
        <button id="btn-estimated-plan" className="icon-only" aria-label="Estimated Execution Plan" title="Display the estimated execution plan without running the query (Ctrl+L)" disabled={state.runDisabled}
            onClick={() => void app.runQuery(true)}><Icon className="plan-icon" path="M1 6h4v4H1zM11 1.5h4v4h-4zM11 10.5h4v4h-4zM5 8h3M8 3.5v9M8 3.5h3M8 12.5h3" /></button>
        <button id="btn-edit-fetchxml" className="icon-only" aria-label="Edit in FetchXML Studio" title="Edit in FetchXML Studio" disabled={app.fetchXmlForEditing === null}
            onClick={() => { const xml = app.fetchXmlForEditing; if (xml !== null) void app.launchFetchXmlStudio(xml); }}><Icon path="M5 3L1 8l4 5M11 3l4 5-4 5M9 2L7 14" /></button>
        <SplitMenu id="convert" label="Convert" disabled={state.convertDisabled} command={
            <button id="btn-convert" title={`Convert to ${app.convertTarget === "fetchxml" ? "FetchXML" : "Power BI (M)"}`} disabled={state.convertDisabled}
                onClick={() => void app.transformQuery("convert")}><Icon path="M2 5h11l-3-3M14 11H3l3 3M13 5l-3 3M3 11l3-3" /> Convert</button>}>
            <button id="btn-convert-fetchxml" role="menuitemradio" aria-checked={app.convertTarget === "fetchxml"} disabled={state.convertDisabled} onClick={() => app.selectConversion("fetchxml")}>FetchXML</button>
            <button id="btn-convert-m" role="menuitemradio" aria-checked={app.convertTarget === "m"} disabled={state.convertDisabled} onClick={() => app.selectConversion("m")}>Power BI (M)</button>
        </SplitMenu>
        <span className="toolbar-separator" />
        <button id="btn-actual-plan" className="toggle icon-only" aria-label="Actual Execution Plan" title="Include the actual execution plan when running the query (Ctrl+M)" aria-pressed={app.includeActualPlan} disabled={!app.editorReady}
            onClick={() => void app.toggleActualPlan()}><Icon className="plan-icon" path="M1 6h4v4H1zM11 1.5h4v4h-4zM11 10.5h4v4h-4zM5 8h3M8 3.5v9M8 3.5h3M8 12.5h3M1 2l1 1 2-2" /></button>
        <button id="btn-results-text" className="toggle icon-only" aria-label="Results to Text" title="Results to Text (Ctrl+T); applies to the next execution" aria-pressed={tab?.resultsMode === "text"} disabled={state.resultsDisabled}
            onClick={() => app.setResultsMode("text")}><Icon path="M2 2h12v12H2zM4 5h8M4 8h8M4 11h5" /></button>
        <button id="btn-results-grid" className="toggle icon-only" aria-label="Results to Grid" title="Results to Grid (Ctrl+D); applies to the next execution" aria-pressed={!!tab && tab.resultsMode !== "text"} disabled={state.resultsDisabled}
            onClick={() => app.setResultsMode("grid")}><Icon path="M2 2h12v12H2zM2 6h12M2 10h12M6 2v12M10 2v12" /></button>
        <span className="toolbar-separator" />
        <SplitMenu id="format" label="Format" disabled={state.convertDisabled} command={
            <button id="btn-format" title="Format SQL document" disabled={state.formatDisabled} onClick={() => void app.transformQuery("format")}><Icon path="M5 3h9M5 6h6M5 9h9M5 12h6M1 3h1M1 6h1M1 9h1M1 12h1" /> Format</button>}>
            <button id="btn-display-names" role="menuitem" disabled={state.convertDisabled} onClick={() => void app.transformQuery("format", true)}>Add Display Names</button>
        </SplitMenu>
        <button id="btn-settings" title="Settings" disabled={!app.editorReady} onClick={() => void app.editSettings()}><svg className="icon" viewBox="0 0 16 16" aria-hidden="true"><path d="M6 1h4l.5 2 2 .5L14 6v4l-1.5 2.5-2 .5-.5 2H6l-.5-2-2-.5L2 10V6l1.5-2.5 2-.5z" /><circle cx="8" cy="8" r="2.5" /></svg> Settings</button>
        <span className="toolbar-spacer" />
        <a className="toolbar-credit" href="https://markcarrington.dev/sql-4-cds/" target="_blank" rel="noopener noreferrer">SQL 4 CDS by Mark Carrington</a>
    </div>;
});
