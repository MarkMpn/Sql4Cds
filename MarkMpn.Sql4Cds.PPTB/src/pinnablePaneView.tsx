import { useEffect, useLayoutEffect, useRef, useState, type ReactNode } from "react";
import { observer } from "mobx-react-lite";
import { useApp } from "./appContext";

export const PinnablePane = observer(function PinnablePane({ name, settingName, children }: {
    name: string;
    settingName: "objectExplorerPinned" | "propertiesPinned";
    children: ReactNode;
}) {
    const app = useApp();
    const pinned = app.settings[settingName];
    const explorer = settingName === "objectExplorerPinned";
    const id = explorer ? "explorer" : "properties";
    const paneId = explorer ? "object-explorer" : "properties";
    const [flyout, setFlyout] = useState(false);
    const [saving, setSaving] = useState(false);
    const pane = useRef<HTMLElement>(null);
    const tab = useRef<HTMLButtonElement>(null);
    const stopResize = useRef<(() => void) | null>(null);
    const focusTab = useRef(false);
    const open = pinned || flyout;
    useLayoutEffect(() => {
        setFlyout(false);
        if (focusTab.current && !pinned) tab.current?.focus();
        focusTab.current = false;
    }, [pinned]);
    useLayoutEffect(() => {
        if (flyout) pane.current?.querySelector<HTMLElement>('[tabindex="0"]')?.focus();
    }, [flyout]);
    useEffect(() => {
        if (pinned || !flyout) return;
        const outside = (event: Event) => {
            if (!(event.target instanceof Node) || pane.current?.contains(event.target) || tab.current?.contains(event.target)) return;
            if (explorer && event.target instanceof Element && event.target.closest(".explorer-context-menu")) return;
            setFlyout(false);
        };
        const keydown = (event: KeyboardEvent) => {
            if (event.key !== "Escape") return;
            event.preventDefault();
            setFlyout(false);
            tab.current?.focus();
        };
        document.addEventListener("pointerdown", outside);
        document.addEventListener("focusin", outside);
        document.addEventListener("keydown", keydown);
        return () => {
            document.removeEventListener("pointerdown", outside);
            document.removeEventListener("focusin", outside);
            document.removeEventListener("keydown", keydown);
        };
    }, [pinned, flyout, explorer]);
    useEffect(() => () => stopResize.current?.(), []);
    const sideTab = <button ref={tab} id={`${id}-tab`} className="pane-tab" aria-label={`Show ${name}`}
        aria-controls={paneId} aria-expanded={!pinned && open} hidden={pinned} disabled={!app.settingsReady}
        onClick={() => setFlyout(!flyout)}>{name}</button>;
    const splitter = <div id={`${id}-splitter`} className="pane-splitter" role="separator"
        aria-label={`Resize ${name}`} aria-orientation="vertical" hidden={!pinned}
        onMouseDown={(event) => {
            event.preventDefault();
            stopResize.current?.();
            const startX = event.clientX;
            const width = pane.current!.getBoundingClientRect().width;
            const move = (moveEvent: MouseEvent) => {
                const max = Math.max(160, document.body.clientWidth - 300);
                const next = width + (moveEvent.clientX - startX) * (explorer ? 1 : -1);
                pane.current!.style.width = `${Math.min(max, Math.max(160, next))}px`;
            };
            const up = () => {
                document.body.classList.remove("resizing-pane");
                window.removeEventListener("mousemove", move);
                window.removeEventListener("mouseup", up);
                stopResize.current = null;
            };
            stopResize.current = up;
            document.body.classList.add("resizing-pane");
            window.addEventListener("mousemove", move);
            window.addEventListener("mouseup", up);
        }} />;
    return <>
        {explorer ? sideTab : splitter}
        <aside ref={pane} id={paneId} className={`side-pane ${explorer ? "object-explorer" : "properties-pane"}${!pinned ? " auto-hide" : ""}`}
            aria-label={name} hidden={!open}>
            <div className="pane-heading">
                <strong>{name}</strong>
                <button id={`${id}-pin`} className="pane-pin" title={`${pinned ? "Unpin" : "Pin"} ${name}`}
                    aria-label={`${pinned ? "Unpin" : "Pin"} ${name}`} aria-pressed={pinned} disabled={saving || !app.settingsReady}
                    onClick={async () => {
                        setSaving(true);
                        try {
                            focusTab.current = pinned;
                            const save = app.saveSetting(settingName, !pinned);
                            await save;
                        } finally { setSaving(false); }
                    }}>
                    <svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true"><path d="M5 2h6M6 2v5l-2 3h8l-2-3V2M8 10v5" /></svg>
                </button>
            </div>
            {children}
        </aside>
        {explorer ? splitter : sideTab}
    </>;
});
