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
    const [resizeValue, setResizeValue] = useState({ width: 160, max: 160 });
    const pane = useRef<HTMLElement>(null);
    const tab = useRef<HTMLButtonElement>(null);
    const stopResize = useRef<(() => void) | null>(null);
    const focusTab = useRef(false);
    const open = pinned || flyout;
    const resizeLimit = () => {
        let max = Math.max(160, document.body.clientWidth - 300);
        const element = pane.current;
        if (element) {
            const cssMax = window.getComputedStyle(element).maxWidth;
            const parentWidth = element.parentElement?.clientWidth ?? 0;
            const limit = cssMax.endsWith("%") && parentWidth > 0
                ? parentWidth * parseFloat(cssMax) / 100
                : cssMax.endsWith("px") ? parseFloat(cssMax) : NaN;
            if (Number.isFinite(limit)) max = Math.max(160, Math.min(max, Math.floor(limit)));
        }
        return max;
    };
    const measureResize = () => {
        const max = resizeLimit();
        const width = Math.min(max, Math.max(160, Math.round(pane.current?.getBoundingClientRect().width ?? 160)));
        setResizeValue({ width, max });
    };
    const resize = (width: number) => {
        const max = resizeLimit();
        const next = Math.min(max, Math.max(160, width));
        pane.current!.style.width = `${next}px`;
        setResizeValue({ width: Math.min(max, Math.max(160, Math.round(pane.current!.getBoundingClientRect().width || next))), max });
    };
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
    useLayoutEffect(() => {
        if (!pinned) return;
        measureResize();
        window.addEventListener("resize", measureResize);
        return () => window.removeEventListener("resize", measureResize);
    }, [pinned]);
    const sideTab = <button ref={tab} id={`${id}-tab`} className="pane-tab" aria-label={`Show ${name}`}
        aria-controls={paneId} aria-expanded={!pinned && open} hidden={pinned} disabled={!app.settingsReady}
        onClick={() => setFlyout(!flyout)}>{name}</button>;
    const splitter = <div id={`${id}-splitter`} className="pane-splitter" role="separator"
        aria-label={`Resize ${name}`} aria-orientation="vertical" aria-controls={paneId}
        aria-valuemin={160} aria-valuemax={resizeValue.max} aria-valuenow={resizeValue.width}
        tabIndex={0} hidden={!pinned}
        onFocus={measureResize}
        onKeyDown={(event) => {
            const width = pane.current!.getBoundingClientRect().width;
            let next: number;
            switch (event.key) {
                case "ArrowLeft": next = width + (explorer ? -10 : 10); break;
                case "ArrowRight": next = width + (explorer ? 10 : -10); break;
                case "Home": next = 160; break;
                case "End": next = resizeLimit(); break;
                default: return;
            }
            event.preventDefault();
            resize(next);
        }}
        onMouseDown={(event) => {
            event.preventDefault();
            stopResize.current?.();
            const startX = event.clientX;
            const width = pane.current!.getBoundingClientRect().width;
            const move = (moveEvent: MouseEvent) => {
                const next = width + (moveEvent.clientX - startX) * (explorer ? 1 : -1);
                resize(next);
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
