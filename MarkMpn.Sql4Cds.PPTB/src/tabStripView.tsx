import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { observer } from "mobx-react-lite";
import { useApp } from "./appContext";
import type { QueryTab } from "./query/queryTab";

const TabLabel = observer(function TabLabel({ tab }: { tab: QueryTab }) {
    return <>
        <span className={`tab-indicator${tab.connected ? " connected" : ""}${tab.connecting ? " connecting" : ""}`} />
        <span className="tab-label" title={tab.isDirty ? "Unsaved changes" : tab.title}>{tab.title}</span>
    </>;
});

export const TabStripView = observer(function TabStripView() {
    const app = useApp();
    const strip = useRef<HTMLDivElement>(null);
    const list = useRef<HTMLButtonElement>(null);
    const menu = useRef<HTMLDivElement>(null);
    const [open, setOpen] = useState(false);
    const activeTab = app.activeTab;
    const titles = app.tabs.map(tab => `${tab.title}:${tab.isDirty}`).join("|");
    const title = (tab: QueryTab) => {
        const connection = tab.connectionSlot !== null ? app.connections.get(tab.connectionSlot)?.options.connectionName : undefined;
        return `${tab.title}${connection ? ` - ${connection}` : ""}${tab.isDirty ? " (unsaved changes)" : ""}`;
    };
    const scrollActive = () => {
        const element = strip.current;
        const index = activeTab ? app.tabs.indexOf(activeTab) : -1;
        const child = index >= 0 ? element?.children[index] as HTMLElement | undefined : undefined;
        if (!element || !child) return;
        if (child.offsetLeft < element.scrollLeft) element.scrollLeft = child.offsetLeft;
        else if (child.offsetLeft + child.offsetWidth > element.scrollLeft + element.clientWidth)
            element.scrollLeft = child.offsetLeft + child.offsetWidth - element.clientWidth;
    };
    useLayoutEffect(scrollActive, [activeTab, titles, app.tabs.length]);
    useEffect(() => {
        const element = strip.current!;
        const observer = new ResizeObserver(scrollActive);
        observer.observe(element);
        const wheel = (event: WheelEvent) => {
            const delta = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;
            if (delta) { element.scrollLeft += delta; event.preventDefault(); }
        };
        element.addEventListener("wheel", wheel, { passive: false });
        return () => { observer.disconnect(); element.removeEventListener("wheel", wheel); };
    }, [activeTab, titles, app.tabs.length]);
    useLayoutEffect(() => {
        if (!open || !menu.current || !list.current) return;
        const anchor = list.current.getBoundingClientRect();
        const bounds = menu.current.getBoundingClientRect();
        menu.current.style.left = `${Math.max(0, Math.min(anchor.right - bounds.width, window.innerWidth - bounds.width))}px`;
        menu.current.style.top = `${Math.max(0, Math.min(anchor.bottom, window.innerHeight - bounds.height))}px`;
        const active = menu.current.querySelector<HTMLButtonElement>(".active") ?? menu.current.querySelector<HTMLButtonElement>("button");
        active?.scrollIntoView({ block: "nearest" });
        (active ?? menu.current).focus();
    }, [open]);
    useEffect(() => {
        if (!open) return;
        const dismiss = () => setOpen(false);
        const outside = (event: Event) => {
            if (event.target instanceof Node && !menu.current?.contains(event.target) && !list.current?.contains(event.target)) dismiss();
        };
        const scroll = (event: Event) => {
            if (event.target instanceof Node && !menu.current?.contains(event.target)) dismiss();
        };
        document.addEventListener("pointerdown", outside, true);
        document.addEventListener("focusin", outside);
        document.addEventListener("scroll", scroll, true);
        window.addEventListener("resize", dismiss);
        window.addEventListener("blur", dismiss);
        return () => {
            document.removeEventListener("pointerdown", outside, true);
            document.removeEventListener("focusin", outside);
            document.removeEventListener("scroll", scroll, true);
            window.removeEventListener("resize", dismiss);
            window.removeEventListener("blur", dismiss);
        };
    }, [open]);
    return <div className="tab-bar">
        <div ref={strip} id="tab-strip" className="tab-strip" role="tablist">
            {app.tabs.map(tab => <div key={tab.id} className={`tab${tab === activeTab ? " active" : ""}${tab.running ? " running" : ""}${tab.isDirty ? " modified" : ""}`}
                title={title(tab)} role="tab" aria-selected={tab === activeTab} tabIndex={tab === activeTab ? 0 : -1}
                onClick={() => app.activateTab(tab)}
                onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") { event.preventDefault(); app.activateTab(tab); }
                }}
                onAuxClick={(event) => { if (event.button === 1) void app.closeTab(tab); }}>
                <TabLabel tab={tab} />
                <button className="tab-close" title="Close" aria-label={`Close ${tab.title}`}
                    onClick={(event) => { event.stopPropagation(); void app.closeTab(tab); }}>{"\u00D7"}</button>
            </div>)}
        </div>
        <button ref={list} id="btn-tab-list" className="tab-list-button" title="Show all open queries" aria-label="Show all open queries"
            aria-haspopup="menu" aria-expanded={open} onClick={() => setOpen(!open)}
            onKeyDown={(event) => { if (event.key === "ArrowDown") { event.preventDefault(); setOpen(true); } }}>{"\u22EF"}</button>
        {open && createPortal(<div ref={menu} className="tab-list-menu" role="menu" aria-label="Open queries" tabIndex={-1}
            onKeyDown={(event) => {
                if (event.key === "Escape" || event.key === "Tab") {
                    setOpen(false); list.current?.focus();
                    if (event.key === "Escape") event.preventDefault();
                } else if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
                    event.preventDefault();
                    const buttons = Array.from(menu.current?.querySelectorAll<HTMLButtonElement>("button") ?? []);
                    const index = buttons.indexOf(document.activeElement as HTMLButtonElement);
                    const next = event.key === "Home" ? 0 : event.key === "End" ? buttons.length - 1
                        : (index + (event.key === "ArrowDown" ? 1 : -1) + buttons.length) % buttons.length;
                    buttons[next]?.focus();
                }
            }}>
            {app.tabs.map(tab => <button key={tab.id} role="menuitem" className={`${tab === activeTab ? "active" : ""}${tab.isDirty ? " modified" : ""}`}
                title={title(tab)} onClick={() => { setOpen(false); app.activateTab(tab); app.focusEditor(); }}><TabLabel tab={tab} /></button>)}
        </div>, document.body)}
    </div>;
});
