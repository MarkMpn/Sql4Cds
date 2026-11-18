import { useLayoutEffect, useRef, useState, type KeyboardEvent } from "react";
import { observer } from "mobx-react-lite";
import { useApp } from "./appContext";
import { propertyRows, type PropertyRow } from "./executionPlan/propertyRows";

export const PropertiesPanel = observer(function PropertiesPanel() {
    const node = useApp().activeTab?.selectedPlanNode ?? null;
    const properties = node?.properties ?? null;
    const name = node?.name.split(/\r?\n/)[0] ?? "";
    const [state, setState] = useState({
        properties,
        expanded: new Set<string>(),
        selectedKey: "",
    });
    if (state.properties !== properties) {
        setState({ properties, expanded: new Set(), selectedKey: "" });
    }
    const current = state.properties === properties ? state : { expanded: new Set<string>(), selectedKey: "" };
    const rows = propertyRows(properties ?? [], current.expanded);
    const selectedKey = rows.some(row => row.key === current.selectedKey) ? current.selectedKey : rows[0]?.key;
    const grid = useRef<HTMLDivElement>(null);
    const focused = useRef(false);
    useLayoutEffect(() => {
        if (!focused.current) return;
        const active = document.activeElement;
        if (active === document.body || grid.current?.contains(active)) {
            grid.current?.querySelector<HTMLElement>('[tabindex="0"]')?.focus();
        }
    }, [properties, selectedKey]);
    const select = (key: string) => setState(previous => ({ ...previous, selectedKey: key }));
    const toggle = (key: string) => setState(previous => {
        const expanded = new Set(previous.expanded);
        if (!expanded.delete(key)) expanded.add(key);
        return { ...previous, expanded };
    });
    const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
        const index = rows.findIndex(row => row.key === event.currentTarget.dataset.key);
        const row = rows[index];
        if (!row) return;
        let target: PropertyRow | undefined;
        switch (event.key) {
            case "ArrowDown": target = rows[index + 1]; break;
            case "ArrowUp": target = rows[index - 1]; break;
            case "Home": target = rows[0]; break;
            case "End": target = rows[rows.length - 1]; break;
            case "ArrowRight":
                if (row.expandable && !row.expanded) toggle(row.key);
                else if (row.expanded) target = rows[index + 1];
                break;
            case "ArrowLeft":
                if (row.expanded) toggle(row.key);
                else target = rows.slice(0, index).reverse().find(parent => row.key.startsWith(`${parent.key}/`));
                break;
            case "Enter":
            case " ":
                if (row.expandable) toggle(row.key);
                break;
            default: return;
        }
        event.preventDefault();
        if (target) select(target.key);
    };
    return <>
        <div id="properties-title" className="properties-title" title={name}>{name}</div>
        <div ref={grid} id="properties-grid" className="properties-grid" role="tree" aria-label="Properties of the selected item"
            tabIndex={rows.length === 0 ? 0 : -1}
            onFocusCapture={() => { focused.current = true; }}
            onBlurCapture={event => {
                if (event.relatedTarget instanceof Node && !event.currentTarget.contains(event.relatedTarget)) focused.current = false;
            }}>
            {!properties ? <div className="properties-placeholder">Select an execution plan node to show its properties</div>
                : rows.length === 0 ? <div className="properties-placeholder">No properties</div>
                    : rows.map(row => {
                        const display = row.property.displayValue ?? (typeof row.property.value === "string" ? row.property.value : "");
                        return <div key={row.key} className="property-row" role="treeitem" data-key={row.key}
                            aria-level={row.level} aria-setsize={row.size} aria-posinset={row.position} aria-selected={row.key === selectedKey}
                            aria-expanded={row.expandable ? row.expanded : undefined} tabIndex={row.key === selectedKey ? 0 : -1}
                            onFocus={() => select(row.key)}
                            onKeyDown={onKeyDown}
                            onClick={event => {
                                event.currentTarget.focus();
                                if (row.expandable) toggle(row.key);
                            }}>
                            <div className="property-name" style={{ paddingLeft: (row.level - 1) * 12 + 4 }} title={row.property.name}>
                                <span className="property-toggle" aria-hidden="true">{row.expandable ? row.expanded ? "\u25BE" : "\u25B8" : ""}</span>
                                {row.property.name}
                            </div>
                            <div className="property-value" title={display}>{display}</div>
                        </div>;
                    })}
        </div>
    </>;
});
