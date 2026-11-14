import { createRoot } from "react-dom/client";
import { observer } from "mobx-react-lite";
import type { StatusBar } from "./statusBar";

const StatusBarView = observer(function StatusBarView({ statusBar }: { statusBar: StatusBar }) {
    const display = statusBar.display;
    return <>
        <span id="status-message" role="status" title={display.text}>
            <span id="status-icon" aria-hidden="true" className={`status-icon ${display.kind}`}>{display.icon}</span>
            <span id="status-text">{display.text}</span>
        </span>
        <span id="status-progress" className="status-progress">{display.progress}</span>
        <span id="status-server" title={display.serverTitle}>
            <span id="status-lock" aria-hidden="true">{display.lock}</span>
            <span id="status-domain">{display.domain}</span>
        </span>
        <span id="status-user" title={`Connected user: ${display.user}`}>{display.user}</span>
        <span id="status-database" title={`Current database: ${display.database}`}>{display.database}</span>
        <span id="status-elapsed" title="Elapsed query time">{display.elapsed}</span>
        <span id="status-rows" title="Total rows across all result sets">{display.rows}</span>
    </>;
});

export function mountStatusBar(element: HTMLElement, statusBar: StatusBar): () => void {
    const root = createRoot(element);
    root.render(<StatusBarView statusBar={statusBar} />);
    return () => root.unmount();
}
