import { observer } from "mobx-react-lite";
import { useApp } from "./appContext";

export const StatusBarView = observer(function StatusBarView() {
    const display = useApp().statusBar.display;
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
