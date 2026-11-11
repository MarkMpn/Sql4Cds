import type { ConnectionSlot } from "./connections";
import type { QueryTab } from "./query/queryTab";

interface StatusBarElements {
    status: HTMLElement;
    statusMessage: HTMLElement;
    statusIcon: HTMLElement;
    progress: HTMLElement;
    server: HTMLElement;
    lock: HTMLElement;
    domain: HTMLElement;
    user: HTMLElement;
    database: HTMLElement;
    elapsed: HTMLElement;
    rows: HTMLElement;
}

export class StatusBar {
    private timer: ReturnType<typeof setInterval> | null = null;

    constructor(private readonly el: StatusBarElements, private readonly refresh: () => void) {}

    update(tab: QueryTab | null, ready: boolean, progressMessage: string, entry: ConnectionSlot | null): void {
        if (this.timer !== null && (!ready || !tab?.running)) {
            clearInterval(this.timer);
            this.timer = null;
        } else if (ready && tab?.running && this.timer === null) {
            this.timer = setInterval(this.refresh, 100);
        }

        if (ready) {
            this.setStatus(tab?.gridStatusIsError ? tab.gridStatus : tab?.statusMessage ?? "Ready",
                tab?.gridStatusIsError ? "error" : tab?.statusKind ?? "success");
        }
        this.el.progress.textContent = [tab?.gridStatusIsError ? "" : tab?.gridStatus, tab?.running ? progressMessage : ""]
            .filter(Boolean).join(" | ");

        const summary = tab?.connectionSummary;
        this.el.domain.textContent = entry ? summary?.serverName ?? new URL(entry.options.url.includes("://") ? entry.options.url : `https://${entry.options.url}`).hostname : "";
        const secure = entry !== null && (!entry.options.url.includes("://") || new URL(entry.options.url).protocol === "https:");
        this.el.lock.textContent = entry ? secure ? "\uD83D\uDD12" : "\uD83D\uDD13" : "";
        this.el.server.title = entry ? `${secure ? "Secure connection" : "Unencrypted connection"}: ${entry.options.url}` : "Not connected";
        this.el.user.textContent = entry ? summary?.userName ?? "" : "";
        this.el.user.title = `Connected user: ${this.el.user.textContent}`;
        this.el.database.textContent = entry ? summary?.databaseName ?? entry.options.connectionName : "";
        this.el.database.title = `Current database: ${this.el.database.textContent}`;

        const seconds = Math.floor((tab?.elapsedMilliseconds ?? 0) / 1000);
        this.el.elapsed.textContent = [Math.floor(seconds / 3600), Math.floor(seconds / 60) % 60, seconds % 60]
            .map((part) => String(part).padStart(2, "0")).join(":");
        const rows = tab?.totalRowCount ?? 0;
        this.el.rows.textContent = `${rows.toLocaleString()} row${rows === 1 ? "" : "s"}`;
    }

    setStatus(text: string, kind: QueryTab["statusKind"] = "busy"): void {
        this.el.status.textContent = text;
        this.el.statusMessage.title = text;
        this.el.statusIcon.textContent = { success: "\u2713", error: "\u2757", warning: "\u26A0", busy: "\u25F7" }[kind];
        this.el.statusIcon.className = `status-icon ${kind}`;
    }
}
