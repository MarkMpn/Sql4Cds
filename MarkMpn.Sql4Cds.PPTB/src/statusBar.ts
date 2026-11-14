import { action, computed, makeObservable, observable, reaction, runInAction, type IReactionDisposer } from "mobx";
import type { ConnectionRegistry } from "./connections";
import type { QueryTab } from "./query/queryTab";
import { mountStatusBar } from "./statusBarView";

type StatusKind = QueryTab["statusKind"];

export class StatusBarState {
    activeTab: QueryTab | null = null;
    ready = false;
    progressMessage = "";
    message = "Loading\u2026";
    kind: StatusKind = "busy";

    constructor() {
        makeObservable(this, {
            activeTab: observable.ref,
            ready: observable,
            progressMessage: observable,
            message: observable,
            kind: observable,
            setStatus: action,
        });
    }

    setStatus(message: string, kind: StatusKind = "busy"): void {
        this.message = message;
        this.kind = kind;
    }
}

export class StatusBar {
    private timer: ReturnType<typeof setInterval> | null = null;
    private tick = 0;
    private readonly stopTimer: IReactionDisposer;
    private readonly unmount: () => void;
    private disposed = false;

    constructor(element: HTMLElement, readonly state: StatusBarState, private readonly connections: ConnectionRegistry) {
        makeObservable<this, "tick">(this, {
            tick: observable,
            display: computed,
        });
        this.stopTimer = reaction(
            () => state.ready && state.activeTab?.running === true,
            (running) => {
                if (running) {
                    this.timer = setInterval(() => runInAction(() => { this.tick++; }), 100);
                } else {
                    this.clearTimer();
                }
            },
            { fireImmediately: true },
        );
        this.unmount = mountStatusBar(element, this);
    }

    get display() {
        const { activeTab: tab, ready, progressMessage } = this.state;
        const text = ready
            ? tab?.gridStatusIsError ? tab.gridStatus : tab?.statusMessage ?? "Ready"
            : this.state.message;
        const kind = ready
            ? tab?.gridStatusIsError ? "error" : tab?.statusKind ?? "success"
            : this.state.kind;
        const progress = [tab?.gridStatusIsError ? "" : tab?.gridStatus, tab?.running ? progressMessage : ""]
            .filter(Boolean).join(" | ");
        const entry = tab?.connected && tab.connectionSlot !== null ? this.connections.get(tab.connectionSlot) : null;
        const summary = tab?.connectionSummary;
        const domain = entry ? summary?.serverName ?? new URL(entry.options.url.includes("://") ? entry.options.url : `https://${entry.options.url}`).hostname : "";
        const secure = entry !== null && (!entry.options.url.includes("://") || new URL(entry.options.url).protocol === "https:");
        const user = entry ? summary?.userName ?? "" : "";
        const database = entry ? summary?.databaseName ?? entry.options.connectionName : "";
        // Date.now() is not observable; only a running foreground query needs clock ticks.
        if (ready && tab?.running) void this.tick;
        const seconds = Math.floor((tab?.elapsedMilliseconds ?? 0) / 1000);
        const elapsed = [Math.floor(seconds / 3600), Math.floor(seconds / 60) % 60, seconds % 60]
            .map((part) => String(part).padStart(2, "0")).join(":");
        const rows = tab?.totalRowCount ?? 0;
        return {
            text, kind, progress, domain, user, database, elapsed,
            icon: { success: "\u2713", error: "\u2757", warning: "\u26A0", busy: "\u25F7" }[kind],
            lock: entry ? secure ? "\uD83D\uDD12" : "\uD83D\uDD13" : "",
            serverTitle: entry ? `${secure ? "Secure connection" : "Unencrypted connection"}: ${entry.options.url}` : "Not connected",
            rows: `${rows.toLocaleString()} row${rows === 1 ? "" : "s"}`,
        };
    }

    private clearTimer(): void {
        if (this.timer !== null) clearInterval(this.timer);
        this.timer = null;
    }

    dispose(): void {
        if (this.disposed) return;
        this.disposed = true;
        this.stopTimer();
        this.clearTimer();
        this.unmount();
    }
}
