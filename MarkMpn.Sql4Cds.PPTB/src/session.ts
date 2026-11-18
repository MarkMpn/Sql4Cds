/** A single query tab persisted as part of the session, equivalent to TabContent in the XrmToolBox tool */
export interface SessionTab {
    /** The path of the file the tab was opened from or saved to, if any */
    filename: string | null;
    /** The editor content, or null for file-backed tabs with no unsaved changes */
    query: string | null;
    /** Indicates if the tab had unsaved changes */
    dirty: boolean;
    /** The PPTB connection id the tab was connected to, if session connections are remembered */
    connectionId: string | null;
}

/** The set of open query tabs persisted between sessions */
export interface SessionState {
    tabs: SessionTab[];
    /** Index into tabs of the tab that was active */
    activeTab: number;
}

const SESSION_KEY = "session";

export async function loadSession(): Promise<SessionState | null> {
    try {
        return sanitizeSession(await window.toolboxAPI.settings.get(SESSION_KEY));
    } catch {
        return null;
    }
}

/** Persists the session, or removes any previously persisted session when null */
export async function saveSession(state: SessionState | null): Promise<void> {
    await window.toolboxAPI.settings.set(SESSION_KEY, state);
}

/** Validates a previously persisted session, discarding anything malformed */
export function sanitizeSession(stored: unknown): SessionState | null {
    if (typeof stored !== "object" || stored === null) return null;

    const state = stored as Partial<SessionState>;
    if (!Array.isArray(state.tabs)) return null;

    const tabs = state.tabs
        .map((tab: unknown): SessionTab | null => {
            if (typeof tab !== "object" || tab === null) return null;
            const t = tab as Partial<SessionTab>;
            const filename = typeof t.filename === "string" && t.filename ? t.filename : null;
            const query = typeof t.query === "string" ? t.query : null;
            if (filename === null && query === null) return null;
            return {
                filename,
                query,
                dirty: t.dirty === true,
                connectionId: typeof t.connectionId === "string" && t.connectionId ? t.connectionId : null,
            };
        })
        .filter((tab): tab is SessionTab => tab !== null);

    if (tabs.length === 0) return null;

    const activeTab = typeof state.activeTab === "number" && Number.isInteger(state.activeTab) &&
        state.activeTab >= 0 && state.activeTab < tabs.length
        ? state.activeTab
        : tabs.length - 1;

    return { tabs, activeTab };
}
