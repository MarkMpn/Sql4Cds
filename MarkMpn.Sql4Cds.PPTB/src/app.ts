import "./styles.css";
import { ConnectionRegistry } from "./connections";
import { configureMonaco, monaco } from "./editor/monacoSetup";
import { Sql4CdsClient } from "./lsp/client";
import { ConfirmationResult, type ConfirmationParams } from "./lsp/protocol";
import { QueryTab, type QueryTabHost } from "./query/queryTab";
import { loadSettings, saveSettings, showSettingsDialog, type Sql4CdsSettings } from "./settings";

const DEFAULT_QUERY = "-- Enter your SQL query here and press F5 or Ctrl+E to run it\nSELECT TOP 10 *\nFROM   account\n";

class App implements QueryTabHost {
    readonly client: Sql4CdsClient;
    private readonly connections: ConnectionRegistry;
    private readonly tabs: QueryTab[] = [];
    private activeTab: QueryTab | null = null;
    private editor!: monaco.editor.IStandaloneCodeEditor;
    private settings!: Sql4CdsSettings;
    private ready = false;
    private confirmationQueue: Promise<void> = Promise.resolve();

    private readonly el = {
        tabStrip: byId("tab-strip"),
        editor: byId("editor"),
        results: byId("results-host"),
        splitter: byId("splitter"),
        workspace: byId("workspace"),
        newTab: byId<HTMLButtonElement>("btn-new"),
        run: byId<HTMLButtonElement>("btn-run"),
        cancel: byId<HTMLButtonElement>("btn-cancel"),
        settings: byId<HTMLButtonElement>("btn-settings"),
        connection: byId<HTMLSelectElement>("connection-select"),
        status: byId("status-text"),
        progress: byId("status-progress"),
    };

    constructor() {
        this.client = new Sql4CdsClient({
            progress: (message) => (this.el.progress.textContent = message),
            log: (params) => {
                if (params.type === 1) console.error(params.message);
                else console.log(params.message);
            },
            exit: (snapshot) => {
                this.ready = false;
                this.setStatus(`SQL 4 CDS engine stopped${snapshot.failure ? `: ${snapshot.failure}` : ""}`, true);
                this.updateToolbar();
            },
        });
        this.connections = new ConnectionRegistry(this.client);
    }

    async start(): Promise<void> {
        this.settings = await loadSettings();
        await this.applyTheme();
        this.createEditor();
        this.bindToolbar();
        this.bindSplitter();

        configureMonaco(this.client, (uri) => this.tabs.some((t) => t.uri === uri && t.connected));

        window.toolboxAPI.events.on((_event, payload) => {
            if (payload.event.startsWith("connection:")) void this.onConnectionsChanged();
            else if (payload.event === "settings:updated") void this.applyTheme();
        });

        this.setStatus("Starting SQL 4 CDS engine…");
        try {
            await this.client.start({ ...this.settings });
        } catch (err) {
            this.setStatus(`Unable to start the SQL 4 CDS engine: ${errorMessage(err)}`, true);
            await notify("Unable to start SQL 4 CDS", errorMessage(err), "error");
            return;
        }

        this.ready = true;
        this.setStatus("Ready");

        await this.connections.refresh();
        this.renderConnectionList();

        // Register every connection so they can all be used in cross-instance queries
        await Promise.all(
            this.connections.all.map((c) =>
                this.connections.register(c.slot).catch((err) => notify(`Unable to connect to ${c.connection.name}`, errorMessage(err), "error")),
            ),
        );
        this.renderConnectionList();
        this.addTab(DEFAULT_QUERY);
    }

    // Editor & tabs

    private createEditor(): void {
        this.editor = monaco.editor.create(this.el.editor, {
            language: "sql",
            automaticLayout: true,
            minimap: { enabled: false },
            wordBasedSuggestions: "off",
            quickSuggestions: { other: true, comments: false, strings: false },
            suggestOnTriggerCharacters: true,
            fixedOverflowWidgets: true,
            scrollBeyondLastLine: false,
            fontSize: 13,
            tabSize: 4,
            model: null,
        });

        this.editor.addAction({
            id: "sql4cds.run",
            label: "Run Query",
            keybindings: [monaco.KeyCode.F5, monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyE],
            contextMenuGroupId: "navigation",
            contextMenuOrder: 0,
            run: () => this.runQuery(),
        });
        this.editor.addAction({
            id: "sql4cds.newTab",
            label: "New Query",
            keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyMod.Alt | monaco.KeyCode.KeyN],
            run: () => {
                this.addTab();
            },
        });
    }

    private addTab(text = ""): QueryTab {
        // Connect new tabs to the same connection as the current tab, or the first available connection
        const slot = this.activeTab?.connectionSlot ?? this.connections.all[0]?.slot;

        const tab = new QueryTab(this, text);
        this.tabs.push(tab);
        this.el.results.appendChild(tab.resultsElement);
        this.activateTab(tab);

        if (slot !== undefined && slot !== null) void this.connectTab(tab, slot);

        return tab;
    }

    private activateTab(tab: QueryTab): void {
        if (this.activeTab === tab) return;

        if (this.activeTab) this.activeTab.viewState = this.editor.saveViewState();

        this.activeTab = tab;
        this.editor.setModel(tab.model);
        if (tab.viewState) this.editor.restoreViewState(tab.viewState);
        this.editor.focus();

        for (const t of this.tabs) t.resultsElement.hidden = t !== tab;
        this.renderTabs();
        this.renderConnectionList();
        this.updateToolbar();
    }

    private async closeTab(tab: QueryTab): Promise<void> {
        const index = this.tabs.indexOf(tab);
        if (index < 0) return;

        this.tabs.splice(index, 1);
        if (this.activeTab === tab) {
            this.activeTab = null;
            if (this.tabs.length === 0) {
                this.addTab();
            } else {
                this.activateTab(this.tabs[Math.min(index, this.tabs.length - 1)]);
            }
        }

        this.renderTabs();
        await tab.dispose();
    }

    private renderTabs(): void {
        const fragment = document.createDocumentFragment();

        for (const tab of this.tabs) {
            const el = document.createElement("div");
            el.className = "tab";
            el.classList.toggle("active", tab === this.activeTab);
            el.classList.toggle("running", tab.running);
            el.title = tab.connectionSlot !== null ? `${tab.title} - ${this.connections.get(tab.connectionSlot)?.connection.name ?? ""}` : tab.title;
            el.addEventListener("click", () => this.activateTab(tab));
            el.addEventListener("auxclick", (e) => {
                if (e.button === 1) void this.closeTab(tab);
            });

            const indicator = document.createElement("span");
            indicator.className = "tab-indicator";
            indicator.classList.toggle("connected", tab.connected);
            indicator.classList.toggle("connecting", tab.connecting);

            const label = document.createElement("span");
            label.className = "tab-label";
            label.textContent = tab.title;

            const close = document.createElement("button");
            close.className = "tab-close";
            close.title = "Close";
            close.textContent = "×";
            close.addEventListener("click", (e) => {
                e.stopPropagation();
                void this.closeTab(tab);
            });

            el.append(indicator, label, close);
            fragment.appendChild(el);
        }

        this.el.tabStrip.replaceChildren(fragment);
    }

    // Connections

    private renderConnectionList(): void {
        const select = this.el.connection;
        select.replaceChildren();

        const placeholder = document.createElement("option");
        placeholder.value = "";
        placeholder.textContent = this.connections.all.length ? "Select a connection…" : "No connections available";
        placeholder.disabled = true;
        select.appendChild(placeholder);

        for (const entry of this.connections.all) {
            const option = document.createElement("option");
            option.value = String(entry.slot);
            option.textContent = `${entry.connection.name} (${entry.options.connectionName})${entry.error ? " ⚠" : ""}`;
            option.title = entry.error ?? entry.connection.url;
            select.appendChild(option);
        }

        const slot = this.activeTab?.connectionSlot;
        select.value = slot !== null && slot !== undefined && this.connections.get(slot) ? String(slot) : "";
    }

    private async connectTab(tab: QueryTab, slot: number): Promise<void> {
        try {
            await this.connections.register(slot);
            await tab.connect(slot);
        } catch (err) {
            await notify("Connection failed", errorMessage(err), "error");
        }
    }

    private async onConnectionsChanged(): Promise<void> {
        if (!this.ready) return;

        const changed = await this.connections.refresh();
        if (changed.length === 0) return;

        for (const slot of changed) {
            const tabs = this.tabs.filter((t) => t.connectionSlot === slot);
            await Promise.all(tabs.map((t) => t.disconnect()));

            if (this.connections.get(slot)) {
                await this.connections.register(slot).catch((err) => notify("Connection failed", errorMessage(err), "error"));
                await Promise.all(tabs.map((t) => this.connectTab(t, slot)));
            } else {
                for (const t of tabs) t.connectionSlot = null;
            }
        }

        this.renderConnectionList();
        this.renderTabs();
        this.updateToolbar();
    }

    // Query execution

    private async runQuery(): Promise<void> {
        const tab = this.activeTab;
        if (!tab || tab.running || !this.ready) return;

        try {
            await tab.execute(this.editor.getSelection());
        } catch (err) {
            await notify("Unable to run query", errorMessage(err), "warning");
        }
    }

    confirm(params: ConfirmationParams): void {
        // Only show one confirmation at a time
        this.confirmationQueue = this.confirmationQueue.then(async () => {
            const result = await showConfirmation(params.msg);
            await this.client.confirm(params.ownerUri, result);
        });
    }

    getConnectionOptions(slot: number) {
        return this.connections.get(slot)?.options ?? null;
    }

    onStateChanged(tab: QueryTab): void {
        this.renderTabs();
        if (tab === this.activeTab) {
            this.renderConnectionList();
            this.updateToolbar();
        }
    }

    copyToClipboard(text: string): void {
        void window.toolboxAPI.utils.copyToClipboard(text);
    }

    // Toolbar & layout

    private bindToolbar(): void {
        this.el.newTab.addEventListener("click", () => this.addTab());
        this.el.run.addEventListener("click", () => void this.runQuery());
        this.el.cancel.addEventListener("click", () => void this.activeTab?.cancel());
        this.el.settings.addEventListener("click", () => void this.editSettings());
        this.el.connection.addEventListener("change", () => {
            const tab = this.activeTab;
            const slot = Number.parseInt(this.el.connection.value, 10);
            if (tab && Number.isFinite(slot)) void this.connectTab(tab, slot);
        });

        window.addEventListener("keydown", (e) => {
            if (e.key === "F5" || ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "e")) {
                e.preventDefault();
                void this.runQuery();
            }
        });
    }

    private updateToolbar(): void {
        const tab = this.activeTab;
        this.el.run.disabled = !this.ready || !tab?.connected || tab.running;
        this.el.cancel.disabled = !this.ready || !tab?.running;
        this.el.connection.disabled = !this.ready || !tab || tab.running || tab.connecting;

        if (tab?.running) this.setStatus("Executing query…");
        else if (tab?.connecting) this.setStatus("Connecting…");
        else if (this.ready) this.setStatus(tab?.connected ? "Connected" : "Not connected");

        if (!tab?.running) this.el.progress.textContent = "";
    }

    private bindSplitter(): void {
        let startY = 0;
        let startHeight = 0;

        const move = (e: MouseEvent) => {
            const total = this.el.workspace.clientHeight;
            const height = Math.min(total - 80, Math.max(60, startHeight - (e.clientY - startY)));
            this.el.results.style.flexBasis = `${height}px`;
        };
        const up = () => {
            document.body.classList.remove("resizing");
            window.removeEventListener("mousemove", move);
            window.removeEventListener("mouseup", up);
        };

        this.el.splitter.addEventListener("mousedown", (e) => {
            e.preventDefault();
            startY = e.clientY;
            startHeight = this.el.results.clientHeight;
            document.body.classList.add("resizing");
            window.addEventListener("mousemove", move);
            window.addEventListener("mouseup", up);
        });
    }

    private async applyTheme(): Promise<void> {
        let theme: "light" | "dark" = "light";
        try {
            theme = await window.toolboxAPI.utils.getCurrentTheme();
        } catch {
            // Use the default theme
        }

        document.body.classList.toggle("dark", theme === "dark");
        monaco.editor.setTheme(theme === "dark" ? "vs-dark" : "vs");
    }

    private async editSettings(): Promise<void> {
        const updated = await showSettingsDialog(this.settings);
        if (!updated) return;

        this.settings = updated;
        try {
            await saveSettings(updated);
            if (this.ready) await this.client.changeConfiguration({ ...updated });
        } catch (err) {
            await notify("Unable to save settings", errorMessage(err), "error");
        }
    }

    private setStatus(text: string, isError = false): void {
        this.el.status.textContent = text;
        this.el.status.classList.toggle("error", isError);
    }
}

function showConfirmation(message: string): Promise<ConfirmationResult> {
    return new Promise((resolve) => {
        const dialog = document.createElement("dialog");
        dialog.className = "confirm-dialog";

        const form = document.createElement("form");
        form.method = "dialog";

        const title = document.createElement("h2");
        title.textContent = "Confirm";

        const text = document.createElement("p");
        text.textContent = message;

        const buttons = document.createElement("div");
        buttons.className = "dialog-buttons";

        for (const [label, value, primary] of [
            ["Yes", ConfirmationResult.Yes, true],
            ["Yes to all", ConfirmationResult.All, false],
            ["No", ConfirmationResult.No, false],
        ] as const) {
            const button = document.createElement("button");
            button.value = String(value);
            button.textContent = label;
            if (primary) button.className = "primary";
            buttons.appendChild(button);
        }

        form.append(title, text, buttons);
        dialog.appendChild(form);
        document.body.appendChild(dialog);

        dialog.addEventListener("close", () => {
            dialog.remove();
            const value = Number.parseInt(dialog.returnValue, 10);
            resolve(value === ConfirmationResult.Yes || value === ConfirmationResult.All ? value : ConfirmationResult.No);
        });

        dialog.showModal();
    });
}

function byId<T extends HTMLElement = HTMLElement>(id: string): T {
    const el = document.getElementById(id);
    if (!el) throw new Error(`Missing element #${id}`);
    return el as T;
}

function errorMessage(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
}

async function notify(title: string, body: string, type: "info" | "success" | "warning" | "error"): Promise<void> {
    try {
        await window.toolboxAPI.utils.showNotification({ title, body, type });
    } catch {
        console.error(`${title}: ${body}`);
    }
}

if (window.toolboxAPI) {
    void new App().start().catch((err) => {
        console.error(err);
        void notify("SQL 4 CDS", errorMessage(err), "error");
    });
} else {
    document.getElementById("status-text")!.textContent = "SQL 4 CDS must be run inside Power Platform ToolBox";
}
