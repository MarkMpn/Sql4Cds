import "./styles.css";
import { configureMonaco, monaco, toMonacoRange } from "./editor/monacoSetup";
import { ConnectionRegistry } from "./connections";
import { Sql4CdsClient } from "./lsp/client";
import { ConfirmationResult, type ConfirmationParams, type EntityReferenceValue, type Range } from "./lsp/protocol";
import { QueryTab, type QueryTabHost } from "./query/queryTab";
import { ObjectExplorer } from "./objectExplorer";
import { PinnablePane } from "./pinnablePane";
import { PropertiesPanel } from "./propertiesPanel";
import { getFetchXmlQuery } from "./executionPlan/executionPlanView";
import { loadSettings, saveSettings, showSettingsDialog, type Sql4CdsSettings } from "./settings";

const DEFAULT_QUERY = "-- Enter your SQL query here and press F5 or Ctrl+E to run it\nSELECT TOP 10 *\nFROM   account\n";

class App implements QueryTabHost {
    readonly client: Sql4CdsClient;
    private readonly connections: ConnectionRegistry;
    private readonly objectExplorer: ObjectExplorer;
    private readonly tabs: QueryTab[] = [];
    private activeTab: QueryTab | null = null;
    private editor!: monaco.editor.IStandaloneCodeEditor;
    private settings!: Sql4CdsSettings;
    private ready = false;
    private confirmationQueue: Promise<void> = Promise.resolve();
    private statusTimer: ReturnType<typeof setInterval> | null = null;
    private progressMessage = "";
    private closePreventionApplied = false;
    private closePreventionQueue: Promise<void> = Promise.resolve();
    private connectionChangeQueue: Promise<void> = Promise.resolve();
    private explorerPane: PinnablePane | null = null;
    private propertiesPane: PinnablePane | null = null;
    private propertiesPanel: PropertiesPanel | null = null;
    private convertTarget: "fetchxml" | "m" = "fetchxml";
    private transforming = false;

    private readonly el = {
        tabStrip: byId("tab-strip"),
        editor: byId("editor"),
        results: byId("results-host"),
        splitter: byId("splitter"),
        workspace: byId("workspace"),
        explorer: byId("object-explorer"),
        explorerTree: byId("explorer-tree"),
        explorerSplitter: byId("explorer-splitter"),
        explorerPin: byId<HTMLButtonElement>("explorer-pin"),
        explorerTab: byId<HTMLButtonElement>("explorer-tab"),
        properties: byId("properties"),
        propertiesTitle: byId("properties-title"),
        propertiesGrid: byId("properties-grid"),
        propertiesSplitter: byId("properties-splitter"),
        propertiesPin: byId<HTMLButtonElement>("properties-pin"),
        propertiesTab: byId<HTMLButtonElement>("properties-tab"),
        newTab: byId<HTMLButtonElement>("btn-new"),
        open: byId<HTMLButtonElement>("btn-open"),
        save: byId<HTMLButtonElement>("btn-save"),
        saveAs: byId<HTMLButtonElement>("btn-save-as"),
        run: byId<HTMLButtonElement>("btn-run"),
        cancel: byId<HTMLButtonElement>("btn-cancel"),
        estimatedPlan: byId<HTMLButtonElement>("btn-estimated-plan"),
        actualPlan: byId<HTMLButtonElement>("btn-actual-plan"),
        editFetchXml: byId<HTMLButtonElement>("btn-edit-fetchxml"),
        saveMenu: byId<HTMLButtonElement>("btn-save-menu"),
        convert: byId<HTMLButtonElement>("btn-convert"),
        convertMenu: byId<HTMLButtonElement>("btn-convert-menu"),
        convertFetchXml: byId<HTMLButtonElement>("btn-convert-fetchxml"),
        convertM: byId<HTMLButtonElement>("btn-convert-m"),
        resultsText: byId<HTMLButtonElement>("btn-results-text"),
        resultsGrid: byId<HTMLButtonElement>("btn-results-grid"),
        format: byId<HTMLButtonElement>("btn-format"),
        formatMenu: byId<HTMLButtonElement>("btn-format-menu"),
        displayNames: byId<HTMLButtonElement>("btn-display-names"),
        settings: byId<HTMLButtonElement>("btn-settings"),
        connection: byId<HTMLSelectElement>("connection-select"),
        status: byId("status-text"),
        statusMessage: byId("status-message"),
        statusIcon: byId("status-icon"),
        progress: byId("status-progress"),
        server: byId("status-server"),
        lock: byId("status-lock"),
        domain: byId("status-domain"),
        user: byId("status-user"),
        database: byId("status-database"),
        elapsed: byId("status-elapsed"),
        rows: byId("status-rows"),
    };

    constructor() {
        this.client = new Sql4CdsClient({
            objectExplorerSessionCreated: (params) => this.objectExplorer.sessionCreated(params),
            objectExplorerExpandComplete: (params) => this.objectExplorer.expandComplete(params),
            objectExplorerSessionDisconnected: (params) => this.objectExplorer.sessionDisconnected(params),
            progress: (message) => {
                this.progressMessage = message;
                this.updateStatusBar();
            },
            log: (params) => {
                if (params.type === 1) console.error(params.message);
                else if (params.type === 2) console.warn(params.message);
                else console.log(params.message);
                if (params.type === 1 || params.type === 2)
                    void notify("SQL 4 CDS", params.message, params.type === 1 ? "error" : "warning");
            },
            failed: (message) => {
                this.ready = false;
                this.progressMessage = "";
                this.objectExplorer.engineStopped();
                for (const tab of this.tabs) tab.engineStopped(message);
                this.updateToolbar();
                this.updateStatusBar();
                this.setStatus(message, "error");
                void notify("SQL 4 CDS engine failed", message, "error");
            },
        });
        this.connections = new ConnectionRegistry(this.client);
        this.objectExplorer = new ObjectExplorer(this.client, this.el.explorerTree,
            (message) => { void notify("Object Explorer", message, "error"); },
            (text) => this.insertObjectName(text));
    }

    private async openQuery(): Promise<void> {
        try {
            const path = await window.toolboxAPI.fileSystem.selectPath({
                type: "file",
                title: "Open SQL query",
                buttonLabel: "Open",
                filters: [{ name: "SQL queries", extensions: ["sql"] }, { name: "All files", extensions: ["*"] }],
            });
            if (!path) return;

            const text = await window.toolboxAPI.fileSystem.readText(path);
            const tab = this.addTab(text);
            tab.setFile(path);
            tab.markSaved();
        } catch (err) {
            await notify("Unable to open query", errorMessage(err), "error");
        }
    }

    private async saveTab(tab: QueryTab, saveAs = false): Promise<boolean> {
        try {
            const content = tab.model.getValue();
            if (tab.filePath && !saveAs) {
                await window.toolboxAPI.fileSystem.writeText(tab.filePath, content);
            } else {
                const filename = tab.title.toLowerCase().endsWith(".sql") ? tab.title : `${tab.title}.sql`;
                const path = await window.toolboxAPI.fileSystem.saveFile(
                    filename,
                    content,
                    [{ name: "SQL queries", extensions: ["sql"] }],
                );
                if (!path) return false;
                tab.setFile(path);
            }

            tab.markSaved(content);
            return true;
        } catch (err) {
            await notify("Unable to save query", errorMessage(err), "error");
            return false;
        }
    }

    private syncClosePrevention(): void {
        this.closePreventionQueue = this.closePreventionQueue.then(async () => {
            const shouldPreventClose = this.tabs.some((tab) => tab.isDirty);
            if (shouldPreventClose === this.closePreventionApplied) return;

            try {
                if (shouldPreventClose)
                    await window.toolboxAPI.preventClose();
                else
                    await window.toolboxAPI.releasePreventClose();
                this.closePreventionApplied = shouldPreventClose;
            } catch (err) {
                await notify("Unable to update close protection", errorMessage(err), "error");
            }
        });
    }

    async start(): Promise<void> {
        this.settings = await loadSettings();
        await this.applyTheme();
        this.createEditor();
        this.bindToolbar();
        this.bindSplitter();
        this.bindObjectExplorer();
        this.bindProperties();

        configureMonaco(this.client, (uri) => this.tabs.some((t) => t.uri === uri && t.connected));

        window.toolboxAPI.events.on((_event, payload) => {
            if (payload.event.startsWith("connection:")) {
                this.connectionChangeQueue = this.connectionChangeQueue
                    .then(() => this.onConnectionsChanged())
                    .catch((error) => notify("Unable to update connections", errorMessage(error), "error"));
            }
            else if (payload.event === "settings:updated") void this.applyTheme();
        });

        this.setStatus("Starting SQL 4 CDS engine…");
        try {
            await this.client.start({ ...this.settings });
        } catch (err) {
            this.setStatus(`Unable to start the SQL 4 CDS engine: ${errorMessage(err)}`, "error");
            await notify("Unable to start SQL 4 CDS", errorMessage(err), "error");
            return;
        }

        this.ready = true;
        this.setStatus("Ready", "success");

        await this.connections.refresh();
        this.renderConnectionList();

        // Register every connection so they can all be used in cross-instance queries
        await Promise.all(
            this.connections.all.map((c) =>
                this.connections.register(c.slot).catch((err) => notify(`Unable to connect to ${c.options.connectionName}`, errorMessage(err), "error")),
            ),
        );
        this.renderConnectionList();
        this.addTab(DEFAULT_QUERY);
        await this.objectExplorer.setConnections(this.connections.all);
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
            folding: true,
            foldingStrategy: "auto",
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
            id: "sql4cds.estimatedPlan",
            label: "Display Estimated Execution Plan",
            keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyL],
            contextMenuGroupId: "navigation",
            contextMenuOrder: 1,
            run: () => this.runQuery(true),
        });
        this.editor.addAction({
            id: "sql4cds.actualPlan",
            label: "Include Actual Execution Plan",
            keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyM],
            run: () => this.toggleActualPlan(),
        });
        this.editor.addAction({
            id: "sql4cds.newTab",
            label: "New Query",
            keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyMod.Alt | monaco.KeyCode.KeyN],
            run: () => {
                this.addTab();
            },
        });
        this.editor.addAction({
            id: "sql4cds.resultsText",
            label: "Results to Text",
            keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyT],
            run: () => this.setResultsMode("text"),
        });
        this.editor.addAction({
            id: "sql4cds.resultsGrid",
            label: "Results to Grid",
            keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyD],
            run: () => this.setResultsMode("grid"),
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

    openFetchXml(fetchXml: string): void {
        this.openPreview(fetchXml, "xml", "FetchXML");
    }

    revealRange(tab: QueryTab, range: Range): void {
        this.activateTab(tab);
        const monacoRange = toMonacoRange(range);
        this.editor.setSelection(monacoRange);
        this.editor.revealRangeInCenterIfOutsideViewport(monacoRange);
        this.editor.focus();
    }

    private openPreview(text: string, language: string, title: string): void {
        const tab = new QueryTab(this, text, {
            language,
            readOnly: true,
            isFetchXmlPreview: language === "xml",
        });
        tab.title = title;
        this.tabs.push(tab);
        this.el.results.appendChild(tab.resultsElement);
        this.activateTab(tab);
    }

    private activateTab(tab: QueryTab): void {
        if (this.activeTab === tab) return;

        if (this.activeTab) this.activeTab.viewState = this.editor.saveViewState();

        this.activeTab = tab;
        this.editor.setModel(tab.model);
        this.editor.updateOptions({ readOnly: tab.readOnly });
        if (tab.viewState) this.editor.restoreViewState(tab.viewState);
        this.editor.focus();

        for (const t of this.tabs) t.resultsElement.hidden = t !== tab;
        this.updateResultsVisibility();
        this.renderTabs();
        this.renderConnectionList();
        this.updateToolbar();
        this.updateProperties();
    }

    private async closeTab(tab: QueryTab): Promise<void> {
        const index = this.tabs.indexOf(tab);
        if (index < 0) return;

        if (tab.isDirty) {
            const choice = await showUnsavedChangesDialog(tab.title);
            if (choice === "cancel") return;
            if (choice === "save" && (!(await this.saveTab(tab)) || tab.isDirty)) return;
        }

        this.tabs.splice(index, 1);
        this.syncClosePrevention();
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
            el.classList.toggle("modified", tab.isDirty);
            el.title = tab.connectionSlot !== null ? `${tab.title} - ${this.connections.get(tab.connectionSlot)?.options.connectionName ?? ""}` : tab.title;
            if (tab.isDirty) el.title += " (unsaved changes)";
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
            label.title = tab.isDirty ? "Unsaved changes" : tab.title;

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
            option.textContent = `${entry.options.connectionName}${entry.error ? " ⚠" : ""}`;
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
            if (!this.ready) return;
            tab.statusMessage = `Connection failed: ${errorMessage(err)}`;
            tab.statusKind = "error";
            this.onStateChanged(tab);
            await notify("Connection failed", errorMessage(err), "error");
        }
    }

    private async onConnectionsChanged(): Promise<void> {
        if (!this.ready) return;

        const changed = await this.connections.refresh();
        if (changed.length === 0) {
            await this.objectExplorer.setConnections(this.connections.all);
            return;
        }

        await Promise.all(changed.map((slot) => this.objectExplorer.removeConnection(slot)));
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
        await this.objectExplorer.setConnections(this.connections.all);
    }

    // Query execution

    private insertObjectName(text: string): void {
        const selection = this.editor.getSelection();
        if (!this.activeTab || !selection) return;
        this.editor.pushUndoStop();
        this.editor.executeEdits("object-explorer", [{ range: selection, text, forceMoveMarkers: true }]);
        this.editor.pushUndoStop();
        this.editor.focus();
    }

    private async runQuery(estimatedPlan = false): Promise<void> {
        const tab = this.activeTab;
        if (!tab || tab.readOnly || tab.running || !this.ready) return;

        try {
            this.progressMessage = "";
            await tab.execute(this.editor.getSelection(), estimatedPlan);
        } catch (err) {
            await notify(estimatedPlan ? "Unable to display estimated plan" : "Unable to run query", errorMessage(err), "warning");
        }
    }

    get includeActualPlan(): boolean {
        return this.settings.includeActualPlan;
    }

    get showMessageLineNumbers(): boolean {
        return this.settings.showMessageLineNumbers;
    }

    private async toggleActualPlan(): Promise<void> {
        const save = this.saveSetting("includeActualPlan", !this.settings.includeActualPlan);
        this.updateToolbar();
        await save;
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

    openRecord(reference: EntityReferenceValue): void {
        void this.connections.openRecord(reference).catch((err) =>
            notify("Unable to open record", errorMessage(err), "error"));
    }

    copyRecordUrl(reference: EntityReferenceValue): void {
        void (async () => {
            await window.toolboxAPI.utils.copyToClipboard(this.connections.getRecordUrl(reference));
        })().catch((err) => notify("Unable to copy record URL", errorMessage(err), "error"));
    }

    getRecordSelectStatement(reference: EntityReferenceValue, currentDataSource?: string): Promise<string> {
        return this.connections.getRecordSelectStatement(reference, currentDataSource);
    }

    onStateChanged(tab: QueryTab): void {
        this.renderTabs();
        this.syncClosePrevention();
        if (tab === this.activeTab) {
            this.updateResultsVisibility();
            this.renderConnectionList();
            this.updateToolbar();
        }
    }

    private updateResultsVisibility(): void {
        const visible = this.activeTab?.hasResults === true;
        this.el.splitter.hidden = !visible;
        this.el.results.hidden = !visible;
    }

    onStatusChanged(tab: QueryTab): void {
        if (tab === this.activeTab) this.updateStatusBar();
    }

    onSelectedPlanNodeChanged(tab: QueryTab): void {
        if (tab === this.activeTab) this.updateProperties();
    }

    private updateProperties(): void {
        const node = this.activeTab?.selectedPlanNode ?? null;
        this.propertiesPanel?.show(node ? node.name.split(/\r?\n/)[0] : null, node ? node.properties ?? [] : null);
        this.el.editFetchXml.disabled = this.fetchXmlForEditing === null;
    }

    private get fetchXmlForEditing(): string | null {
        const tab = this.activeTab;
        if (tab?.isFetchXmlPreview) return tab.model.getValue();
        const node = tab?.selectedPlanNode;
        return node ? getFetchXmlQuery(node) : null;
    }

    copyToClipboard(text: string): void {
        void window.toolboxAPI.utils.copyToClipboard(text);
    }

    // Toolbar & layout

    private bindToolbar(): void {
        this.bindToolbarMenu(this.el.saveMenu, byId("save-menu"));
        this.bindToolbarMenu(this.el.convertMenu, byId("convert-menu"));
        this.bindToolbarMenu(this.el.formatMenu, byId("format-menu"));
        this.el.newTab.addEventListener("click", () => this.addTab());
        this.el.open.addEventListener("click", () => void this.openQuery());
        this.el.save.addEventListener("click", () => { if (this.activeTab) void this.saveTab(this.activeTab); });
        this.el.saveAs.addEventListener("click", () => { if (this.activeTab) void this.saveTab(this.activeTab, true); });
        this.el.run.addEventListener("click", () => void this.runQuery());
        this.el.cancel.addEventListener("click", () => void this.activeTab?.cancel());
        this.el.estimatedPlan.addEventListener("click", () => void this.runQuery(true));
        this.el.actualPlan.addEventListener("click", () => void this.toggleActualPlan());
        this.el.editFetchXml.addEventListener("click", () => {
            const fetchXml = this.fetchXmlForEditing;
            if (fetchXml !== null) void this.launchFetchXmlStudio(fetchXml);
        });
        this.el.convert.addEventListener("click", () => void this.transformQuery("convert"));
        this.el.convertFetchXml.addEventListener("click", () => this.selectConversion("fetchxml"));
        this.el.convertM.addEventListener("click", () => this.selectConversion("m"));
        this.el.format.addEventListener("click", () => void this.transformQuery("format"));
        this.el.displayNames.addEventListener("click", () => void this.transformQuery("format", true));
        this.el.resultsText.addEventListener("click", () => this.setResultsMode("text"));
        this.el.resultsGrid.addEventListener("click", () => this.setResultsMode("grid"));
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
                return;
            }

            if (!(e.ctrlKey || e.metaKey) || e.altKey) return;
            const key = e.key.toLowerCase();
            if (key === "o" && !e.shiftKey) {
                e.preventDefault();
                void this.openQuery();
                return;
            }
            if (key === "s") {
                e.preventDefault();
                if (this.activeTab) void this.saveTab(this.activeTab, e.shiftKey);
                return;
            }
            if (e.shiftKey) return;
            if (key === "t" || key === "d") {
                if (e.target instanceof Node && this.el.editor.contains(e.target)) return;
                e.preventDefault();
                this.setResultsMode(key === "t" ? "text" : "grid");
                return;
            }

            // The editor handles these shortcuts itself when it has focus
            if (e.target instanceof Node && this.el.editor.contains(e.target)) return;
            if (key === "l") {
                e.preventDefault();
                void this.runQuery(true);
            } else if (key === "m") {
                e.preventDefault();
                void this.toggleActualPlan();
            }
        });
    }

    private bindToolbarMenu(toggle: HTMLButtonElement, menu: HTMLElement): void {
        const items = () => Array.from(menu.querySelectorAll<HTMLButtonElement>("button:not(:disabled)"));
        const close = (restoreFocus = false) => {
            menu.hidden = true;
            toggle.setAttribute("aria-expanded", "false");
            if (restoreFocus) toggle.focus();
        };
        const open = () => {
            if (toggle.disabled) return;
            menu.hidden = false;
            toggle.setAttribute("aria-expanded", "true");
            items()[0]?.focus();
        };
        toggle.addEventListener("click", () => menu.hidden ? open() : close());
        toggle.addEventListener("keydown", (event) => {
            if (event.key === "ArrowDown") {
                event.preventDefault();
                open();
            }
        });
        menu.addEventListener("click", (event) => {
            if (event.target instanceof Element && event.target.closest("button")) close(true);
        });
        menu.addEventListener("keydown", (event) => {
            if (event.key === "Escape") {
                event.preventDefault();
                close(true);
            } else if (event.key === "ArrowDown" || event.key === "ArrowUp" || event.key === "Home" || event.key === "End") {
                event.preventDefault();
                const buttons = items();
                const index = buttons.indexOf(document.activeElement as HTMLButtonElement);
                const next = event.key === "Home" ? 0 : event.key === "End" ? buttons.length - 1
                    : (index + (event.key === "ArrowDown" ? 1 : -1) + buttons.length) % buttons.length;
                buttons[next]?.focus();
            }
        });
        document.addEventListener("pointerdown", (event) => {
            if (event.target instanceof Node && !menu.parentElement?.contains(event.target)) close();
        });
        document.addEventListener("focusin", (event) => {
            if (event.target instanceof Node && !menu.parentElement?.contains(event.target)) close();
        });
    }

    private selectConversion(target: "fetchxml" | "m"): void {
        this.convertTarget = target;
        this.updateToolbar();
        void this.transformQuery("convert");
    }

    private setResultsMode(mode: "text" | "grid"): void {
        if (!this.activeTab || this.activeTab.readOnly) return;
        this.activeTab.resultsMode = mode;
        this.updateToolbar();
    }

    private async transformQuery(operation: "format" | "convert", addDisplayNames = false): Promise<void> {
        const tab = this.activeTab;
        if (!this.ready || !tab || tab.readOnly || tab.running || this.transforming) return;
        if ((operation === "convert" || addDisplayNames) && !tab.connected) return;
        const selection = this.editor.getSelection();
        const range = selection && !selection.isEmpty() ? selection : tab.model.getFullModelRange();
        const version = tab.model.getVersionId();
        const query = tab.model.getValueInRange(range);
        const target = this.convertTarget;
        this.transforming = true;
        this.updateToolbar();
        try {
            if (operation === "format") {
                const options = tab.model.getOptions();
                const edits = await this.client.formatDocument(tab.uri, {
                    tabSize: options.tabSize,
                    insertSpaces: options.insertSpaces,
                    addDisplayNames,
                });
                if (tab.model.isDisposed()) throw new Error("The query tab was closed while formatting");
                if (tab.model.getVersionId() !== version) throw new Error("The query changed while formatting. Please try again.");
                if (!edits?.length) return;
                tab.model.pushStackElement();
                tab.model.pushEditOperations([], edits.map((edit) => ({
                    range: new monaco.Range(edit.range.start.line + 1, edit.range.start.character + 1, edit.range.end.line + 1, edit.range.end.character + 1),
                    text: edit.newText,
                })), () => null);
                tab.model.pushStackElement();
            } else {
                const result = await this.client.transformQuery(tab.uri, query, target);
                for (const text of result)
                    this.openPreview(text, target === "fetchxml" ? "xml" : "powerquery", target === "fetchxml" ? "FetchXML" : "Power BI (M)");
            }
        } catch (err) {
            await notify(operation === "format" ? "Unable to format query" : "Unable to convert query", errorMessage(err), "error");
        } finally {
            this.transforming = false;
            this.updateToolbar();
        }
    }

    private updateToolbar(): void {
        const tab = this.activeTab;
        this.el.run.disabled = !this.ready || !tab?.connected || tab.readOnly || tab.running;
        this.el.estimatedPlan.disabled = this.el.run.disabled;
        this.el.cancel.disabled = !this.ready || !tab?.running;
        this.el.actualPlan.setAttribute("aria-pressed", String(this.settings.includeActualPlan));
        this.el.connection.disabled = !this.ready || !tab || tab.readOnly || tab.running || tab.connecting;
        this.el.editFetchXml.disabled = this.fetchXmlForEditing === null;
        this.el.save.disabled = !tab || tab.readOnly || (!tab.isDirty && !!tab.filePath);
        this.el.saveAs.disabled = !tab || tab.readOnly;
        this.el.saveMenu.disabled = this.el.saveAs.disabled;
        this.el.format.disabled = !this.ready || !tab || tab.readOnly || tab.running || this.transforming;
        this.el.formatMenu.disabled = this.el.format.disabled || !tab?.connected;
        this.el.displayNames.disabled = this.el.formatMenu.disabled;
        this.el.convert.disabled = this.el.format.disabled || !tab?.connected;
        this.el.convertMenu.disabled = this.el.convert.disabled;
        this.el.convertFetchXml.disabled = this.el.convert.disabled;
        this.el.convertM.disabled = this.el.convert.disabled;
        this.el.convert.title = `Convert to ${this.convertTarget === "fetchxml" ? "FetchXML" : "Power BI (M)"}`;
        this.el.convertFetchXml.setAttribute("aria-checked", String(this.convertTarget === "fetchxml"));
        this.el.convertM.setAttribute("aria-checked", String(this.convertTarget === "m"));
        this.el.resultsText.disabled = !tab || tab.readOnly;
        this.el.resultsGrid.disabled = this.el.resultsText.disabled;
        this.el.resultsText.setAttribute("aria-pressed", String(tab?.resultsMode === "text"));
        this.el.resultsGrid.setAttribute("aria-pressed", String(!!tab && tab.resultsMode !== "text"));
        for (const [toggle, menuId] of [[this.el.saveMenu, "save-menu"], [this.el.convertMenu, "convert-menu"], [this.el.formatMenu, "format-menu"]] as const) {
            if (toggle.disabled) {
                byId(menuId).hidden = true;
                toggle.setAttribute("aria-expanded", "false");
            }
        }

        this.updateStatusBar();
    }

    private async launchFetchXmlStudio(fetchXml: string): Promise<void> {
        try {
            await window.toolboxAPI.invocation.launchTool(
                "@mohsinonxrm/pptb-fetchxml-studio",
                { fetchXml },
                { noReturn: true },
            );
        } catch (err) {
            await notify("Unable to open FetchXML Studio", errorMessage(err), "error");
        }
    }

    private async saveSetting(setting: "objectExplorerPinned" | "propertiesPinned" | "includeActualPlan", value: boolean): Promise<void> {
        this.settings = { ...this.settings, [setting]: value };
        try {
            await saveSettings(this.settings);
        } catch (err) {
            await notify("Unable to save settings", errorMessage(err), "error");
        }
    }

    private bindObjectExplorer(): void {
        this.explorerPane = new PinnablePane({
            name: "Object Explorer",
            pane: this.el.explorer,
            pin: this.el.explorerPin,
            tab: this.el.explorerTab,
            splitter: this.el.explorerSplitter,
            content: this.el.explorerTree,
            side: "left",
            pinned: this.settings.objectExplorerPinned,
            savePinned: (pinned) => this.saveSetting("objectExplorerPinned", pinned),
            isPartOfPane: (target) => target.closest(".explorer-context-menu") !== null,
        });
        window.addEventListener("pagehide", () => { void this.objectExplorer.dispose(); });
    }

    private bindProperties(): void {
        this.propertiesPanel = new PropertiesPanel(this.el.propertiesGrid, this.el.propertiesTitle);
        this.propertiesPane = new PinnablePane({
            name: "Properties",
            pane: this.el.properties,
            pin: this.el.propertiesPin,
            tab: this.el.propertiesTab,
            splitter: this.el.propertiesSplitter,
            content: this.el.propertiesGrid,
            side: "right",
            pinned: this.settings.propertiesPinned,
            savePinned: (pinned) => this.saveSetting("propertiesPinned", pinned),
        });
        this.updateProperties();
    }

    private updateStatusBar(): void {
        const tab = this.activeTab;
        if (this.statusTimer !== null && (!this.ready || !tab?.running)) {
            clearInterval(this.statusTimer);
            this.statusTimer = null;
        } else if (this.ready && tab?.running && this.statusTimer === null) {
            this.statusTimer = setInterval(() => this.updateStatusBar(), 100);
        }

        if (this.ready) {
            this.setStatus(tab?.gridStatusIsError ? tab.gridStatus : tab?.statusMessage ?? "Ready",
                tab?.gridStatusIsError ? "error" : tab?.statusKind ?? "success");
        }
        this.el.progress.textContent = [tab?.gridStatusIsError ? "" : tab?.gridStatus, tab?.running ? this.progressMessage : ""]
            .filter(Boolean).join(" | ");

        const entry = tab?.connected && tab.connectionSlot !== null ? this.connections.get(tab.connectionSlot) : null;
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

        updated.objectExplorerPinned = this.explorerPane?.pinned ?? this.settings.objectExplorerPinned;
        updated.propertiesPinned = this.propertiesPane?.pinned ?? this.settings.propertiesPinned;
        this.settings = updated;
        try {
            await saveSettings(updated);
            if (this.ready) await this.client.changeConfiguration({ ...updated });
        } catch (err) {
            await notify("Unable to save settings", errorMessage(err), "error");
        }
    }

    private setStatus(text: string, kind: QueryTab["statusKind"] = "busy"): void {
        this.el.status.textContent = text;
        this.el.statusMessage.title = text;
        this.el.statusIcon.textContent = { success: "\u2713", error: "\u2757", warning: "\u26A0", busy: "\u25F7" }[kind];
        this.el.statusIcon.className = `status-icon ${kind}`;
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

function showUnsavedChangesDialog(title: string): Promise<"save" | "discard" | "cancel"> {
    return new Promise((resolve) => {
        const dialog = document.createElement("dialog");
        dialog.className = "unsaved-dialog";

        const form = document.createElement("form");
        form.method = "dialog";

        const heading = document.createElement("h2");
        heading.textContent = "Unsaved changes";

        const message = document.createElement("p");
        message.textContent = `Save changes to ${title} before closing?`;

        const buttons = document.createElement("div");
        buttons.className = "dialog-buttons";
        for (const [value, label, primary] of [
            ["save", "Save", true],
            ["discard", "Discard", false],
            ["cancel", "Cancel", false],
        ] as const) {
            const button = document.createElement("button");
            button.value = value;
            button.textContent = label;
            if (primary) button.className = "primary";
            buttons.appendChild(button);
        }

        form.append(heading, message, buttons);
        dialog.appendChild(form);
        document.body.appendChild(dialog);

        dialog.addEventListener("close", () => {
            dialog.remove();
            const value = dialog.returnValue;
            resolve(value === "save" || value === "discard" ? value : "cancel");
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
