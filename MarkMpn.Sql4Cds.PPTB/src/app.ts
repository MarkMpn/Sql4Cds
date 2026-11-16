import "./styles.css";
import { configureMonaco, monaco, toMonacoRange } from "./editor/monacoSetup";
import { ConnectionRegistry } from "./connections";
import { Sql4CdsClient } from "./lsp/client";
import { ConfirmationResult, type ConfirmationParams, type EntityReferenceValue, type Range } from "./lsp/protocol";
import { QueryTab, type QueryTabHost } from "./query/queryTab";
import type { ObjectExplorer } from "./objectExplorer";
import { getFetchXmlQuery } from "./executionPlan/executionPlanView";
import { defaultSettings, loadSettings, saveSettings, showSettingsDialog, type Sql4CdsSettings } from "./settings";
import { defaultResponseLimitBytes, invocationError, parseInvocation, responseSize } from "./mcpInvocation";
import { loadSession, saveSession, type SessionState, type SessionTab } from "./session";
import { showConfirmation, showUnsavedChangesDialog } from "./dialogs";
import { StatusBar, StatusBarState } from "./statusBar";
import { mountApp } from "./appView";
import { action, computed, makeObservable, observable, reaction, runInAction } from "mobx";

const DEFAULT_QUERY = "";

export class App implements QueryTabHost {
    readonly client: Sql4CdsClient;
    readonly connections: ConnectionRegistry;
    objectExplorer: ObjectExplorer | null = null;
    readonly tabs: QueryTab[] = [];
    editorReady = false;
    readonly statusState = new StatusBarState();
    get activeTab(): QueryTab | null { return this.statusState.activeTab; }
    set activeTab(tab: QueryTab | null) { runInAction(() => { this.statusState.activeTab = tab; }); }
    private editor!: monaco.editor.IStandaloneCodeEditor;
    settings: Sql4CdsSettings = { ...defaultSettings };
    settingsReady = false;
    private get ready(): boolean { return this.statusState.ready; }
    private set ready(ready: boolean) { runInAction(() => { this.statusState.ready = ready; }); }
    private confirmationQueue: Promise<void> = Promise.resolve();
    private get progressMessage(): string { return this.statusState.progressMessage; }
    private set progressMessage(message: string) { runInAction(() => { this.statusState.progressMessage = message; }); }
    private closePreventionApplied = false;
    private closePreventionQueue: Promise<void> = Promise.resolve();
    private connectionChangeQueue: Promise<void> = Promise.resolve();
    private sessionRestored = false;
    private sessionSaveTimer: ReturnType<typeof setTimeout> | null = null;
    convertTarget: "fetchxml" | "m" = "fetchxml";
    private transforming = false;
    private readonly disposables: Array<() => void> = [];

    private readonly el: ReturnType<typeof getElements>;

    readonly statusBar: StatusBar;

    get toolbarState() {
        const tab = this.activeTab;
        const resultsDisabled = !tab || tab.readOnly;
        const formatDisabled = !this.ready || resultsDisabled || !!tab?.running || this.transforming;
        return {
            runDisabled: !this.ready || !tab?.connected || tab.readOnly || tab.running,
            cancelDisabled: !this.ready || !tab?.running,
            connectionDisabled: !this.ready || resultsDisabled || !!tab?.running || !!tab?.connecting,
            saveDisabled: resultsDisabled || (!tab?.isDirty && !!tab?.filePath),
            saveAsDisabled: resultsDisabled,
            formatDisabled,
            convertDisabled: formatDisabled || !tab?.connected,
            resultsDisabled,
        };
    }

    constructor() {
        makeObservable<this, "transforming">(this, {
            tabs: observable.shallow,
            editorReady: observable,
            settings: observable.ref,
            settingsReady: observable,
            objectExplorer: observable.ref,
            setObjectExplorer: action,
            convertTarget: observable,
            transforming: observable,
            toolbarState: computed,
            fetchXmlForEditing: computed,
            addTab: action,
            openPreview: action,
            activateTab: action,
            selectConversion: action,
            setResultsMode: action,
        });
        this.client = new Sql4CdsClient({
            objectExplorerSessionCreated: (params) => this.objectExplorer?.sessionCreated(params),
            objectExplorerExpandComplete: (params) => this.objectExplorer?.expandComplete(params),
            objectExplorerSessionDisconnected: (params) => this.objectExplorer?.sessionDisconnected(params),
            progress: (message) => { this.progressMessage = message; },
            log: (params) => {
                if (params.type === 1) console.error(params.message);
                else if (params.type === 2) console.warn(params.message);
                else console.log(params.message);
                if (params.type === 1 || params.type === 2)
                    void notify("SQL 4 CDS", params.message, params.type === 1 ? "error" : "warning");
            },
            failed: action((message) => {
                this.ready = false;
                this.progressMessage = "";
                this.objectExplorer?.engineStopped();
                for (const tab of this.tabs) tab.engineStopped(message);
                this.setStatus(message, "error");
                void notify("SQL 4 CDS engine failed", message, "error");
            }),
        });
        this.connections = new ConnectionRegistry(this.client);
        this.statusBar = new StatusBar(this.statusState, this.connections);
        const unmount = mountApp(byId("app"), this);
        this.el = getElements();
        const stopPersistence = reaction(
            () => [this.settings, ...this.tabs.map(tab => [tab.title, tab.filePath, tab.isDirty, tab.content, tab.connectionSlot]), this.activeTab],
            () => { this.syncClosePrevention(); this.scheduleSessionSave(); },
        );
        window.addEventListener("pagehide", () => {
            stopPersistence();
            this.statusBar.dispose();
            for (const dispose of this.disposables) dispose();
            this.editor?.dispose();
            unmount();
        }, { once: true });
    }

    focusEditor(): void { this.editor.focus(); }

    setObjectExplorer(explorer: ObjectExplorer | null): void { this.objectExplorer = explorer; }

    reportError(title: string, error: unknown): Promise<void> {
        return notify(title, errorMessage(error), "error");
    }

    async openQuery(): Promise<void> {
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

    async saveTab(tab: QueryTab, saveAs = false): Promise<boolean> {
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
            // When the session is remembered any unsaved changes are restored on the next start,
            // so there is no need to prevent the tool from closing
            const shouldPreventClose = !this.settings.rememberSession && this.tabs.some((tab) => tab.isDirty);
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
        const settings = await loadSettings();
        runInAction(() => { this.settings = settings; this.settingsReady = true; });
        await this.applyTheme();
        this.createEditor();
        this.bindToolbar();
        this.bindSplitter();

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
        await this.connections.refresh();
        try {
            await this.client.start({ ...this.settings });
        } catch (err) {
            this.objectExplorer?.engineStopped();
            this.setStatus(`Unable to start the SQL 4 CDS engine: ${errorMessage(err)}`, "error");
            await notify("Unable to start SQL 4 CDS", errorMessage(err), "error");
            return;
        }

        this.ready = true;
        this.setStatus("Ready", "success");

        await this.connections.refresh();

        // Register every connection so they can all be used in cross-instance queries
        await Promise.all(
            this.connections.all.map((c) =>
                this.connections.register(c.slot).catch((err) => notify(`Unable to connect to ${c.options.connectionName}`, errorMessage(err), "error")),
            ),
        );

        const restored = await this.restoreSession();
        this.sessionRestored = true;
        const receivedQuery = await this.openLaunchQuery();
        if (!restored && !receivedQuery) this.addTab(DEFAULT_QUERY);
        window.addEventListener("pagehide", () => { void this.saveSessionNow(); });
    }

    private async openLaunchQuery(): Promise<boolean> {
        let expectsResponse = false;
        let responseLimitBytes = defaultResponseLimitBytes;
        try {
            const context = await window.toolboxAPI.invocation.getLaunchContext();
            const metadata = context?.__pptb;
            expectsResponse = typeof metadata === "object" && metadata !== null &&
                "expectsResponse" in metadata && metadata.expectsResponse === true;
            const input = parseInvocation(context);
            if (!input) {
                if (expectsResponse) throw new Error("Provide sql or fetchXml for this invocation.");
                return false;
            }
            responseLimitBytes = input.responseLimitBytes;
            const connection = this.connections.all[0];
            if (!connection) throw new Error("Select a connection to open the received query.");
            await this.connections.register(connection.slot);
            const queries = input.fetchXml
                ? await this.client.transformQuery(ConnectionRegistry.ownerUri(connection.slot), input.query, "sql")
                : [input.query];
            if (input.execute) {
                const tab = this.addTab(queries.join("\n\n"), connection.slot, false);
                await tab.connect(connection.slot);
                const approved = await this.confirmInvocation(tab);
                const result = approved
                    ? await tab.executeForInvocation(input.responseLimitBytes)
                    : { status: "cancelled", resultSets: [], messages: [], truncated: false, responseLimitBytes: input.responseLimitBytes };
                if (expectsResponse) {
                    await this.saveSessionNow();
                    await window.toolboxAPI.invocation.returnData({ ...result });
                }
            } else {
                for (const query of queries) this.addTab(query, connection.slot);
                if (expectsResponse) {
                    const result = { status: "success", sql: queries };
                    if (responseSize(result) > input.responseLimitBytes) throw new Error("Converted SQL exceeds maxResponseBytes.");
                    await this.saveSessionNow();
                    await window.toolboxAPI.invocation.returnData(result);
                }
            }
            return true;
        } catch (err) {
            await notify("Unable to process incoming query", errorMessage(err), "error");
            if (expectsResponse) {
                try {
                    await window.toolboxAPI.invocation.returnData(invocationError(errorMessage(err), responseLimitBytes));
                } catch (returnError) {
                    await notify("Unable to return invocation results", errorMessage(returnError), "error");
                }
            }
            return false;
        }
    }

    private async confirmInvocation(tab: QueryTab): Promise<boolean> {
        const message = `An external caller requested execution in ${tab.connectionSummary?.databaseName ?? "the selected environment"}. ` +
            "This query may modify data or execute Dataverse messages. Review the SQL below before proceeding. " +
            "SQL 4 CDS cannot undo data modifications. Data modification confirmations will also be shown during execution.\n\n" +
            tab.model.getValue();
        return await showConfirmation(message) !== ConfirmationResult.No;
    }

    // Session persistence

    /** Reopens the query tabs from the previous session. Returns false if there was no session to restore. */
    private async restoreSession(): Promise<boolean> {
        if (!this.settings.rememberSession) return false;

        const session = await loadSession();
        if (!session) return false;

        const restored: Array<QueryTab | null> = [];
        for (const saved of session.tabs) {
            try {
                restored.push(await this.restoreTab(saved));
            } catch (err) {
                restored.push(null);
                await notify("Unable to restore query", errorMessage(err), "warning");
            }
        }

        const tabs = restored.filter((tab): tab is QueryTab => tab !== null);
        if (tabs.length === 0) return false;

        this.activateTab(restored[session.activeTab] ?? tabs[tabs.length - 1]);
        return true;
    }

    private async restoreTab(saved: SessionTab): Promise<QueryTab> {
        let filename = saved.filename;
        let fileText: string | null = null;

        if (filename !== null) {
            try {
                fileText = await window.toolboxAPI.fileSystem.readText(filename);
            } catch (err) {
                // The file is no longer accessible - restore any unsaved content as a new unsaved tab
                if (saved.query === null) throw new Error(`Unable to open ${filename}: ${errorMessage(err)}`);
                filename = null;
            }
        }

        const slot = saved.connectionId !== null
            ? this.connections.all.find((entry) => entry.connection.id === saved.connectionId)?.slot
            : undefined;
        const tab = this.addTab(saved.query ?? fileText ?? "", slot);

        if (filename !== null && fileText !== null) {
            tab.setFile(filename);
            // The editor normalises line endings to LF, so match that when comparing against the file content
            tab.markSaved(saved.query === null ? undefined : fileText.split(/\r\n|\r|\n/).join("\n"));
        } else if (saved.dirty) {
            tab.markSaved("");
        }

        return tab;
    }

    private captureSession(): SessionState {
        const tabs = this.tabs.filter((tab) => !tab.readOnly);
        return {
            tabs: tabs.map((tab) => ({
                filename: tab.filePath,
                // Only persist the query text where it isn't already saved in the file
                query: tab.isDirty || !tab.filePath ? tab.model.getValue() : null,
                dirty: tab.isDirty,
                connectionId: this.settings.rememberSessionConnections && tab.connectionSlot !== null
                    ? this.connections.get(tab.connectionSlot)?.connection.id ?? null
                    : null,
            })),
            activeTab: this.activeTab ? tabs.indexOf(this.activeTab) : -1,
        };
    }

    private scheduleSessionSave(): void {
        if (!this.sessionRestored || !this.settings.rememberSession) return;
        if (this.sessionSaveTimer !== null) clearTimeout(this.sessionSaveTimer);
        this.sessionSaveTimer = setTimeout(() => void this.saveSessionNow(), 500);
    }

    private async saveSessionNow(): Promise<void> {
        if (this.sessionSaveTimer !== null) {
            clearTimeout(this.sessionSaveTimer);
            this.sessionSaveTimer = null;
        }
        if (!this.sessionRestored || !this.settings.rememberSession) return;

        try {
            await saveSession(this.captureSession());
        } catch {
            // Session persistence is best-effort
        }
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
            keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyN],
            run: () => {
                this.addTab();
            },
        });
        this.editor.addAction({
            id: "sql4cds.closeTab",
            label: "Close Query",
            keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyW],
            run: () => {
                if (this.activeTab) this.closeTab(this.activeTab);
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
        runInAction(() => { this.editorReady = true; });
    }

    addTab(text = "", connectionSlot?: number, connect = true): QueryTab {
        // Connect new tabs to the same connection as the current tab, or the first available connection
        const slot = connectionSlot ?? this.activeTab?.connectionSlot ?? this.connections.all[0]?.slot;

        const tab = new QueryTab(this, text);
        this.tabs.push(tab);
        this.activateTab(tab);

        if (connect && slot !== undefined && slot !== null) void this.connectTab(tab, slot);

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

    openPreview(text: string, language: string, title: string): void {
        const tab = new QueryTab(this, text, {
            language,
            readOnly: true,
            isFetchXmlPreview: language === "xml",
        });
        tab.title = title;
        this.tabs.push(tab);
        this.activateTab(tab);
    }

    activateTab(tab: QueryTab): void {
        if (this.activeTab === tab) return;

        if (this.activeTab) this.activeTab.viewState = this.editor.saveViewState();

        this.activeTab = tab;
        this.editor.setModel(tab.model);
        this.editor.updateOptions({ readOnly: tab.readOnly });
        if (tab.viewState) this.editor.restoreViewState(tab.viewState);
        this.editor.focus();

    }

    async closeTab(tab: QueryTab): Promise<void> {
        const index = this.tabs.indexOf(tab);
        if (index < 0) return;

        if (tab.isDirty) {
            const choice = await showUnsavedChangesDialog(tab.title);
            if (choice === "cancel") return;
            if (choice === "save" && (!(await this.saveTab(tab)) || tab.isDirty)) return;
        }

        const currentIndex = this.tabs.indexOf(tab);
        if (currentIndex < 0) return;
        runInAction(() => { this.tabs.splice(currentIndex, 1); });
        if (this.activeTab === tab) {
            this.activeTab = null;
            if (this.tabs.length === 0) {
                this.addTab();
            } else {
                this.activateTab(this.tabs[Math.min(currentIndex, this.tabs.length - 1)]);
            }
        }

        await tab.dispose();
    }

    // Connections

    async connectTab(tab: QueryTab, slot: number): Promise<void> {
        try {
            await this.connections.register(slot);
            await tab.connect(slot);
        } catch (err) {
            if (!this.ready) return;
            runInAction(() => {
                tab.statusMessage = `Connection failed: ${errorMessage(err)}`;
                tab.statusKind = "error";
            });
            await notify("Connection failed", errorMessage(err), "error");
        }
    }

    private async onConnectionsChanged(): Promise<void> {
        if (!this.ready) return;

        const changed = await this.connections.refresh();
        if (changed.length === 0) {
            return;
        }

        for (const slot of changed) {
            const tabs = this.tabs.filter((t) => t.connectionSlot === slot);
            await Promise.all(tabs.map((t) => t.disconnect()));

            if (this.connections.get(slot)) {
                await this.connections.register(slot).catch((err) => notify("Connection failed", errorMessage(err), "error"));
                await Promise.all(tabs.map((t) => this.connectTab(t, slot)));
            } else {
                runInAction(() => { for (const t of tabs) t.connectionSlot = null; });
            }
        }
    }

    // Query execution

    insertObjectName(text: string): void {
        const selection = this.editor.getSelection();
        if (!this.activeTab || !selection) return;
        this.editor.pushUndoStop();
        this.editor.executeEdits("object-explorer", [{ range: selection, text, forceMoveMarkers: true }]);
        this.editor.pushUndoStop();
        this.editor.focus();
    }

    async runQuery(estimatedPlan = false): Promise<void> {
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

    get resultsToText(): boolean {
        return this.settings.resultsToText;
    }

    async toggleActualPlan(): Promise<void> {
        const save = this.saveSetting("includeActualPlan", !this.settings.includeActualPlan);
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

    getRecordSelectStatement(ownerUri: string, reference: EntityReferenceValue): Promise<string> {
        return this.connections.getRecordSelectStatement(ownerUri, reference);
    }

    get fetchXmlForEditing(): string | null {
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
        const keydown = (e: KeyboardEvent) => {
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
        };
        window.addEventListener("keydown", keydown);
        this.disposables.push(() => window.removeEventListener("keydown", keydown));
    }

    selectConversion(target: "fetchxml" | "m"): void {
        this.convertTarget = target;
        void this.transformQuery("convert");
    }

    setResultsMode(mode: "text" | "grid"): void {
        if (!this.activeTab || this.activeTab.readOnly) return;
        this.activeTab.resultsMode = mode;
        // Remember the most recent choice for new tabs and future sessions
        if (this.settings.resultsToText !== (mode === "text")) void this.saveSetting("resultsToText", mode === "text");
    }

    async transformQuery(operation: "format" | "convert", addDisplayNames = false): Promise<void> {
        const tab = this.activeTab;
        if (!this.ready || !tab || tab.readOnly || tab.running || this.transforming) return;
        if ((operation === "convert" || addDisplayNames) && !tab.connected) return;
        const selection = this.editor.getSelection();
        const range = selection && !selection.isEmpty() ? selection : tab.model.getFullModelRange();
        const version = tab.model.getVersionId();
        const query = tab.model.getValueInRange(range);
        const target = this.convertTarget;
        runInAction(() => { this.transforming = true; });
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
            runInAction(() => { this.transforming = false; });
        }
    }

    async launchFetchXmlStudio(fetchXml: string): Promise<void> {
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

    async saveSetting(setting: "objectExplorerPinned" | "propertiesPinned" | "includeActualPlan" | "resultsToText", value: boolean): Promise<void> {
        runInAction(() => { this.settings = { ...this.settings, [setting]: value }; });
        try {
            await saveSettings(this.settings);
        } catch (err) {
            await notify("Unable to save settings", errorMessage(err), "error");
        }
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
        this.disposables.push(up);

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

    async editSettings(): Promise<void> {
        const updated = await showSettingsDialog(this.settings);
        if (!updated) return;

        updated.objectExplorerPinned = this.settings.objectExplorerPinned;
        updated.propertiesPinned = this.settings.propertiesPinned;
        runInAction(() => { this.settings = updated; });
        try {
            await saveSettings(updated);
            if (this.ready) await this.client.changeConfiguration({ ...updated });
        } catch (err) {
            await notify("Unable to save settings", errorMessage(err), "error");
        }

        if (updated.rememberSession) {
            await this.saveSessionNow();
        } else {
            // Discard any previously persisted session, matching the XrmToolBox behaviour
            await saveSession(null).catch(() => {});
        }
    }

    private setStatus(text: string, kind: QueryTab["statusKind"] = "busy"): void {
        this.statusState.setStatus(text, kind);
    }
}

function getElements() {
    return {
        editor: byId("editor"),
        results: byId("results-host"),
        splitter: byId("splitter"),
        workspace: byId("workspace"),
    };
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
    document.getElementById("app")!.textContent = "SQL 4 CDS must be run inside Power Platform ToolBox";
}
