import type { ConnectionRegistry, ConnectionSlot } from "./connections";
import type { Sql4CdsClient } from "./lsp/client";
import type { ObjectExplorerExpandComplete, ObjectExplorerNode, ObjectExplorerSessionCreated, ObjectExplorerSessionDisconnected } from "./lsp/protocol";
import { action, computed, makeObservable, observable, reaction, runInAction, type IReactionDisposer } from "mobx";
import { mountObjectExplorer } from "./objectExplorerView";

interface ExplorerConnection {
    connection: ConnectionSlot;
    root: TreeNode;
    sessionId?: string;
    creating?: Promise<void>;
    ready: boolean;
}

export class TreeNode {
    children: TreeNode[] = [];
    expanded = false;
    state: "unloaded" | "loading" | "loaded" | "error" = "unloaded";
    error: string | undefined = undefined;

    constructor(public info: ObjectExplorerNode, readonly connection: ConnectionSlot, readonly parent?: TreeNode) {
        makeObservable(this, {
            info: observable.ref,
            children: observable.shallow,
            expanded: observable,
            state: observable,
            error: observable,
            loaded: computed,
            loading: computed,
        });
    }

    get loaded(): boolean { return this.state === "loaded"; }
    get loading(): boolean { return this.state === "loading"; }
}

type ScriptAction = "SELECT" | "SELECT_TOP_1000" | "INSERT" | "UPDATE" | "DELETE" | "EXECUTE";
type ScriptDestination = "editor" | "file" | "clipboard";

export class ObjectExplorer {
    private readonly connections = new Map<number, ExplorerConnection>();
    // A session-created notification can arrive before the createsession response.
    private readonly earlySessions = new Map<string, ObjectExplorerSessionCreated>();
    private creatingSessions = 0;
    private readonly items = new Map<TreeNode, HTMLElement>();
    private selection: TreeNode | null = null;
    available = true;
    focusRequest = 0;
    private readonly unmount: () => void;
    private readonly stopConnections: IReactionDisposer;
    private disposed = false;
    private closeContextMenu: (() => void) | null = null;

    constructor(
        private readonly client: Sql4CdsClient,
        private readonly tree: HTMLElement,
        private readonly reportError: (message: string) => void,
        private readonly insertName: (text: string) => void,
        private readonly newQuery: (connection: ConnectionSlot, query: string) => void,
        registry: ConnectionRegistry,
        mount = true,
    ) {
        makeObservable<this, "connections" | "selection" | "onKeyDown">(this, {
            connections: observable.shallow,
            selection: observable.ref,
            available: observable,
            focusRequest: observable,
            selected: computed,
            roots: computed,
            setConnections: action,
            removeConnection: action,
            engineStopped: action,
            sessionCreated: action,
            expandComplete: action,
            sessionDisconnected: action,
            refresh: action,
            select: action,
            toggle: action,
            onKeyDown: action,
        });
        tree.setAttribute("role", "tree");
        tree.setAttribute("aria-label", "Object Explorer");
        tree.addEventListener("keydown", this.handleKeyDown);
        this.unmount = mount ? mountObjectExplorer(tree, this) : () => {};
        this.stopConnections = reaction(
            () => registry.all,
            (connections) => { void this.setConnections(connections).catch((error) => this.reportError(errorMessage(error))); },
            { fireImmediately: true },
        );
    }

    get roots(): TreeNode[] { return Array.from(this.connections.values(), (entry) => entry.root); }

    get selected(): TreeNode | null {
        const nodes = this.visibleNodes();
        return this.selection && nodes.includes(this.selection) ? this.selection : nodes[0] ?? null;
    }

    private readonly handleKeyDown = (event: KeyboardEvent) => this.onKeyDown(event);

    async setConnections(connections: ConnectionSlot[]): Promise<void> {
        if (this.disposed || !this.available) return;
        const incoming = new Map(connections.map((connection) => [connection.slot, connection]));
        const removals: Promise<void>[] = [];
        for (const [slot, entry] of this.connections) {
            if (incoming.get(slot)?.options !== entry.connection.options) removals.push(this.removeConnection(slot));
        }
        for (const connection of connections) {
            const existing = this.connections.get(connection.slot);
            if (existing) {
                existing.connection = connection;
                existing.root.info = { ...existing.root.info, label: connection.options.connectionName };
                continue;
            }
            const entry = observable.object<ExplorerConnection>({
                connection,
                ready: false,
                sessionId: undefined,
                creating: undefined,
                root: new TreeNode({
                    nodePath: `connection:${connection.slot}`, nodeType: "Server",
                    label: connection.options.connectionName, isLeaf: false,
                }, connection),
            }, { connection: observable.ref, root: observable.ref, creating: false }, { deep: false });
            this.connections.set(connection.slot, entry);
        }
        await Promise.all(removals);
    }

    async removeConnection(slot: number): Promise<void> {
        const entry = this.connections.get(slot);
        if (!entry) return;
        if (this.selected?.connection.slot === slot && this.tree.contains(document.activeElement)) this.focusRequest++;
        this.connections.delete(slot);
        if (this.selection?.connection.slot === slot) this.selection = null;
        this.closeContextMenu?.();
        await entry.creating;
        if (!entry.sessionId || !this.client.isRunning) return;
        await this.closeSession(entry.sessionId);
    }

    private async closeSession(sessionId: string): Promise<void> {
        try {
            const response = await this.client.closeObjectExplorerSession(sessionId);
            if (!response.success) throw new Error(response.errorMessage || "The Object Explorer session could not be closed");
        } catch (error) {
            this.reportError(errorMessage(error));
        }
    }

    async dispose(): Promise<void> {
        if (this.disposed) return;
        this.disposed = true;
        this.stopConnections();
        this.tree.removeEventListener("keydown", this.handleKeyDown);
        this.unmount();
        this.closeContextMenu?.();
        await Promise.all(Array.from(this.connections.keys(), (slot) => this.removeConnection(slot)));
        this.earlySessions.clear();
    }

    engineStopped(): void {
        this.available = false;
        this.connections.clear();
        this.earlySessions.clear();
        this.selection = null;
        this.closeContextMenu?.();
    }

    sessionCreated(params: ObjectExplorerSessionCreated): void {
        const entry = this.findSession(params.sessionId);
        if (!entry) {
            if (this.creatingSessions > 0) {
                this.earlySessions.set(params.sessionId, params);
            }
            return;
        }
        if (!params.success || !params.rootNode) {
            entry.ready = false;
            entry.root.state = "error";
            entry.root.error = params.errorMessage || "Unable to create the Object Explorer session";
            this.reportError(entry.root.error);
            return;
        }
        entry.ready = true;
        entry.root.info = { ...params.rootNode, label: entry.connection.options.connectionName };
        entry.root.state = "unloaded";
        if (entry.root.expanded) void this.load(entry.root, false);
    }

    expandComplete(params: ObjectExplorerExpandComplete): void {
        const entry = this.findSession(params.sessionId);
        if (!entry) return;
        const node = this.allNodes(entry.root).find((candidate) => candidate.info.nodePath === params.nodePath);
        if (!node?.loading) return;
        node.state = params.errorMessage ? "error" : "loaded";
        node.error = params.errorMessage || undefined;
        if (node.error) {
            this.reportError(node.error);
        } else {
            if (this.selected && this.selected !== node && this.allNodes(node).includes(this.selected)) {
                this.selection = node;
                if (this.tree.contains(document.activeElement)) this.focusRequest++;
            }
            node.children = (params.nodes ?? []).map((info) => new TreeNode(info, entry.connection, node));
        }
        this.closeContextMenu?.();
    }

    sessionDisconnected(params: ObjectExplorerSessionDisconnected): void {
        const entry = this.findSession(params.sessionId);
        if (!entry) return;
        entry.sessionId = undefined;
        entry.ready = false;
        entry.root.state = "error";
        entry.root.children = [];
        entry.root.error = params.errorMessage || "Object Explorer disconnected. Refresh to reconnect.";
        this.selection = entry.root;
        this.reportError(entry.root.error);
        this.closeContextMenu?.();
    }

    refresh(): void {
        const node = this.selected ?? this.connections.values().next().value?.root;
        if (!node || !this.available || node.connection.status === "connecting") return;
        const target = node.info.isLeaf ? node.parent ?? node : node;
        target.expanded = true;
        void this.load(target, true);
    }

    private findSession(sessionId: string): ExplorerConnection | undefined {
        return Array.from(this.connections.values()).find((entry) => entry.sessionId === sessionId);
    }

    private isCurrent(entry: ExplorerConnection): boolean {
        return !this.disposed && this.available && this.connections.get(entry.connection.slot) === entry;
    }

    private async createSession(entry: ExplorerConnection): Promise<void> {
        this.creatingSessions++;
        try {
            const response = await this.client.createObjectExplorerSession(entry.connection.options);
            runInAction(() => {
                entry.sessionId = response.sessionId;
                const early = this.earlySessions.get(response.sessionId);
                this.earlySessions.delete(response.sessionId);
                if (early && this.isCurrent(entry)) this.sessionCreated(early);
            });
        } catch (error) {
            if (!this.isCurrent(entry)) return;
            runInAction(() => {
                entry.root.state = "error";
                entry.root.error = errorMessage(error);
            });
            this.reportError(errorMessage(error));
        } finally {
            entry.creating = undefined;
            if (--this.creatingSessions === 0) this.earlySessions.clear();
        }
    }

    private async load(node: TreeNode, refresh: boolean): Promise<void> {
        const entry = this.connections.get(node.connection.slot);
        if (node.info.isLeaf || node.loading || !entry || !this.isCurrent(entry) || node.connection.status === "connecting") return;
        const retrySession = !entry.ready && !!entry.root.error;
        runInAction(() => {
            node.state = "loading";
            node.error = undefined;
        });
        if (retrySession && entry.sessionId) {
            const sessionId = entry.sessionId;
            runInAction(() => { entry.sessionId = undefined; });
            await this.closeSession(sessionId);
            if (!this.isCurrent(entry)) return;
        }
        if (!entry.sessionId) {
            entry.creating = this.createSession(entry);
            await entry.creating;
            return;
        }
        if (!entry.ready) return;
        try {
            const accepted = refresh
                ? await this.client.refreshObjectExplorerNode(entry.sessionId, node.info.nodePath)
                : await this.client.expandObjectExplorerNode(entry.sessionId, node.info.nodePath);
            if (!accepted) {
                runInAction(() => {
                    entry.sessionId = undefined;
                    entry.ready = false;
                });
                throw new Error("The Object Explorer request was rejected. Refresh the connection to retry.");
            }
        } catch (error) {
            if (!this.isCurrent(entry)) return;
            runInAction(() => {
                node.state = "error";
                node.error = errorMessage(error);
            });
            this.reportError(errorMessage(error));
        }
    }

    toggle(node: TreeNode): void {
        if (node.info.isLeaf || node.connection.status === "connecting") return;
        node.expanded = !node.expanded;
        if (node.expanded && (!node.loaded || node.error)) void this.load(node, false);
        this.closeContextMenu?.();
    }

    private allNodes(root: TreeNode): TreeNode[] {
        return [root, ...root.children.flatMap((child) => this.allNodes(child))];
    }

    private visibleNodes(): TreeNode[] {
        const collect = (node: TreeNode): TreeNode[] => [node, ...(node.expanded ? node.children.flatMap(collect) : [])];
        return Array.from(this.connections.values()).flatMap((entry) => collect(entry.root));
    }

    private addScriptDestinations(parent: HTMLElement, node: TreeNode, action: ScriptAction, disabled = false): void {
        const submenu = createSubmenu(parent, `${action} To`, disabled);
        if (disabled) return;

        for (const [destination, label] of [
            ["editor", "New Query Editor Window"],
            ["file", "File..."],
            ["clipboard", "Clipboard"],
        ] as const) {
            const option = document.createElement("button");
            option.setAttribute("role", "menuitem");
            option.textContent = label;
            option.addEventListener("click", () => {
                this.closeContextMenu?.();
                this.select(node);
                void this.outputScript(node, action, destination);
            });
            submenu.appendChild(option);
        }
    }

    private async outputScript(node: TreeNode, action: ScriptAction, destination: ScriptDestination): Promise<void> {
        try {
            const sessionId = this.connections.get(node.connection.slot)?.sessionId;
            if (!sessionId) throw new Error("The Object Explorer session is no longer available.");
            const query = await this.client.generateObjectExplorerScript(sessionId, node.info.nodePath, action);
            if (destination === "editor") {
                this.newQuery(node.connection, query);
            } else if (destination === "file") {
                const fileName = `${(node.info.metadata?.name ?? node.info.label).replace(/[<>:"/\\|?*]/g, "_")}.sql`;
                await window.toolboxAPI.fileSystem.saveFile(fileName, query, [{ name: "SQL queries", extensions: ["sql"] }]);
            } else {
                await window.toolboxAPI.utils.copyToClipboard(query);
            }
        } catch (error) {
            this.reportError(`Unable to script ${objectTypeLabel(node.info.nodeType)}: ${errorMessage(error)}`);
        }
    }

    select(node: TreeNode, focus = true): void {
        this.selection = node;
        if (focus) {
            const item = this.items.get(node);
            if (item) item.focus();
            else this.focusRequest++;
        }
    }

    showContextMenu(node: TreeNode, x: number, y: number): void {
        this.closeContextMenu?.();
        if (!this.available || node.connection.status === "connecting") return;
        this.select(node);
        const menu = document.createElement("div");
        menu.className = "explorer-context-menu";
        menu.setAttribute("role", "menu");
        menu.setAttribute("aria-label", node.info.label);
        const dismiss = () => this.closeContextMenu?.();

        if (node.info.nodeType === "Server") {
            // Only show a New Query item for connection nodes
            const newQuery = document.createElement("button");
            newQuery.setAttribute("role", "menuitem");
            newQuery.textContent = "New Query";
            newQuery.title = "New Query (Ctrl+N)";
            menu.appendChild(newQuery);
            newQuery.addEventListener("click", () => {
                dismiss();
                this.select(node);
                this.newQuery(node.connection, "");
            });

            menu.appendChild(document.createElement("hr"));
        }

        if (node.info.nodeType === "Table") {
            const selectTop = document.createElement("button");
            selectTop.setAttribute("role", "menuitem");
            selectTop.textContent = "Select Top 1000 Rows";
            selectTop.addEventListener("click", () => {
                dismiss();
                void this.outputScript(node, "SELECT_TOP_1000", "editor");
            });
            menu.appendChild(selectTop);
        }

        const type = node.info.nodeType;
        if (type === "Table" || type === "TableValuedFunction" || type === "StoredProcedure") {
            const script = createSubmenu(menu, `Script ${objectTypeLabel(type)} as`);
            if (type === "StoredProcedure") {
                createSubmenu(script, "SELECT To", true);
            } else {
                this.addScriptDestinations(script, node, "SELECT");
            }
            this.addScriptDestinations(script, node, "INSERT", type !== "Table");
            this.addScriptDestinations(script, node, "UPDATE", type !== "Table");
            this.addScriptDestinations(script, node, "DELETE", type !== "Table");
            this.addScriptDestinations(script, node, "EXECUTE", type !== "StoredProcedure");
        }

        const refresh = document.createElement("button");
        refresh.setAttribute("role", "menuitem");
        refresh.textContent = "Refresh";
        refresh.title = "Refresh (F5)";
        refresh.addEventListener("click", () => {
            dismiss();
            this.select(node);
            this.refresh();
        });
        menu.appendChild(refresh);

        document.body.appendChild(menu);
        const bounds = menu.getBoundingClientRect();
        menu.style.left = `${Math.max(0, Math.min(x, window.innerWidth - bounds.width))}px`;
        menu.style.top = `${Math.max(0, Math.min(y, window.innerHeight - bounds.height))}px`;
        const outside = (event: Event) => {
            if (event.target instanceof Node && !menu.contains(event.target)) dismiss();
        };
        const stopWatchingConnection = reaction(
            () => node.connection.status,
            () => dismiss(),
        );
        this.closeContextMenu = () => {
            this.closeContextMenu = null;
            stopWatchingConnection();
            document.removeEventListener("pointerdown", outside, true);
            document.removeEventListener("focusin", outside);
            window.removeEventListener("resize", dismiss);
            window.removeEventListener("blur", dismiss);
            this.tree.removeEventListener("scroll", dismiss);
            menu.remove();
        };
        document.addEventListener("pointerdown", outside, true);
        document.addEventListener("focusin", outside);
        window.addEventListener("resize", dismiss);
        window.addEventListener("blur", dismiss);
        this.tree.addEventListener("scroll", dismiss);
        menu.addEventListener("keydown", (event) => {
            if (event.key === "Escape" || event.key === "Tab") {
                dismiss();
                this.select(node);
                if (event.key === "Escape") event.preventDefault();
            } else if (event.key === "ArrowRight") {
                const active = document.activeElement as HTMLElement;
                const submenu = active.parentElement?.querySelector<HTMLElement>(":scope > .explorer-context-submenu");
                if (submenu && !active.hasAttribute("disabled")) {
                    submenu.hidden = false;
                    active.setAttribute("aria-expanded", "true");
                    submenu.querySelector<HTMLButtonElement>("button:enabled")?.focus();
                    event.preventDefault();
                }
            } else if (event.key === "ArrowLeft") {
                const active = document.activeElement as HTMLElement;
                const wrapper = active.closest<HTMLElement>(".explorer-context-submenu");
                const trigger = wrapper?.parentElement?.querySelector<HTMLButtonElement>(":scope > button");
                if (trigger) {
                    wrapper!.hidden = true;
                    trigger.setAttribute("aria-expanded", "false");
                    trigger.focus();
                    event.preventDefault();
                }
            } else if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
                event.preventDefault();
                const buttons = Array.from(menu.querySelectorAll<HTMLButtonElement>("button:enabled"))
                    .filter((button) => !button.closest<HTMLElement>(".explorer-context-submenu[hidden]"));
                let index = buttons.indexOf(document.activeElement as HTMLButtonElement);
                if (index < 0) index = 0;
                if (event.key === "ArrowDown") index = (index + 1) % buttons.length;
                else if (event.key === "ArrowUp") index = (index - 1 + buttons.length) % buttons.length;
                else if (event.key === "Home") index = 0;
                else if (event.key === "End") index = buttons.length - 1;
                buttons[index].focus();
            }
            event.stopPropagation();
        });
        // Focus the first available button
        const firstBtn = menu.querySelector<HTMLButtonElement>("button:enabled");
        if (firstBtn) firstBtn.focus();
    }

    private onKeyDown(event: KeyboardEvent): void {
        const nodes = this.visibleNodes();
        const node = this.selected ?? nodes[0];
        if (!node) return;
        if (event.key === "ContextMenu" || (event.shiftKey && event.key === "F10")) {
            event.preventDefault();
            event.stopPropagation();
            const bounds = this.items.get(node)!.getBoundingClientRect();
            this.showContextMenu(node, bounds.left, bounds.top + 24);
            return;
        }
        let next: TreeNode | undefined;
        switch (event.key) {
            case "ArrowDown": next = nodes[Math.min(nodes.length - 1, nodes.indexOf(node) + 1)]; break;
            case "ArrowUp": next = nodes[Math.max(0, nodes.indexOf(node) - 1)]; break;
            case "Home": next = nodes[0]; break;
            case "End": next = nodes[nodes.length - 1]; break;
            case "ArrowRight":
                if (!node.info.isLeaf && !node.expanded) this.toggle(node);
                else next = node.children[0];
                break;
            case "ArrowLeft":
                if (node.expanded) this.toggle(node);
                else next = node.parent;
                break;
            case "Enter":
            case " ": this.toggle(node); break;
            case "F5": this.refresh(); break;
            default: return;
        }
        event.preventDefault();
        event.stopPropagation();
        this.select(next ?? node);
    }

    bindItem(node: TreeNode, item: HTMLElement | null): void {
        if (item) this.items.set(node, item);
        else this.items.delete(node);
    }

    activate(node: TreeNode): void {
        this.select(node);
        if (node.connection.status === "connecting") return;
        if (["Table", "Column", "TableValuedFunction", "StoredProcedure"].includes(node.info.nodeType)) {
            const name = node.info.nodeType === "Column" ? node.info.label : node.info.metadata?.name ?? node.info.label;
            const schema = node.info.nodeType === "Column" ? undefined : node.info.metadata?.schema;
            this.insertName(schema ? `${schema}.${name}` : name);
        } else {
            this.toggle(node);
        }
    }

    selectTop(node: TreeNode): void {
        this.select(node);
        void this.outputScript(node, "SELECT_TOP_1000", "editor");
    }
}

function errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}

function objectTypeLabel(type: string): string {
    switch (type) {
        case "Table": return "Table";
        case "TableValuedFunction": return "Table-Valued Function";
        case "StoredProcedure": return "Stored Procedure";
        default: return type;
    }
}

function createSubmenu(parent: HTMLElement, label: string, disabled = false): HTMLElement {
    const wrapper = document.createElement("div");
    wrapper.className = "explorer-context-menu-item";
    const trigger = document.createElement("button");
    trigger.setAttribute("role", "menuitem");
    trigger.setAttribute("aria-haspopup", "menu");
    trigger.setAttribute("aria-expanded", "false");
    trigger.textContent = label;
    trigger.disabled = disabled;
    const submenu = document.createElement("div");
    submenu.className = "explorer-context-submenu";
    submenu.setAttribute("role", "menu");
    submenu.setAttribute("aria-label", label);
    submenu.hidden = true;
    const open = () => {
        if (disabled) return;
        for (const sibling of Array.from(parent.children)) {
            if (sibling === wrapper) continue;
            const siblingMenu = sibling.querySelector<HTMLElement>(":scope > .explorer-context-submenu");
            if (!siblingMenu) continue;
            siblingMenu.hidden = true;
            sibling.querySelector<HTMLButtonElement>(":scope > button")?.setAttribute("aria-expanded", "false");
        }
        submenu.hidden = false;
        trigger.setAttribute("aria-expanded", "true");
    };
    trigger.addEventListener("pointerenter", open);
    trigger.addEventListener("focus", open);
    trigger.addEventListener("click", () => {
        if (disabled) return;
        submenu.hidden = !submenu.hidden;
        trigger.setAttribute("aria-expanded", String(!submenu.hidden));
    });
    wrapper.append(trigger, submenu);
    parent.appendChild(wrapper);
    return submenu;
}
