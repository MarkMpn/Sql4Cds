import type { ConnectionSlot } from "./connections";
import type { Sql4CdsClient } from "./lsp/client";
import type { ObjectExplorerExpandComplete, ObjectExplorerNode, ObjectExplorerSessionCreated, ObjectExplorerSessionDisconnected } from "./lsp/protocol";

const NODE_ICONS: Readonly<Record<string, string>> = {
    Server: "\u25A4",
    Folder: "\u25B0",
    Table: "\u25A6",
    Column: "\u25A5",
    TableValuedFunction: "\u0192",
    StoredProcedure: "\u2699",
};

interface ExplorerConnection {
    connection: ConnectionSlot;
    root: TreeNode;
    sessionId?: string;
    creating?: Promise<void>;
    ready: boolean;
}

interface TreeNode {
    info: ObjectExplorerNode;
    connection: ConnectionSlot;
    parent?: TreeNode;
    children: TreeNode[];
    expanded: boolean;
    loaded: boolean;
    loading: boolean;
    error?: string;
}

type ScriptAction = "SELECT" | "SELECT_TOP_1000" | "INSERT" | "UPDATE" | "DELETE" | "EXECUTE";
type ScriptDestination = "editor" | "file" | "clipboard";

export class ObjectExplorer {
    private readonly connections = new Map<number, ExplorerConnection>();
    // A session-created notification can arrive before the createsession response.
    private readonly earlySessions = new Map<string, ObjectExplorerSessionCreated>();
    private creatingSessions = 0;
    private readonly items = new Map<TreeNode, HTMLElement>();
    private selected: TreeNode | null = null;
    private available = true;
    private closeContextMenu: (() => void) | null = null;

    constructor(
        private readonly client: Sql4CdsClient,
        private readonly tree: HTMLElement,
        private readonly reportError: (message: string) => void,
        private readonly insertName: (text: string) => void,
        private readonly newQuery: (connection: ConnectionSlot, query: string) => void
    ) {
        tree.setAttribute("role", "tree");
        tree.setAttribute("aria-label", "Object Explorer");
        tree.addEventListener("keydown", (event) => this.onKeyDown(event));
        this.render();
    }

    async setConnections(connections: ConnectionSlot[]): Promise<void> {
        const incoming = new Map(connections.map((connection) => [connection.slot, connection]));
        for (const [slot, entry] of this.connections) {
            if (incoming.get(slot)?.options !== entry.connection.options) await this.removeConnection(slot);
        }
        for (const connection of connections) {
            const existing = this.connections.get(connection.slot);
            if (existing) {
                existing.connection = connection;
                existing.root.info.label = connection.options.connectionName;
                continue;
            }
            const entry: ExplorerConnection = {
                connection,
                ready: false,
                root: {
                    info: { nodePath: `connection:${connection.slot}`, nodeType: "Server", label: connection.options.connectionName, isLeaf: false },
                    connection,
                    children: [],
                    expanded: false,
                    loaded: false,
                    loading: false,
                },
            };
            this.connections.set(connection.slot, entry);
        }
        this.render();
    }

    async removeConnection(slot: number): Promise<void> {
        const entry = this.connections.get(slot);
        if (!entry) return;
        this.connections.delete(slot);
        if (this.selected?.connection.slot === slot) this.selected = null;
        this.render();
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
        this.closeContextMenu?.();
        await Promise.all(Array.from(this.connections.keys(), (slot) => this.removeConnection(slot)));
        this.earlySessions.clear();
    }

    engineStopped(): void {
        this.available = false;
        this.connections.clear();
        this.earlySessions.clear();
        this.selected = null;
        this.render();
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
            entry.root.loading = false;
            entry.root.error = params.errorMessage || "Unable to create the Object Explorer session";
            this.reportError(entry.root.error);
            this.render();
            return;
        }
        entry.ready = true;
        entry.root.info = { ...params.rootNode, label: entry.connection.options.connectionName };
        entry.root.loading = false;
        if (entry.root.expanded) void this.load(entry.root, false);
        else this.render();
    }

    expandComplete(params: ObjectExplorerExpandComplete): void {
        const entry = this.findSession(params.sessionId);
        if (!entry) return;
        const node = this.allNodes(entry.root).find((candidate) => candidate.info.nodePath === params.nodePath);
        if (!node?.loading) return;
        node.loading = false;
        node.error = params.errorMessage || undefined;
        if (node.error) {
            this.reportError(node.error);
        } else {
            if (this.selected && this.selected !== node && this.allNodes(node).includes(this.selected)) this.selected = node;
            node.children = (params.nodes ?? []).map((info) => ({
                info, connection: entry.connection, parent: node, children: [], expanded: false, loaded: false, loading: false,
            }));
            node.loaded = true;
        }
        this.render();
    }

    sessionDisconnected(params: ObjectExplorerSessionDisconnected): void {
        const entry = this.findSession(params.sessionId);
        if (!entry) return;
        entry.sessionId = undefined;
        entry.ready = false;
        entry.root.loading = false;
        entry.root.loaded = false;
        entry.root.children = [];
        entry.root.error = params.errorMessage || "Object Explorer disconnected. Refresh to reconnect.";
        this.selected = entry.root;
        this.reportError(entry.root.error);
        this.render();
    }

    refresh(): void {
        const node = this.selected ?? this.connections.values().next().value?.root;
        if (!node || !this.available) return;
        const target = node.info.isLeaf ? node.parent ?? node : node;
        target.expanded = true;
        void this.load(target, true);
    }

    private findSession(sessionId: string): ExplorerConnection | undefined {
        return Array.from(this.connections.values()).find((entry) => entry.sessionId === sessionId);
    }

    private isCurrent(entry: ExplorerConnection): boolean {
        return this.available && this.connections.get(entry.connection.slot) === entry;
    }

    private async createSession(entry: ExplorerConnection): Promise<void> {
        this.creatingSessions++;
        try {
            const response = await this.client.createObjectExplorerSession(entry.connection.options);
            entry.sessionId = response.sessionId;
            const early = this.earlySessions.get(response.sessionId);
            this.earlySessions.delete(response.sessionId);
            if (early && this.isCurrent(entry)) this.sessionCreated(early);
        } catch (error) {
            if (!this.isCurrent(entry)) return;
            entry.root.loading = false;
            entry.root.error = errorMessage(error);
            this.reportError(entry.root.error);
            this.render();
        } finally {
            entry.creating = undefined;
            if (--this.creatingSessions === 0) this.earlySessions.clear();
        }
    }

    private async load(node: TreeNode, refresh: boolean): Promise<void> {
        const entry = this.connections.get(node.connection.slot);
        if (node.info.isLeaf || node.loading || !entry || !this.isCurrent(entry)) return;
        const retrySession = !entry.ready && !!entry.root.error;
        node.loading = true;
        node.error = undefined;
        this.render();
        if (retrySession && entry.sessionId) {
            const sessionId = entry.sessionId;
            entry.sessionId = undefined;
            await this.closeSession(sessionId);
            if (!this.isCurrent(entry)) return;
            node.loading = true;
            node.error = undefined;
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
                entry.sessionId = undefined;
                entry.ready = false;
                throw new Error("The Object Explorer request was rejected. Refresh the connection to retry.");
            }
        } catch (error) {
            if (!this.isCurrent(entry)) return;
            node.loading = false;
            node.error = errorMessage(error);
            this.reportError(node.error);
            this.render();
        }
    }

    private toggle(node: TreeNode): void {
        if (node.info.isLeaf) return;
        node.expanded = !node.expanded;
        if (node.expanded && (!node.loaded || node.error)) void this.load(node, false);
        else this.render();
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

    private select(node: TreeNode): void {
        this.selected = node;
        for (const [candidate, item] of this.items) {
            item.setAttribute("aria-selected", String(candidate === node));
            item.tabIndex = candidate === node ? 0 : -1;
        }
        this.items.get(node)?.focus();
    }

    private showContextMenu(node: TreeNode, x: number, y: number): void {
        this.closeContextMenu?.();
        if (!this.available) return;
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
        this.closeContextMenu = () => {
            this.closeContextMenu = null;
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
        this.selected = next ?? node;
        this.render(true);
    }

    private render(focus = false): void {
        this.closeContextMenu?.();
        const hadFocus = this.tree.contains(document.activeElement);
        const visible = this.visibleNodes();
        if (!this.selected || !visible.includes(this.selected)) this.selected = visible[0] ?? null;
        const fragment = document.createDocumentFragment();
        this.items.clear();
        const append = (node: TreeNode, parent: HTMLElement | DocumentFragment, level: number): void => {
            const item = document.createElement("div");
            item.className = "explorer-node";
            item.setAttribute("role", "treeitem");
            item.setAttribute("aria-level", String(level));
            item.setAttribute("aria-selected", String(this.selected === node));
            if (!node.info.isLeaf) item.setAttribute("aria-expanded", String(node.expanded));
            item.setAttribute("aria-busy", String(node.loading));
            item.tabIndex = this.selected === node ? 0 : -1;
            item.dataset.path = node.info.nodePath;
            this.items.set(node, item);
            const row = document.createElement("div");
            row.className = "explorer-row";
            row.style.paddingLeft = `${(level - 1) * 16 + 4}px`;
            row.title = node.error || node.info.errorMessage || node.info.label;
            const expander = document.createElement("span");
            expander.className = "explorer-expander";
            expander.setAttribute("aria-hidden", "true");
            expander.textContent = node.info.isLeaf ? "" : node.expanded ? "\u25BE" : "\u25B8";
            expander.addEventListener("click", (event) => {
                event.stopPropagation();
                this.selected = node;
                this.toggle(node);
                this.render(true);
            });
            expander.addEventListener("dblclick", (event) => event.stopPropagation());
            const icon = document.createElement("span");
            icon.className = `explorer-icon explorer-icon-${node.info.nodeType.toLowerCase()}`;
            icon.setAttribute("aria-hidden", "true");
            icon.textContent = NODE_ICONS[node.info.nodeType] ?? NODE_ICONS.Table;
            const label = document.createElement("span");
            label.textContent = node.info.label;
            row.append(expander, icon, label);
            if (node.info.nodeType === "Table") {
                const selectTop = document.createElement("button");
                selectTop.className = "explorer-select-top";
                selectTop.type = "button";
                selectTop.setAttribute("aria-label", "Select Top 1000 Rows");
                selectTop.title = "Select Top 1000 Rows";
                selectTop.textContent = "\u25B6";
                selectTop.addEventListener("click", (event) => {
                    event.stopPropagation();
                    this.select(node);
                    void this.outputScript(node, "SELECT_TOP_1000", "editor");
                });
                row.appendChild(selectTop);
            }
            row.addEventListener("click", () => this.select(node));
            row.addEventListener("contextmenu", (event) => {
                event.preventDefault();
                event.stopPropagation();
                this.showContextMenu(node, event.clientX, event.clientY);
            });
            row.addEventListener("dblclick", () => {
                this.selected = node;
                if (["Table", "Column", "TableValuedFunction", "StoredProcedure"].includes(node.info.nodeType)) {
                    const name = node.info.nodeType === "Column" ? node.info.label : node.info.metadata?.name ?? node.info.label;
                    const schema = node.info.nodeType === "Column" ? undefined : node.info.metadata?.schema;
                    this.insertName(schema ? `${schema}.${name}` : name);
                } else {
                    this.toggle(node);
                    this.render(true);
                }
            });
            item.addEventListener("focus", (event) => {
                if (event.target === item) this.selected = node;
            });
            item.appendChild(row);
            if (node.expanded) {
                const group = document.createElement("div");
                group.setAttribute("role", "group");
                if (node.loading || node.error || (node.loaded && node.children.length === 0)) {
                    const message = document.createElement("div");
                    message.className = node.error ? "explorer-message error" : "explorer-message";
                    message.style.paddingLeft = `${level * 16 + 24}px`;
                    message.textContent = node.error ? `${node.error} (Refresh to retry)` : node.loading ? "Loading…" : "No objects";
                    message.setAttribute("role", node.error ? "alert" : "status");
                    group.appendChild(message);
                }
                for (const child of node.children) append(child, group, level + 1);
                item.appendChild(group);
            }
            parent.appendChild(item);
        };
        for (const entry of this.connections.values()) append(entry.root, fragment, 1);
        if (this.connections.size === 0) {
            const empty = document.createElement("div");
            empty.className = "explorer-message";
            empty.textContent = this.available ? "No connections available" : "SQL 4 CDS engine stopped";
            fragment.appendChild(empty);
        }
        this.tree.replaceChildren(fragment);
        if (focus || hadFocus) this.tree.querySelector<HTMLElement>('[tabindex="0"]')?.focus();
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
