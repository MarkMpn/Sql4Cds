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

export class ObjectExplorer {
    private readonly connections = new Map<number, ExplorerConnection>();
    // A session-created notification can arrive before the createsession response.
    private readonly earlySessions = new Map<string, ObjectExplorerSessionCreated>();
    private creatingSessions = 0;
    private readonly items = new Map<TreeNode, HTMLElement>();
    private selected: TreeNode | null = null;
    private available = true;

    constructor(
        private readonly client: Sql4CdsClient,
        private readonly tree: HTMLElement,
        private readonly reportError: (message: string) => void,
        private readonly insertName: (text: string) => void,
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
                existing.root.info.label = connection.connection.name;
                continue;
            }
            const entry: ExplorerConnection = {
                connection,
                ready: false,
                root: {
                    info: { nodePath: `connection:${connection.slot}`, nodeType: "Server", label: connection.connection.name, isLeaf: false },
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
        entry.root.info = { ...params.rootNode, label: entry.connection.connection.name };
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

    private select(node: TreeNode): void {
        this.selected = node;
        for (const [candidate, item] of this.items) {
            item.setAttribute("aria-selected", String(candidate === node));
            item.tabIndex = candidate === node ? 0 : -1;
        }
        this.items.get(node)?.focus();
    }

    private onKeyDown(event: KeyboardEvent): void {
        const nodes = this.visibleNodes();
        const node = this.selected ?? nodes[0];
        if (!node) return;
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
            row.addEventListener("click", () => this.select(node));
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
