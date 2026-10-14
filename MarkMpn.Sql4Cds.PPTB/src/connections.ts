import type { Sql4CdsClient } from "./lsp/client";
import type { ConnectionCompleteParams, PptbConnectionOptions } from "./lsp/protocol";

export interface ConnectionSlot {
    slot: number;
    connection: ToolBoxAPI.Connection;
    options: PptbConnectionOptions;
    /** Set once the slot has been registered with the language server */
    error?: string;
    registration?: Promise<void>;
}

/**
 * Tracks the Dataverse connections that PPTB has made available to this tool and registers each of them with the
 * language server. Each connection is registered under a hidden document URI so that every connection is always
 * available for cross-instance queries, regardless of which connection each query tab is using.
 */
export class ConnectionRegistry {
    private slots: Array<ConnectionSlot | null> = [];

    constructor(private readonly client: Sql4CdsClient) {}

    get all(): ConnectionSlot[] {
        return this.slots.filter((s): s is ConnectionSlot => s !== null);
    }

    get(slot: number): ConnectionSlot | null {
        return this.slots[slot] ?? null;
    }

    static ownerUri(slot: number): string {
        return `pptb://connection/${slot}`;
    }

    /**
     * Reloads the connections from PPTB and registers any new or changed connections with the language server.
     * Returns the slot numbers whose connection has changed so that any editors using them can be reconnected.
     */
    async refresh(): Promise<number[]> {
        const connections = await window.toolboxAPI.connections.getConnections();
        const usedNames = new Set<string>();
        const updated: Array<ConnectionSlot | null> = connections.map((connection, slot) => {
            if (!connection) return null;

            return {
                slot,
                connection,
                options: {
                    authenticationType: "PPTB",
                    url: connection.url,
                    connectionName: uniqueName(getConnectionName(connection), usedNames),
                    connectionId: connection.id,
                    connectionTarget: slot,
                },
            };
        });

        const changed: number[] = [];
        const count = Math.max(updated.length, this.slots.length);
        for (let slot = 0; slot < count; slot++) {
            const before = this.slots[slot] ?? null;
            const after = updated[slot] ?? null;
            if (sameOptions(before?.options, after?.options)) {
                if (before && after) {
                    before.connection = after.connection;
                    updated[slot] = before;
                }
                continue;
            }

            changed.push(slot);
            if (before) await this.client.disconnect(ConnectionRegistry.ownerUri(slot)).catch(() => false);
        }

        this.slots = updated;
        return changed;
    }

    /** Registers a connection slot with the language server */
    register(slot: number): Promise<void> {
        const entry = this.slots[slot];
        if (!entry) return Promise.reject(new Error("The selected connection is not available"));

        if (!entry.registration) {
            entry.registration = this.registerEntry(entry).catch((error) => {
                entry.registration = undefined;
                throw error;
            });
        }

        return entry.registration;
    }

    private async registerEntry(entry: ConnectionSlot): Promise<void> {
        const ownerUri = ConnectionRegistry.ownerUri(entry.slot);
        const complete = new Promise<ConnectionCompleteParams>((resolve) => {
            this.client.addListener(ownerUri, { connectionComplete: resolve });
        });

        try {
            const accepted = await this.client.connect(ownerUri, entry.options);
            if (!accepted) throw new Error("The connection request was rejected");
            const result = await complete;
            entry.error = result.errorMessage || undefined;
        } catch (err) {
            entry.error = err instanceof Error ? err.message : String(err);
        } finally {
            this.client.removeListener(ownerUri);
        }

        if (entry.error) throw new Error(entry.error);
    }
}

/** Generates a short name for the connection to use as its database name in SQL queries */
function getConnectionName(connection: ToolBoxAPI.Connection): string {
    try {
        const url = new URL(connection.url.includes("://") ? connection.url : `https://${connection.url}`);
        const label = url.hostname.split(".")[0];
        if (label) return label.toLowerCase();
    } catch {
        // Fall back to the display name
    }

    return connection.name.replace(/[^A-Za-z0-9_]+/g, "_").toLowerCase() || "dataverse";
}

function uniqueName(name: string, used: Set<string>): string {
    let candidate = name;
    for (let i = 2; used.has(candidate); i++) candidate = `${name}_${i}`;
    used.add(candidate);
    return candidate;
}

function sameOptions(a: PptbConnectionOptions | undefined, b: PptbConnectionOptions | undefined): boolean {
    if (!a || !b) return a === b;
    return a.url === b.url && a.connectionId === b.connectionId && a.connectionName === b.connectionName && a.connectionTarget === b.connectionTarget;
}
