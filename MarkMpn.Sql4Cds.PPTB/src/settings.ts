/** Settings passed to the language server in `workspace/didChangeConfiguration` */
export interface Sql4CdsSettings {
    useTdsEndpoint: boolean;
    blockDeleteWithoutWhere: boolean;
    blockUpdateWithoutWhere: boolean;
    useBulkDelete: boolean;
    batchSize: number;
    maxDegreeOfParallelism: number;
    useLocalTimeZone: boolean;
    bypassCustomPlugins: boolean;
    quotedIdentifiers: boolean;
    deleteWarnThreshold: number;
    updateWarnThreshold: number;
    insertWarnThreshold: number;
    selectLimit: number;
    maxRetrievesPerQuery: number;
    localFormatDates: boolean;
    useSchemaColumnOrdering: boolean;
}

export const defaultSettings: Sql4CdsSettings = {
    useTdsEndpoint: false,
    blockDeleteWithoutWhere: true,
    blockUpdateWithoutWhere: true,
    useBulkDelete: false,
    batchSize: 100,
    maxDegreeOfParallelism: 0,
    useLocalTimeZone: false,
    bypassCustomPlugins: false,
    quotedIdentifiers: false,
    deleteWarnThreshold: 0,
    updateWarnThreshold: 0,
    insertWarnThreshold: 1,
    selectLimit: 0,
    maxRetrievesPerQuery: 100,
    localFormatDates: false,
    useSchemaColumnOrdering: false,
};

interface SettingDescriptor {
    key: keyof Sql4CdsSettings;
    label: string;
    description: string;
    min?: number;
    /** Settings that rely on direct access to Dataverse and can't be proxied through PPTB */
    unsupported?: boolean;
}

const settingDescriptors: SettingDescriptor[] = [
    { key: "blockDeleteWithoutWhere", label: "Block DELETE without WHERE", description: "Prevents DELETE statements from running unless they include a WHERE clause" },
    { key: "blockUpdateWithoutWhere", label: "Block UPDATE without WHERE", description: "Prevents UPDATE statements from running unless they include a WHERE clause" },
    { key: "deleteWarnThreshold", label: "DELETE warning threshold", description: "Ask for confirmation before deleting more than this number of records", min: 0 },
    { key: "updateWarnThreshold", label: "UPDATE warning threshold", description: "Ask for confirmation before updating more than this number of records", min: 0 },
    { key: "insertWarnThreshold", label: "INSERT warning threshold", description: "Ask for confirmation before inserting more than this number of records", min: 0 },
    { key: "batchSize", label: "Batch size", description: "Number of records to modify in each request", min: 1 },
    { key: "selectLimit", label: "SELECT limit", description: "Maximum number of records to retrieve for a SELECT query (0 = unlimited)", min: 0 },
    { key: "maxRetrievesPerQuery", label: "Max retrieves per query", description: "Maximum number of pages to retrieve for a single query", min: 1 },
    { key: "bypassCustomPlugins", label: "Bypass custom plugins", description: "Bypasses custom plugin logic when modifying data" },
    { key: "quotedIdentifiers", label: "Quoted identifiers", description: "Treats double-quoted strings as identifiers rather than string literals" },
    { key: "useLocalTimeZone", label: "Use local time zone", description: "Shows and accepts date/time values in your local time zone rather than UTC" },
    { key: "localFormatDates", label: "Locally format dates", description: "Formats date/time values using your local formatting preferences" },
    { key: "useSchemaColumnOrdering", label: "Schema column ordering", description: "Orders columns by schema position rather than alphabetically" },
    { key: "useTdsEndpoint", label: "Use TDS Endpoint", description: "Not available in Power Platform ToolBox", unsupported: true },
    { key: "useBulkDelete", label: "Use bulk delete", description: "Not available in Power Platform ToolBox", unsupported: true },
    { key: "maxDegreeOfParallelism", label: "Max degree of parallelism", description: "Maximum concurrent operations (0 = Dataverse recommendation, or 1 until a recommendation is received)", min: 0 },
];

const SETTINGS_KEY = "settings";

export async function loadSettings(): Promise<Sql4CdsSettings> {
    try {
        const stored = (await window.toolboxAPI.settings.get(SETTINGS_KEY)) as Partial<Sql4CdsSettings> | undefined;
        return sanitize({ ...defaultSettings, ...(stored ?? {}) });
    } catch {
        return { ...defaultSettings };
    }
}

export async function saveSettings(settings: Sql4CdsSettings): Promise<void> {
    await window.toolboxAPI.settings.set(SETTINGS_KEY, settings);
}

/** Forces settings that can't be supported when running through PPTB back to their defaults */
function sanitize(settings: Sql4CdsSettings): Sql4CdsSettings {
    for (const descriptor of settingDescriptors) {
        if (descriptor.unsupported) {
            (settings as unknown as Record<string, unknown>)[descriptor.key] = defaultSettings[descriptor.key];
        }
    }
    return settings;
}

/** Shows the settings dialog. Resolves with the updated settings, or null if the dialog was cancelled. */
export function showSettingsDialog(current: Sql4CdsSettings): Promise<Sql4CdsSettings | null> {
    return new Promise((resolve) => {
        const dialog = document.createElement("dialog");
        dialog.className = "settings-dialog";

        const form = document.createElement("form");
        form.method = "dialog";

        const title = document.createElement("h2");
        title.textContent = "SQL 4 CDS Settings";
        form.appendChild(title);

        const list = document.createElement("div");
        list.className = "settings-list";
        form.appendChild(list);

        const inputs = new Map<keyof Sql4CdsSettings, HTMLInputElement>();

        for (const descriptor of settingDescriptors) {
            const value = current[descriptor.key];
            const row = document.createElement("label");
            row.className = "setting-row";
            row.title = descriptor.description;

            const input = document.createElement("input");
            if (typeof value === "boolean") {
                input.type = "checkbox";
                input.checked = value;
            } else {
                input.type = "number";
                input.value = String(value);
                if (descriptor.min !== undefined) input.min = String(descriptor.min);
            }
            input.disabled = !!descriptor.unsupported;
            inputs.set(descriptor.key, input);

            const text = document.createElement("span");
            text.className = "setting-label";
            text.textContent = descriptor.label;

            const help = document.createElement("small");
            help.textContent = descriptor.description;
            text.appendChild(help);

            if (input.type === "checkbox") {
                row.append(input, text);
            } else {
                row.append(text, input);
            }
            list.appendChild(row);
        }

        const buttons = document.createElement("div");
        buttons.className = "dialog-buttons";

        const reset = document.createElement("button");
        reset.type = "button";
        reset.textContent = "Reset to defaults";
        reset.addEventListener("click", () => {
            for (const [key, input] of inputs) {
                const value = defaultSettings[key];
                if (typeof value === "boolean") input.checked = value;
                else input.value = String(value);
            }
        });

        const cancel = document.createElement("button");
        cancel.value = "cancel";
        cancel.textContent = "Cancel";

        const ok = document.createElement("button");
        ok.value = "ok";
        ok.className = "primary";
        ok.textContent = "Save";

        buttons.append(reset, cancel, ok);
        form.appendChild(buttons);
        dialog.appendChild(form);
        document.body.appendChild(dialog);

        dialog.addEventListener("close", () => {
            dialog.remove();

            if (dialog.returnValue !== "ok") {
                resolve(null);
                return;
            }

            const updated: Record<string, unknown> = { ...current };
            for (const [key, input] of inputs) {
                if (typeof defaultSettings[key] === "boolean") {
                    updated[key] = input.checked;
                } else {
                    const parsed = Number.parseInt(input.value, 10);
                    updated[key] = Number.isFinite(parsed) ? parsed : defaultSettings[key];
                }
            }
            resolve(sanitize(updated as unknown as Sql4CdsSettings));
        });

        dialog.showModal();
    });
}
