import * as monaco from "monaco-editor/esm/vs/editor/edcore.main";
import "monaco-editor/esm/vs/basic-languages/sql/sql.contribution";
import "monaco-editor/esm/vs/basic-languages/xml/xml.contribution";
import "monaco-editor/esm/vs/basic-languages/powerquery/powerquery.contribution";
import editorWorkerUrl from "monaco-editor/esm/vs/editor/editor.worker.js?worker&url";
import type { Sql4CdsClient } from "../lsp/client";
import type { CompletionItem, Diagnostic, MarkupContent, PublishDiagnosticsParams } from "../lsp/protocol";

export { monaco };

const MARKER_OWNER = "sql4cds";

/** LSP CompletionItemKind values are 1-based and in this order */
const lspCompletionKinds: Array<keyof typeof monaco.languages.CompletionItemKind> = [
    "Text", "Method", "Function", "Constructor", "Field", "Variable", "Class", "Interface", "Module", "Property",
    "Unit", "Value", "Enum", "Keyword", "Snippet", "Color", "File", "Reference", "Folder", "EnumMember",
    "Constant", "Struct", "Event", "Operator", "TypeParameter",
];

let configured = false;

/**
 * Configures the global Monaco environment and registers the SQL language feature providers that are backed by
 * the SQL 4 CDS language server. Providers are shared by all editors and route requests by model URI.
 */
export function configureMonaco(client: Sql4CdsClient, isConnected: (uri: string) => boolean): void {
    if (configured) return;
    configured = true;

    // PPTB's CSP blocks blob: workers, so load the editor worker from the tool's own origin. If that is not
    // possible, Monaco falls back to running the (lightweight) worker code on the UI thread.
    self.MonacoEnvironment = {
        getWorker() {
            return new Worker(new URL(editorWorkerUrl, document.baseURI), { name: "editorWorker" });
        },
    };

    monaco.languages.registerDocumentFormattingEditProvider("sql", {
        async provideDocumentFormattingEdits(model, options) {
            const edits = await client.formatDocument(model.uri.toString(), {
                tabSize: options.tabSize,
                insertSpaces: options.insertSpaces,
                addDisplayNames: false,
            });
            return edits?.map((edit) => ({ range: toMonacoRange(edit.range), text: edit.newText })) ?? [];
        },
    });

    monaco.languages.registerCompletionItemProvider("sql", {
        triggerCharacters: ["."],
        async provideCompletionItems(model, position, context) {
            const uri = model.uri.toString();
            if (!isConnected(uri)) return { suggestions: [] };

            const response = await safely(() => client.completion(
                uri,
                position.lineNumber - 1,
                position.column - 1,
                context.triggerKind === monaco.languages.CompletionTriggerKind.TriggerCharacter ? context.triggerCharacter : undefined,
            ));

            const items: CompletionItem[] = !response ? [] : Array.isArray(response) ? response : response.items;
            const word = model.getWordUntilPosition(position);
            const range = new monaco.Range(position.lineNumber, word.startColumn, position.lineNumber, word.endColumn);

            return {
                suggestions: items.map((item, index) => ({
                    label: item.label,
                    kind: monaco.languages.CompletionItemKind[lspCompletionKinds[(item.kind ?? 1) - 1] ?? "Text"],
                    detail: item.detail,
                    documentation: toMarkdown(item.documentation),
                    insertText: item.insertText ?? item.label,
                    filterText: item.filterText,
                    // Preserve the server's ordering
                    sortText: item.sortText ?? index.toString().padStart(6, "0"),
                    range,
                })),
            };
        },
    });

    monaco.languages.registerHoverProvider("sql", {
        async provideHover(model, position) {
            const uri = model.uri.toString();
            if (!isConnected(uri)) return null;

            const hover = await safely(() => client.hover(uri, position.lineNumber - 1, position.column - 1));
            if (!hover) return null;

            const contents = Array.isArray(hover.contents) ? hover.contents : [hover.contents];
            return {
                range: hover.range ? toMonacoRange(hover.range) : undefined,
                contents: contents.map((c) => typeof c === "string" ? { value: c } : "kind" in c ? toMarkdown(c)! : { value: "```" + c.language + "\n" + c.value + "\n```" }),
            };
        },
    });

    monaco.languages.registerSignatureHelpProvider("sql", {
        signatureHelpTriggerCharacters: ["(", ","],
        signatureHelpRetriggerCharacters: [","],
        async provideSignatureHelp(model, position) {
            const uri = model.uri.toString();
            if (!isConnected(uri)) return null;

            const help = await safely(() => client.signatureHelp(uri, position.lineNumber - 1, position.column - 1));
            if (!help || !help.signatures?.length) return null;

            return {
                value: {
                    activeSignature: help.activeSignature ?? 0,
                    activeParameter: help.activeParameter ?? 0,
                    signatures: help.signatures.map((s) => ({
                        label: s.label,
                        documentation: toMarkdown(s.documentation),
                        parameters: (s.parameters ?? []).map((p) => ({ label: p.label, documentation: toMarkdown(p.documentation) })),
                    })),
                },
                dispose() {},
            };
        },
    });
}

/** Language feature requests are best-effort - log failures rather than surfacing them as unexpected errors */
async function safely<T>(request: () => Promise<T>): Promise<T | null> {
    try {
        return await request();
    } catch (err) {
        console.warn("SQL 4 CDS language feature request failed", err);
        return null;
    }
}

/** Displays the diagnostics published by the server as editor markers */
export function applyDiagnostics(model: monaco.editor.ITextModel, params: PublishDiagnosticsParams): void {
    monaco.editor.setModelMarkers(model, MARKER_OWNER, params.diagnostics.map(toMarker));
}

function toMarker(diagnostic: Diagnostic): monaco.editor.IMarkerData {
    const severity = {
        1: monaco.MarkerSeverity.Error,
        2: monaco.MarkerSeverity.Warning,
        3: monaco.MarkerSeverity.Info,
        4: monaco.MarkerSeverity.Hint,
    }[diagnostic.severity ?? 1];

    return {
        severity,
        message: diagnostic.message,
        source: diagnostic.source,
        startLineNumber: diagnostic.range.start.line + 1,
        startColumn: diagnostic.range.start.character + 1,
        endLineNumber: diagnostic.range.end.line + 1,
        endColumn: diagnostic.range.end.character + 1,
    };
}

function toMonacoRange(range: { start: { line: number; character: number }; end: { line: number; character: number } }): monaco.Range {
    return new monaco.Range(range.start.line + 1, range.start.character + 1, range.end.line + 1, range.end.character + 1);
}

function toMarkdown(value: string | MarkupContent | undefined): monaco.IMarkdownString | undefined {
    if (value === undefined || value === null) return undefined;
    if (typeof value === "string") return { value: escapeMarkdown(value) };
    return { value: value.kind === "markdown" ? value.value : escapeMarkdown(value.value) };
}

function escapeMarkdown(text: string): string {
    return text.replace(/[\\`*_{}[\]()#+\-.!|<>]/g, "\\$&");
}
