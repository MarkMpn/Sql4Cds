import * as vscode from "vscode";
import type { LanguageClient } from "vscode-languageclient/node";
import { DocumentFormattingRequest } from "vscode-languageclient/node";

/** Formats the active query using the language server, adding display name aliases to the selected columns. */
export async function addDisplayNames(client: LanguageClient): Promise<void> {
  const editor = vscode.window.activeTextEditor;
  if (!editor || editor.document.languageId !== "sql4cds") {
    void vscode.window.showInformationMessage("Open a SQL 4 CDS query editor first.");
    return;
  }
  const document = editor.document;
  const version = document.version;
  const options = editor.options;
  const edits = await vscode.window.withProgress(
    { location: vscode.ProgressLocation.Window, title: "Adding display names..." },
    () => client.sendRequest(DocumentFormattingRequest.type, {
      textDocument: { uri: document.uri.toString() },
      options: {
        tabSize: typeof options.tabSize === "number" ? options.tabSize : 4,
        insertSpaces: typeof options.insertSpaces === "boolean" ? options.insertSpaces : true,
        addDisplayNames: true
      }
    }));
  if (!edits?.length) { return; }
  if (document.isClosed || document.version !== version) {
    throw new Error("The query changed while adding display names. Please try again.");
  }
  const workspaceEdit = new vscode.WorkspaceEdit();
  workspaceEdit.set(document.uri, await client.protocol2CodeConverter.asTextEdits(edits));
  if (!await vscode.workspace.applyEdit(workspaceEdit)) {
    throw new Error("The display names could not be added to the query.");
  }
}
