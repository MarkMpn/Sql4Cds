import * as vscode from "vscode";
import type { LanguageClient } from "vscode-languageclient/node";
import type { DocumentConnectionManager } from "./documentConnections";
import { Methods } from "./protocol";

export async function convertToFetchXml(client: LanguageClient, connections: DocumentConnectionManager): Promise<void> {
  const editor = vscode.window.activeTextEditor;
  if (!editor || editor.document.languageId !== "sql4cds") {
    void vscode.window.showInformationMessage("Open a SQL 4 CDS query editor first.");
    return;
  }
  const document = editor.document;
  const ownerUri = document.uri.toString();
  const query = document.getText(editor.selection.isEmpty ? undefined : editor.selection);
  if (!query.trim()) {
    void vscode.window.showInformationMessage("Select or enter a query first.");
    return;
  }
  if (!connections.get(ownerUri) && !await connections.connectEditor(undefined, editor)) { return; }
  const results = await vscode.window.withProgress(
    { location: vscode.ProgressLocation.Window, title: "Converting SQL to FetchXML..." },
    () => client.sendRequest<string[]>(Methods.transformQuery, { ownerUri, query, target: "fetchxml" }));
  for (const content of results) {
    const preview = await vscode.workspace.openTextDocument({ language: "xml", content });
    await vscode.window.showTextDocument(preview, { preview: false });
  }
}
