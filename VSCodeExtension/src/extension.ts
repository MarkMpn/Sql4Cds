import * as vscode from "vscode";
import { DocumentConnectionManager, errorMessage } from "./documentConnections";
import { ObjectExplorerProvider, selectTopQuery, sqlIdentifier, Sql4CdsTreeItem } from "./objectExplorer";
import { ConnectionProfile } from "./protocol";
import { ProfileStore } from "./profileStore";
import { QueryController } from "./queryController";
import { Sql4CdsService } from "./serviceClient";
import { ServicePrerequisiteError } from "./serviceRuntime";

let service: Sql4CdsService | undefined;

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  const profiles = new ProfileStore(context);
  await vscode.commands.executeCommand("setContext", "sql4cds.hasProfiles", profiles.profiles.length > 0);
  await vscode.commands.executeCommand("setContext", "sql4cds.connected", false);
  await vscode.commands.executeCommand("setContext", "sql4cds.queryRunning", false);

  service = new Sql4CdsService(context);
  context.subscriptions.push(profiles, service);
  try {
    await service.start();
  } catch (error) {
    const message = `SQL 4 CDS could not start: ${errorMessage(error)}`;
    void vscode.window.showErrorMessage(message, "Show Output", ...(error instanceof ServicePrerequisiteError && error.helpUrl ? ["Install .NET 8"] : [])).then(async selection => {
      if (selection === "Show Output") { service?.showOutput(); }
      if (selection === "Install .NET 8" && error instanceof ServicePrerequisiteError && error.helpUrl) {
        await vscode.env.openExternal(vscode.Uri.parse(error.helpUrl));
      }
    });
    throw error;
  }

  const documentConnections = new DocumentConnectionManager(service, profiles);
  const objectExplorer = new ObjectExplorerProvider(service, profiles);
  const queries = new QueryController(service, documentConnections);
  context.subscriptions.push(documentConnections, objectExplorer, queries);
  context.subscriptions.push(vscode.window.registerTreeDataProvider("sql4cds.objectExplorer", objectExplorer));
  context.subscriptions.push(vscode.window.registerWebviewViewProvider("sql4cdsQueryResults", queries, {
    webviewOptions: { retainContextWhenHidden: true }
  }));

  context.subscriptions.push(
    vscode.commands.registerCommand("sql4cds.addConnection", async () => { await profiles.addInteractive(); }),
    vscode.commands.registerCommand("sql4cds.manageConnections", async () => manageConnections(profiles, objectExplorer, documentConnections)),
    vscode.commands.registerCommand("sql4cds.connect", async () => { await documentConnections.connectEditor(); }),
    vscode.commands.registerCommand("sql4cds.switchConnection", async () => { await documentConnections.switchEditor(); }),
    vscode.commands.registerCommand("sql4cds.reconnect", async () => { await documentConnections.reconnectEditor(); }),
    vscode.commands.registerCommand("sql4cds.manageEditorConnection", async () => manageEditorConnection(documentConnections, profiles)),
    vscode.commands.registerCommand("sql4cds.disconnect", async () => { await documentConnections.disconnectEditor(); }),
    vscode.commands.registerCommand("sql4cds.newQuery", async (item?: Sql4CdsTreeItem) => { await documentConnections.newQuery(item?.profile); }),
    vscode.commands.registerCommand("sql4cds.editConnection", async (item?: Sql4CdsTreeItem) => {
      const profile = item?.profile ?? await profiles.pick("Select a connection to edit");
      if (profile) { await profiles.editInteractive(profile.id); }
    }),
    vscode.commands.registerCommand("sql4cds.renameConnection", async (item?: Sql4CdsTreeItem) => {
      const profile = item?.profile ?? await profiles.pick("Select a connection to rename");
      if (profile) { await profiles.renameInteractive(profile.id); }
    }),
    vscode.commands.registerCommand("sql4cds.testConnection", async (item?: Sql4CdsTreeItem) => { await documentConnections.testConnection(item?.profile); }),
    vscode.commands.registerCommand("sql4cds.runQuery", async () => { await queries.execute(); }),
    vscode.commands.registerCommand("sql4cds.cancelQuery", async () => { await queries.cancel(); }),
    vscode.commands.registerCommand("sql4cds.refreshObjectExplorer", (item?: Sql4CdsTreeItem) => objectExplorer.refresh(item)),
    vscode.commands.registerCommand("sql4cds.searchTables", async (item?: Sql4CdsTreeItem) => {
      const result = await objectExplorer.searchTables(item?.profile);
      const query = selectTopQuery(result?.node.metadata);
      if (result && query) { await documentConnections.newQuery(result.profile, query); }
    }),
    vscode.commands.registerCommand("sql4cds.copyName", async (item?: Sql4CdsTreeItem) => {
      const name = item?.node?.metadata?.name ?? item?.node?.label;
      if (!name) { return; }
      await vscode.env.clipboard.writeText(name);
      void vscode.window.setStatusBarMessage(`Copied '${name}'`, 2000);
    }),
    vscode.commands.registerCommand("sql4cds.insertName", async (item?: Sql4CdsTreeItem) => {
      const name = sqlIdentifier(item?.node?.metadata, item?.node?.label);
      const editor = vscode.window.activeTextEditor;
      if (!name || !editor) { return; }
      await editor.edit(edit => edit.replace(editor.selection, name));
    }),
    vscode.commands.registerCommand("sql4cds.removeConnection", async (item?: Sql4CdsTreeItem) => {
      if (item) { await objectExplorer.removeProfile(item); return; }
      const profile = await profiles.pick("Select a connection to remove");
      if (profile) { await profiles.remove(profile.id); }
    }),
    vscode.commands.registerCommand("sql4cds.selectTop", async (item?: Sql4CdsTreeItem) => {
      const query = selectTopQuery(item?.node?.metadata);
      if (query && item) { await documentConnections.newQuery(item.profile, query); }
    })
  );
}

async function manageConnections(profiles: ProfileStore, explorer: ObjectExplorerProvider, connections: DocumentConnectionManager): Promise<void> {
  const selection = await vscode.window.showQuickPick<{ label: string; description?: string; profile?: ConnectionProfile; action: "add" | "manage" }>([
    { label: "$(add) Add connection", action: "add" },
    ...profiles.profiles.map(profile => ({ label: `$(database) ${profile.name}`, description: profile.url ?? "Connection string", profile, action: "manage" as const }))
  ], { title: "Manage SQL 4 CDS connections" });
  if (!selection) { return; }
  if (selection.action === "add") { await profiles.addInteractive(); }
  else if (selection.profile) { await manageProfile(selection.profile, profiles, explorer, connections); }
}

async function manageProfile(profile: ConnectionProfile, profiles: ProfileStore, explorer: ObjectExplorerProvider, connections: DocumentConnectionManager): Promise<void> {
  const selection = await vscode.window.showQuickPick([
    { label: "$(edit) Edit connection", action: "edit" },
    { label: "$(edit) Rename connection", action: "rename" },
    { label: "$(beaker) Test connection", action: "test" },
    { label: "$(new-file) New query", action: "query" },
    { label: "$(trash) Remove connection", action: "remove" }
  ], { title: `Manage ${profile.name}`, ignoreFocusOut: true });
  if (!selection) { return; }
  if (selection.action === "edit") { await profiles.editInteractive(profile.id); }
  else if (selection.action === "rename") { await profiles.renameInteractive(profile.id); }
  else if (selection.action === "test") { await connections.testConnection(profile); }
  else if (selection.action === "query") { await connections.newQuery(profile); }
  else if (selection.action === "remove") { await explorer.removeProfile(new Sql4CdsTreeItem(profile)); }
}

async function manageEditorConnection(connections: DocumentConnectionManager, profiles: ProfileStore): Promise<void> {
  const editor = vscode.window.activeTextEditor;
  const profile = editor ? connections.get(editor.document.uri.toString()) : undefined;
  if (!profile) { await connections.connectEditor(); return; }
  const selection = await vscode.window.showQuickPick([
    { label: "$(arrow-swap) Switch connection", action: "switch" },
    { label: "$(refresh) Reconnect", action: "reconnect" },
    { label: "$(beaker) Test connection", action: "test" },
    { label: "$(edit) Edit saved connection", action: "edit" },
    { label: "$(debug-disconnect) Disconnect", action: "disconnect" }
  ], { title: profile.name, ignoreFocusOut: true });
  if (!selection) { return; }
  if (selection.action === "switch") { await connections.switchEditor(); }
  else if (selection.action === "reconnect") { await connections.reconnectEditor(); }
  else if (selection.action === "test") { await connections.testConnection(profile); }
  else if (selection.action === "edit") {
    await profiles.editInteractive(profile.id);
  } else if (selection.action === "disconnect") { await connections.disconnectEditor(); }
}

export async function deactivate(): Promise<void> {
  await service?.dispose();
}
