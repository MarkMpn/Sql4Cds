import * as crypto from "node:crypto";
import * as vscode from "vscode";
import { State } from "vscode-languageclient/node";
import { ConnectionProfile } from "./protocol";
import { ProfileChangeEvent, ProfileStore } from "./profileStore";
import { Sql4CdsService } from "./serviceClient";
import { redactSensitiveText } from "./serviceRuntime";

export class DocumentConnectionManager implements vscode.Disposable {
  private readonly connections = new Map<string, ConnectionProfile>();
  private readonly connecting = new Set<string>();
  private readonly invalidated = new Set<string>();
  private readonly changedEmitter = new vscode.EventEmitter<void>();
  private readonly status = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 100);
  private readonly disposables: vscode.Disposable[] = [];
  private disposed = false;
  public readonly onDidChange = this.changedEmitter.event;

  constructor(private readonly service: Sql4CdsService, private readonly profiles: ProfileStore) {
    this.status.command = "sql4cds.connect";
    this.status.name = "SQL 4 CDS Connection";
    this.disposables.push(
      vscode.window.onDidChangeActiveTextEditor(() => this.updateContext()),
      vscode.workspace.onDidCloseTextDocument(document => void this.disconnectUri(document.uri.toString())),
      profiles.onDidChange(event => void this.handleProfileChange(event)),
      service.languageClient.onDidChangeState(event => {
        if (event.newState === State.Stopped) { this.handleServiceStopped(); }
      })
    );
    this.updateContext();
  }

  public get(uri: string): ConnectionProfile | undefined { return this.connections.get(uri); }

  public async connectEditor(profile?: ConnectionProfile, editor = vscode.window.activeTextEditor): Promise<boolean> {
    if (!editor) { void vscode.window.showInformationMessage("Open a SQL 4 CDS query editor first."); return false; }
    if (editor.document.languageId !== "sql4cds") {
      await vscode.languages.setTextDocumentLanguage(editor.document, "sql4cds");
    }
    profile ??= await this.profiles.pick();
    if (!profile) { return false; }
    profile = this.profiles.get(profile.id) ?? profile;

    const uri = editor.document.uri.toString();
    if (this.connecting.has(uri)) {
      void vscode.window.showInformationMessage("A SQL 4 CDS connection is already in progress for this editor.");
      return false;
    }

    const previous = this.connections.get(uri);
    const profileRevision = this.profiles.revision(profile.id);
    let previousDisconnected = false;
    this.connecting.add(uri);
    this.invalidated.delete(uri);
    try {
      this.showConnecting(profile.name);
      const details = await this.profiles.toConnectionDetails(profile);
      if (previous) {
        await this.disconnectService(uri);
        previousDisconnected = true;
      }
      await this.service.connect(uri, details);
      if (this.disposed || editor.document.isClosed || this.invalidated.has(uri)) {
        await this.disconnectService(uri);
        return false;
      }
      const latest = this.profiles.get(profile.id);
      if (!latest || profileRevision !== this.profiles.revision(profile.id) || !sameConnectionSettings(profile, latest)) {
        await this.disconnectService(uri);
        throw new Error(`The saved connection '${profile.name}' changed while connecting. Try again.`);
      }
      this.connections.set(uri, latest);
      this.fireChanged();
      return true;
    } catch (error) {
      const restored = previous && !this.disposed && !editor.document.isClosed && !this.invalidated.has(uri)
        ? (!previousDisconnected || await this.restoreConnection(uri, previous)) : false;
      if (!restored) { this.connections.delete(uri); }
      this.fireChanged();
      const reason = errorMessage(error).replace(/[.\s]+$/, "");
      const suffix = restored ? ` The previous connection '${previous?.name}' was restored.` : "";
      void vscode.window.showErrorMessage(`SQL 4 CDS connection failed: ${reason}.${suffix}`);
      return false;
    } finally {
      this.connecting.delete(uri);
      this.invalidated.delete(uri);
      this.updateContext();
    }
  }

  public async switchEditor(): Promise<boolean> {
    const editor = vscode.window.activeTextEditor;
    if (!editor) { void vscode.window.showInformationMessage("Open a SQL 4 CDS query editor first."); return false; }
    const current = this.connections.get(editor.document.uri.toString());
    const profile = await this.profiles.pick("Switch this query to another connection", current?.id);
    return profile ? this.connectEditor(profile, editor) : false;
  }

  public async reconnectEditor(): Promise<boolean> {
    const editor = vscode.window.activeTextEditor;
    if (!editor) { void vscode.window.showInformationMessage("Open a SQL 4 CDS query editor first."); return false; }
    const profile = this.connections.get(editor.document.uri.toString());
    if (!profile) { return this.connectEditor(undefined, editor); }
    return this.connectEditor(this.profiles.get(profile.id) ?? profile, editor);
  }

  public async testConnection(profile?: ConnectionProfile): Promise<boolean> {
    profile ??= await this.profiles.pick("Select a connection to test");
    if (!profile) { return false; }
    profile = this.profiles.get(profile.id) ?? profile;
    const ownerUri = `sql4cds-test://${crypto.randomUUID()}`;
    try {
      const response = await vscode.window.withProgress({
        location: vscode.ProgressLocation.Notification,
        title: `Testing SQL 4 CDS connection '${profile.name}'…`,
        cancellable: false
      }, async () => {
        const details = await this.profiles.toConnectionDetails(profile!);
        // A separate name forces fresh authentication instead of reusing a live data source.
        return this.service.connect(ownerUri, { options: { ...details.options, connectionName: ownerUri } });
      });
      const target = response.connectionSummary?.serverName ?? response.connectionSummary?.databaseName ?? profile.name;
      void vscode.window.showInformationMessage(`Connected successfully to ${target}.`);
      return true;
    } catch (error) {
      void vscode.window.showErrorMessage(`SQL 4 CDS connection test failed: ${errorMessage(error)}`);
      return false;
    } finally {
      await this.disconnectService(ownerUri);
    }
  }

  public async disconnectEditor(): Promise<void> {
    const editor = vscode.window.activeTextEditor;
    if (editor) { await this.disconnectUri(editor.document.uri.toString()); }
  }

  public async newQuery(profile?: ConnectionProfile, query = ""): Promise<void> {
    profile ??= await this.profiles.pick();
    if (!profile) { return; }
    const document = await vscode.workspace.openTextDocument({ language: "sql4cds", content: query });
    const editor = await vscode.window.showTextDocument(document);
    await this.connectEditor(profile, editor);
  }

  private async restoreConnection(uri: string, previous: ConnectionProfile): Promise<boolean> {
    try {
      const current = this.profiles.get(previous.id);
      if (!current) { return false; }
      await this.service.connect(uri, await this.profiles.toConnectionDetails(current));
      if (this.disposed || this.invalidated.has(uri) || !vscode.workspace.textDocuments.some(document => !document.isClosed && document.uri.toString() === uri)) {
        await this.disconnectService(uri);
        return false;
      }
      this.connections.set(uri, current);
      return true;
    } catch {
      return false;
    }
  }

  private async disconnectUri(uri: string): Promise<void> {
    if (this.connecting.has(uri)) { this.invalidated.add(uri); }
    if (!this.connections.delete(uri)) { return; }
    await this.disconnectService(uri);
    this.fireChanged();
  }

  private async disconnectService(uri: string): Promise<void> {
    try { await this.service.disconnect(uri); } catch { /* The service may already be stopping or disconnected. */ }
  }

  private async handleProfileChange(event: ProfileChangeEvent): Promise<void> {
    const affected = [...this.connections].filter(([, profile]) => profile.id === event.profile.id);
    if (event.type === "removed") {
      await Promise.all(affected.map(([uri]) => this.disconnectUri(uri)));
      return;
    }
    if (event.type !== "updated") { return; }

    if (event.previous && !event.credentialsChanged && sameConnectionSettings(event.previous, event.profile)) {
      for (const [uri] of affected) { this.connections.set(uri, event.profile); }
      this.fireChanged();
      return;
    }

    await Promise.all(affected.map(([uri]) => this.disconnectUri(uri)));
    if (affected.length > 0) {
      void vscode.window.showInformationMessage(`Connection '${event.profile.name}' was updated. Reconnect affected query editors to use the new settings.`);
    }
  }

  private showConnecting(name: string): void {
    this.status.text = `$(sync~spin) SQL 4 CDS: Connecting to ${name}`;
    this.status.tooltip = `Connecting to ${name}`;
    this.status.show();
  }

  private handleServiceStopped(): void {
    for (const uri of this.connecting) { this.invalidated.add(uri); }
    if (this.connections.size === 0) { return; }
    this.connections.clear();
    this.fireChanged();
    void vscode.window.showWarningMessage("SQL 4 CDS disconnected because the language service stopped. Reconnect after the service is available.");
  }

  private fireChanged(): void {
    if (!this.disposed) { this.changedEmitter.fire(); }
    this.updateContext();
  }

  private updateContext(): void {
    if (this.disposed) { return; }
    const editor = vscode.window.activeTextEditor;
    const profile = editor ? this.connections.get(editor.document.uri.toString()) : undefined;
    void vscode.commands.executeCommand("setContext", "sql4cds.connected", Boolean(profile));
    if (editor?.document.languageId === "sql4cds") {
      this.status.text = profile ? `$(database) ${profile.name}` : "$(debug-disconnect) SQL 4 CDS: Disconnected";
      this.status.tooltip = profile ? `${profile.name}\n${profile.url ?? "Connection string"}\nClick to manage the connection` : "Select a SQL 4 CDS connection";
      this.status.command = profile ? "sql4cds.manageEditorConnection" : "sql4cds.connect";
      this.status.show();
    } else {
      this.status.hide();
    }
  }

  public dispose(): void {
    this.disposed = true;
    for (const disposable of this.disposables) { disposable.dispose(); }
    for (const uri of this.connections.keys()) { void this.disconnectService(uri); }
    this.connections.clear();
    this.status.dispose();
    this.changedEmitter.dispose();
  }
}

export function errorMessage(error: unknown): string {
  return redactSensitiveText(error instanceof Error ? error.message : String(error));
}

function sameConnectionSettings(left: ConnectionProfile, right: ConnectionProfile): boolean {
  return left.name === right.name && left.authenticationType === right.authenticationType &&
    left.url === right.url &&
    left.user === right.user &&
    left.clientId === right.clientId &&
    left.redirectUrl === right.redirectUrl;
}
