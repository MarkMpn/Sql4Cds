import * as path from "node:path";
import { randomUUID } from "node:crypto";
import { mkdir } from "node:fs/promises";
import * as vscode from "vscode";
import { LanguageClient, LanguageClientOptions, ServerOptions, State, TransportKind } from "vscode-languageclient/node";
import {
  ConnectionCompleteParams,
  ConnectionDetails,
  ExpandResponse,
  Methods,
  SessionCreatedParams
} from "./protocol";
import { PendingRequests } from "./pendingRequests";
import { assertServicePrerequisites } from "./serviceRuntime";

export class Sql4CdsService implements vscode.Disposable {
  private client: LanguageClient | undefined;
  private readonly connectionPending = new PendingRequests<ConnectionCompleteParams>();
  private readonly sessionPending = new PendingRequests<SessionCreatedParams>();
  private readonly sessionCreated = new vscode.EventEmitter<SessionCreatedParams>();
  private readonly expandPending = new PendingRequests<ExpandResponse>();
  private readonly output: vscode.OutputChannel;
  private stopping = false;
  private stoppedTimer: NodeJS.Timeout | undefined;
  private disposal: Promise<void> | undefined;

  constructor(private readonly context: vscode.ExtensionContext) {
    this.output = vscode.window.createOutputChannel("SQL 4 CDS");
    context.subscriptions.push(this.output);
  }

  public get languageClient(): LanguageClient {
    if (!this.client) { throw new Error("SQL 4 CDS service has not started."); }
    return this.client;
  }

  public showOutput(): void {
    this.output.show(true);
  }

  public async start(): Promise<void> {
    if (this.client) { return; }
    const configuredPath = vscode.workspace.getConfiguration("SQL4CDS").get<string | null>("servicePath");
    const serviceRoot = configuredPath || path.join(this.context.extensionPath, "out", "sql4cdstoolsservice");
    const dll = serviceRoot.toLowerCase().endsWith(".dll") ? serviceRoot : path.join(serviceRoot, "MarkMpn.Sql4Cds.LanguageServer.dll");
    await assertServicePrerequisites(dll);
    const logRoot = this.context.logUri.fsPath;
    const dataRoot = this.context.globalStorageUri.fsPath;
    await mkdir(dataRoot, { recursive: true });
    const args = [dll, `--log-dir=${logRoot}`];
    if (vscode.workspace.getConfiguration("SQL4CDS").get<boolean>("logDebugInfo")) { args.push("--enable-logging"); }

    const serverOptions: ServerOptions = {
      command: "dotnet",
      args,
      transport: TransportKind.stdio,
      options: {
        env: {
          ...process.env,
          SQL4CDS_DATA_DIR: dataRoot,
          SQL4CDS_DISABLE_TELEMETRY: "1"
        }
      }
    };
    const clientOptions: LanguageClientOptions = {
      documentSelector: [{ scheme: "file", language: "sql4cds" }, { scheme: "untitled", language: "sql4cds" }],
      synchronize: { configurationSection: "SQL4CDS" },
      outputChannel: this.output
    };
    const client = new LanguageClient("sql4cds", "SQL 4 CDS Language Service", serverOptions, clientOptions);
    this.client = client;
    this.registerNotifications(client);
    this.registerLifecycle(client);
    this.output.appendLine(`Starting SQL 4 CDS language service with .NET ${path.basename(dll)}.`);
    try {
      await client.start();
      this.output.appendLine("SQL 4 CDS language service is ready.");
    } catch (error) {
      if (this.client === client) { this.client = undefined; }
      this.rejectAllPending(new Error("The SQL 4 CDS language service could not start."));
      throw error;
    }
  }

  public async connect(ownerUri: string, details: ConnectionDetails, type = "Default"): Promise<ConnectionCompleteParams> {
    const requestId = randomUUID();
    const client = this.languageClient;
    try {
      return await this.connectionPending.run(requestId, 120_000, "Connection timed out", async () => {
        const accepted = await client.sendRequest<boolean>(Methods.connect, {
          ownerUri,
          requestId,
          connection: details,
          type,
          purpose: type === "ObjectExplorer" ? "ObjectExplorer" : "GeneralConnection"
        });
        if (!accepted) { throw new Error("The SQL 4 CDS service rejected the connection request."); }
      });
    } catch (error) {
      void Promise.resolve().then(() => client.sendRequest(Methods.cancelConnect, { ownerUri, requestId })).catch(() => {});
      throw error;
    }
  }

  public async disconnect(ownerUri: string, type = "Default"): Promise<void> {
    await this.languageClient.sendRequest<boolean>(Methods.disconnect, { ownerUri, type });
  }

  public async createObjectExplorerSession(details: ConnectionDetails): Promise<SessionCreatedParams> {
    const key = randomUUID();
    let sessionId: string | undefined;
    let finished = false;
    let succeeded = false;
    const early = new Map<string, SessionCreatedParams>();
    const complete = (params: SessionCreatedParams): void => {
      if (!params.success || params.errorMessage) {
        this.sessionPending.reject(key, new Error(params.errorMessage || "Unable to create Object Explorer session."));
      } else { this.sessionPending.resolve(key, params); }
    };
    // Subscribe before sending: transport scheduling must not depend on the server's delay.
    const subscription = this.sessionCreated.event((params: SessionCreatedParams) => {
      if (!sessionId) { early.set(params.sessionId, params); }
      else if (params.sessionId === sessionId) { complete(params); }
    });
    try {
      const created = await this.sessionPending.run(key, 120_000, "Object Explorer connection timed out", async () => {
        const response = await this.languageClient.sendRequest<{ sessionId: string }>(Methods.createObjectExplorerSession, details);
        sessionId = response.sessionId;
        if (finished) { await this.closeObjectExplorerSession(sessionId); return; }
        const notification = early.get(sessionId);
        early.clear();
        if (notification) { complete(notification); }
      });
      succeeded = true;
      return created;
    } finally {
      finished = true;
      subscription.dispose();
      early.clear();
      if (!succeeded && sessionId) { void this.closeObjectExplorerSession(sessionId).catch(() => {}); }
    }
  }

  public async expandObjectExplorer(sessionId: string, nodePath: string): Promise<ExpandResponse> {
    const key = `${sessionId}\n${nodePath}`;
    return this.expandPending.run(key, 60_000, "Object Explorer expansion timed out", async () => {
      const accepted = await this.languageClient.sendRequest<boolean>(Methods.expandObjectExplorer, { sessionId, nodePath });
      if (!accepted) {
        throw new Error("Object Explorer expansion was rejected.");
      }
    });
  }

  public async closeObjectExplorerSession(sessionId: string): Promise<void> {
    await this.languageClient.sendRequest(Methods.closeObjectExplorerSession, { sessionId });
  }

  private registerNotifications(client: LanguageClient): void {
    client.onNotification(Methods.connectionComplete, (params: ConnectionCompleteParams) => {
      if (!params.requestId) { return; }
      if (params.errorMessage) { this.connectionPending.reject(params.requestId, new Error(params.errorMessage)); }
      else { this.connectionPending.resolve(params.requestId, params); }
    });
    client.onNotification(Methods.objectExplorerSessionCreated, (params: SessionCreatedParams) => this.sessionCreated.fire(params));
    client.onNotification(Methods.objectExplorerExpanded, (params: ExpandResponse) => {
      const key = `${params.sessionId}\n${params.nodePath}`;
      if (params.errorMessage) { this.expandPending.reject(key, new Error(params.errorMessage)); }
      else { this.expandPending.resolve(key, params); }
    });
    client.onNotification(Methods.progress, (message: string) => {
      void vscode.window.setStatusBarMessage(`SQL 4 CDS: ${message}`, 3000);
    });
    client.onNotification(Methods.confirmation, async (message: { ownerUri: string; msg: string }) => {
      const answer = await vscode.window.showWarningMessage(message.msg, { modal: true }, "Yes", "All", "No");
      await client.sendNotification(Methods.confirm, { ownerUri: message.ownerUri, result: answer ?? "No" });
    });
  }

  private registerLifecycle(client: LanguageClient): void {
    client.onDidChangeState(({ newState }) => {
      if (newState !== State.Stopped) {
        if (this.stoppedTimer) {
          clearTimeout(this.stoppedTimer);
          this.stoppedTimer = undefined;
        }
        return;
      }

      this.rejectAllPending(new Error("The SQL 4 CDS language service stopped before the operation completed."));
      if (this.stopping) { return; }

      // The language client performs a few automatic crash-restart attempts. Wait briefly
      // before presenting recovery so a successful automatic restart stays unobtrusive.
      this.stoppedTimer = setTimeout(() => {
        this.stoppedTimer = undefined;
        if (this.stopping || this.client !== client || client.state !== State.Stopped) { return; }
        this.output.appendLine("SQL 4 CDS language service stopped unexpectedly. See the service log above for details.");
        void vscode.window.showErrorMessage(
          "The SQL 4 CDS language service stopped unexpectedly.",
          "Restart Service",
          "Show Output"
        ).then(async selection => {
          if (selection === "Show Output") { this.output.show(true); }
          if (selection === "Restart Service") {
            try {
              await client.start();
              this.output.appendLine("SQL 4 CDS language service restarted successfully.");
            }
            catch (error) {
              this.output.appendLine(`Language service restart failed: ${error instanceof Error ? error.message : String(error)}`);
              this.output.show(true);
              void vscode.window.showErrorMessage("SQL 4 CDS could not restart. See the SQL 4 CDS output for details.");
            }
          }
        });
      }, 1_000);
    });
  }

  private rejectAllPending(reason: Error): void {
    this.connectionPending.rejectAll(reason);
    this.sessionPending.rejectAll(reason);
    this.expandPending.rejectAll(reason);
  }

  public dispose(): Promise<void> {
    return this.disposal ??= this.stop();
  }

  private async stop(): Promise<void> {
    this.stopping = true;
    if (this.stoppedTimer) { clearTimeout(this.stoppedTimer); }
    this.rejectAllPending(new Error("The SQL 4 CDS extension is shutting down."));
    if (this.client) { await this.client.stop(); }
    this.client = undefined;
    this.sessionCreated.dispose();
  }
}
