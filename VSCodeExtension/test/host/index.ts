import assert from "node:assert/strict";
import * as vscode from "vscode";
import { State } from "vscode-languageclient/node";
import { DocumentConnectionManager } from "../../src/documentConnections";
import { ObjectExplorerProvider, Sql4CdsTreeItem } from "../../src/objectExplorer";
import { ProfileStore } from "../../src/profileStore";
import { QueryController } from "../../src/queryController";
import { Methods } from "../../src/protocol";
import type { ConnectionProfile, SessionCreatedParams } from "../../src/protocol";
import type { Sql4CdsService } from "../../src/serviceClient";
import { Sql4CdsLanguageClient } from "../../src/serviceClient";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}

function fixture() {
  const profile: ConnectionProfile = { id: "stable-profile-id", name: "Test profile", authenticationType: "None", url: "https://example.crm.dynamics.com", clientId: "11111111-1111-1111-1111-111111111111" };
  const state = new Map<string, unknown>([["sql4cds.connectionProfiles", [profile]]]);
  const secrets = new Map([["sql4cds.profile.stable-profile-id.clientSecret", "test-secret;with=punctuation"]]);
  const context = {
    globalState: { get: (key: string, fallback: unknown) => state.get(key) ?? fallback, update: async (key: string, value: unknown) => { state.set(key, value); } },
    secrets: { get: async (key: string) => secrets.get(key), store: async (key: string, value: string) => { secrets.set(key, value); }, delete: async (key: string) => { secrets.delete(key); } }
  } as unknown as vscode.ExtensionContext;
  return { profile, profiles: new ProfileStore(context), state };
}

export async function run(): Promise<void> {
  if (process.env.SQL4CDS_TEST_PATH) { process.env.PATH = process.env.SQL4CDS_TEST_PATH; }
  let timer: NodeJS.Timeout | undefined;
  try {
    await Promise.race([runSuite(), new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("Extension host tests timed out")), 90_000); })]);
  } finally { clearTimeout(timer); }
}

async function runSuite(): Promise<void> {
  const extension = vscode.extensions.getExtension("markcarrington.vscode-sql4cds");
  assert.ok(extension, "The development extension must be installed");
  await extension.activate();
  assert.equal(extension.isActive, true);
  const commands = new Set(await vscode.commands.getCommands(true));
  for (const item of extension.packageJSON.contributes.commands) { assert.ok(commands.has(item.command), `Missing command ${item.command}`); }
  console.log("PASS: real extension activates and registers every advertised command");

  const identityClient = new Sql4CdsLanguageClient(extension.packageJSON.version,
    { command: "unused-test-server" }, { documentSelector: [] });
  try {
    const params = { capabilities: {}, clientInfo: { name: "Editor host", version: "editor-version" } };
    Reflect.get(identityClient, "fillInitializeParams").call(identityClient, params);
    assert.deepEqual(params.clientInfo, { name: "VS Code", version: extension.packageJSON.version });
  } finally { await identityClient.dispose(); }
  console.log("PASS: initialize advertises VS Code and the packaged extension version rather than the editor version");

  const { profile, profiles, state } = fixture();
  const details = await profiles.toConnectionDetails(profile);
  assert.equal(details.options.connectionId, profile.id);
  assert.equal(details.options.clientsecret, "test-secret;with=punctuation");
  assert.ok(!JSON.stringify([...state]).includes("test-secret"));
  console.log("PASS: profile identity is separate from display name and credentials stay out of metadata");

  const stopped = new vscode.EventEmitter<{ newState: State }>();
  const completion = deferred<any>();
  const started = deferred<void>();
  const disconnected: string[] = [];
  const fake = {
    languageClient: { onDidChangeState: stopped.event },
    connect: async () => { started.resolve(); return completion.promise; },
    disconnect: async (uri: string) => { disconnected.push(uri); }
  } as unknown as Sql4CdsService;
  const connections = new DocumentConnectionManager(fake, profiles);
  const document = await vscode.workspace.openTextDocument({ language: "sql4cds" });
  const editor = await vscode.window.showTextDocument(document);
  const connecting = connections.connectEditor(profile, editor);
  await started.promise;
  await connections.disconnectEditor();
  completion.resolve({});
  assert.equal(await connecting, false);
  assert.equal(connections.get(document.uri.toString()), undefined);
  assert.ok(disconnected.includes(document.uri.toString()));
  connections.dispose();
  console.log("PASS: disconnect during authentication prevents a late connection from being attached");

  const queryDocument = await vscode.workspace.openTextDocument({ language: "sql4cds", content: "SELECT 1;\n    SELECT missing\nFROM account;" });
  const queryEditor = await vscode.window.showTextDocument(queryDocument);
  const executionCases = [
    { selection: new vscode.Selection(0, 0, 0, 0), expected: undefined },
    { selection: new vscode.Selection(2, 13, 1, 4), expected: { startLine: 1, startColumn: 4, endLine: 2, endColumn: 13 } }
  ];
  for (const executionCase of executionCases) {
    queryEditor.selection = executionCase.selection;
    const requests: Array<{ method: string; params: any }> = [];
    const queryService = {
      languageClient: {
        onDidChangeState: stopped.event,
        onNotification: () => new vscode.Disposable(() => {}),
        sendRequest: async (method: string, params: any) => { requests.push({ method, params }); return {}; }
      }
    } as unknown as Sql4CdsService;
    const queryConnections = { get: () => profile } as unknown as DocumentConnectionManager;
    const controller = new QueryController(queryService, queryConnections);
    try {
      await controller.execute();
      assert.equal(requests.length, 1);
      assert.equal(requests[0].method, Methods.executeDocumentSelection);
      assert.equal(requests[0].params.ownerUri, queryDocument.uri.toString());
      assert.deepEqual(requests[0].params.querySelection, executionCase.expected);
      assert.equal(requests[0].params.query, undefined);
    } finally { controller.dispose(); }
  }
  console.log("PASS: query execution preserves document offsets for selections and whole documents");

  const resultNotifications = new Map<string, (params: any) => void>();
  const resultRequests: Array<{ method: string; params: any }> = [];
  const publishedStates: any[] = [];
  const responses = new Map<number, ReturnType<typeof deferred<any>>>();
  let receiveResultMessage!: (message: any) => void;
  const resultService = {
    languageClient: {
      onDidChangeState: stopped.event,
      onNotification: (method: string, handler: (params: any) => void) => {
        resultNotifications.set(method, handler);
        return new vscode.Disposable(() => {});
      },
      sendRequest: async (method: string, params: any) => {
        resultRequests.push({ method, params });
        if (method === Methods.subset) {
          return { resultSubset: { rowCount: 1, rows: [[{ isNull: false, invariantCultureDisplayValue: "42" }]] } };
        }
        return {};
      }
    }
  } as unknown as Sql4CdsService;
  const resultController = new QueryController(resultService, { get: () => profile } as unknown as DocumentConnectionManager,
    extension.extensionUri);
  const webview = {
    options: {}, html: "", cspSource: "https://webview.invalid",
    asWebviewUri: (uri: vscode.Uri) => uri,
    onDidReceiveMessage: (handler: (message: any) => void) => {
      receiveResultMessage = handler;
      return new vscode.Disposable(() => {});
    },
    postMessage: async (message: any) => {
      if (message.type === "state") { publishedStates.push(message); }
      if (message.type === "response") { responses.get(message.requestId)?.resolve(message); }
      return true;
    }
  };
  resultController.resolveWebviewView({
    webview, show: () => {}, onDidDispose: () => new vscode.Disposable(() => {})
  } as unknown as vscode.WebviewView);
  let requestId = 0;
  const resultAction = async (message: object) => {
    const id = ++requestId;
    const response = deferred<any>();
    responses.set(id, response);
    receiveResultMessage({ requestId: id, ...message });
    return response.promise;
  };
  try {
    await resultController.execute(true);
    assert.deepEqual(resultRequests.at(-1)?.params.executionPlanOptions,
      { includeEstimatedExecutionPlanXml: true, includeActualExecutionPlanXml: false });
    const ownerUri = queryDocument.uri.toString();
    const plan = { root: { id: 1, type: "scan", nodeType: "FetchXmlScan", name: "Scan", cost: 1 } };
    resultNotifications.get(Methods.resultSetAvailable)!({
      ownerUri, resultSetSummary: { id: 0, batchId: 0, rowCount: 1, complete: true, columnInfo: [],
        specialAction: { expectYukonXMLShowPlan: true } }
    });
    resultNotifications.get(Methods.resultSetUpdated)!({
      ownerUri, resultSetSummary: { id: 0, batchId: 0, rowCount: 1, complete: true, columnInfo: [] }, executionPlans: [plan]
    });
    resultNotifications.get(Methods.queryComplete)!({ ownerUri, batchSummaries: [] });
    assert.equal(publishedStates.at(-1).results.length, 0);
    assert.deepEqual(publishedStates.at(-1).plans[0].graphs, [plan]);
    assert.equal(publishedStates.at(-1).estimatedPlan, true);

    resultController.toggleActualPlan();
    await resultController.execute();
    assert.deepEqual(resultRequests.at(-1)?.params.executionPlanOptions,
      { includeEstimatedExecutionPlanXml: false, includeActualExecutionPlanXml: true });
    const current = publishedStates.at(-1);
    assert.equal(current.plans.length, 0);
    resultNotifications.get(Methods.resultSetAvailable)!({
      ownerUri, resultSetSummary: { id: 2, batchId: 3, rowCount: 301, complete: false, columnInfo: [{ columnName: "value" }] }
    });
    const pendingExport = await resultAction({ type: "export", ownerUri, runId: current.runId, key: "3:2", format: "Csv" });
    assert.match(pendingExport.error, /finish before exporting/);
    const rows = await resultAction({ type: "rows", ownerUri, runId: current.runId, key: "3:2", start: 200, count: 200 });
    assert.equal(rows.value[0][0].displayValue, "42");
    assert.deepEqual(resultRequests.at(-1), {
      method: Methods.subset, params: { ownerUri, batchIndex: 3, resultSetIndex: 2, rowsStartIndex: 200, rowsCount: 101 }
    });
    const stale = await resultAction({ type: "rows", ownerUri, runId: current.runId - 1, key: "3:2", start: 0, count: 200 });
    assert.match(stale.error, /no longer available/);
    const invalid = await resultAction({ type: "rows", ownerUri, runId: current.runId, key: "3:2", start: -1, count: 201 });
    assert.match(invalid.error, /Invalid result row range/);
    assert.ok(webview.html.includes("results.js") && webview.html.includes("results.css"));
    console.log("PASS: results bridge wires plans, excludes plan grids, bounds rows, rejects stale requests and reports errors");
  } finally { resultController.dispose(); }

  let creates = 0;
  let expands = 0;
  const session = deferred<SessionCreatedParams>();
  const sessionStarted = deferred<void>();
  const closed: string[] = [];
  const explorerService = {
    languageClient: { onDidChangeState: stopped.event },
    createObjectExplorerSession: async () => { creates++; sessionStarted.resolve(); return session.promise; },
    expandObjectExplorer: async () => { expands++; return { nodes: [] }; },
    closeObjectExplorerSession: async (id: string) => { closed.push(id); }
  } as unknown as Sql4CdsService;
  const explorer = new ObjectExplorerProvider(explorerService, profiles);
  const item = new Sql4CdsTreeItem(profile);
  const first = explorer.getChildren(item);
  const second = explorer.getChildren(item);
  await sessionStarted.promise;
  explorer.refresh();
  session.resolve({ sessionId: "obsolete", success: true, rootNode: { label: "Root", nodePath: "objectexplorer://obsolete", nodeType: "Server", isLeaf: false } });
  await Promise.all([first, second]);
  assert.equal(creates, 1);
  assert.equal(expands, 0);
  assert.deepEqual(closed, ["obsolete"]);
  explorer.dispose();
  stopped.dispose();
  profiles.dispose();
  console.log("PASS: concurrent expansion shares authentication and refresh closes a late session");
}
