import assert from "node:assert/strict";
import * as vscode from "vscode";
import { State } from "vscode-languageclient/node";
import { DocumentConnectionManager } from "../../src/documentConnections";
import { ObjectExplorerProvider, Sql4CdsTreeItem } from "../../src/objectExplorer";
import { ProfileStore } from "../../src/profileStore";
import type { ConnectionProfile, SessionCreatedParams } from "../../src/protocol";
import type { Sql4CdsService } from "../../src/serviceClient";

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
