import assert from "node:assert/strict";
import { createRequire } from "node:module";
import test from "node:test";
import vm from "node:vm";
import { buildSync } from "esbuild";

const script = buildSync({
  entryPoints: ["src/serviceClient.ts"], bundle: true, write: false, platform: "node",
  format: "cjs", external: ["vscode", "vscode-languageclient/node"]
}).outputFiles[0].text;

function fixture(failure?: Error) {
  const calls: string[] = [];
  const requests: { method: string; params: unknown }[] = [];
  const notifications = new Map<string, (params: { type: number; message: string }) => void>();
  const visible: { type: string; message: string }[] = [];
  const logged: { type: string; message: string }[] = [];
  let initialized = false;
  const nodeRequire = createRequire(__filename);
  class LanguageClient {
    outputChannel = { appendLine: (message: string) => logged.push({ type: "log", message }) };
    onNotification(method: string, handler: (params: { type: number; message: string }) => void): void {
      notifications.set(method, handler);
    }
    error(message: string): void { logged.push({ type: "error", message }); }
    warn(message: string): void { logged.push({ type: "warning", message }); }
    info(message: string): void { logged.push({ type: "info", message }); }
    async start(): Promise<void> {
      initialized = false;
      calls.push("initialize");
      await Promise.resolve();
      initialized = true;
      calls.push("initialized");
    }
    async sendRequest(method: string, params: unknown): Promise<unknown> {
      assert.equal(initialized, true);
      calls.push(method);
      requests.push({ method, params });
      if (failure) throw failure;
      return null;
    }
  }
  const module = { exports: {} as typeof import("../src/serviceClient") };
  vm.runInNewContext(script, {
    module, exports: module.exports,
    require: (name: string) => name === "vscode" ? { window: {
      showErrorMessage: (message: string) => { visible.push({ type: "error", message }); },
      showWarningMessage: (message: string) => { visible.push({ type: "warning", message }); }
    } } : name === "vscode-languageclient/node" ? {
      LanguageClient, LogMessageNotification: { type: "window/logMessage" },
      MessageType: { Error: 1, Warning: 2, Info: 3, Log: 4 }
    } : nodeRequire(name)
  });
  const client = new module.exports.Sql4CdsLanguageClient("1.2.3", { command: "unused" }, { documentSelector: [] });
  return { client, calls, requests, notifications, visible, logged };
}

test("VS Code logs every server message and visibly notifies only warnings and errors", async () => {
  const { client, notifications, visible, logged } = fixture();
  await client.start();
  const receive = notifications.get("window/logMessage")!;
  for (const type of [1, 2, 3, 4]) receive({ type, message: `message ${type}` });
  assert.deepEqual(visible, [
    { type: "error", message: "message 1" }, { type: "warning", message: "message 2" }
  ]);
  assert.deepEqual(logged, [
    { type: "error", message: "message 1" }, { type: "warning", message: "message 2" },
    { type: "info", message: "message 3" }, { type: "log", message: "message 4" }
  ]);
});

test("VS Code requests capabilities after initialization on startup and restart", async () => {
  const { client, calls, requests } = fixture();
  await client.start();
  await client.start();
  assert.deepEqual(calls, [
    "initialize", "initialized", "capabilities/list",
    "initialize", "initialized", "capabilities/list"
  ]);
  assert.equal(requests.length, 2);
  for (const request of requests) {
    assert.equal(request.method, "capabilities/list");
    assert.equal(JSON.stringify(request.params), JSON.stringify({ hostName: "VS Code", hostVersion: "1.2.3" }));
  }
});

test("VS Code capabilities request failures propagate from startup", async () => {
  const failure = new Error("Capabilities unavailable");
  await assert.rejects(fixture(failure).client.start(), error => error === failure);
});
