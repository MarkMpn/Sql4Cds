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
  let initialized = false;
  const nodeRequire = createRequire(__filename);
  class LanguageClient {
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
    require: (name: string) => name === "vscode" ? {} :
      name === "vscode-languageclient/node" ? { LanguageClient } : nodeRequire(name)
  });
  const client = new module.exports.Sql4CdsLanguageClient("1.2.3", { command: "unused" }, { documentSelector: [] });
  return { client, calls, requests };
}

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
