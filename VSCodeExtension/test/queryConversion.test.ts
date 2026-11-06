import assert from "node:assert/strict";
import test from "node:test";
import vm from "node:vm";
import { buildSync } from "esbuild";

const script = buildSync({
  entryPoints: ["src/queryConversion.ts"], bundle: true, write: false, platform: "node",
  format: "cjs", external: ["vscode"]
}).outputFiles[0].text;

function fixture() {
  const requests: { method: string; params: unknown }[] = [];
  const opened: { language: string; content: string }[] = [];
  const shown: unknown[] = [];
  const messages: string[] = [];
  const editor = {
    document: {
      languageId: "sql4cds", uri: { toString: () => "untitled:query" },
      getText: (selection?: unknown): string => selection ? "SELECT name FROM account" : "SELECT * FROM account"
    },
    selection: { isEmpty: true }
  };
  let connected = true;
  let connectResult = true;
  let failure: Error | undefined;
  let connectCalls = 0;
  const connections = {
    get: () => connected,
    connectEditor: async (_profile: unknown, source: unknown) => {
      assert.equal(source, editor);
      connectCalls++;
      return connectResult;
    }
  };
  const client = {
    sendRequest: async (method: string, params: unknown) => {
      requests.push({ method, params });
      if (failure) { throw failure; }
      return ["<fetch />", "<!-- Additional processing required -->\n<fetch />"];
    }
  };
  const module = { exports: {} as typeof import("../src/queryConversion") };
  vm.runInNewContext(script, {
    module, exports: module.exports,
    require: () => ({
      ProgressLocation: { Window: 10 },
      window: {
        activeTextEditor: editor,
        showInformationMessage: (message: string) => messages.push(message),
        withProgress: (_options: unknown, action: () => Promise<unknown>) => action(),
        showTextDocument: async (document: unknown, options: { preview: boolean }) => {
          assert.equal(options.preview, false);
          shown.push(document);
        }
      },
      workspace: { openTextDocument: async (options: { language: string; content: string }) => {
        opened.push(options);
        return options;
      } }
    })
  });
  return {
    run: () => module.exports.convertToFetchXml(
      client as unknown as Parameters<typeof module.exports.convertToFetchXml>[0],
      connections as unknown as Parameters<typeof module.exports.convertToFetchXml>[1]),
    editor, requests, opened, shown, messages,
    disconnect: (result = true) => { connected = false; connectResult = result; },
    fail: (error: Error) => { failure = error; },
    get connectCalls() { return connectCalls; }
  };
}

for (const selected of [false, true]) {
  test(`FetchXML conversion uses PPTB's RPC with ${selected ? "selected SQL" : "the whole document"}`, async () => {
    const f = fixture();
    f.editor.selection.isEmpty = !selected;
    await f.run();
    assert.equal(JSON.stringify(f.requests), JSON.stringify([{
      method: "sql4cds/transform",
      params: { ownerUri: "untitled:query", query: selected ? "SELECT name FROM account" : "SELECT * FROM account", target: "fetchxml" }
    }]));
    assert.deepEqual(f.opened.map(doc => doc.language), ["xml", "xml"]);
    assert.equal(f.opened[1].content, "<!-- Additional processing required -->\n<fetch />");
    assert.equal(f.shown.length, 2);
  });
}

test("FetchXML conversion prompts for a connection and respects cancellation", async () => {
  for (const result of [false, true]) {
    const f = fixture();
    f.disconnect(result);
    await f.run();
    assert.equal(f.connectCalls, 1);
    assert.equal(f.requests.length, result ? 1 : 0);
    assert.equal(f.opened.length, result ? 2 : 0);
  }
});

test("FetchXML conversion rejects errors without opening an output document", async () => {
  const f = fixture();
  f.fail(new Error("Cannot represent this query as FetchXML"));
  await assert.rejects(f.run(), /Cannot represent/);
  assert.equal(f.opened.length, 0);
});

test("FetchXML conversion reports invalid editors and empty SQL", async () => {
  for (const invalidEditor of [false, true]) {
    const f = fixture();
    if (invalidEditor) { f.editor.document.languageId = "xml"; }
    else { f.editor.document.getText = () => " \n "; }
    await f.run();
    assert.equal(f.messages.length, 1);
    assert.equal(f.requests.length, 0);
  }
});
