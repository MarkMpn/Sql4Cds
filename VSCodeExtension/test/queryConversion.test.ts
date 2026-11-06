import assert from "node:assert/strict";
import test from "node:test";
import vm from "node:vm";
import { buildSync } from "esbuild";
import { createRequire } from "node:module";

const script = buildSync({
  entryPoints: ["src/queryConversion.ts"], bundle: true, write: false, platform: "node",
  format: "cjs", external: ["vscode"]
}).outputFiles[0].text;

test("Power BI resolves connection-string URLs without putting credentials in the output", async () => {
  const profileScript = buildSync({
    entryPoints: ["src/profileStore.ts"], bundle: true, write: false, platform: "node",
    format: "cjs", external: ["vscode"]
  }).outputFiles[0].text;
  const module = { exports: {} as typeof import("../src/profileStore") };
  const nodeRequire = createRequire(__filename);
  vm.runInNewContext(profileScript, {
    module, exports: module.exports,
    require: (name: string) => name === "vscode"
      ? { EventEmitter: class { event = () => {}; } } : nodeRequire(name),
    URL
  });
  let connectionString = "";
  const store = new module.exports.ProfileStore({
    secrets: { get: async () => connectionString }
  } as unknown as ConstructorParameters<typeof module.exports.ProfileStore>[0]);
  const profile = { id: "profile", name: "Test", authenticationType: "ConnectionString" as const };
  for (const key of ["Url", "ServiceUri", "Service Uri", "Server"]) {
    connectionString = `AuthType=ClientSecret;${key}="https://org.crm.dynamics.com/";ClientSecret=test-secret`;
    const url = await store.getUrl(profile);
    assert.equal(url, "https://org.crm.dynamics.com");
    const output = fixture().toPowerBiM("SELECT 1", new URL(url).hostname);
    assert.ok(!output.includes("test-secret"));
  }
  connectionString = "AuthType=ClientSecret;ClientSecret=test-secret";
  await assert.rejects(store.getUrl(profile), /does not specify a Dataverse URL/);
  assert.equal(await store.getUrl({ ...profile, authenticationType: "AzureMFA", url: "https://other.crm.dynamics.com/" }),
    "https://other.crm.dynamics.com");
});

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
    get: () => connected ? { id: "profile", name: "Test", authenticationType: "AzureMFA", url: "https://org.crm.dynamics.com" } : undefined,
    connectEditor: async (_profile: unknown, source: unknown) => {
      assert.equal(source, editor);
      connectCalls++;
      connected = connectResult;
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
    URL,
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
    runPowerBi: () => module.exports.convertToPowerBi(
      connections as unknown as Parameters<typeof module.exports.convertToPowerBi>[0],
      { getUrl: async () => "https://org.crm.dynamics.com/path" } as unknown as Parameters<typeof module.exports.convertToPowerBi>[1]),
    toPowerBiM: module.exports.toPowerBiM,
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

  test("Power BI conversion escapes SQL without losing comments or M escape literals", () => {
    const { toPowerBiM } = fixture();
    assert.equal(toPowerBiM('SELECT "name", \'#(lf)\' -- comment\r\n\tFROM account', "org.crm.dynamics.com"),
      'let\n' +
      '    Source = CommonDataService.Database("org.crm.dynamics.com"),\n' +
      '    DataverseSQL = Value.NativeQuery(Source, "SELECT ""name"", \'#(#)(lf)\' -- comment#(cr)#(lf)#(tab)FROM account", null, [EnableFolding=true])\n' +
      'in\n    DataverseSQL');
  });

  for (const selected of [false, true]) {
    test(`Power BI conversion runs locally for ${selected ? "selected SQL" : "the whole document"}`, async () => {
      const f = fixture();
      f.editor.selection.isEmpty = !selected;
      await f.runPowerBi();
      assert.equal(f.requests.length, 0);
      assert.equal(f.opened.length, 1);
      assert.equal(f.opened[0].language, "powerquery");
      assert.equal(f.opened[0].content, f.toPowerBiM(
        selected ? "SELECT name FROM account" : "SELECT * FROM account", "org.crm.dynamics.com"));
      assert.equal(f.shown.length, 1);
    });
  }

  test("Power BI conversion prompts for a connection and respects cancellation", async () => {
    for (const result of [false, true]) {
      const f = fixture();
      f.disconnect(result);
      await f.runPowerBi();
      assert.equal(f.connectCalls, 1);
      assert.equal(f.requests.length, 0);
      assert.equal(f.opened.length, result ? 1 : 0);
    }
  });

  test("Power BI conversion reports invalid editors and empty SQL", async () => {
    for (const invalidEditor of [false, true]) {
      const f = fixture();
      if (invalidEditor) { f.editor.document.languageId = "xml"; }
      else { f.editor.document.getText = () => " \n "; }
      await f.runPowerBi();
      assert.equal(f.messages.length, 1);
      assert.equal(f.opened.length, 0);
    }
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
