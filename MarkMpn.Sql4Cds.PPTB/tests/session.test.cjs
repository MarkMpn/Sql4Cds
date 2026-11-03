const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");
const ts = require("typescript");

function load(file, stored) {
    const saved = [];
    const module = { exports: {} };
    const filename = path.resolve(__dirname, "..", "src", file);
    const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText;
    vm.runInNewContext(code, {
        module, exports: module.exports,
        document: { body: { appendChild() {} }, createElement: () => ({ append() {}, appendChild() {}, addEventListener() {} }) },
        window: { toolboxAPI: { settings: {
            get: async (key) => stored?.[key],
            set: async (key, value) => saved.push({ key, value }),
        } } },
    }, { filename });
    return { exports: module.exports, saved };
}

test("session is remembered by default, including connections", async () => {
    const { exports: settings } = load("settings.ts");
    assert.equal(settings.defaultSettings.rememberSession, true);
    assert.equal(settings.defaultSettings.rememberSessionConnections, true);

    const loaded = await load("settings.ts", { settings: { rememberSession: "false", rememberSessionConnections: 0 } })
        .exports.loadSettings();
    assert.equal(loaded.rememberSession, true);
    assert.equal(loaded.rememberSessionConnections, true);
});

test("session round-trips through PPTB tool settings", async () => {
    const state = {
        tabs: [
            { filename: "C:\\queries\\accounts.sql", query: null, dirty: false, connectionId: "conn-1" },
            { filename: null, query: "SELECT 1", dirty: true, connectionId: null },
        ],
        activeTab: 1,
    };

    const { exports: session, saved } = load("session.ts");
    await session.saveSession(state);
    assert.deepEqual(saved, [{ key: "session", value: state }]);

    const restored = await load("session.ts", { session: state }).exports.loadSession();
    assert.deepEqual(JSON.parse(JSON.stringify(restored)), state);
});

test("saving a null session clears any previous session", async () => {
    const { exports: session, saved } = load("session.ts");
    await session.saveSession(null);
    assert.deepEqual(saved, [{ key: "session", value: null }]);
    assert.equal(await session.loadSession(), null);
});

test("malformed sessions are discarded", () => {
    const { exports: session } = load("session.ts");
    for (const stored of [undefined, null, "session", 42, {}, { tabs: "none" }, { tabs: [] }, { tabs: [null, {}, { filename: null, query: null }] }])
        assert.equal(session.sanitizeSession(stored), null);
});

test("malformed tabs and indexes within a session are sanitized", () => {
    const { exports: session } = load("session.ts");
    const state = session.sanitizeSession({
        tabs: [
            "invalid",
            { filename: 42, query: "SELECT 1", dirty: "yes", connectionId: 1 },
            { filename: "C:\\queries\\accounts.sql" },
        ],
        activeTab: 99,
    });
    assert.deepEqual(JSON.parse(JSON.stringify(state)), {
        tabs: [
            { filename: null, query: "SELECT 1", dirty: false, connectionId: null },
            { filename: "C:\\queries\\accounts.sql", query: null, dirty: false, connectionId: null },
        ],
        activeTab: 1,
    });
});
