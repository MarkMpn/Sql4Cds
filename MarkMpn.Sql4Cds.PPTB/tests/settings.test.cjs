const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");
const ts = require("typescript");

function fixture(stored) {
    const elements = [];
    const saved = [];
    const document = {
        body: { appendChild() {} },
        createElement(tag) {
            const element = {
                tag, children: [], events: {},
                append(...children) { this.children.push(...children); },
                appendChild(child) { this.children.push(child); },
                addEventListener(name, handler) { this.events[name] = handler; },
                remove() {},
                showModal() {},
            };
            elements.push(element);
            return element;
        },
    };
    const module = { exports: {} };
    const filename = path.resolve(__dirname, "..", "src", "settings.ts");
    const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText;
    vm.runInNewContext(code, {
        module, exports: module.exports, document,
        window: { toolboxAPI: { settings: {
            get: async () => stored,
            set: async (key, value) => saved.push({ key, value }),
        } } },
    }, { filename });
    return { settings: module.exports, elements, saved };
}

test("detailed error telemetry is opt-in in PPTB and VS Code", async () => {
    for (const stored of [undefined, {}, { includeDetailedErrorTelemetry: "true" }]) {
        const { settings } = fixture(stored);
        assert.equal((await settings.loadSettings()).includeDetailedErrorTelemetry, false);
    }
    for (const enabled of [false, true]) {
        const { settings } = fixture({ includeDetailedErrorTelemetry: enabled });
        assert.equal((await settings.loadSettings()).includeDetailedErrorTelemetry, enabled);
    }
    const manifest = JSON.parse(fs.readFileSync(path.resolve(__dirname, "..", "..", "VSCodeExtension", "package.json"), "utf8"));
    const setting = manifest.contributes.configuration.properties["SQL4CDS.includeDetailedErrorTelemetry"];
    assert.equal(setting.type, "boolean");
    assert.equal(setting.default, false);
    assert.equal(setting.scope, "application");
    assert.match(setting.description, /SQL query text.*exception details.*sensitive data/);
});

test("settings dialog exposes, saves and resets detailed error telemetry", async () => {
    const { settings, elements, saved } = fixture();
    const result = settings.showSettingsDialog({ ...settings.defaultSettings });
    const label = elements.find(element => element.className === "setting-label" &&
        element.textContent === "Include detailed error telemetry");
    assert.ok(label);
    const row = elements.find(element => element.children.includes(label));
    const checkbox = row.children.find(element => element.tag === "input");
    assert.equal(checkbox.type, "checkbox");
    assert.equal(checkbox.checked, false);
    assert.match(label.children[0].textContent, /SQL query text.*exception details.*sensitive data/);
    checkbox.checked = true;
    elements.find(element => element.textContent === "Reset to defaults").events.click();
    assert.equal(checkbox.checked, false);
    checkbox.checked = true;
    const dialog = elements.find(element => element.tag === "dialog");
    dialog.returnValue = "ok";
    dialog.events.close();
    const updated = await result;
    assert.equal(updated.includeDetailedErrorTelemetry, true);
    await settings.saveSettings(updated);
    assert.equal(saved[0].key, "settings");
    assert.equal(saved[0].value.includeDetailedErrorTelemetry, true);
});
