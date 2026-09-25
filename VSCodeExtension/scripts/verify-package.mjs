import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { open } from "yauzl";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
const files = new Map();
const jsonFiles = new Map();
const checkedJson = new Set(["extension/package.json", "extension/out/sql4cdstoolsservice/MarkMpn.Sql4Cds.LanguageServer.runtimeconfig.json"]);
await new Promise((resolve, reject) => {
  open(join(root, "sql4cds.vsix"), { lazyEntries: true }, (error, zip) => {
    if (error) { reject(error); return; }
    zip.on("error", reject);
    zip.on("end", resolve);
    zip.on("entry", entry => {
      files.set(entry.fileName, entry.uncompressedSize);
      if (checkedJson.has(entry.fileName)) {
        zip.openReadStream(entry, (error, stream) => {
          if (error) { reject(error); return; }
          const chunks = [];
          stream.on("error", reject);
          stream.on("data", chunk => chunks.push(chunk));
          stream.on("end", () => {
            try { jsonFiles.set(entry.fileName, JSON.parse(Buffer.concat(chunks).toString("utf8"))); }
            catch (error) { reject(error); return; }
            zip.readEntry();
          });
        });
        return;
      }
      zip.readEntry();
    });
    zip.readEntry();
  });
});
const required = [
  "extension.vsixmanifest", "[Content_Types].xml", "extension/package.json",
  "extension/dist/THIRD_PARTY_NOTICES.txt",
  `extension/${manifest.main.replace(/^\.\//, "")}`, "extension/readme.md", "extension/changelog.md", "extension/LICENSE.txt",
  `extension/${manifest.icon}`, "extension/language-configuration.json", "extension/snippets/sql4cds.json", "extension/syntaxes/sql4cds.tmLanguage.json",
  ...["MarkMpn.Sql4Cds.LanguageServer.dll", "MarkMpn.Sql4Cds.LanguageServer.deps.json", "MarkMpn.Sql4Cds.LanguageServer.runtimeconfig.json", "MarkMpn.Sql4Cds.Engine.dll", "MarkMpn.Sql4Cds.Export.dll"].map(file => `extension/out/sql4cdstoolsservice/${file}`)
];
for (const file of required) { assert.ok(files.get(file) > 0, `Missing or empty release asset: ${file}`); }
const packagedManifest = jsonFiles.get("extension/package.json");
for (const field of ["name", "publisher", "version", "main"]) {
  assert.equal(packagedManifest[field], manifest[field], `Stale packaged manifest field: ${field}`);
}
const runtime = jsonFiles.get("extension/out/sql4cdstoolsservice/MarkMpn.Sql4Cds.LanguageServer.runtimeconfig.json").runtimeOptions;
assert.equal(runtime.tfm, "net10.0", "The service must match the documented .NET runtime requirement");
assert.equal(runtime.framework?.name, "Microsoft.NETCore.App", "The service must be framework-dependent");
for (const file of files.keys()) {
  assert.ok(!/^extension\/(?:src|test|scripts|node_modules|\.test-dist|\.vscode-test)\//.test(file), `Development file shipped: ${file}`);
  assert.ok(!/\.(?:vsix|pdb|map|log)$/i.test(file), `Unexpected generated file shipped: ${file}`);
  assert.ok(!/(?:^|\/)(?:TokenCache|Metadata)(?:\/|$)/.test(file), `Runtime cache shipped: ${file}`);
}
console.log(`Verified ${manifest.publisher}.${manifest.name} ${manifest.version}: ${files.size} packaged files and all required release assets.`);
