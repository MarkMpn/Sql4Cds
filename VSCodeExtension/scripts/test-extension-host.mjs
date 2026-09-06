import { build } from "esbuild";
import { runTests } from "@vscode/test-electron";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
// Electron-backed terminals may export this for their Node helpers.
delete process.env.ELECTRON_RUN_AS_NODE;
const tests = join(root, ".test-dist", "host.cjs");
await build({ entryPoints: [join(root, "test", "host", "index.ts")], bundle: true, platform: "node", format: "cjs", external: ["vscode"], outfile: tests });
const profile = await mkdtemp(join(tmpdir(), "sql4cds-"));
try {
  await runTests({
    extensionDevelopmentPath: root,
    extensionTestsPath: tests,
    extensionTestsEnv: { SQL4CDS_TEST_PATH: process.env.PATH },
    version: process.env.VSCODE_TEST_VERSION || "1.96.0",
    launchArgs: ["--disable-extensions", "--skip-welcome", "--skip-release-notes", "--disable-workspace-trust", `--user-data-dir=${join(profile, "data")}`, `--extensions-dir=${join(profile, "extensions")}`]
  });
} finally { await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 }); }
