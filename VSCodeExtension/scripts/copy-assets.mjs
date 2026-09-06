import { readFile, readdir, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const output = join(root, "out", "sql4cdstoolsservice");
await rm(output, { recursive: true, force: true });
const published = spawnSync("dotnet", [
  "publish", join(dirname(root), "MarkMpn.Sql4Cds.LanguageServer", "MarkMpn.Sql4Cds.LanguageServer.csproj"),
  "--configuration", "Release", "--no-self-contained", "-p:UseAppHost=false", "--output", output
], { stdio: "inherit" });
if (published.error) { throw published.error; }
if (published.status !== 0) { throw new Error("The Release language service could not be published. Install the .NET 8 SDK or later and retry."); }

// Bundling removes node_modules, so carry the runtime packages' license texts into dist.
const lock = JSON.parse(await readFile(join(root, "package-lock.json"), "utf8"));
const notices = ["Third-party notices for the bundled JavaScript dependencies\n"];
for (const [relative, info] of Object.entries(lock.packages)) {
  if (!relative || info.dev) { continue; }
  const directory = join(root, relative);
  const license = (await readdir(directory)).find(name => /^(?:licen[cs]e|copying)(?:\..*)?$/i.test(name));
  if (!license) { throw new Error(`Missing runtime dependency license: ${relative}`); }
  const dependency = JSON.parse(await readFile(join(directory, "package.json"), "utf8"));
  notices.push(`${dependency.name} ${dependency.version}\n${await readFile(join(directory, license), "utf8")}`);
}
await writeFile(join(root, "dist", "THIRD_PARTY_NOTICES.txt"), notices.join("\n\n---\n\n"));
