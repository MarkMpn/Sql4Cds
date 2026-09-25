import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { dotnetHost } from "../scripts/dotnet-host.mjs";

test("explicit .NET installation ignores a competing executable on PATH", async () => {
  const root = await mkdtemp(join(tmpdir(), "sql4cds-host-"));
  const previousRoot = process.env.DOTNET_ROOT;
  const previousPath = process.env.PATH;
  try {
    const trusted = join(root, "sdk");
    const untrusted = join(root, "path");
    await mkdir(trusted);
    await mkdir(untrusted);
    const name = process.platform === "win32" ? "dotnet.exe" : "dotnet";
    await writeFile(join(trusted, name), "", { mode: 0o700 });
    await writeFile(join(untrusted, name), "", { mode: 0o700 });
    process.env.DOTNET_ROOT = trusted;
    process.env.PATH = untrusted;
    const { realpath } = await import("node:fs/promises");
    assert.equal(await dotnetHost(), await realpath(join(trusted, name)));
    process.env.DOTNET_ROOT = join(root, "missing");
    await assert.rejects(dotnetHost(), /Could not find an executable/);
    process.env.DOTNET_ROOT = "relative-sdk";
    await assert.rejects(dotnetHost(), /absolute installation directory/);
  } finally {
    if (previousRoot === undefined) { delete process.env.DOTNET_ROOT; } else { process.env.DOTNET_ROOT = previousRoot; }
    if (previousPath === undefined) { delete process.env.PATH; } else { process.env.PATH = previousPath; }
    await rm(root, { recursive: true, force: true });
  }
});
