import { access, realpath, stat } from "node:fs/promises";
import { constants } from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, join } from "node:path";

/** Build/test tooling trusts an explicitly configured installation, never PATH. */
export async function dotnetHost() {
  const configuredRoot = process.env.DOTNET_ROOT;
  const roots = configuredRoot
    ? [configuredRoot]
    : process.platform === "win32"
      ? [join(process.env.ProgramFiles || "C:\\Program Files", "dotnet")]
      : ["/usr/local/share/dotnet", "/usr/share/dotnet", join(homedir(), ".dotnet")];
  for (const root of roots) {
    if (!isAbsolute(root)) { throw new Error("DOTNET_ROOT must be an absolute installation directory."); }
    try {
      const executable = await realpath(join(root, process.platform === "win32" ? "dotnet.exe" : "dotnet"));
      if (!(await stat(executable)).isFile()) { continue; }
      await access(executable, constants.X_OK);
      return executable;
    } catch (error) {
      if (!["ENOENT", "ENOTDIR", "EACCES"].includes(error.code)) { throw error; }
    }
  }
  throw new Error("Could not find an executable .NET host. Install .NET 10 or set DOTNET_ROOT to its absolute installation directory.");
}
