import { constants } from "node:fs";
import { access } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export const requiredDotnetMajor = 10;

export function redactSensitiveText(value: string): string {
  return value
    .replace(/((?:password|pwd|client\s*secret|access\s*token|refresh\s*token|connection\s*string)\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^;\s,]+)/gi, "$1[redacted]")
    .replace(/(\bBearer\s+)[A-Za-z0-9._~+/=-]+/gi, "$1[redacted]")
    .replace(/(https?:\/\/[^/:@\s]+:)[^@/\s]+@/gi, "$1[redacted]@");
}

export class ServicePrerequisiteError extends Error {
  constructor(message: string, public readonly helpUrl?: string) {
    super(message);
    this.name = "ServicePrerequisiteError";
  }
}

export function installedDotnetRuntimeMajors(output: string): number[] {
  const majors = new Set<number>();
  for (const line of output.split(/\r?\n/)) {
    const match = /^Microsoft\.NETCore\.App\s+(\d+)\./.exec(line.trim());
    if (match) { majors.add(Number(match[1])); }
  }
  return [...majors].sort((left, right) => left - right);
}

export async function assertServicePrerequisites(
  dllPath: string,
  dependencies: {
    accessFile?: (path: string) => Promise<void>;
    listRuntimes?: () => Promise<string>;
  } = {}
): Promise<void> {
  const accessFile = dependencies.accessFile ?? (async file => access(file, constants.R_OK));
  const listRuntimes = dependencies.listRuntimes ?? listDotnetRuntimes;

  try {
    await accessFile(dllPath);
  } catch {
    throw new ServicePrerequisiteError(
      `The SQL 4 CDS language service was not found at ${dllPath}. Reinstall the extension or configure SQL4CDS.servicePath to a valid service DLL.`
    );
  }

  let output: string;
  try {
    output = await listRuntimes();
  } catch (error) {
    const detail = error instanceof Error && error.message ? ` (${error.message})` : "";
    throw new ServicePrerequisiteError(
      `.NET could not be started${detail}. Install the .NET ${requiredDotnetMajor} Runtime and restart VS Code.`,
      "https://dotnet.microsoft.com/download/dotnet/10.0"
    );
  }

  const majors = installedDotnetRuntimeMajors(output);
  if (!majors.includes(requiredDotnetMajor)) {
    const detected = majors.length > 0 ? ` Detected runtime major versions: ${majors.join(", ")}.` : " No .NET runtimes were detected.";
    throw new ServicePrerequisiteError(
      `SQL 4 CDS requires the .NET ${requiredDotnetMajor} Runtime.${detected}`,
      "https://dotnet.microsoft.com/download/dotnet/10.0"
    );
  }
}

async function listDotnetRuntimes(): Promise<string> {
  const { stdout } = await execFileAsync("dotnet", ["--list-runtimes"], {
    encoding: "utf8",
    timeout: 15_000,
    windowsHide: true
  });
  return stdout;
}
