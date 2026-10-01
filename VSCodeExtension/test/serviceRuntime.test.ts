import assert from "node:assert/strict";
import test from "node:test";
import {
  assertServicePrerequisites,
  installedDotnetRuntimeMajors,
  redactSensitiveText,
  ServicePrerequisiteError
} from "../src/serviceRuntime";

test("diagnostic redaction removes common credential forms", () => {
  const message = "Password=hunter2;Client Secret: abc123, Authorization: Bearer eyJ.secret.token https://user:pass@example.test";
  const redacted = redactSensitiveText(message);
  assert.equal(redacted.includes("hunter2"), false);
  assert.equal(redacted.includes("abc123"), false);
  assert.equal(redacted.includes("eyJ.secret.token"), false);
  assert.equal(redacted.includes(":pass@"), false);
  assert.match(redacted, /Password=\[redacted]/);
});

test("installedDotnetRuntimeMajors returns sorted, unique .NET runtime majors", () => {
  const output = [
    "Microsoft.AspNetCore.App 8.0.7 [/dotnet/shared/Microsoft.AspNetCore.App]",
    "Microsoft.NETCore.App 9.0.1 [/dotnet/shared/Microsoft.NETCore.App]",
    "Microsoft.NETCore.App 8.0.7 [/dotnet/shared/Microsoft.NETCore.App]",
    "Microsoft.NETCore.App 8.0.8 [/dotnet/shared/Microsoft.NETCore.App]"
  ].join("\n");
  assert.deepEqual(installedDotnetRuntimeMajors(output), [8, 9]);
});

test("prerequisite check accepts a readable service and the .NET 10 runtime", async () => {
  await assertServicePrerequisites("/extension/service.dll", {
    accessFile: async () => undefined,
    listRuntimes: async () => "Microsoft.NETCore.App 10.0.1 [/dotnet/shared/Microsoft.NETCore.App]"
  });
});

test("prerequisite check explains a missing packaged service", async () => {
  await assert.rejects(
    assertServicePrerequisites("/extension/service.dll", {
      accessFile: async () => { throw new Error("ENOENT"); },
      listRuntimes: async () => "Microsoft.NETCore.App 10.0.1 [/dotnet/shared/Microsoft.NETCore.App]"
    }),
    (error: unknown) => error instanceof ServicePrerequisiteError &&
      error.message.includes("language service was not found") &&
      error.message.includes("/extension/service.dll")
  );
});

for (const major of [8, 9, 11]) {
  test(`prerequisite check rejects .NET ${major} without .NET 10`, async () => {
    await assert.rejects(
      assertServicePrerequisites("/extension/service.dll", {
        accessFile: async () => undefined,
        listRuntimes: async () => `Microsoft.NETCore.App ${major}.0.1 [/dotnet/shared/Microsoft.NETCore.App]`
      }),
      (error: unknown) => error instanceof ServicePrerequisiteError &&
        error.message.includes("requires the .NET 10 Runtime") &&
        error.helpUrl?.includes("dotnet/10.0") === true
    );
  });
}

test("prerequisite check turns dotnet launch failures into actionable guidance", async () => {
  await assert.rejects(
    assertServicePrerequisites("/extension/service.dll", {
      accessFile: async () => undefined,
      listRuntimes: async () => { throw new Error("spawn dotnet ENOENT"); }
    }),
    (error: unknown) => error instanceof ServicePrerequisiteError &&
      error.message.includes("Install the .NET 10 Runtime")
  );
});
