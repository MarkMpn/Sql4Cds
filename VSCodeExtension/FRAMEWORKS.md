# VS Code extension framework audit

Checked 2026-09-06. Scope: the VS Code extension, its packaging workflow, and the service it launches.

| Component | Version / decision |
| --- | --- |
| .NET service and connection tests | Move from 8 to 10 LTS. .NET 8 support ends November 10, 2026; .NET 10 is supported through November 2028. |
| Service framework package | Microsoft.Extensions.DependencyInjection 8.0.0 retained in the shared project; package lifecycle is separate from the .NET 10 runtime upgrade. |
| Build Node.js | Move CI and developer `.nvmrc` from Node 22 to 24 LTS, supported through April 2028. Node 22 remains supported through April 2027. |
| VS Code API and minimum host | Keep 1.96.0 compatibility; CI also tests current stable. The host supplies Electron and Node; these are not bundled or upgraded by this extension. |
| Node API types | Keep 20.19.43 to match the older minimum extension host, independently of build Node 24. |
| TypeScript | Locked 5.9.3 (declared ^5.7.2); registry latest 7.0.2. Compiler migration deferred; current type checks pass. |
| Language client | 9.0.1; registry latest 10.1.1. Retained to preserve minimum-host compatibility. |
| esbuild | 0.25.12; registry latest 0.28.2. Build tooling update deferred. |
| jsdom / types | 26.1.0 / 21.1.7; registry latest 30.0.1 / 30.0.0. Test-only major migration deferred. |
| Extension packaging / host tests | vsce 3.9.2 / test-electron 3.1.0; current according to npm outdated. |
| Webview | Plain HTML/CSS/JavaScript, no React, Vue, Angular, or other UI framework. ES2022 compilation target retained. |

The npm audit (including development dependencies) reports zero known vulnerabilities. Newer package versions above are available updates, not a claim that the installed versions are unsupported.

The shared language-server project selects `net10.0` only when `Sql4CdsVSCodeBuild=true`, supplied by VS Code publishing and connection-test commands. Other consumers retain `net8.0`. Engine and Export remain unchanged; their compatible net8.0 library assemblies run inside the .NET 10 service and do not require a separate .NET 8 runtime.

Existing NuGet restore warnings include vulnerable MessagePack 2.5.187 and System.Security.Cryptography.Pkcs 6.0.1 dependencies. The runtime upgrade does not remediate these package advisories. Shared dependency upgrades require a separate review by their owners.

Sources: [.NET support policy](https://dotnet.microsoft.com/en-us/platform/support/policy), [Node release schedule](https://github.com/nodejs/Release), and the npm registry via `npm outdated` / `npm audit`. Version availability is a snapshot, not a pinning recommendation.
