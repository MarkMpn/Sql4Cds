# Releasing SQL 4 CDS for Visual Studio Code

The release candidate retains the existing version, **0.3.0**, and publisher identity,
**markcarrington.vscode-sql4cds**. Confirm that the releasing maintainer has access to
that Marketplace publisher before publishing. Do not change the publisher or extension
name after release; those fields form the extension's identity.

## Build and automated verification

Use Node.js 22+, a .NET 8 SDK or later, and the .NET 8 Runtime. From this directory:

```sh
npm ci
npm run package
npm run test:connections
npm run test:service
npm run test:host
```

On Linux, use `xvfb-run -a npm run test:host`. Set `VSCODE_TEST_VERSION=stable`
to run against current stable VS Code; the default is the minimum supported 1.96.0.
The tests use temporary editor profiles and controlled authentication completions.
They do not contact a Dataverse environment.

`npm run package` checks source and test types, runs the unit and DOM suites,
then invokes `vsce package`. Its prepublish hook performs a fresh Release
`dotnet publish` with no platform-specific apphost, and bundles the TypeScript client.
The final check reads the VSIX archive and checks required assets and excluded
source, test, build, cache, and diagnostic files. The result is `sql4cds.vsix`.

The GitHub Actions workflow tests Windows, macOS, and Linux with VS Code 1.96.0,
plus current stable VS Code on Linux. All matrix jobs must pass before merging.
The Linux minimum-version job uploads the VSIX with the commit SHA in the artifact
name. Publishing is intentionally a separate maintainer action.

## Live release acceptance

Before publishing, install the exact candidate VSIX in a fresh editor profile
and record the OS, VS Code version, authentication method, and results below in
the release PR. These checks require a disposable Dataverse environment; automated
coordinator tests are not a substitute for real authentication and query execution.

| Check | Expected result |
| --- | --- |
| Interactive sign-in and client-secret application user | Connect, expand tables and columns, and run a read-only query. |
| Any advertised on-premises authentication modes | Validate against a supported deployment on its supported OS; document limitations found. |
| Two editors connected to different environments | Each query and status indicator refer to the correct environment. |
| Rename A to B, then create a new A for another environment | A never silently reuses B's data source. |
| Cancel or close during sign-in, then retry | Late authentication cannot attach to the closed editor or replace the retry. |
| Whole-document and selected SQL; multiple result sets; invalid SQL | Results, messages, and errors correspond to the executed text. |
| Query cancellation and data-modification confirmations | Cancel completes; declining a confirmation makes no changes. |
| Search, filter, sort, page, reorder, and sparse copy | Values and selection agree across pages and formats. |
| CSV, Excel, JSON, Markdown, and XML export | Full retained result exports correctly; text opens in VS Code and Excel offers system actions. |
| Missing runtime, service restart, credential edit, and disconnect | Actionable feedback and recoverable connection state. |

Remote Development, WSL, Dev Containers, and the browser editor remain outside the
validated first-release support scope. Do not broaden the README's platform claims
without testing those configurations.

## Publish the verified artifact

1. Confirm the version is unused in the publisher's Marketplace listing and give
   the changelog entry its actual release date.
2. Merge only after CI and live acceptance pass. Build the final merged revision
   or download its verified CI artifact; use that same VSIX for publication.
3. Upload `sql4cds.vsix` through the Marketplace publisher management page, or use
   the maintained `vsce` publishing workflow with the publisher's configured
   credentials. Never commit access tokens.
4. Install the Marketplace version in a fresh profile and repeat connection,
   query, and export smoke checks. Attach the version and artifact to the release.

See the official [VS Code publishing guide](https://code.visualstudio.com/api/working-with-extensions/publishing-extension)
for current publisher authentication and artifact-upload procedures, and the
[Workspace Trust guide](https://code.visualstudio.com/api/extension-guides/workspace-trust)
for the declared Restricted Mode behavior.
