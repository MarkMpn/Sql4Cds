# SQL 4 CDS for Visual Studio Code

Query and manage Microsoft Dataverse data with the SQL 4 CDS engine directly from Visual Studio Code. The extension provides saved connections, Dataverse metadata browsing, SQL editor assistance, query execution, result inspection, and export while remaining isolated from the Microsoft MSSQL language mode.

The first release combines the SQL 4 CDS query engine with a dedicated Dataverse connection explorer and results grid.

## Requirements

- Visual Studio Code 1.96 or later on Windows, macOS, or Linux.
- The [.NET 10 Runtime](https://dotnet.microsoft.com/download/dotnet/10.0) available through the `dotnet` command.
- Network access and permissions for the target Dataverse environment.
- A trusted VS Code workspace. The extension is disabled in Restricted Mode and virtual workspaces.

The language service is included in the extension, but it is framework-dependent. A .NET SDK is not required to use the extension. VS Code for the Web is not supported. Remote Development, Dev Containers, and WSL have not yet been validated for this release; in those configurations, install .NET 10 in the environment where the extension host runs.

## Install

For a release VSIX, run **Extensions: Install from VSIX…** from the Command Palette, select `sql4cds.vsix`, and reload VS Code.

## Get started

1. Open the **SQL 4 CDS** activity-bar view.
2. Select **Add Connection** and choose an authentication method.
3. Expand the saved connection to browse Dataverse metadata, or use **New Query** from its context menu.
4. Enter a query and run it with `Ctrl+Shift+E` (`Cmd+Shift+E` on macOS). If text is selected, only the selection is executed.
5. Review each result set and the Messages output. Select cells, rows, or columns to copy them in several formats, or export the full result set as CSV, Excel, JSON, Markdown, or XML.

SQL 4 CDS uses the `.sql4cds` extension and the `sql4cds` language identifier. To use an existing `.sql` document, select the language indicator in the VS Code status bar and choose **SQL 4 CDS**. This does not change the document's filename and avoids taking over `.sql` files used by other database extensions.

## Working with results

The results grid renders the current page and caches up to three pages in the browser while maintaining selection across pages. Click or drag to select cells, use the row-number gutter or column name to select a complete row or column, and use `Shift` to extend a range. `Ctrl+A` (`Cmd+A` on macOS) selects the complete filtered result view. Paging and column reordering preserve selection; changing search, filters, or sort clears it because those operations change the logical row positions.

Each column header has separate controls for dragging, selecting the column by name, cycling its sort through original/ascending/descending, opening its filter, and resizing. Quick search and column filters apply to the complete retained result view, not only the visible 200-row page.

Right-click a selection to copy it as TSV, CSV, JSON, XML, Markdown, or a SQL `IN (...)` clause. Large selections are retrieved in chunks and can be cancelled. Cells that look like JSON objects, JSON arrays, or XML have a viewer indicator; double-click one to open a prettified, read-only document beside the query.

## Connections and credentials

The first release supports:

- Microsoft Entra interactive authentication for Dataverse Online.
- Client ID and client secret for an application user.
- Username and password for supported internet-facing deployments.
- Windows integrated authentication for supported on-premises deployments.
- An advanced Dataverse connection string.

Profile names, environment URLs, user hints, and client IDs are saved in VS Code global extension state. Passwords, client secrets, and full connection strings are saved through VS Code Secret Storage and are removed when the corresponding profile is deleted. Renaming a profile or changing its settings disconnects affected query editors; reconnect them to use the updated profile. **Test Connection** performs fresh authentication even when another editor is connected.

Tokens are managed by the Dataverse client, cached in the extension's VS Code global-storage directory, and are not written to extension settings or the installation directory.

## Query safety

Queries run with the permissions of the connected Dataverse user and can modify or delete data. SQL 4 CDS enables safeguards for `UPDATE` and `DELETE` statements without a `WHERE` clause by default and can prompt before operations exceed configured thresholds. Keep these safeguards enabled unless you have a deliberate administrative use case.

Important settings are available under **Settings → Extensions → SQL 4 CDS**:

| Setting | Purpose |
| --- | --- |
| `SQL4CDS.blockDeleteWithoutWhere` | Block unfiltered `DELETE` statements. |
| `SQL4CDS.blockUpdateWithoutWhere` | Block unfiltered `UPDATE` statements. |
| `SQL4CDS.insertWarnThreshold` | Prompt before inserting more than the configured number of records. |
| `SQL4CDS.updateWarnThreshold` | Prompt before updating more than the configured number of records. |
| `SQL4CDS.deleteWarnThreshold` | Prompt before deleting more than the configured number of records. |
| `SQL4CDS.maxResultRows` | Limit rows rendered in the results view; exports are handled separately. |
| `SQL4CDS.logDebugInfo` | Enable detailed language-service diagnostic logging. |

`SQL4CDS.servicePath` is a development/recovery override. It can reference either a directory containing `MarkMpn.Sql4Cds.LanguageServer.dll` or the DLL itself.

## Troubleshooting

### The extension says .NET 10 is missing

Install the [.NET 10 Runtime](https://dotnet.microsoft.com/download/dotnet/10.0), restart VS Code, and verify this command lists `Microsoft.NETCore.App 10.x`:

```text
dotnet --list-runtimes
```

Installing only .NET 8, .NET 9, or a later major version such as .NET 11 does not normally satisfy a framework-dependent .NET 10 application.

### The language service was not found

Reinstall the VSIX or Marketplace extension first. Developers using a custom service build can set `SQL4CDS.servicePath` to its output directory or DLL. Do not point this setting at the project source directory.

### The language service stopped

The client attempts automatic recovery. If it cannot recover, choose **Restart Service** in the notification. Open **View → Output** and select **SQL 4 CDS** for details. Set `SQL4CDS.logDebugInfo` only while diagnosing an issue because detailed logs can grow and may contain environment metadata.

### Authentication or Object Explorer fails

- Confirm the environment URL uses HTTPS and opens for the same account.
- Check that the user or application user has Dataverse access and the required security roles.
- Use **Edit Connection** to replace an expired password or client secret.
- If a name is still in use with different settings, disconnect its query editors, refresh Object Explorer, and reconnect.
- Refresh the connection after authentication or metadata changes.
- For interactive authentication, complete any account or consent window opened by the Dataverse client.

### Results are truncated

The results view applies `SQL4CDS.maxResultRows` to keep the editor responsive. Reduce the query result with `TOP` or a selective `WHERE` clause, or export the result where supported.

## Privacy and diagnostics

The Visual Studio Code extension disables SQL 4 CDS engine usage and error telemetry in its language-service process and does not add analytics of its own. It communicates with the Dataverse environment and Microsoft identity endpoints required by the selected authentication flow.

The language service caches Dataverse entity and attribute metadata as compressed JSON below VS Code's per-session extension log directory, under `Metadata`. Authentication token caching uses the extension's VS Code global-storage directory. Diagnostic output and logs remain on the machine or remote extension host where VS Code runs. Logs can include SQL text, environment metadata, and error details; review and redact them before attaching them to an issue. Passwords, client secrets, connection strings, and access tokens should never be included in support reports.

## Support and known limitations

Report reproducible problems in the [SQL 4 CDS issue tracker](https://github.com/MarkMpn/Sql4Cds/issues). Include the extension version, VS Code version, operating system, authentication type (never the credential), concise reproduction steps, and sanitized SQL 4 CDS output.

Known first-release limitations:

- A .NET 10 runtime must be installed separately.
- VS Code for the Web is unsupported, and remote extension-host scenarios are not yet fully validated.
- The displayed row limit is intended for interactive use and is not a Dataverse query limit.
- SQL 4 CDS language features apply automatically to `.sql4cds` files; `.sql` files require selecting the SQL 4 CDS language mode.

## Development

Development requires Node.js 24 or later, a .NET 10 SDK or later, and the .NET 10 Runtime. From `VSCodeExtension`:

```bash
npm ci
npm run package
npm run test:connections
npm run test:service
npm run test:host
```

Packaging type-checks source and tests, runs unit and DOM tests, publishes a fresh framework-dependent Release service, bundles the extension, and verifies the VSIX contents. It never falls back to Debug output.

The host tests download VS Code 1.96.0 and use an isolated temporary profile. Set `VSCODE_TEST_VERSION=stable` to test the current stable editor. On Linux, run `xvfb-run -a npm run test:host`. The connection coordinator tests use controlled authentication completions and do not require Dataverse credentials. See [RELEASING.md](https://github.com/MarkMpn/Sql4Cds/blob/HEAD/VSCodeExtension/RELEASING.md) for the release process and live-environment checks.

SQL 4 CDS is licensed under the [MIT License](https://github.com/MarkMpn/Sql4Cds/blob/HEAD/VSCodeExtension/LICENSE). See the [changelog](https://github.com/MarkMpn/Sql4Cds/blob/HEAD/VSCodeExtension/CHANGELOG.md) for release details.
