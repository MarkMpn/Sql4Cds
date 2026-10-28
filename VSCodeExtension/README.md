# SQL 4 CDS for Visual Studio Code

Query and modify Dataverse data using standard SQL syntax inside Visual Studio Code.

All standard table data is available to query using `SELECT` statements. Data can also be modified using `INSERT`, `UPDATE` and `DELETE` statements.

## Requirements

- Visual Studio Code 1.96 or later on Windows, macOS, or Linux.
- The [.NET 10 Runtime](https://dotnet.microsoft.com/download/dotnet/10.0) available through the `dotnet` command.
- Network access and permissions for the target Dataverse environment.
- A trusted VS Code workspace. The extension is disabled in Restricted Mode and virtual workspaces.

The language service is included in the extension, but it is framework-dependent. A .NET SDK is not required to use the extension. VS Code for the Web is not supported. Remote Development, Dev Containers, and WSL have not yet been validated for this release; in those configurations, install .NET 10 in the environment where the extension host runs.

## Install

Run **Extensions: Install from VSIX…** from the Command Palette, select `sql4cds.vsix`, and reload VS Code.

## Get started

1. Open the **SQL 4 CDS** activity-bar view.
2. Select **Add Connection** and choose an authentication method.
3. Expand the saved connection to browse Dataverse metadata, or use **New Query** from its context menu.
4. Enter a query and run it with `Ctrl+Shift+E` (`Cmd+Shift+E` on macOS). If text is selected, only the selection is executed.
5. Review the Results, Messages, and Execution Plan tabs. Select cells, rows, or columns to copy them, or right-click a grid to export the full result set as CSV, Excel, JSON, Markdown, or XML.

SQL 4 CDS uses the `.sql4cds` extension and the `sql4cds` language identifier. To use an existing `.sql` document, select the language indicator in the VS Code status bar and choose **SQL 4 CDS**. This does not change the document's filename and avoids taking over `.sql` files used by other database extensions.

## Working with results

The results will be shown at the bottom of the screen. By default, any data will be shown in a grid view. If your query produces multiple resultsets, each one will be shown
in a separate grid - scroll down to review all your results. If desired, switch the output to text to have a simple method to copy & paste all the output.

The "Messages" tab will contain any additional status messages that are produced by your query. This is also where you can find any error details, although these will also
be highlighted with red wavy underlining for the problem section of your query.

In the results grid, right-click to get options to export the results in a variety of formats. Any values which are links to other records (from Lookup or primary key
type columns) will be shown as links - click these to open the record they refer to in your web browser, or right-click them for options to copy the URL for the record or
create a `SELECT` statement to get more details.

## Connections and credentials

SQL 4 CDS supports connecting to Dataverse with:

- Microsoft Entra interactive authentication.
- Client ID and client secret for an application user.
- Username and password for supported on-premises internet-facing deployments.
- Windows integrated authentication for supported on-premises deployments.
- An advanced Dataverse connection string.

Profile names, environment URLs, user hints, and client IDs are saved in VS Code global extension state. Passwords, client secrets, and full connection strings are saved through VS Code Secret Storage and are removed when the corresponding profile is deleted. Renaming a profile or changing its settings disconnects affected query editors; reconnect them to use the updated profile. **Test Connection** performs fresh authentication even when another editor is connected.

Tokens are managed by the Dataverse client, cached in the extension's VS Code global-storage directory, and are not written to extension settings or the installation directory.

## Query safety

If you are executing a command that will change data, a confirmation prompt will appear first. You can adjust the thresholds at which these confirmations appear in the settings.
By default, all `DELETE` and `UPDATE` statements will produce a confirmation, as will any `INSERT` statement that inserts more than 1 record. If you are running a batch of
multiple statements you can accept the change for an individual statement by clicking "Yes", or accept all remaining changes from other statements in the batch as well by clicking
"Yes To All". Any `DELETE` or `UPDATE` statements without a corresponding `WHERE` clause will be blocked by default, allow this in the settings if necessary or add a `WHERE 1=1`
clause to allow a query to intentionally affect all records in the table.

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

The Visual Studio Code extension communicates with the Dataverse environment and Microsoft identity endpoints required by the selected authentication flow.

Metadata is cached in individual compressed JSON files below VS Code's per-session extension log directory, under `Metadata`. Authentication token caching uses the extension's
VS Code global-storage directory. Diagnostic output and logs remain on the machine or remote extension host where VS Code runs. Logs can include SQL text, environment metadata,
and error details; review and redact them before attaching them to an issue. Passwords, client secrets, connection strings, and access tokens should never be included in support
reports.

Basic usage telemetry is reported to Microsoft Application Insights. This includes the type of statement being executed (e.g. SELECT, INSERT, UPDATE or DELETE) or, if an error
occurred, the SQL error number. By default, no SQL query text, result data or connection details are included.

Enable `SQL4CDS.includeDetailedErrorTelemetry` to include SQL query text and full exception details, including stack traces, in error telemetry.
These details may contain sensitive data. This option is off by default, and changes apply to existing connections without restarting.

## Support and known limitations

Report reproducible problems in the [SQL 4 CDS issue tracker](https://github.com/MarkMpn/Sql4Cds/issues). Include the extension version, VS Code version, operating system, authentication type (never the credential), concise reproduction steps, and sanitized SQL 4 CDS output.

Known first-release limitations:

- A .NET 10 runtime must be installed separately.
- VS Code for the Web is unsupported, and remote extension-host scenarios are not yet fully validated.
- SQL 4 CDS language features apply automatically to `.sql4cds` files; `.sql` files require selecting the SQL 4 CDS language mode.
