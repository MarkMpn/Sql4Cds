# Azure Data Studio baseline source audit

Baseline: `c551e70ec37965b11a78b51a5692d81cc5ce17de`, the parent of `74ab7bdf` (the first VS Code extension commit). This distinguishes inherited service features from subsequent VS Code changes. Findings describe source support, not a live Azure Data Studio/Dataverse test or the latest upstream release.

The ADS extension is a thin adapter around `SqlOpsDataClient`. Its README explicitly credits ADS with results tables/charting, dashboards, and saved/grouped connections. These host features should count in the user-facing comparison, but should not be described as custom implementations in the ADS extension. [README](../AzureDataStudioExtension/README.md), [adapter](../AzureDataStudioExtension/src/main.ts), [manifest](../AzureDataStudioExtension/package.json).

| Baseline feature | Evidence and ownership |
| --- | --- |
| Connect to online Dataverse and on-premises CRM | README; manifest connection provider; service `ConnectionManager.CreateDataSource`. |
| Interactive Microsoft sign-in/MFA, client secret, username/password, integrated authentication, raw connection strings | Manifest and service `ConnectionManager`. ADS supplies the Azure account token for interactive login; username/password and integrated modes use `OnPremiseClient`. Do not present all authentication modes as equivalent for every deployment/platform. |
| Save, name and group connections | ADS host capability explicitly advertised in README; custom extension supplies connection fields. |
| Execute SQL documents/selections and strings, modify data, cancel running queries | Service `QueryExecutionHandler` registers document/string execution and cancel handlers; engine executes SQL. |
| Messages, progress, query error diagnostics, DML confirmations | Service emits messages/diagnostics and confirmation notifications; adapter presents progress and Yes/All/No choices. Diagnostics here include execution errors; do not imply a complete live SQL validator. |
| Multiple result sets and paged result retrieval | Service emits result-set events and serves subsets; ADS provides result-grid presentation. |
| Result charts | ADS host capability explicitly advertised in README. |
| CSV, Excel, JSON, Markdown, XML exports | All five service export handlers predate the VS Code port. Excel and Markdown writers receive entity-reference URL generators. These export formats are inherited capabilities, even though VS Code adds its own UI. |
| Object Explorer | Service supports tables/columns, synthetic name/type columns, metadata tables/columns, table-valued functions, stored procedures, conditional Long Term Retention and Recycle Bin folders, expansion and refresh. ADS supplies the tree UI. |
| Select Top 1000 scripting | Service `ScriptingHandler` generates SELECT statements. Other scripting operations explicitly throw `NotImplementedException`. |
| SQL completion, hover, signature help | Service `CapabilitiesHandler` advertises all three; `AutocompleteHandler` registers them. |
| Server/database properties dashboard | Manifest contributes organization unique name, organization ID, URL, current user, server, version, and edition; admin/connection service handlers provide values. ADS renders dashboards. |
| Switch query connection/database | Service `GetDatabaseInfoHandler` lists known connections for query-tab switching and handles `ChangeDatabase`. The connection-dialog database list itself intentionally returns an empty list. |
| Estimated and actual execution plans, including FetchXML | Query execution emits real graph objects with node details and FetchXML graph-file content. ADS renders the plans. The separate `HandleQueryExecutionPlan` XML retrieval endpoint returns an empty placeholder, so do not claim complete standalone SQL Server XML-plan serialization. |
| Query engine settings | TDS endpoint preference; block UPDATE/DELETE without WHERE; bulk delete; batch size; parallelism; local time zone; bypass plugins; quoted identifiers; INSERT/UPDATE/DELETE warning thresholds; SELECT limit; retrieval limit; local date formatting. These settings already appear in the ADS manifest. |

## Verified limitations relevant to the comparison

- The current VS Code query controller sends `executionPlanOptions: undefined`, and its manifest has no plan command. Estimated/actual plan rendering and SQL-to-FetchXML inspection are genuine ADS UI gaps. The shared engine/service capability alone does not supply the missing VS Code UI. [Query controller](src/queryController.ts), [manifest](package.json).
- Current VS Code source has no chart or dashboard UI. Both are explicitly advertised ADS host features. [VS Code manifest](package.json), [result webview](src/resultWebview.ts).
- ADS connection grouping is an advertised host feature; VS Code profiles/tree should be compared separately from merely saving and naming profiles. [Profile store](src/profileStore.ts), [Object Explorer](src/objectExplorer.ts).
- Baseline connection cancellation is a stub: `ConnectionHandler.HandleCancelConnect` returns `true` without canceling work. Query cancellation is implemented separately and must not be confused with connection cancellation.
- Baseline scripting beyond SELECT is unimplemented. Do not mark INSERT/UPDATE/DELETE *script generation* as an ADS feature merely because SQL DML execution works.
- Contract definitions alone are insufficient evidence for features such as plan comparison or editable results; this audit found no corresponding implemented UI/service workflow to claim them.

## Reproduce the historical checks

Use `git show c551e70e:<path>` for the baseline versions of these files (current shared service files include subsequent port changes):

- `MarkMpn.Sql4Cds.LanguageServer/Connection/ConnectionManager.cs`
- `MarkMpn.Sql4Cds.LanguageServer/Connection/ConnectionHandler.cs`
- `MarkMpn.Sql4Cds.LanguageServer/QueryExecution/QueryExecutionHandler.cs`
- `MarkMpn.Sql4Cds.LanguageServer/ObjectExplorer/ObjectExplorerHandler.cs`
- `MarkMpn.Sql4Cds.LanguageServer/Admin/GetDatabaseInfoHandler.cs`
- `MarkMpn.Sql4Cds.LanguageServer/Autocomplete/AutocompleteHandler.cs`
- `MarkMpn.Sql4Cds.LanguageServer/Capabilities/CapabilitiesHandler.cs`
- `MarkMpn.Sql4Cds.LanguageServer/Scripting/ScriptingHandler.cs`

Baseline source was inspected directly with `git show`; no application or integration tests were run for this inventory.
