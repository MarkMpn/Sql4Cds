# SQL 4 CDS for Power Platform ToolBox

Query and modify Dataverse data using standard SQL syntax inside [Power Platform ToolBox](https://www.powerplatformtoolbox.com/) (PPTB), powered by the SQL 4 CDS engine.

## Features

- Multiple query tabs, each with a Monaco SQL editor
- A resizable Object Explorer pane lists the tool's connections and lazily loads tables, columns, metadata, and programmability objects from the language server. Expand or collapse with the mouse or arrow keys; refresh the selected node with the refresh button or `F5` while the tree has focus. Use the toolbar's **Object Explorer** button to hide or show the pane.
- Double-click a table, column, table-valued function, or stored procedure to insert its name at the current query selection. Tables, functions, and procedures include their schema when available; columns insert just the column name. Use the expand arrow to browse a table's columns.
- IntelliSense: autocomplete, hover information, signature help and live error diagnostics
- Run the whole script or just the selected text (`F5` / `Ctrl+E`), and cancel long-running queries
- Virtualized results grids that fetch rows on demand, so large result sets are not held in the browser
- A single tool-wide status bar follows the active query tab: status with a success/error/warning icon, organization domain and connection security, connected user's login name (`systemuser.domainname`), database name, elapsed query time, and total rows across all result sets. Time and row totals update during execution and remain visible afterwards; copy progress and errors also appear here.
- Select a rectangle of result cells by dragging, Shift+clicking, or using Shift+navigation keys. Selection highlighting survives scrolling and page eviction. Ctrl+C (Cmd+C on macOS) copies the rectangle as tab-separated columns and newline-separated rows, fetching missing pages as needed; add Shift to include column headers. Drag beyond the grid's edges to scroll while extending the selection.
- Click a row or column header to select that entire row or column. Ctrl+A (Cmd+A on macOS), or the top-left header square, selects all result cells currently available.
- Entity-reference links in results open records in the browser profile of their source connection, including cross-instance query results
- Query against any of the connections assigned to the tool, including cross-instance queries using the connection name as the database name (e.g. `SELECT * FROM contoso.dbo.account`)
- Confirmation prompts before large `INSERT`/`UPDATE`/`DELETE` operations
- Optional asynchronous bulk delete jobs for eligible `DELETE` queries, with the job reference available through `@@IDENTITY`
- Light and dark themes following the PPTB theme

## Architecture

```
┌───────────────── PPTB tool (TypeScript) ────────────────┐        ┌──── .NET worker (sql4cds-lsp) ────┐
│ Monaco editors ─┐                                       │ JSON-  │ MarkMpn.Sql4Cds.LanguageServer    │
│ Results grids ──┼─ Sql4CdsClient ── toolboxAPI.workers ─┼─ RPC ──┤  LSP + query/* + connection/*     │
│                 │        ▲                              │ stdio  │                                   │
│                 │        │ dataverse/executeBatch       │        │  PptbOrganizationService          │
│ dataverseAPI ◀──┘────────┘ (reverse request)            │◀───────┤   (IOrganizationService → Web API)│
└─────────────────────────────────────────────────────────┘        └───────────────────────────────────┘
```

- The existing language server runs as a [PPTB .NET worker](https://docs.powerplatformtoolbox.com/tool-development/dotnet-workers) (`dotnet-tool`, declared in [pptb.config.json](./pptb.config.json)). PPTB performs the `platform/initialize` handshake; the tool then uses standard LSP messages (`initialize`, `initialized`, `workspace/didChangeConfiguration`, `textDocument/didOpen|didChange|didClose`, `textDocument/completion|hover|signatureHelp`, `textDocument/publishDiagnostics`).
- Each connection assigned to the tool is registered with `connection/connect` using a hidden document URI, so every connection is available for cross-instance queries. Each query tab waits for registration to complete before using `connection/associate` to attach its document URI without re-establishing the connection. Concurrent tabs share the same registration attempt. Tabs have independent SQL sessions; closing a tab does not disconnect the registered connection.
- Object Explorer creates an independent session with `objectexplorer/createsession` when a connection is first expanded, and waits for `objectexplorer/sessioncreated` before requesting children. `objectexplorer/expand` and `objectexplorer/refresh` results arrive through `objectexplorer/expandCompleted`. Sessions are closed with `objectexplorer/closesession` when connections are removed/replaced or the tool unloads; hiding the pane preserves the tree. Loading and RPC failures appear in the tree and as notifications, with refresh available to retry.
- Object Explorer table and column expansion uses the shared `IAttributeMetadataCache` instead of executing metadata SQL queries. Table lists await the shared background entity-list load; retention flags are cached with that list, and columns reuse per-entity metadata, including synthetic name/type columns. Re-expansion and refresh reuse the cache.
- The PPTB connection exposes its organization URI through `IServiceClient.Uri`, allowing Object Explorer to show **Long Term Retention** for online (`*.dynamics.com`) environments as it does for SDK connections.
- **Run** sends `query/executeDocumentSelection`. When `query/resultSetAvailable` arrives a grid is created from its columns; `query/resultSetUpdated`/`query/resultSetComplete` only update the row count. Rows are requested in pages via `query/subset` as the user scrolls, and only a small number of pages are cached.
- PPTB never exposes access tokens, so the worker can't talk to Dataverse directly. Instead the connection uses the `PPTB` authentication type, which creates a `PptbOrganizationService`. This `IOrganizationService` converts SDK requests (FetchXML queries, CRUD, metadata, `ExecuteMultiple`, etc.) into Dataverse Web API requests and sends them back to the tool as a `dataverse/executeBatch` JSON-RPC request. The tool passes them straight to `dataverseAPI.executeBatch` (or `executeTransaction`) for the connection slot, and returns the responses. The client advertises PPTB's 1 MiB message limit during LSP initialization; the Dataverse bridge then transfers both requests and responses as size-checked `dataverse/executeBatch/chunk` notifications, reassembled by correlation ID and sequence number.

## Development

PPTB does not preload a full metadata cache. Entity metadata is loaded on demand and persisted in individual cache files, separate from other hosts' full caches. Web API request serialization and result parsing use this same data source cache, including attribute types, date formats, and relationship navigation properties; there is no separate Web API metadata download or cache. The first metadata access checks the metadata version stamp. Persisted entries are read only when requested and invalidated when the metadata stamp prefix (before `!`) or Dataverse organization version changes; the stamp's retrieval-time suffix is ignored. Autocomplete loads a lightweight entity list only when needed and persists it on disk, including display names, descriptions, object type codes, and data provider IDs. Minimal entity lookups reuse the persisted per-entity metadata. Recycle-bin entity names are also persisted, with synchronous and non-blocking lookups sharing the same cache, including empty and unsupported results. These entries use the same version checks as full entities. `GetAllEntities`, `TryGetMinimalData`, and `TryGetRecycleBinEntities` read valid disk entries synchronously so the first call can return cached data immediately. A disk hit validates the metadata stamp synchronously on first use; a missing, stale, or unreadable entry is loaded and persisted in the background instead. Constructing completion insertion text does not preload metadata for every suggested table. Background entity loads are deduplicated and processed one at a time, with other loads waiting asynchronously rather than occupying worker threads. Message and recycle-bin caches also share a single background load across repeated autocomplete requests. Completion, hover, and signature-help work runs asynchronously off the RPC dispatcher so metadata and message-cache loads can receive their Dataverse response notifications while they wait.

Prerequisites: Node.js 20+, .NET 10 SDK, and Power Platform ToolBox with the Debug menu enabled.

```bash
npm install
npm run pack:worker        # packs the language server as a .NET tool into dotnet/feed
npm run build              # or: npm run dev-watch
npx pptb-validate --skip-url-checks
```

To load the tool locally:

1. Set the `PPTB_DOTNET_LOCAL_NUGET_FEED` environment variable to the full path of the `dotnet/feed` folder before starting PPTB, so it can install the worker package without it being published to nuget.org.
2. In PPTB, open the Debug sidebar → **Load Local Tool** and select this folder (not `dist/`).
3. After rebuilding the TypeScript, close and reopen the tool tab. After changing the language server, increase the version in both the `.csproj` and `pptb.config.json` (`packageVersion`) and run `npm run pack:worker` again – PPTB caches worker packages by version.

> **Note:** The `readmeUrl` check in `pptb-validate` will fail until this README is available on the `master` branch.

## Limitations

The following are not available when running inside PPTB:

- The TDS endpoint

Party-list updates (such as `to` and `from` on activities) retrieve the existing party-list values and include unchanged roles in the activity's combined Web API payload. This may cause plugins to be triggered differently to when the same
Retrieved activity-party collections are split into individual party-list columns by participation type. This may cause plugins to be triggered differently to when the same
`UPDATE` query is executed via the XrmToolBox version.

Requests that include custom HTTP headers (e.g. `BypassCustomPluginExecution` or `Prefer` headers) may cause PPTB to ask for consent the first time they are used.
