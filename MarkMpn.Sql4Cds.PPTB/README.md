# SQL 4 CDS for Power Platform ToolBox

Query and modify Dataverse data using standard SQL syntax inside [Power Platform ToolBox](https://www.powerplatformtoolbox.com/) (PPTB), powered by the SQL 4 CDS engine.

## Features

- Multiple query tabs, each with a Monaco SQL editor
- IntelliSense: autocomplete, hover information, signature help and live error diagnostics
- Run the whole script or just the selected text (`F5` / `Ctrl+E`), and cancel long-running queries
- Virtualized results grids that fetch rows on demand, so large result sets are not held in the browser
- Query against any of the connections assigned to the tool, including cross-instance queries using the connection name as the database name (e.g. `SELECT * FROM contoso.dbo.account`)
- Confirmation prompts before large `INSERT`/`UPDATE`/`DELETE` operations
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
- **Run** sends `query/executeDocumentSelection`. When `query/resultSetAvailable` arrives a grid is created from its columns; `query/resultSetUpdated`/`query/resultSetComplete` only update the row count. Rows are requested in pages via `query/subset` as the user scrolls, and only a small number of pages are cached.
- PPTB never exposes access tokens, so the worker can't talk to Dataverse directly. Instead the connection uses the `PPTB` authentication type, which creates a `PptbOrganizationService`. This `IOrganizationService` converts SDK requests (FetchXML queries, CRUD, metadata, `ExecuteMultiple`, etc.) into Dataverse Web API requests and sends them back to the tool as a `dataverse/executeBatch` JSON-RPC request. The tool passes them straight to `dataverseAPI.executeBatch` (or `executeTransaction`) for the connection slot, and returns the responses. The client advertises PPTB's 1 MiB message limit during LSP initialization; the Dataverse bridge then transfers both requests and responses as size-checked `dataverse/executeBatch/chunk` notifications, reassembled by correlation ID and sequence number.

## Development

PPTB does not preload a full metadata cache. Entity metadata is loaded on demand and persisted in individual cache files, separate from other hosts' full caches. The first entity access checks the metadata version stamp with a query that returns no entities. Persisted entries are read only when requested and invalidated when the metadata stamp or Dataverse organization version changes. Autocomplete loads a lightweight entity list only when needed; constructing completion insertion text does not preload metadata for every suggested table. Background entity loads are deduplicated and processed one at a time, with other loads waiting asynchronously rather than occupying worker threads. Message and recycle-bin caches also share a single background load across repeated autocomplete requests. Completion, hover, and signature-help work runs asynchronously off the RPC dispatcher so metadata and message-cache loads can receive their Dataverse response notifications while they wait.

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

- The TDS endpoint, parallel execution (`maxDegreeOfParallelism`) and the `BulkDelete` job-based delete option
- Impersonation (`EXECUTE AS`)
- Updating party-list attributes (e.g. `to`/`from` on activities)
- Some bound actions / messages that can't be mapped to the Web API automatically

Requests that include custom HTTP headers (e.g. `BypassCustomPluginExecution` or `Prefer` headers) may cause PPTB to ask for consent the first time they are used.
