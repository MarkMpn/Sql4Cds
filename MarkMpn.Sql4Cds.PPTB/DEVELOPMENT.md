
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
- Execution plans are requested through the `executionPlanOptions` of `query/executeDocumentSelection`. The server returns them as special result sets flagged with `specialAction.expectYukonXMLShowPlan`, whose `query/resultSetUpdated` notifications carry the converted `executionPlans` graphs. These result sets don't create grids or count towards the row total. Node icons are copied at build time from `MarkMpn.Sql4Cds.Controls/Images`; each node's image is chosen from the SQL 4 CDS node class name the server reports in `nodeType`, using the same substitutions and fallbacks as the WinForms control.
- PPTB never exposes access tokens, so the worker can't talk to Dataverse directly. Instead the connection uses the `PPTB` authentication type, which creates a `PptbOrganizationService`. This `IOrganizationService` converts SDK requests (FetchXML queries, CRUD, metadata, `ExecuteMultiple`, etc.) into Dataverse Web API requests. The tool passes the flat request list straight to `dataverseAPI.executeBatch` for the connection slot, with transaction members grouped by `changesetId`, and returns the responses. The client advertises PPTB's 32 MiB message limit during LSP initialization. Each request and response independently uses a `dataverse/executeBatch/message` notification containing `{ id, data }`, with the payload kept as a JSON object, if the complete serialized notification fits the UTF-8 byte limit. Only oversized payloads use `dataverse/executeBatch/chunk` notifications containing `{ id, sequence, complete, data }`, reassembled by correlation ID and sequence number. Direction is implicit: JavaScript receives requests and the worker receives responses. This avoids escaping and splitting ordinary payloads and allows a small request to receive a chunked response, or vice versa. Clients without a negotiated message limit retain the legacy `dataverse/executeBatch` request/response path.
- JavaScript RPC console logs include a UTC ISO 8601 timestamp with milliseconds for each request, response, notification, chunk, and error, for example `[SQL 4 CDS RPC] [2026-10-25T17:01:06.088Z] -> query/subset request`, to help correlate activity and identify delays.

### Compact query notifications

The PPTB client opts into version 1 with
`initialize.capabilities.experimental.sql4cds.queryEvents.version = 1`, alongside
the PPTB message-size limit. The server acknowledges the version in
`capabilities.experimental.sql4cds.queryEvents.version`. Without a supported
opt-in, other clients keep the existing query notifications unchanged.

Clients can also request `queryEvents.initialRowCount` during initialization;
the server echoes the accepted count alongside the version. Omitted or zero
means no proactive rows. PPTB requests 200 rows per ordinary result set, matching
its first grid page. While those rows are being read, result updates are queued
without waiting for the usual row-count reporting interval.

In compact mode, query execution requests include an `executionId`, echoed in
`sql4cds/queryEvents` notifications shaped as `{ ownerUri, executionId, events }`.
Each event has a `type` and, where applicable, `data`. Result-set events use
`type: "resultSet"` with IDs, row count and completion state; column metadata,
special actions and visualization metadata are introduced once. Execution plans
are attached to the corresponding result event. Other event types are
`query/batchStart`, `query/batchComplete`, `query/message`, `sql4cds/progress` and
`query/complete`. Batch/query completion does not repeat result metadata.

With initial rows enabled, result events optionally include `rowsStartIndex` and
`rows` (the same formatted cell arrays returned by `query/subset`). These are
contiguous deltas, never a repeated prefix: for example, an update containing rows
0-9 may be followed by rows 10-19. Deltas share the normal batching window and byte
limits, so a slowly executing query can display partial results before completing.
Execution-plan result sets do not push rows. If one row cannot fit the negotiated
message limit, proactive delivery stops at that row; ordinary paging remains
available for it and the remaining rows.

The worker batches events over a 10 ms window, merges adjacent result-state
updates, keeps the latest pending progress, and preserves messages and plan order.
It flushes at 64 events, before exceeding a 256 KiB target or the negotiated
message limit, and immediately on query completion. A single larger event may
exceed the target but never the negotiated limit. Queued data is snapshotted.
Final row-count changes, including partially read results after errors or
cancellation, are included before batch completion.

The client reconstructs the existing UI callbacks and completion summaries,
discarding events for superseded, disposed or closed executions. Confirmations
and diagnostics remain immediate. The client caches the pushed prefix before
invoking result callbacks and serves `query/subset` calls from it where possible,
requesting only a missing suffix or later pages from the worker. The prefix
remains cached after completion for grid scrolling, copying and text output, and
is released on query disposal, editor closure, re-execution or worker shutdown.

## Development

Outgoing RPC objects omit undefined properties recursively before being passed
to PPTB's strict JSON validator, including absent bodies on HTTP 204 responses.
Undefined array elements become null to preserve their positions, matching JSON
serialization semantics. Host response objects are not modified.

If the language-server worker fails or exits unexpectedly, all open query tabs
stop executing and display an error in Messages. A tool notification and the
status bar explain that SQL 4 CDS must be closed and reopened to restart the
engine. SQL text and unsaved edits are preserved; execution, connection changes
and result export are disabled. Ordinary SQL or Dataverse query errors do not
trigger this restart-required state.

The Web API bridge preserves individual response headers and outer batch headers separately. The worker uses the batch-level `x-ms-dop-hint` header to update its recommended degree of parallelism. The bridge retries trailing 429 responses up to 10 times after the batch-level `Retry-After` delay, retrying only the uncompleted suffix and never splitting a changeset: a transaction is retried only when every operation in it is throttled. Incomplete response lists are not retried. Any final HTTP 429 response becomes an organization service fault with error code 429 and the batch-level `Retry-After` delay (in seconds) exposed as a `TimeSpan` for the engine's throttling retries.

`RetrieveMetadataChanges`, `RetrieveEntity`, and `RetrieveAllOptionSets` use the same prepared Web API request and response-mapping path as standard requests. With `ExecuteMultiple`, they can share a batch with other operations while preserving client-side metadata filters and per-request faults. Explicit transaction contents are sent to Dataverse for validation.

Transactions are used only for explicit `ExecuteTransaction` requests, including transactions containing a single write request. Each transaction is translated into a flat list of requests sharing a unique `changesetId`. Standalone transactions use the same `executeBatch` path as `ExecuteMultiple`, which can mix ordinary requests with multiple independent transactions. Ordinary operations never automatically become transactions. `ExecuteMultiple` batches requests for either `ContinueOnError` setting: when enabled, the bridge adds `Prefer: odata.continue-on-error` to the outer batch headers, not the individual requests. When disabled, the preference is omitted and a partial response ending in a fault represents a batch stopped at the first error. Transaction responses preserve child responses and `ExecuteTransactionFault.FaultedRequestIndex`. Operations that expand to multiple Web API requests, such as associations, use a non-transactional batch unless explicitly wrapped in `ExecuteTransaction`.

`ExecuteMultiple` translates all requests before sending one batch. Translation failures throw immediately without executing the batch, regardless of `ContinueOnError`. Transactions may be children of `ExecuteMultiple`; nested `ExecuteMultiple` containers and containers inside transactions are rejected. Oversized batches are sent intact rather than split, leaving size-limit validation and errors to the server. Batch-wide transport failures propagate to the caller rather than becoming per-request results. Changeset support requires the PPTB API represented by `@pptb/types` 1.2.8-beta.1 or later.

`Retrieve` uses native Web API record URLs, including alternate keys, with `$select` for requested columns and `$expand` for related entities and activity parties. Lookup columns use their `_name_value` properties, and response annotations preserve formatted values, lookup targets, and row versions. Related `QueryExpression` requests support column selection, nested AND/OR filters using comparisons and null checks, ordering, and top counts for collections. Links, distinct queries, explicit paging, and unsupported filter operators fail during translation rather than being ignored.

The language server uses the same lazy-loaded metadata cache for every client and connection type, including PPTB, VS Code, direct SDK connections, and on-premises connections. It does not preload a full metadata cache. Entity metadata is loaded on demand and persisted in individual cache files; legacy full-cache files are no longer read or updated. Query execution, autocomplete, Object Explorer, table-size estimates, and message discovery use the same data source metadata cache. Web API request serialization and result parsing also use this cache, including attribute types, date formats, and relationship navigation properties; there is no separate Web API metadata download or cache. The first metadata access checks the metadata version stamp. Persisted entries are read only when requested and invalidated when the metadata stamp prefix (before `!`) or Dataverse organization version changes; the stamp's retrieval-time suffix is ignored. Autocomplete loads a lightweight entity list only when needed and persists it on disk, including display names, descriptions, object type codes, and data provider IDs. Minimal entity lookups reuse the persisted per-entity metadata. Recycle-bin entity names are also persisted, with synchronous and non-blocking lookups sharing the same cache, including empty and unsupported results. These entries use the same version checks as full entities. `GetAllEntities`, `TryGetMinimalData`, and `TryGetRecycleBinEntities` read valid disk entries synchronously so the first call can return cached data immediately. A disk hit validates the metadata stamp synchronously on first use; a missing, stale, or unreadable entry is loaded and persisted in the background instead. Constructing completion insertion text does not preload metadata for every suggested table. Background entity loads are deduplicated and processed one at a time, with other loads waiting asynchronously rather than occupying worker threads. Message and recycle-bin caches also share a single background load across repeated autocomplete requests. Completion, hover, and signature-help work runs asynchronously off the RPC dispatcher so metadata and message-cache loads can receive their Dataverse response notifications while they wait.

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

### Client identity and release versions

The client sends `Power Platform ToolBox` and its npm package version in the
standard LSP `initialize.clientInfo` property. Vite embeds the package version at
build time. The language server uses the client name as each connection's
`ApplicationName` (with `SQL 4 CDS` as the fallback for clients without a name).

The root Azure release pipeline stamps the client version from
`GitVersion.SemVer` before building, and stamps the worker manifest and .NET
tool package with `GitVersion.NuGetVersion`. It produces a `PPTBPackages`
artifact containing the npm tarball and worker NuGet package; it does not publish
them to a registry. For manual npm releases, `npm run publish-package` increments
the version before building so the embedded identity matches the published package.
