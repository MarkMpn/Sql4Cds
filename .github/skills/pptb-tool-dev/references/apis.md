# API Reference: toolboxAPI, dataverseAPI, powerplatformAPI

All three are `window.*` globals available only when a tool is loaded inside PPTB — never in a standalone browser tab. Install `@pptb/types` for full TypeScript definitions and add `/// <reference types="@pptb/types" />` at the top of your entry file for editor autocomplete.

## toolboxAPI

### connections
- `toolboxAPI.connections.getActiveConnection()` → `Promise<Connection | null>` — primary connection.
- `toolboxAPI.connections.getSecondaryConnection()` → `Promise<Connection | null>` — only meaningful if `features.multiConnection` is set.

```ts
interface Connection {
  id: string; name: string; url: string;
  environment: 'Dev' | 'Test' | 'UAT' | 'Production';
  environmentColor?: string; category?: string; categoryColor?: string;
  createdAt: string; lastUsedAt?: string;
  enabledForPowerPlatformAPI?: boolean;
  scopesForPowerPlatformAPI?: string[];
}
```
`DataverseConnection` is deprecated in favor of `Connection` — use `Connection`.

### utils
- `showNotification({ title, body, type: 'info'|'success'|'warning'|'error', duration })` — `duration: 0` = persistent. This is the standard way to surface both success and error states — don't use `alert()`.
- `copyToClipboard(text)`
- `getCurrentTheme()` → `'light' | 'dark'`
- `executeParallel(...promises)` — convenience wrapper for `Promise.all`-style fan-out across API calls.
- `openInConnectionBrowser(url, connectionTarget?)` — opens a URL in the browser profile tied to the active connection (so the user is already authenticated); only `http(s)` URLs allowed.

`saveFile()` and `selectPath()` have **moved** from `toolboxAPI.utils` to `toolboxAPI.fileSystem` — if you see old code calling them under `utils`, that's stale.

### settings (per-tool, persisted between sessions)
- `getAll()`, `get(key)`, `set(key, value)`, `setAll(settings)`
- Use namespaced keys (`ui.pageSize`, not `pageSize`) to avoid collisions.
- Only store user preferences here — not transient UI state (current page number, selected rows). Use component/local state for that.

### fileSystem
- Read: `readText(path)`, `readBinary(path)` (returns Buffer)
- Query: `exists(path)`, `stat(path)` → `{ type, size, mtime }`, `readDirectory(path)` → `{ name, type }[]`
- Write: `writeText(path, content)` (no dialog), `createDirectory(path)` (recursive)
- Interactive (native dialogs): `saveFile(defaultPath, content, filters?)` → path or `null` if cancelled, `selectPath({ type: 'file'|'folder', title, filters, defaultPath })` → path or `null`
- **All paths must be absolute.** Relative paths fail.

### terminal (context-aware to your tool)
- `create({ name, cwd, env })`, `execute(terminalId, command)`, `setVisibility(terminalId, visible)`, `list()`, `close(terminalId)`
- Always `close()` terminals in a `finally` block.

### events
- `events.on((event, payload) => {...})` — register **once** during init, route on `payload.event` inside a single handler (don't register multiple handlers for the same events).
- Key event names: `tool:loaded`/`tool:unloaded`, `connection:created`/`updated`/`deleted`, `settings:updated`, `notification:shown`, `terminal:created`/`closed`/`output`/`command:completed`/`error`.
- Wrap handler logic in try/catch — an uncaught error in a handler shouldn't take down the tool.

### invocation
See `invocation.md` for the full contract (`launchTool`, `getLaunchContext`, `returnData`, `findToolsByCapability`).

### getToolContext()
```ts
interface ToolContext {
  toolId: string | null; instanceId?: string | null;
  connectionUrl: string | null; connectionId?: string | null;
  secondaryConnectionUrl?: string | null; secondaryConnectionId?: string | null;
}
```
Deliberately has **no access tokens** — always go through `dataverseAPI`/`powerplatformAPI` for authenticated calls, never hand-roll a fetch with a bearer token.

## dataverseAPI

Every method takes an optional trailing `connectionTarget: 'primary' | 'secondary'` (defaults to `'primary'`).

**CRUD**: `create(entity, record, target?)`, `retrieve(entity, id, columns?, target?)`, `update(entity, id, record, target?)`, `delete(entity, id, target?)`, `createMultiple(entity, records[], target?)`, `updateMultiple(entity, records[], target?)`

**Relationships**: `associate(primaryEntity, primaryId, relationshipName, relatedEntity, relatedId, target?)`, `disassociate(...)`

**Queries**: `fetchXmlQuery(fetchXml, target?)` → `{ value, '@odata.context', pagingCookie? }`; `retrieveMultiple` is an alias; `queryData(odataQuery, target?)` for `$select/$filter/$orderby/$top/$expand` style queries

**Metadata**: `getEntityMetadata(logicalNameOrId, searchByLogicalName, properties?, target?)`, `getEntityRelatedMetadata(entity, relatedPath, properties?, target?)` (e.g. `'Attributes'`, `"Attributes(LogicalName='name')"`, `'OneToManyRelationships'`), `getAllEntitiesMetadata(properties?, target?)`, `getEntitySetName(logicalName)`, `getCSDLDocument(target?)` (full CSDL/EDMX XML, 1–5MB — cache if reusing)

**Schema authoring** (always call `publishCustomizations()` after): `buildLabel(text, languageCode?)`, `getAttributeODataType(type)`, `createEntityDefinition`/`updateEntityDefinition`/`deleteEntityDefinition`, `createAttribute`/`updateAttribute`/`deleteAttribute`, `createPolymorphicLookupAttribute`, `createRelationship`/`updateRelationship`/`deleteRelationship`, `createGlobalOptionSet`/`updateGlobalOptionSet`/`deleteGlobalOptionSet`, `insertOptionValue`/`updateOptionValue`/`deleteOptionValue`/`orderOption`

**Actions/functions**: `execute({ entityName?, entityId?, operationName, operationType: 'action'|'function', parameters? }, target?)` — bound ops need `entityName`+`entityId`, unbound (global) ops omit them.

**Solutions**: `getSolutions(selectColumns[], target?)`, `deploySolution(content, options?, target?)` → `{ ImportJobId }`, `getImportJobStatus(importJobId, target?)`, `publishCustomizations(tableLogicalName?, target?)`

## powerplatformAPI

Requires setup **before** use: an Entra app registration with Power Platform API permissions, the connection's Client ID populated, and "Enable for Power Platform API" checked on the connection (see `authentication/entra-app-registration` docs for the exact redirect URIs and scopes). Check `Connection.enabledForPowerPlatformAPI` / `scopesForPowerPlatformAPI` before assuming a call will succeed.

Every namespace shares the same shape: `Get(path?, target?, headers?)`, `Post(path?, body?, target?, headers?)`, `Put`, `Patch`, `Delete(path?, target?, headers?, body?)`.

Namespaces: `Analytics`, `AppManagement`, `Authorization`, `Connectivity`, `CopilotStudio`, `Dynamics`, `EnvironmentManagement`, `Governance`, `Licensing`, `PowerApps`, `PowerAutomate`, `PowerPages`, `ResourceQuery`, `UserManagement`, `WorkflowAgents`.

```ts
const response = await window.powerplatformAPI.EnvironmentManagement.Get(
  'environments?api-version=2024-10-01',
);
```

## Error handling conventions

- Always try/catch every API call; never let a rejected promise crash the tool silently.
- Show user-facing errors via `toolboxAPI.utils.showNotification` with a plain-language message; log the technical `error` (including `.status`/`.message` for Dataverse HTTP errors) to `console.error` — don't put stack traces in the notification body.
- Map common Dataverse HTTP status codes to plain language: 401 → "Authentication failed, please reconnect", 403 → "You don't have permission", 404 → "Record not found, it may have been deleted", 429 → "Too many requests, please wait", 5xx → "Server error, try again later".
- For batch operations, decide explicitly whether to fail-fast or collect a per-record success/failure report — don't silently swallow partial failures.
