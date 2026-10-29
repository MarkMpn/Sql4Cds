# Agent / MCP Integration

Exposes a PPTB tool to AI assistants through PPTB's built-in MCP server. Builds directly on the `invocation` contract in `invocation.md` — read that first. Here, the "caller" is an external assistant instead of another PPTB tool, but the `prefill`/`returnTopic` shapes are shared.

**Before using this**: confirm the person actually wants agent/assistant discoverability, not just tool-to-tool invocation — this is a separate opt-in (`agents.invokable`), and not every tool that supports invocation should be agent-invokable.

## Declaring agent metadata (pptb.config.json)

```json
{
  "invocation": {
    "version": "1.0.0",
    "capabilities": ["fetchxml-builder"],
    "prefill": { "properties": { "entityName": { "type": "string" } } },
    "returnTopic": { "properties": { "fetchXml": { "type": "string" } } }
  },
  "agents": {
    "version": "1.0.0",
    "invokable": true,
    "modes": ["one-way", "two-way"],
    "defaultMode": "two-way",
    "timeoutMS": 12000,
    "headless": true,
    "executionModes": ["windowed", "headless"],
    "defaultExecutionMode": "headless",
    "headlessEntry": "dist/headless.js"
  }
}
```

| Field | Meaning |
|---|---|
| `agents.invokable` | Gates whether the tool appears in MCP discovery at all. |
| `agents.modes` | `one-way` (fire-and-forget) and/or `two-way` (result-returning) calls supported. |
| `agents.defaultMode` | Fallback when the caller doesn't specify. |
| `agents.timeoutMS` | Hint for how long a two-way call may take. |
| `agents.headless` | Whether automated (no-UI) execution is supported at all. |
| `agents.executionModes` | Which of `windowed` / `headless` are supported. |
| `agents.defaultExecutionMode` | Fallback execution mode. |
| `agents.headlessEntry` | Path to the compiled headless runtime entry. |

**Terminology note**: the docs use "interactive tool run" (= `executionMode: "windowed"`, PPTB opens the tool's normal UI) and "automated tool run" (= `executionMode: "headless"`, runs without opening the UI) in prose, but the actual field values are the literal strings `"windowed"`/`"headless"` — use those exact strings in code and config, not the prose synonyms.

## Windowed (interactive) runs

No extra runtime code needed — this reuses the invocation callee pattern from `invocation.md`. When launched via MCP, `getLaunchContext()` returns the prefill data plus a `__pptb` metadata object:

```json
{ "entityName": "account", "__pptb": { "source": "mcp", "mode": "two-way", "correlationId": "mcp-...", "timeoutMs": 60000, "expectsResponse": true } }
```

Treat `ctx.__pptb` as optional metadata — a tool that's only ever launched by other PPTB tools (not agents) will never see it, so don't require its presence.

## Headless (automated) runs

Requires an exported `invokeHeadless(input, context)` from a discoverable entry file. PPTB looks in this order: `agents.headlessEntry` → `dist/headless.js` → `headless.js` → `package.json.main`.

```js
/// <reference types="@pptb/types" />

async function invokeHeadless(input, context) {
  const { toolId, toolName, invocationMode, authToken, updateProgress, logger } = context;

  logger.info(`Starting headless run for ${toolName} (${toolId}) in mode ${invocationMode}`);
  updateProgress(10, 'validating input');

  const entityName = typeof input.entityName === 'string' && input.entityName.trim() !== ''
    ? input.entityName.trim() : 'account';

  updateProgress(80, 'building FetchXML');
  const fetchXml = `<fetch top="10"><entity name="${entityName}">...</entity></fetch>`;
  updateProgress(100, 'done');

  return { fetchXml }; // must match invocation.returnTopic for two-way calls
}

module.exports = { invokeHeadless };
```

- `context` gives you `toolId`, `toolName`, `invocationMode`, an optional `authToken` (from the caller, may be absent — handle both), `updateProgress(pct, message)`, and a `logger`.
- For two-way calls, the return value must match `invocation.returnTopic`.
- Keep secrets out of both logs and the returned payload.

## Designing for both run styles

If a tool supports both windowed and headless execution:
- Accept the **same** core input shape in the UI path and `invokeHeadless`.
- Return the **same** result shape regardless of how it ran.
- Push business logic into shared functions called by both the UI event handlers and `invokeHeadless` — don't duplicate logic per path.
- Treat headless runs as task-oriented operations, not "hidden UI automation" — no simulated clicks, just direct computation/API calls.

## `arguments.__pptb` (assistant → tool metadata)

Assistants can pass PPTB-specific hints alongside the declared `prefill` fields:

```json
{ "entityName": "account", "__pptb": { "mode": "two-way", "executionMode": "headless", "timeoutMs": 60000, "authToken": "optional-caller-token", "connectionName": "optional-saved-connection" } }
```

## Validation and testing

1. Validate `pptb.config.json` locally (`invocation.md`'s validator also checks `agents.*`).
2. Confirm the tool only appears in MCP discovery when `agents.invokable: true`.
3. Test both `one-way` and `two-way` calls if both are advertised.
4. Confirm the headless runtime's return payload actually matches `returnTopic`.
5. MCP Inspector is the recommended manual test harness for discovery + tool calls.

## A caveat worth surfacing to whoever's asking

This documented headless-execution model (`invokeHeadless`, `executionModes`, `headlessEntry` discovery order) is fairly complete and specific — more so than "planned, not yet built." Before assuming it's live in whatever PPTB desktop-app version the person is running, it's worth a quick sanity check (changelog, or just trying MCP Inspector against a real headless-declared tool) rather than building against it blind.
