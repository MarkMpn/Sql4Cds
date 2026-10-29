# Inter-Tool Invocation

Lets one installed PPTB tool launch another, pass it prefill data, and optionally get a result back. This is the foundation both for tool-to-tool workflows AND for agent/MCP integration (`agent-integration.md` builds on the same contract) — read this file first even if the end goal is agent integration.

## The contract: pptb.config.json

Lives at the tool package root, next to `package.json`.

```json
{
  "invocation": {
    "version": "1.0.0",
    "capabilities": ["entity-picker"],
    "prefill": {
      "properties": {
        "entityName": { "type": "string" },
        "allowMultiSelect": { "type": "boolean" }
      }
    },
    "returnTopic": {
      "properties": {
        "selectedId": { "type": "string" },
        "selectedName": { "type": "string" }
      }
    }
  }
}
```

- `invocation.version` — required whenever `invocation` is present. Bump it (semver) when you change the shape of `prefill` or `returnTopic` — PPTB does **not** enforce version compatibility at runtime, so this is purely a signal to callers; design for graceful degradation on both sides.
- `invocation.capabilities` — tags other tools can discover you by (see below). Unrecognized tags produce a validation warning, not an error.
- `prefill` / `returnTopic` — JSON-schema-style property maps. Supported `type`: `string`, `number`, `boolean`, `object`, `array`. `enum` restricts a string to fixed values, `items` describes array element type.
- This contract is **validated** by `pptb-validate` but **not enforced at runtime** — a caller can send whatever it wants, so the callee should defensively check/cast values from `getLaunchContext()` rather than trusting the schema blindly.
- Must be included in the published npm package (`"files": ["dist", "pptb.config.json", ...]` in `package.json`) or it won't be discoverable at all.

## Being a callee (accepting invocations)

```ts
async function main() {
  const ctx = await toolboxAPI.invocation.getLaunchContext();
  // ctx is Record<string, unknown> | null

  if (ctx) {
    // Invoked mode: pre-populate from ctx, show a compact/targeted UI
    const entityName = (ctx.entityName as string) ?? 'account';
    renderPicker(entityName, async (selected) => {
      await toolboxAPI.invocation.returnData({
        selectedId: selected.id,
        selectedName: selected.name,
      });
      // PPTB auto-closes this window after returnData() completes — don't close it yourself
    });
  } else {
    // Standalone mode: normal launch by the user, show the full UI
    renderFullExplorer();
  }
}
```

A well-behaved tool works correctly in **both** modes without special config — standalone launch (`getLaunchContext()` → `null`) shows the full UI; invoked launch shows a targeted/compact flow. `returnData()` is a safe no-op if the tool wasn't launched via invocation, so it's fine to call it unconditionally from a "confirm" action.

`agents` metadata from MCP callers also flows through `getLaunchContext()` as `ctx.__pptb` — see `agent-integration.md`.

## Being a caller (launching other tools)

```ts
const result = await toolboxAPI.invocation.launchTool(
  '@my-org/entity-picker',        // target tool's npm package name — must be installed
  { entityName: 'account' },      // prefill, should match callee's prefill schema
  { /* primaryConnectionId?, secondaryConnectionId?, noReturn? */ },
);
// result: Record<string, unknown> the callee returned, or null
```

- `result` is `null` if the callee closes without calling `returnData`, **or** if the user clicks "Return to [Caller]" before the callee returns data. Always handle both.
- **One callee at a time per caller** — a second `launchTool` while one is active rejects with `"A callee invocation is already in progress"`.
- Connection is **auto-inherited** from the caller unless `options.primaryConnectionId`/`secondaryConnectionId` override it. Pass `primaryConnectionId: null` to launch with no connection at all.
- If the callee needs a secondary connection and none is provided, PPTB automatically shows the multi-connection selector before launching — if the user cancels that, `launchTool` throws `"Connection selection cancelled"`.
- `options.noReturn: true` — use for fire-and-forget hand-offs (e.g. "Send To ▾" flyouts) where the caller doesn't expect data back; it just suppresses the "Return to Caller" banner in the callee.
- `launchTool` throws (doesn't just resolve null) if: the target isn't installed, a callee is already active, or connection selection was cancelled. It resolves (possibly to `null`) for every other outcome — wrap the call itself in try/catch, but treat resolution to `null` as a normal user action, not an error.

## Capability-tag discovery

Instead of hard-coding a target tool ID, discover installed tools by tag — useful for dynamic "Send To ▾" menus:

```ts
const pickers = await toolboxAPI.invocation.findToolsByCapability('entity-picker');
// [] if none installed; each entry has at least { id, name }
```

Well-known tags: `fetchxml`, `entity-picker`, `record-selector`, `solution-selector`, `odata`. The full registry is fetched via `getKnownCapabilityTags()` (cached 5 min, falls back to a built-in list offline) and can grow without an app release — if a needed tag doesn't exist yet, it needs to be requested from the PPTB team, not invented locally (an unrecognized tag still works for `findToolsByCapability`, but won't show up in the shared registry for other tools to discover).

For IDE autocomplete on known tags: `import type { CapabilityTag } from "@pptb/types/pptbConfig"`.

## Troubleshooting

- **"Tool not found"** — target not installed, or `targetToolId` doesn't exactly match the target's `package.json` `name`.
- **`getLaunchContext()` returns `null` unexpectedly** — the tool was opened by the user directly, not via `launchTool`; or the caller used a different launch path.
- **Changes to `pptb.config.json` not picked up** — capabilities/contract are read at **install** time. For a locally-loaded dev tool, reload/reinstall after editing.
- **`findToolsByCapability` returns empty** — target tool's `pptb.config.json` doesn't have the tag, or wasn't reinstalled after adding it.
