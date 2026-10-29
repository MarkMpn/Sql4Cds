# Manifest Reference: package.json + pptb.config.json

Two files drive PPTB's understanding of a tool. `package.json` is always required. `pptb.config.json` is only required if the tool participates in inter-tool invocation and/or agent integration (see `invocation.md` / `agent-integration.md`).

## package.json — required fields

These are enforced by `pptb-validate` and the registry review pipeline. Missing or malformed = hard error (exit code 1).

| Field | Type | Notes |
|---|---|---|
| `name` | string | Scoped, e.g. `"@myorg/my-tool"`. Lowercase, no spaces. |
| `version` | string | SemVer. |
| `displayName` | string | Shown in the PPTB UI. |
| `description` | string | 1–2 sentences. |
| `main` | string | Entry point relative to `dist/` root — usually `"index.html"`. |
| `icon` | string | SVG path relative to `dist/` root, e.g. `"icons/tool.svg"`. Use `fill="currentColor"` so it adapts to light/dark theme. **Required by `pptb-validate`'s review pipeline even though the manifest overview lists it as optional — treat it as required.** |
| `license` | string | Must be one of: `MIT`, `Apache-2.0`, `BSD-2-Clause`, `BSD-3-Clause`, `GPL-2.0`, `GPL-3.0`, `LGPL-3.0`, `ISC`, `AGPL-3.0-only`. |
| `contributors` | array | Non-empty. Each entry needs at least `{ "name": "..." }`, `url` optional. |
| `configurations` | object | See below — **this is where `repository` and `readmeUrl` live, not at the top level.** |

### configurations object

| Field | Required | Notes |
|---|---|---|
| `repository` | **Yes** | Plain string URL, e.g. `"https://github.com/org/tool"`. Shown in PPTB's help menu. (Don't confuse with the optional top-level npm-standard `repository: { type, url }` object — you can have both, but `configurations.repository` is the one `pptb-validate` checks.) |
| `readmeUrl` | **Yes** | Must be a `raw.githubusercontent.com` URL. Used to render the README inside PPTB. |
| `website` | No | HTTPS URL if present. |

⚠️ PPTB's own quickstart doc shows a minimal example that omits `contributors` and `configurations` entirely (flat `author`/`repository` object instead). That example predates or simplifies past the actual validation rules — don't copy it verbatim. Use the shape in this file, which matches what `pptb-validate` and the registry submission actually check.

## package.json — optional fields

| Field | Type | Notes |
|---|---|---|
| `homepage` | string | Public homepage. |
| `repository` | object | Standard npm `{ type, url }` shape — separate from `configurations.repository`. |
| `funding` | — | Under `configurations.funding`, must be a valid URL if present. |
| `cspExceptions` | object | See "CSP exceptions" in `build-and-csp.md`. Omit entirely if the tool only talks to Dataverse. |
| `features` | object | See below. |

### features object

| Field | Values | Notes |
|---|---|---|
| `multiConnection` | `"optional"` \| `"required"` \| `"none"` | Whether the tool needs a second Dataverse connection (see `apis.md` for `connectionTarget: 'secondary'`). |
| `minAPI` | SemVer string | Minimum PPTB API version required. Set this to the **highest** "Requires vX.Y.Z" badge among the API methods the tool actually calls — don't set it higher than necessary, since that locks out users on older-but-compatible PPTB versions. Omit if the tool only uses APIs available since v1.0.17 (the initial public surface). |

## Full example (passes pptb-validate)

```json
{
  "name": "@myorg/my-awesome-tool",
  "version": "1.0.0",
  "displayName": "My Awesome Tool",
  "description": "Manage Dataverse solutions across environments with ease.",
  "main": "index.html",
  "icon": "icons/tool.svg",
  "license": "MIT",
  "contributors": [
    { "name": "Jane Dev", "url": "https://janedev.com" },
    { "name": "John Doe" }
  ],
  "configurations": {
    "repository": "https://github.com/myorg/my-awesome-tool",
    "website": "https://docs.myorg.com/my-awesome-tool",
    "readmeUrl": "https://raw.githubusercontent.com/myorg/my-awesome-tool/main/README.md"
  },
  "features": {
    "multiConnection": "optional",
    "minAPI": "1.2.0"
  },
  "cspExceptions": {
    "connect-src": [
      {
        "domain": "https://api.example.com",
        "exceptionReason": "Used to **fetch** live configuration data for the dashboard."
      }
    ]
  },
  "keywords": ["dataverse", "power-platform", "solutions"],
  "repository": { "type": "git", "url": "https://github.com/myorg/my-awesome-tool.git" },
  "devDependencies": {
    "@pptb/types": "^1.0.0"
  }
}
```

## Icon must actually exist in dist/

`pptb-validate` and manual review both check the `icon` path resolves inside `dist/` after a build. If there's no bundler plugin copying it there:
- **Vite**: put the icon under `public/` (e.g. `public/icons/tool.svg`) — Vite copies `public/` into `dist/` automatically. Set `icon: "icons/tool.svg"` in package.json.
- **Webpack**: use `copy-webpack-plugin` to copy a `public/` (or similar) folder into `dist/`.
- **No bundler**: use `shx cp -r icon/ dist/icon/` (or similar) as an explicit build step — `shx` is cross-platform (Windows/macOS/Linux), plain `cp` is not.

## pptb.config.json — only when needed

Lives at the tool package root, next to `package.json`. Not required unless the tool is a callee/caller in inter-tool invocation, or wants agent/MCP discoverability. Full schema and examples in `invocation.md` and `agent-integration.md`. If you add it, make sure it's actually published:

```json
{
  "files": ["dist", "npm-shrinkwrap.json", "pptb.config.json"]
}
```

If `pptb.config.json` is missing from the published npm package, the tool won't be discoverable by capability tag and callers won't know its prefill/return shape — this is a common publish mistake.
