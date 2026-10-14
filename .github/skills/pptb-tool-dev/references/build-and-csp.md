# Build Output & CSP Configuration

## The IIFE bundling requirement (easy to get wrong)

PPTB loads a tool's `index.html` directly from `dist/` inside its own `BrowserView` — it does **not** serve it over a dev server with proper module resolution. This means the default output of `vite build` for React/Vue/Svelte (ES module `<script type="module">` chunks with `crossorigin`) will silently fail to run.

**Do not** let a framework scaffold ship its default multi-chunk ES-module output for a PPTB tool. The build must produce:
- **A single IIFE bundle** — not code-split ES modules.
- `<script>` tags **without** `type="module"` or `crossorigin` attributes.
- Script tag(s) moved to the **end of `<body>`**, so DOM elements exist before the IIFE executes.
- All dependencies bundled in — no runtime resolution of bare imports, since there's no module loader serving `file://` paths correctly.

`generator-pptb`'s Vite config already handles this (single-file plugin + HTML post-processing) for React/Vue/Svelte scaffolds — don't hand-roll a different Vite config that reintroduces ES-module output. If a tool was NOT generated with `yo pptb` and reports "works in `npm run dev` but blank/broken when loaded in PPTB," this is almost always the cause: check the actual `dist/index.html` for `type="module"` or multiple `<script src="./chunk-*.js">` tags.

The HTML sample (vanilla TS, no framework) doesn't hit this because there's no bundler splitting output — it's naturally a single script.

## Debugging with source maps

- Vite projects (generator output): `sourcemap: mode === 'development'` in `vite.config.ts`, already wired by the generator.
- Webpack projects: `devtool: argv.mode === 'development' ? 'source-map' : false`.
- No-bundler projects: nothing needed, source is already what's loaded.

Dev loop: `npm run dev-watch` (equivalent to `vite build --mode development --watch`) → PPTB Settings → toggle "Show Debug Menu" → Debug sidebar → Load Local Tool → point at the project **root** (the folder containing `package.json`, not `dist/`) → Help menu → Toggle Tool DevTools. DevTools attaches only to the tool's own webview context, not the PPTB host. There's no hot-reload — close the tool tab and click Load Tool again after every rebuild.

## CSP exceptions

PPTB's default CSP per tool is strict:
```
default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline';
img-src 'self' data: https:; font-src 'self' data:; connect-src 'self';
```
A tool that only calls Dataverse (via `dataverseAPI`, which goes through the host, not a direct fetch) needs **no** `cspExceptions` at all. Only add them for genuinely external resources: a CDN-loaded library, a third-party API the tool calls directly, external fonts, embedded iframes, or `mailto:` links.

### Declaring exceptions (in package.json)

```json
"cspExceptions": {
  "connect-src": [
    { "domain": "https://api.example.com", "exceptionReason": "Used to **fetch** live pricing data.", "optional": false }
  ],
  "script-src": [
    { "domain": "https://cdn.jsdelivr.net/npm/mermaid@10", "exceptionReason": "Loads the Mermaid diagram library." }
  ],
  "style-src": [
    { "domain": "https://cdn.jsdelivr.net/npm/mermaid@10", "exceptionReason": "Mermaid's bundled stylesheet.", "optional": true }
  ]
}
```

Supported directives: `connect-src`, `script-src`, `style-src`, `img-src`, `font-src`, `frame-src`, `media-src`, `mailto`. Each entry is either a bare domain string or `{ domain, exceptionReason?, optional? }`.

### Rules that matter for registry review

- **Never** request `"*"` or a bare `https:` — this will be flagged.
- Prefer the most specific domain possible (`https://cdn.example.com` over `https://*.example.com`).
- Always fill in `exceptionReason` (supports Markdown) — it's shown to the user in the consent dialog before they grant the exception, and unnecessary/unexplained exceptions get flagged during registry submission.
- Set `"optional": true` when the tool's core function works without the exception and it only unlocks something extra (e.g. a nicer font). This changes how the consent dialog frames the request to the user.
- Consider alternatives first: can the library be bundled instead of CDN-loaded? Can `dataverseAPI`/`powerplatformAPI` replace a direct external call?

### User experience side (context, not something you implement)

First launch after adding exceptions → consent dialog shown → user accepts/declines → decision persisted so they aren't asked again → if declined, the tool doesn't load. If exceptions changed since last install, the tool needs a reload to re-trigger the dialog. There's no in-app UI yet to revoke consent (planned) — for now it's done via `window.toolboxAPI.revokeCspConsent('tool-id')` in DevTools or by editing the settings file.
