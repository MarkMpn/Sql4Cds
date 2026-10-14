# Debugging & Publishing

## Debugging inside PPTB (full loop)

1. **Sourcemaps** — `yo pptb` scaffolds already have this; if hand-built, add `sourcemap: mode === 'development'` (Vite) or `devtool: argv.mode === 'development' ? 'source-map' : false'` (Webpack).
2. **`npm run dev-watch`** — rebuilds to `dist/` on every save (`vite build --mode development --watch`). Leave the terminal open.
3. **Enable the debug menu once** — PPTB Settings → toggle "Show Debug Menu" → Save.
4. **Load the tool** — Debug sidebar → Load Local Tool → Browse → select the project **root** (must contain `package.json`; PPTB reads the manifest from root and serves `dist/` automatically) → Load Tool.
5. **Open DevTools** — with the tool tab active, Help menu → Toggle Tool DevTools. This attaches only to the tool's own webview — you won't see PPTB host code here.
6. **Iterate** — set breakpoints in the source-mapped files (look for a `vite://` or `webpack://` virtual tree in the Sources panel, not the compiled `dist/` output directly). Use the Console panel as a live REPL against `toolboxAPI`/`dataverseAPI`. **No hot-reload** — close the tool tab and click Load Tool again after every rebuild; DevTools needs reopening too.
7. **Before shipping** — stop `dev-watch`, run `npm run build` (production), and re-test the production build in PPTB before publishing.

### Common gotchas
- Blank/broken tool in PPTB but fine in a browser dev server → almost always the IIFE bundling issue, see `build-and-csp.md`.
- Sources panel shows only minified code → confirm you're opening from the `vite://`/`webpack://` virtual folder, not raw `dist/` files, and that dev mode isn't accidentally minifying.
- Breakpoints never hit → confirm the code path actually executes (add a temporary `console.log`) and that the breakpoint is set in the source-mapped file, not compiled output.

## pptb-validate CLI

```bash
npm install --save-dev @pptb/types   # installs pptb-validate to node_modules/.bin
npx pptb-validate                    # full run, including URL reachability checks
npx pptb-validate --skip-url-checks  # fast/offline
npx pptb-validate --json             # machine-readable, for CI
npx pptb-validate path/to/package.json --skip-url-checks
```

Add a `validate` npm script (`"validate": "pptb-validate"`) as the conventional entry point. `pptb-validate` mirrors the same rules the registry's automated review runs — passing it locally significantly reduces failed intake reviews. It's a standalone snapshot of those rules; keep `@pptb/types` updated to stay in sync if the rules change upstream.

**Errors** (exit code 1, must fix): missing/invalid `name`, `version`, `displayName`, `description`, `license`, `contributors`, `configurations.repository`, `configurations.readmeUrl`.
**Warnings** (exit 0, fix where practical): absent `icon`, `configurations.website`, `configurations.funding`; malformed-but-present optional fields (bad URL, Windows-style path, backslash separators in `icon`, etc.)

### CI integration

```yaml
# .github/workflows/validate.yml
name: Validate tool package
on: [push, pull_request]
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: '20' }
      - run: npm ci
      - run: npx pptb-validate --skip-url-checks
```
Use `--skip-url-checks` in CI to avoid flaky failures from transient network issues or rate limiting.

## Publishing

1. **Prepare** — confirm `package.json` is complete (see `manifest.md`), icon exists in `dist/` at the referenced path, README exists (Markdown only — no raw HTML in the README, it's stripped for security; use full URLs for any linked images/resources).
2. **Build** — `npm run build`; verify `dist/` has `index.html`, the icon, and all compiled assets.
3. **Validate** — `npm run validate` (or `npx pptb-validate`); fix all errors, address warnings where practical.
4. **Finalize** — `npm run finalize-package` (prepares file structure/deps for npm).
5. **Publish** — `npm login` (first time), then `npm publish --access public` for scoped packages (required — scoped packages default to private without this flag).
6. **Verify** — `npm view @org/tool-name`, or check `https://www.npmjs.com/package/@org/tool-name`.
7. **Test the published version** — PPTB Debug section → "Install from npm" → enter the package name → Install → test thoroughly before registry submission.
8. **Submit to the registry** — Tool Submission Form (login required) with npm package name and up to 3 category tags (Comparisons, Data, Development, Diagrams, Documentation, Environments, Migration, Solutions, Troubleshooting, Users & Security). Automated checks confirm the package exists, has proper metadata, an appropriate license, and no known vulnerabilities. Manual review (typically 48–72h) checks security, quality, functionality, and documentation.

### Versioning

Standard SemVer: `npm version patch|minor|major` before each `npm run build && npm publish --access public`. If `invocation.prefill`/`returnTopic` shape changes, also bump `invocation.version` in `pptb.config.json` (see `invocation.md`) — PPTB doesn't enforce this, it's a signal to callers.

### Common publish failures
- `"You must be logged in to publish packages"` → `npm login`, retry.
- `"Package name too similar to existing package"` → use scoped naming (`@org/tool-name`).
- Tool not appearing in registry after submission → confirm automated validation passed, npm package is public, and all required manifest fields are present — check the submission issue for maintainer feedback.
