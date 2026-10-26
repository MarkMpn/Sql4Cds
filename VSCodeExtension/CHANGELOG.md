# Changelog

All notable changes to the SQL 4 CDS extension for Visual Studio Code are documented here.

## [0.3.0] - Unreleased

### Changed

- The VS Code language service now requires .NET 10 LTS; development and CI use Node.js 24 LTS.
- Replace the separate VS Code grid with the PPTB virtualized grid, stacked results, Messages tab, and execution-plan viewer with node properties and FetchXML inspection, adapted to VS Code themes.
- Replace results-only search/filter/sort/reordering, multi-format selection copying, and JSON/XML cell inspection with PPTB-style selection, clipboard, record links, and context-menu exports.
- Remove the unused language-server result-view filtering/sorting API and its duplicate SQL-typed row storage; ordinary subset paging and full-result exports retain original result order.
- Match PPTB default tab selection: errors and no-results queries show Messages, result sets show Results, and only estimated plans automatically show Execution Plan.

### Added

- A dedicated Query Results view in the VS Code bottom panel, consistent with the MSSQL extension experience.
- Per-editor result state with virtualized grids and copy actions.
- Full-result export to CSV, Excel, JSON, Markdown, and XML.
- Text exports open automatically in VS Code; Excel exports offer actions to open the workbook in its default application or reveal it in Finder/Explorer.
- Query metadata in the VS Code status bar, including rows returned, rows affected, execution state, and elapsed time.
- Spreadsheet-style cell, row, column, range, and cross-page selection, with keyboard support and copying with or without headers.
- Estimated and actual execution-plan commands, graphical plans using the shared SQL 4 CDS node icons, node properties, and FetchXML inspection in side editors.

### Fixed

- Send editor diagnostics as named LSP parameters so VS Code accepts and displays them.

- Prevent renamed profiles and changed credentials from silently reusing another cached connection.
- Isolate authentication retries so late or cancelled attempts cannot attach the wrong environment.
- Make Test Connection authenticate independently of existing editor sessions.
- Clean up authentication and Object Explorer sessions after disconnects, refreshes, and shutdown.
- Preserve keyboard focus during result selection and scrolling.
- Correlate exports with the current query run and bound the webview page cache.

### Changed

- Package a fresh Release service through the standard VS Code prepublish hook.
- Declare workspace trust and virtual-workspace support, and make the service override a machine setting.
- Add DOM, connection-race, extension-host, and VSIX-content checks; test Windows, macOS, Linux, and the minimum supported editor in CI.
- Update build dependencies to resolve reported npm vulnerabilities.

- Query execution starts on Messages, switches to Results when a result set becomes available, and remains on Messages for errors or queries that return no result sets.
- Results share PPTB's grid-focused layout and context-menu export controls.

### Initial extension features

- Saved Dataverse connection profiles with secrets stored in VS Code Secret Storage.
- Interactive, application-user, username/password, Windows-integrated, and connection-string authentication options.
- Per-editor connections and a connection status indicator.
- Dataverse Object Explorer with metadata browsing and query creation.
- SQL execution for the complete document or the current selection, cancellation, messages, and multiple result sets.
- Result copying and CSV export.
- SQL language-service completion and diagnostics for `.sql4cds` documents.
- Configurable safeguards for data modification and result limits.
- Startup validation and actionable diagnostics for the bundled language service and .NET 8 Runtime.
- Automatic language-service crash recovery with manual restart guidance when recovery is exhausted.
- Authentication token caching moved from the extension installation directory to VS Code global storage.
- Common password, client-secret, bearer-token, and credentialed-URL forms redacted from user-visible errors.
- Automated TypeScript checks, runtime tests, language-service builds, and VSIX packaging in CI.
