import { randomBytes } from "node:crypto";
import type * as vscode from "vscode";

export interface ResultsAssets { script: string; styles: string; images: string; }

export function resultsHtml(webview: vscode.Webview, assets: ResultsAssets): string {
  const nonce = randomBytes(16).toString("hex");
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; img-src ${webview.cspSource}; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}';">
  <link rel="stylesheet" href="${assets.styles}">
  <title>SQL 4 CDS Results</title>
</head>
<body data-images="${assets.images}">
  <div class="query-results">
    <nav class="results-tabs" role="tablist" aria-label="Query results">
      <button id="results-tab" role="tab" aria-controls="results">Results</button>
      <button id="messages-tab" role="tab" aria-controls="messages">Messages</button>
      <button id="plan-tab" role="tab" aria-controls="plan" hidden>Execution Plan</button>
    </nav>
    <div id="results" class="results-pane" role="tabpanel" aria-labelledby="results-tab" hidden></div>
    <div id="messages" class="messages-pane" role="tabpanel" aria-labelledby="messages-tab"></div>
    <div id="plan" class="plan-view" role="tabpanel" aria-labelledby="plan-tab" hidden>
      <div id="plan-canvas" class="plan-pane"></div>
      <aside class="plan-properties" aria-label="Execution plan properties">
        <div id="properties-title" class="properties-title"></div>
        <div id="properties" class="properties-grid"></div>
      </aside>
    </div>
    <div id="status" role="status"></div>
  </div>
  <script nonce="${nonce}" src="${assets.script}"></script>
</body>
</html>`;
}
