import { randomBytes } from "node:crypto";
import type * as vscode from "vscode";

/** Builds the self-contained results grid webview. Grid state deliberately lives in
 * the webview so paging only renders a small window while selection can span the
 * complete transformed result. */
export function resultsHtml(webview: vscode.Webview): string {
  const nonce = createNonce();
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'nonce-${nonce}'; script-src 'nonce-${nonce}';">
  <style nonce="${nonce}">
    :root{color-scheme:light dark}*{box-sizing:border-box}body{height:100vh;margin:0;overflow:hidden;font-family:var(--vscode-font-family);color:var(--vscode-foreground);background:var(--vscode-editor-background)}button,input,select{font:inherit;color:inherit}
    .tabs{display:flex;gap:2px;overflow:auto;height:36px;padding:4px 8px 0;border-bottom:1px solid var(--vscode-panel-border)}.tabs:empty{display:none}.tabs:empty+main{height:100vh}.tab{cursor:pointer;color:var(--vscode-foreground);background:transparent;border:0;border-bottom:2px solid transparent;padding:4px 10px}.tab:hover{background:var(--vscode-toolbar-hoverBackground)}.tab.active{border-bottom-color:var(--vscode-focusBorder);font-weight:600}
    main{height:calc(100vh - 36px);padding:6px 8px}.toolbar{display:flex;align-items:center;min-height:30px;gap:4px;margin-bottom:4px}.toolbar .meta{min-width:90px;color:var(--vscode-descriptionForeground);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.search{display:flex;align-items:center;min-width:150px;max-width:300px;flex:1;margin-right:auto}.search input{min-width:0;width:100%;height:25px;padding:2px 24px 2px 7px;border:1px solid var(--vscode-input-border,transparent);background:var(--vscode-input-background);color:var(--vscode-input-foreground)}.search .clear{width:22px;height:22px;margin-left:-23px;border:0;background:transparent;cursor:pointer}.search .clear:hover{background:var(--vscode-toolbar-hoverBackground)}.page-label,.selection-status{padding:0 4px;color:var(--vscode-descriptionForeground);font-variant-numeric:tabular-nums;white-space:nowrap}.selection-status{margin-left:4px}
    .icon-button{display:grid;place-items:center;width:26px;height:26px;padding:0;color:var(--vscode-foreground);background:transparent;border:0;border-radius:3px;font-size:16px;line-height:1;cursor:pointer}.icon-button:hover:not(:disabled),.header-button:hover{background:var(--vscode-toolbar-hoverBackground)}.icon-button:disabled{opacity:.35;cursor:default}.tab:focus-visible,.icon-button:focus-visible,.header-button:focus-visible,.grid:focus-visible{outline:1px solid var(--vscode-focusBorder);outline-offset:-1px}
    .result-area{display:flex;min-width:0;height:calc(100% - 34px)}.grid{min-width:0;flex:1;overflow:auto;border-top:1px solid var(--vscode-panel-border);border-left:1px solid var(--vscode-panel-border);outline:none}.grid-actions{display:flex;flex:0 0 32px;flex-direction:column;align-items:center;gap:2px;padding:2px 3px;border-left:1px solid var(--vscode-panel-border)}
    table{border-collapse:separate;border-spacing:0;min-width:100%;table-layout:fixed;font-family:var(--vscode-editor-font-family);font-size:var(--vscode-editor-font-size)}th,td{height:26px;padding:3px 7px;border-right:1px solid var(--vscode-panel-border);border-bottom:1px solid var(--vscode-panel-border);text-align:left;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;vertical-align:middle}th{position:sticky;top:0;z-index:3;padding:0;min-width:90px;background:var(--vscode-editorGroupHeader-tabsBackground);user-select:none}.corner,.row-gutter{position:sticky;left:0;z-index:4;width:52px;min-width:52px;max-width:52px;text-align:right;background:var(--vscode-editorGroupHeader-tabsBackground);color:var(--vscode-descriptionForeground);font-variant-numeric:tabular-nums;cursor:pointer}.corner{z-index:5;text-align:center}.header{display:flex;align-items:stretch;height:25px}.header-button{height:25px;padding:2px 5px;border:0;background:transparent;cursor:pointer}.drag-handle{width:19px;padding:2px;color:var(--vscode-descriptionForeground);cursor:grab}.column-name{min-width:0;flex:1;text-align:left;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.sort.active,.filter.active{color:var(--vscode-textLink-foreground);background:var(--vscode-toolbar-activeBackground)}.resize-handle{position:absolute;right:-2px;top:0;z-index:8;width:5px;height:100%;cursor:col-resize}td{cursor:cell}.null{color:var(--vscode-descriptionForeground);font-style:italic}td.structured{position:relative;padding-right:25px;text-decoration:underline;text-decoration-style:dotted;text-decoration-color:var(--vscode-textLink-foreground)}td.structured::after{content:'↗';position:absolute;right:6px;color:var(--vscode-textLink-foreground);font-family:var(--vscode-font-family);font-style:normal}.selected{background:var(--vscode-list-activeSelectionBackground)!important;color:var(--vscode-list-activeSelectionForeground)!important}.focused{outline:1px solid var(--vscode-focusBorder);outline-offset:-2px}.column-drag{opacity:.55}
    td.structured{cursor:pointer}
    .popover,.context-menu{position:fixed;z-index:20;min-width:210px;padding:6px;background:var(--vscode-menu-background,var(--vscode-editorWidget-background));color:var(--vscode-menu-foreground,var(--vscode-foreground));border:1px solid var(--vscode-menu-border,var(--vscode-widget-border));box-shadow:0 2px 8px var(--vscode-widget-shadow);border-radius:3px}.popover{display:grid;gap:6px}.popover label{font-size:.9em;color:var(--vscode-descriptionForeground)}.popover select,.popover input{width:100%;height:26px;border:1px solid var(--vscode-input-border,transparent);background:var(--vscode-input-background);color:var(--vscode-input-foreground)}.popover-actions{display:flex;justify-content:flex-end;gap:5px}.popover-actions button{cursor:pointer}.context-menu{padding:4px;min-width:190px}.menu-item{display:block;width:100%;padding:5px 9px;border:0;text-align:left;background:transparent;cursor:pointer}.menu-item:hover{background:var(--vscode-menu-selectionBackground);color:var(--vscode-menu-selectionForeground)}.menu-separator{height:1px;margin:3px 5px;background:var(--vscode-menu-separatorBackground,var(--vscode-panel-border))}
    .empty,.error{padding:20px;color:var(--vscode-descriptionForeground)}.error,.message.error{color:var(--vscode-errorForeground)}.messages{height:100%;margin:0;padding:0;overflow:auto;list-style:none}.message{padding:7px 9px;border-bottom:1px solid var(--vscode-panel-border);white-space:pre-wrap}.message time{display:block;margin-bottom:2px;color:var(--vscode-descriptionForeground);font-size:.9em}
  </style>
</head>
<body>
  <nav id="tabs" class="tabs" aria-label="Query results"></nav><main id="content"><div class="empty">Waiting for query results…</div></main>
  <script nonce="${nonce}">
    const vscode = acquireVsCodeApi();
    const tabs = document.getElementById('tabs');
    const content = document.getElementById('content');
    let state;
    let active;
    let currentPage = 0;
    let currentPageData;
    let userSelectedTab = false;
    let dragSelecting = false;
    let searchTimer;
    let restoreGridFocus = false;
    let restoreSearchFocus = false;
    const pages = new Map();
    const pending = new Set();
    const views = new Map();
    const defaultColumnWidth = 180;
    const rowGutterWidth = 52;

    function freshView(result) {
      return {
        searchText: '',
        filters: new Map(),
        sort: undefined,
        viewVersion: 0,
        columnOrder: result.columns.map((_, index) => index),
        widths: new Map(),
        autoWidths: new Map(),
        terms: [],
        anchor: undefined,
        focus: undefined
      };
    }

    function viewFor(result) {
      let view = views.get(result.key);
      if (!view) {
        view = freshView(result);
        views.set(result.key, view);
      }
      return view;
    }

    function spec(view) {
      return {
        searchText: view.searchText,
        filters: [...view.filters].map(([columnIndex, filter]) => ({
          columnIndex,
          operator: filter.operator,
          value: filter.value
        })),
        sort: view.sort,
        viewVersion: view.viewVersion
      };
    }

    function autoSizeColumnsEnabled() {
      return state?.autoSizeColumns !== false;
    }

    function columnWidth(view, columnIndex) {
      if (view.widths.has(columnIndex)) {
        return view.widths.get(columnIndex);
      }
      if (autoSizeColumnsEnabled()) {
        return view.autoWidths.get(columnIndex) ?? defaultColumnWidth;
      }
      return defaultColumnWidth;
    }

    function applyHeaderWidth(header, view, columnIndex) {
      header.style.width = columnWidth(view, columnIndex) + 'px';
    }

    function applyCellWidth(cell, view, columnIndex) {
      cell.style.width = columnWidth(view, columnIndex) + 'px';
    }

    function resizeHandleState(handle) {
      handle.style.display = '';
    }

    function updateTableWidth(table, view) {
      const width = rowGutterWidth + view.columnOrder.reduce((total, columnIndex) => total + columnWidth(view, columnIndex), 0);
      table.style.width = width + 'px';
    }

    function updateVisibleColumnWidth(view, columnIndex) {
      const table = content.querySelector('.result-area table');
      if (!table) { return; }

      const header = table.querySelector('th[data-column="' + columnIndex + '"]');
      if (header) {
        applyHeaderWidth(header, view, columnIndex);
      }

      table.querySelectorAll('td[data-column="' + columnIndex + '"]').forEach(cell => {
        applyCellWidth(cell, view, columnIndex);
      });
      updateTableWidth(table, view);
    }

    function ensureAutoWidths(result, view, page) {
      if (!autoSizeColumnsEnabled()) { return; }

      for (const columnIndex of view.columnOrder) {
        if (view.widths.has(columnIndex)) { continue; }
        if (!view.autoWidths.has(columnIndex)) {
          view.autoWidths.set(columnIndex, estimateAutoWidth(result, page, columnIndex));
        }
      }
    }

    function estimateAutoWidth(result, page, columnIndex) {
      const headerText = result.columns[columnIndex] || '(unnamed)';
      let longest = headerText.length;
      for (const row of page.rows) {
        const textValue = cellText(row[columnIndex]);
        const text = textValue === null ? 'NULL' : String(textValue ?? '');
        longest = Math.max(longest, text.length);
      }

      return Math.max(90, Math.min(420, longest * 8 + 24));
    }

    function invalidate(result, clearSelection) {
      const view = viewFor(result);
      restoreSearchFocus = document.activeElement?.matches('.search input') || false;
      view.viewVersion++;
      if (clearSelection) {
        view.terms = [];
        view.anchor = undefined;
        view.focus = undefined;
      }
      currentPage = 0;
      currentPageData = undefined;

      const prefix = state.runId + ':' + result.key + ':';
      for (const key of [...pages.keys()]) {
        if (key.startsWith(prefix)) { pages.delete(key); }
      }
      for (const key of [...pending]) {
        if (key.startsWith(prefix)) { pending.delete(key); }
      }

      showActive();
    }

    window.addEventListener('message', event => handleMessage(event.data));
    window.addEventListener('pointerup', () => { dragSelecting = false; });
    window.addEventListener('blur', () => {
      dragSelecting = false;
      dismissOverlays();
    });
    document.addEventListener('pointerdown', event => {
      if (!event.target.closest('.popover,.context-menu,.filter')) {
        dismissOverlays();
      }
    });
    document.addEventListener('keydown', event => {
      if (event.key === 'Escape') { dismissOverlays(); }
    });

    function handleMessage(message) {
      if (message.type === 'state') {
        handleStateMessage(message);
        return;
      }
      if (message.type === 'empty') {
        handleEmptyMessage();
        return;
      }
      if (!state || message.runId !== state.runId) { return; }
      if (message.type === 'page') {
        handlePageMessage(message);
      } else if (message.type === 'pageError') {
        handlePageErrorMessage(message);
      } else if (message.type === 'resetFilter') {
        handleResetFilterMessage(message);
      }
    }

    function handleStateMessage(message) {
      const changedRun = !state || state.runId !== message.runId;
      state = message;
      if (changedRun) { resetUiState(); }
      renderState();
    }

    function handleEmptyMessage() {
      resetUiState();
      state = undefined;
      active = undefined;
      dismissOverlays();
      tabs.replaceChildren();
      content.replaceChildren(makeNode('div', 'empty', 'Run a query to see its results.'));
    }

    function handlePageMessage(message) {
      const result = state.results.find(item => item.key === message.key);
      if (!result) { return; }

      const view = viewFor(result);
      const responseVersion = message.viewVersion ?? view.viewVersion;
      const cacheKey = pageKey(message.key, message.page, responseVersion);
      pending.delete(cacheKey);
      if (responseVersion !== view.viewVersion) { return; }

      pages.set(cacheKey, message);
      while (pages.size > 3) {
        pages.delete(pages.keys().next().value);
      }

      if (active === message.key && currentPage === message.page) {
        renderResult();
      }
    }

    function handlePageErrorMessage(message) {
      const result = state.results.find(item => item.key === message.key);
      if (!result) { return; }

      const view = viewFor(result);
      const responseVersion = message.viewVersion ?? view.viewVersion;
      pending.delete(pageKey(message.key, message.page, responseVersion));
      if (responseVersion !== view.viewVersion) { return; }

      if (active === message.key && currentPage === message.page) {
        showError(message.message);
      }
    }

    function handleResetFilterMessage(message) {
      const result = state.results.find(item => item.key === message.key);
      if (!result || !Number.isInteger(message.columnIndex)) { return; }

      const view = viewFor(result);
      view.filters.delete(message.columnIndex);
      if (active === message.key) {
        invalidate(result, true);
      }
    }

    function resetUiState() {
      clearTimeout(searchTimer);
      restoreGridFocus = false;
      restoreSearchFocus = false;
      pages.clear();
      pending.clear();
      views.clear();
      active = 'messages';
      userSelectedTab = false;
      currentPage = 0;
      currentPageData = undefined;
    }

    function renderState() {
      tabs.replaceChildren();
      for (const result of state.results) {
        addTab(result.key, state.results.length === 1 ? 'Results' : 'Result ' + result.ordinal);
      }
      addTab('messages', 'Messages' + (state.messages.length ? ' (' + state.messages.length + ')' : ''));

      const hasErrors = state.status === 'failed' || state.messages.some(message => message.isError);
      if (hasErrors || !state.results.length) {
        active = 'messages';
        userSelectedTab = false;
      } else if (active === 'messages' && !userSelectedTab) {
        active = state.results[0].key;
      } else if (!active || (active !== 'messages' && !state.results.some(result => result.key === active))) {
        active = state.results[0].key;
      }

      for (const tab of tabs.children) {
        tab.classList.toggle('active', tab.dataset.key === active);
      }
      showActive();
    }

    function addTab(key, label) {
      const tab = makeNode('button', 'tab', label);
      tab.dataset.key = key;
      tab.setAttribute('role', 'tab');
      tab.addEventListener('click', () => {
        clearTimeout(searchTimer);
        restoreSearchFocus = false;
        restoreGridFocus = false;
        active = key;
        userSelectedTab = true;
        currentPage = 0;
        dismissOverlays();
        for (const item of tabs.children) {
          item.classList.toggle('active', item.dataset.key === active);
        }
        showActive();
      });
      tabs.append(tab);
    }

    function showActive() {
      dismissOverlays();
      if (active === 'messages') {
        renderMessages();
        return;
      }

      const result = state.results.find(item => item.key === active);
      if (!result) {
        const text = state.status === 'running' || state.status === 'cancelling'
          ? 'Waiting for query results…'
          : 'No result sets were returned.';
        content.replaceChildren(makeNode('div', 'empty', text));
        return;
      }
      if (!result.complete) {
        content.replaceChildren(makeNode('div', 'empty', 'Waiting for this result set to finish…'));
        return;
      }

      const view = viewFor(result);
      const knownRows = currentPageData && currentPageData.key === active
        ? currentPageData.displayRows
        : result.displayRowCount;
      const lastPage = Math.max(0, Math.ceil(knownRows / result.pageSize) - 1);
      currentPage = Math.min(currentPage, lastPage);

      const cachedPage = pages.get(pageKey(active, currentPage, view.viewVersion));
      if (cachedPage) {
        renderResult();
        return;
      }

      const area = content.querySelector('.result-area');
      if (area) {
        requestPage(result);
        return;
      }

      content.replaceChildren(makeNode('div', 'empty', 'Loading result page…'));
      requestPage(result);
    }

    function requestPage(result) {
      const view = viewFor(result);
      const key = pageKey(result.key, currentPage, view.viewVersion);
      if (pending.has(key)) { return; }
      pending.add(key);
      vscode.postMessage({
        type: 'page',
        ownerUri: state.ownerUri,
        runId: state.runId,
        key: result.key,
        page: currentPage,
        ...spec(view)
      });
    }

    function renderResult() {
      const result = state.results.find(item => item.key === active);
      if (!result) { return; }

      const view = viewFor(result);
      const page = pages.get(pageKey(active, currentPage, view.viewVersion));
      if (!page) { return; }

      currentPageData = { ...page, key: active };
      const focused = document.activeElement;
      const keepGrid = restoreGridFocus || focused?.classList.contains('grid');
      const keepSearch = restoreSearchFocus || focused?.matches('.search input');
      const transformedRows = page.displayRows;
      const toolbar = buildToolbar(result, view, page, focused);

      if (!result.columns.length) {
        content.replaceChildren(makeNode('div', 'empty', 'This result set has no columns.'));
        return;
      }
      if (reuseGrid(result, view, page, toolbar, keepSearch, keepGrid, transformedRows)) {
        return;
      }

      content.replaceChildren();
      content.append(toolbar);

      const area = makeNode('div', 'result-area');
      area.dataset.key = result.key;

      const grid = createGrid(result, view, page, transformedRows);
      const table = createResultTable(result, view, page, transformedRows);
      grid.append(table);
      area.append(grid);

      const actions = createGridActions(result, view);
      area.append(actions);
      content.append(area);

      restoreFocusedElement(toolbar, grid, keepSearch, keepGrid);
      finishResultRender(view);
    }

    function buildToolbar(result, view, page, focused) {
      const toolbar = makeNode('div', 'toolbar');
      toolbar.append(createSearchBox(result, view, focused));
      toolbar.append(makeNode('span', 'meta', resultSummary(result, page)));

      const transformedRows = page.displayRows;
      const lastPage = Math.max(0, Math.ceil(transformedRows / result.pageSize) - 1);
      toolbar.append(
        iconButton('First page', '⇤', () => changePage(0), currentPage === 0),
        iconButton('Previous page', '‹', () => changePage(currentPage - 1), currentPage === 0),
        makeNode('span', 'page-label', (currentPage + 1) + ' / ' + (lastPage + 1)),
        iconButton('Next page', '›', () => changePage(currentPage + 1), currentPage >= lastPage),
        iconButton('Last page', '⇥', () => changePage(lastPage), currentPage >= lastPage)
      );
      toolbar.append(makeNode('span', 'selection-status', selectionSummary(view, result, transformedRows)));

      if (view.searchText || view.filters.size || view.sort) {
        toolbar.append(iconButton('Clear search, filters and sort', '↺', () => {
          view.searchText = '';
          view.filters.clear();
          view.sort = undefined;
          invalidate(result, true);
        }, false));
      }

      return toolbar;
    }

    function createSearchBox(result, view, focused) {
      const search = makeNode('div', 'search');
      const input = document.createElement('input');
      input.type = 'search';
      input.placeholder = 'Search all columns';
      input.setAttribute('aria-label', 'Quick search results');
      input.value = focused?.matches('.search input') ? focused.value : view.searchText;
      input.addEventListener('input', () => {
        clearTimeout(searchTimer);
        searchTimer = setTimeout(() => {
          if (view.searchText === input.value) { return; }
          view.searchText = input.value;
          invalidate(result, true);
        }, 250);
      });

      const clear = makeNode('button', 'clear', '×');
      clear.title = 'Clear search';
      clear.addEventListener('click', () => {
        input.value = '';
        if (view.searchText) {
          view.searchText = '';
          invalidate(result, true);
        }
      });

      search.append(input, clear);
      return search;
    }

    function resultSummary(result, page) {
      const first = page.displayRows ? page.start + 1 : 0;
      const lastRow = page.start + page.rows.length;
      const originalTotal = page.totalRows ?? result.rowCount;
      const filteredTotal = page.filteredRows ?? page.displayRows;
      const transformedRows = page.displayRows;
      const isFiltered = filteredTotal !== originalTotal;

      if (page.truncated) {
        return first.toLocaleString() + '–' + lastRow.toLocaleString() + ' of ' + transformedRows.toLocaleString() +
          ' displayed (' +
          (isFiltered ? filteredTotal.toLocaleString() + ' matching; ' : '') +
          originalTotal.toLocaleString() + ' total; display limit reached)';
      }

      return first.toLocaleString() + '–' + lastRow.toLocaleString() + ' of ' + transformedRows.toLocaleString() +
        (isFiltered ? ' matching (' + originalTotal.toLocaleString() + ' total)' : '');
    }

    function createGrid(result, view, page, transformedRows) {
      const grid = makeNode('div', 'grid');
      grid.tabIndex = 0;
      grid.setAttribute('role', 'grid');
      grid.setAttribute('aria-rowcount', String(transformedRows + 1));
      grid.setAttribute('aria-colcount', String(result.columns.length + 1));
      grid.addEventListener('keydown', event => gridKeyDown(event, result, page, view));
      grid.addEventListener('pointerdown', () => grid.focus({ preventScroll: true }));
      return grid;
    }

    function createResultTable(result, view, page, transformedRows) {
      ensureAutoWidths(result, view, page);
      const table = document.createElement('table');
      const thead = document.createElement('thead');
      const headerRow = document.createElement('tr');
      const corner = makeNode('th', 'corner', '◩');
      corner.title = 'Select all filtered results';
      corner.onclick = event => {
        if (transformedRows > 0) {
          selectArea(view, 0, transformedRows - 1, view.columnOrder, event, false);
        }
      };
      headerRow.append(corner);

      for (const originalIndex of view.columnOrder) {
        headerRow.append(makeHeader(result, view, originalIndex, transformedRows));
      }

      thead.append(headerRow);
      table.append(thead);

      const tbody = document.createElement('tbody');
      syncBodyRows(tbody, result, view, page);
      table.append(tbody);
      updateTableWidth(table, view);
      return table;
    }

    function createGridActions(result, view) {
      const actions = makeNode('aside', 'grid-actions');
      actions.setAttribute('aria-label', 'Result actions');
      actions.append(
        iconButton('Copy selection', '⧉', () => copySelection(result, view, 'tsv', false), !view.terms.length),
        iconButton('Copy selection with headers', '⧉⁺', () => copySelection(result, view, 'tsv', true), !view.terms.length),
        iconButton('Export full result set…', '⇩', () => vscode.postMessage({ type: 'export', ownerUri: state.ownerUri, runId: state.runId, key: active }), !result.complete)
      );
      return actions;
    }

    function restoreFocusedElement(toolbar, grid, keepSearch, keepGrid) {
      const input = toolbar.querySelector('.search input');
      if (keepSearch && input) {
        input.focus({ preventScroll: true });
      } else if (keepGrid) {
        grid.focus({ preventScroll: true });
      }
    }

    function finishResultRender(view) {
      restoreGridFocus = false;
      restoreSearchFocus = false;
      paintSelection(view);
    }

    function reuseGrid(result, view, page, nextToolbar, keepSearch, keepGrid, transformedRows) {
      const oldToolbar = content.querySelector('.toolbar');
      const area = content.querySelector('.result-area');
      const grid = area?.querySelector('.grid');
      const table = grid?.querySelector('table');
      if (!oldToolbar || !area || !grid || !table || area.dataset.key !== result.key) {
        return false;
      }

      const headRow = table.tHead?.rows?.[0];
      if (!headRow || headRow.cells.length !== view.columnOrder.length + 1) {
        return false;
      }
      for (let index = 0; index < view.columnOrder.length; index++) {
        const header = headRow.cells[index + 1];
        if (Number(header.dataset.column) !== view.columnOrder[index]) {
          return false;
        }
      }

      oldToolbar.replaceWith(nextToolbar);
      if (keepSearch) {
        const oldInput = oldToolbar.querySelector('.search input');
        const nextInput = nextToolbar.querySelector('.search input');
        if (oldInput && nextInput) {
          nextInput.value = oldInput.value;
          nextInput.focus({ preventScroll: true });
        }
      }

      grid.setAttribute('aria-rowcount', String(transformedRows + 1));
      grid.setAttribute('aria-colcount', String(result.columns.length + 1));
      grid.onkeydown = event => gridKeyDown(event, result, page, view);
      grid.onpointerdown = () => grid.focus({ preventScroll: true });

      const corner = headRow.cells[0];
      if (corner) {
        corner.onclick = event => {
          if (transformedRows > 0) {
            selectArea(view, 0, transformedRows - 1, view.columnOrder, event, false);
          }
        };
      }

      const tbody = table.tBodies[0] || table.appendChild(document.createElement('tbody'));
      ensureAutoWidths(result, view, page);
      syncBodyRows(tbody, result, view, page);
      syncHeaderState(headRow, result, view, transformedRows);
      syncActionButtons(area, result, view);

      if (keepGrid) {
        grid.focus({ preventScroll: true });
      }
      finishResultRender(view);
      updateSelectionStatus(view);
      return true;
    }

    function syncHeaderState(headRow, result, view, transformedRows) {
      for (let index = 0; index < view.columnOrder.length; index++) {
        const columnIndex = view.columnOrder[index];
        const header = headRow.cells[index + 1];
        const direction = view.sort && view.sort.columnIndex === columnIndex ? view.sort.direction : undefined;
        const name = header.querySelector('.column-name');
        const sort = header.querySelector('.sort');
        const filter = header.querySelector('.filter');
        const resize = header.querySelector('.resize-handle');

        applyHeaderWidth(header, view, columnIndex);

        if (name) {
          name.onclick = event => {
            if (transformedRows > 0) {
              selectArea(view, 0, transformedRows - 1, [columnIndex], event, false);
            }
          };
        }
        if (sort) {
          sort.classList.toggle('active', !!direction);
          sort.textContent = direction === 'asc' ? '↑' : direction === 'desc' ? '↓' : '↕';
          sort.title = direction ? 'Sorted ' + direction + '. Click to change.' : 'Sort column';
          sort.onclick = event => {
            event.stopPropagation();
            const current = view.sort && view.sort.columnIndex === columnIndex ? view.sort.direction : undefined;
            view.sort = !current ? { columnIndex, direction: 'asc' } : current === 'asc' ? { columnIndex, direction: 'desc' } : undefined;
            invalidate(result, true);
          };
        }
        if (filter) {
          filter.classList.toggle('active', view.filters.has(columnIndex));
          filter.onclick = event => {
            event.stopPropagation();
            openFilter(event, result, view, columnIndex);
          };
        }
        if (resize) {
          resizeHandleState(resize);
        }
      }

      const table = headRow.closest('table');
      if (table) {
        updateTableWidth(table, view);
      }
    }

    function syncActionButtons(area, result, view) {
      const actionButtons = area.querySelectorAll('.grid-actions button');
      if (actionButtons.length < 3) { return; }

      actionButtons[0].disabled = !view.terms.length;
      actionButtons[0].onclick = () => copySelection(result, view, 'tsv', false);
      actionButtons[1].disabled = !view.terms.length;
      actionButtons[1].onclick = () => copySelection(result, view, 'tsv', true);
      actionButtons[2].disabled = !result.complete;
      actionButtons[2].onclick = () => vscode.postMessage({ type: 'export', ownerUri: state.ownerUri, runId: state.runId, key: active });
    }

    function makeHeader(result, view, columnIndex, rowCount) {
      const th = document.createElement('th');
      th.dataset.column = String(columnIndex);
      applyHeaderWidth(th, view, columnIndex);

      const box = makeNode('div', 'header');
      box.append(
        createDragHandle(th, view, columnIndex),
        createColumnSelectButton(result, view, columnIndex, rowCount),
        createSortButton(result, view, columnIndex),
        createFilterButton(result, view, columnIndex)
      );
      th.append(box);

      const resize = makeNode('span', 'resize-handle');
      resizeHandleState(resize);
      resize.addEventListener('pointerdown', event => startResize(event, th, view, columnIndex));
      th.append(resize);
      return th;
    }

    function createDragHandle(th, view, columnIndex) {
      const drag = makeNode('button', 'header-button drag-handle', '⋮⋮');
      drag.title = 'Drag to reorder column';
      drag.draggable = true;
      drag.addEventListener('dragstart', event => {
        event.dataTransfer.setData('text/plain', String(columnIndex));
        th.classList.add('column-drag');
      });
      drag.addEventListener('dragend', () => th.classList.remove('column-drag'));

      th.addEventListener('dragover', event => event.preventDefault());
      th.addEventListener('drop', event => {
        event.preventDefault();
        const from = Number(event.dataTransfer.getData('text/plain'));
        const to = view.columnOrder.indexOf(columnIndex);
        if (!Number.isInteger(from) || from === columnIndex) { return; }

        const oldIndex = view.columnOrder.indexOf(from);
        if (oldIndex < 0) { return; }

        view.columnOrder.splice(oldIndex, 1);
        view.columnOrder.splice(to, 0, from);
        renderResult();
      });

      return drag;
    }

    function createColumnSelectButton(result, view, columnIndex, rowCount) {
      const name = makeNode('button', 'header-button column-name', result.columns[columnIndex] || '(unnamed)');
      name.title = 'Select column ' + (result.columns[columnIndex] || columnIndex + 1);
      name.onclick = event => {
        if (rowCount > 0) {
          selectArea(view, 0, rowCount - 1, [columnIndex], event, false);
        }
      };
      return name;
    }

    function createSortButton(result, view, columnIndex) {
      const direction = view.sort && view.sort.columnIndex === columnIndex ? view.sort.direction : undefined;
      const sort = makeNode('button', 'header-button sort' + (direction ? ' active' : ''), sortGlyph(direction));
      sort.title = direction ? 'Sorted ' + direction + '. Click to change.' : 'Sort column';
      sort.onclick = event => {
        event.stopPropagation();
        const current = view.sort && view.sort.columnIndex === columnIndex ? view.sort.direction : undefined;
        view.sort = !current ? { columnIndex, direction: 'asc' } : current === 'asc' ? { columnIndex, direction: 'desc' } : undefined;
        invalidate(result, true);
      };
      return sort;
    }

    function createFilterButton(result, view, columnIndex) {
      const filter = makeNode('button', 'header-button filter' + (view.filters.has(columnIndex) ? ' active' : ''), '⌄');
      filter.title = 'Filter column';
      filter.onclick = event => {
        event.stopPropagation();
        openFilter(event, result, view, columnIndex);
      };
      return filter;
    }
    function syncBodyRows(tbody,result,view,page){
      const columnCount=view.columnOrder.length;
      while(tbody.rows.length<page.rows.length){
        tbody.append(createResultRow(columnCount));
      }
      while(tbody.rows.length>page.rows.length)tbody.deleteRow(tbody.rows.length-1);
      for(let i=0;i<page.rows.length;i++)updateResultRow(tbody.rows[i],result,view,page,i);
    }
    function createResultRow(columnCount){
      const tr=document.createElement('tr');
      tr.append(makeNode('th','row-gutter',''));
      for(let i=0;i<columnCount;i++)tr.append(makeNode('td','',''));
      return tr;
    }
    function updateResultRow(row,result,view,page,localRow){
      const columnCount=view.columnOrder.length;
      const logicalRow=page.start+localRow;
      const sourceRow=page.rows[localRow];
      const gutter=row.cells[0];
      gutter.className='row-gutter';
      gutter.textContent=String(logicalRow+1);
      gutter.scope='row';
      const gutterSignal=resetNodeListeners(gutter);
      gutter.addEventListener('pointerdown',event=>{if(event.button!==0)return;selectArea(view,logicalRow,logicalRow,view.columnOrder,event,true);dragSelecting=true;},{signal:gutterSignal});
      gutter.addEventListener('pointerenter',event=>{if(dragSelecting&&(event.buttons&1))extendArea(view,logicalRow,view.columnOrder);},{signal:gutterSignal});
      gutter.addEventListener('contextmenu',event=>openContext(event,result,view,logicalRow,undefined),{signal:gutterSignal});

      for(let v=0;v<view.columnOrder.length;v++){
        const originalIndex=view.columnOrder[v];
        let td=row.cells[v+1];
        if(!td){td=makeNode('td','','');row.append(td);}
        updateResultCell(td,result,view,logicalRow,originalIndex,sourceRow[originalIndex]);
      }
      while(row.cells.length>columnCount+1)row.deleteCell(row.cells.length-1);
    }
    function updateResultCell(td,result,view,logicalRow,originalIndex,value){
      const text = cellText(value);
      const candidate=isStructuredCandidate(value);
      const dataverseRecord = isDataverseRecordCandidate(value);
      td.className=(text===null?'null':'')+(candidate?' structured':'');
      td.textContent=text===null?'NULL':String(text);
      td.dataset.row=String(logicalRow);
      td.dataset.column=String(originalIndex);
      applyCellWidth(td, view, originalIndex);
      td.title=dataverseRecord?'Double-click to open Dataverse record':candidate?'Double-click to view formatted value':text===null?'NULL':String(text);
      decorateCell(td,view,logicalRow,originalIndex);
      const cellSignal=resetNodeListeners(td);
      td.addEventListener('pointerdown',event=>{if(event.button!==0)return;selectArea(view,logicalRow,logicalRow,[originalIndex],event,true);dragSelecting=true;},{signal:cellSignal});
      td.addEventListener('pointerenter',event=>{if(dragSelecting&&(event.buttons&1))extendCell(view,logicalRow,originalIndex);},{signal:cellSignal});
      if(candidate)td.addEventListener('dblclick',()=>vscode.postMessage({type:'viewCell',ownerUri:state.ownerUri,runId:state.runId,key:active,row:logicalRow,columnIndex:originalIndex,text:text,...spec(view)}),{signal:cellSignal});
      td.addEventListener('contextmenu',event=>openContext(event,result,view,logicalRow,originalIndex),{signal:cellSignal});
    }
    function resetNodeListeners(node){
      node._sql4cdsListeners?.abort();
      const controller=new AbortController();
      node._sql4cdsListeners=controller;
      return controller.signal;
    }
    function startResize(event, th, view, column) {
      event.preventDefault();
      event.stopPropagation();

      const start = event.clientX;
      const width = th.getBoundingClientRect().width || columnWidth(view, column);

      function move(pointerEvent) {
        const next = Math.max(52, width + pointerEvent.clientX - start);
        view.widths.set(column, next);
        updateVisibleColumnWidth(view, column);
      }

      function up() {
        window.removeEventListener('pointermove', move);
        window.removeEventListener('pointerup', up);
      }

      window.addEventListener('pointermove', move);
      window.addEventListener('pointerup', up);
    }

    function openFilter(event, result, view, columnIndex) {
      dismissOverlays();

      const current = view.filters.get(columnIndex) || { operator: 'contains', value: '' };
      const popover = makeNode('div', 'popover');
      positionOverlay(popover, event.clientX, event.clientY + 18, 225, 145);
      popover.append(makeNode('label', '', result.columns[columnIndex] || 'Column ' + (columnIndex + 1)));

      const select = document.createElement('select');
      for (const [value, text] of filterOptions()) {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = text;
        select.append(option);
      }
      select.value = current.operator;

      const input = document.createElement('input');
      input.placeholder = 'Filter value';
      input.value = current.value;

      const updateDisabled = () => {
        input.disabled = select.value === 'isEmpty' || select.value === 'isNotEmpty';
      };
      select.addEventListener('change', updateDisabled);
      updateDisabled();

      const buttons = makeNode('div', 'popover-actions');
      const clear = makeNode('button', '', 'Clear');
      clear.addEventListener('click', () => {
        view.filters.delete(columnIndex);
        dismissOverlays();
        invalidate(result, true);
      });

      const apply = makeNode('button', '', 'Apply');
      apply.addEventListener('click', () => {
        view.filters.set(columnIndex, { operator: select.value, value: input.value });
        dismissOverlays();
        invalidate(result, true);
      });

      input.addEventListener('keydown', keyEvent => {
        if (keyEvent.key === 'Enter') { apply.click(); }
      });

      buttons.append(clear, apply);
      popover.append(select, input, buttons);
      document.body.append(popover);
      input.focus();
    }

    function filterOptions() {
      return [
        ['contains', 'Contains'],
        ['equals', 'Equals'],
        ['notEquals', 'Does not equal'],
        ['startsWith', 'Starts with'],
        ['endsWith', 'Ends with'],
        ['greaterThan', 'Greater than'],
        ['greaterThanOrEqual', 'Greater than or equal'],
        ['lessThan', 'Less than'],
        ['lessThanOrEqual', 'Less than or equal'],
        ['isEmpty', 'Is empty'],
        ['isNotEmpty', 'Is not empty']
      ];
    }

    function positionOverlay(element, left, top, width, height) {
      element.style.left = Math.max(4, Math.min(left, window.innerWidth - width)) + 'px';
      element.style.top = Math.max(4, Math.min(top, window.innerHeight - height)) + 'px';
    }

    function selectArea(view, rowStart, rowEnd, columnIds, event, setAnchor) {
      const additive = event.metaKey || event.ctrlKey;
      if (event.shiftKey && view.anchor) {
        view.terms = additive ? view.terms : [];
        addTerm(view, view.anchor.row, rowEnd, columnsBetween(view, view.anchor.column, columnIds[columnIds.length - 1]), true);
      } else {
        const selected = areCellsSelected(view, rowStart, rowEnd, columnIds);
        if (!additive) {
          view.terms = [];
        }
        addTerm(view, rowStart, rowEnd, columnIds, !(additive && selected));
        if (setAnchor || !view.anchor) {
          view.anchor = { row: rowStart, column: columnIds[0] };
        }
      }

      view.focus = { row: rowEnd, column: columnIds[columnIds.length - 1] };
      paintSelection(view);
    }
    function extendArea(view, row, columnIds) {
      if (!view.anchor) { return; }
      view.terms = view.terms.filter(term => !term.preview);
      const term = addTerm(view, view.anchor.row, row, columnsBetween(view, view.anchor.column, columnIds[columnIds.length - 1]), true);
      term.preview = true;
      view.focus = { row, column: columnIds[columnIds.length - 1] };
      paintSelection(view);
    }

    function extendCell(view, row, column) {
      extendArea(view, row, [column]);
    }

    function addTerm(view, startRow, endRow, columnIds, selected) {
      const term = {
        rowStart: Math.min(startRow, endRow),
        rowEnd: Math.max(startRow, endRow),
        columnIds: [...new Set(columnIds)],
        selected
      };
      view.terms.push(term);
      return term;
    }

    function columnsBetween(view, startColumn, endColumn) {
      const startIndex = view.columnOrder.indexOf(startColumn);
      const endIndex = view.columnOrder.indexOf(endColumn);
      return view.columnOrder.slice(Math.min(startIndex, endIndex), Math.max(startIndex, endIndex) + 1);
    }

    function areCellsSelected(view, rowStart, rowEnd, columnIds) {
      for (let row = rowStart; row <= rowEnd; row++) {
        for (const column of columnIds) {
          if (!isSelected(view, row, column)) {
            return false;
          }
        }
      }
      return true;
    }

    function isSelected(view, row, column) {
      let selected = false;
      for (const term of view.terms) {
        if (row >= term.rowStart && row <= term.rowEnd && term.columnIds.includes(column)) {
          selected = term.selected;
        }
      }
      return selected;
    }

    function decorateCell(cell, view, row, column) {
      const selected = isSelected(view, row, column);
      cell.classList.toggle('selected', selected);
      cell.classList.toggle('focused', !!view.focus && view.focus.row === row && view.focus.column === column);
      cell.setAttribute('aria-selected', String(selected));
    }

    function paintSelection(view) {
      document.querySelectorAll('td[data-row]').forEach(cell => {
        decorateCell(cell, view, Number(cell.dataset.row), Number(cell.dataset.column));
      });

      document.querySelectorAll('th[data-column]').forEach(header => {
        const column = Number(header.dataset.column);
        const selected = view.terms.length > 0 && currentPageData &&
          isSelected(view, 0, column) &&
          isSelected(view, Math.max(0, currentPageData.displayRows - 1), column);
        header.classList.toggle('selected', !!selected);
      });

      document.querySelectorAll('.row-gutter').forEach((gutter, index) => {
        const row = (currentPageData?.start || 0) + index;
        gutter.classList.toggle('selected', view.columnOrder.every(column => isSelected(view, row, column)));
      });

      const anySelection = selectionRanges(view).length > 0;
      document.querySelectorAll('.grid-actions button[title^="Copy selection"]').forEach(button => {
        button.disabled = !anySelection;
      });
      updateSelectionStatus(view);
    }
    function selectionRanges(view){
      if(!view.terms.length)return[];const boundaries=new Set();for(const t of view.terms){boundaries.add(t.rowStart);boundaries.add(t.rowEnd+1);}const rows=[...boundaries].sort((a,b)=>a-b),out=[];
      for(let i=0;i<rows.length-1;i++){const rs=rows[i],re=rows[i+1]-1;let start=-1;for(let c=0;c<=view.columnOrder.length;c++){const yes=c<view.columnOrder.length&&isSelected(view,rs,view.columnOrder[c]);if(yes&&start<0)start=c;if(!yes&&start>=0){out.push({rowStart:rs,rowEnd:re,columnStart:start,columnEnd:c-1});start=-1;}}}
      const merged=[];for(const range of out){const prior=merged.find(x=>x.columnStart===range.columnStart&&x.columnEnd===range.columnEnd&&x.rowEnd+1===range.rowStart);if(prior)prior.rowEnd=range.rowEnd;else merged.push({...range});}return merged;
    }
    function selectionSummary(view, result, rowCount) {
      const ranges = selectionRanges(view);
      if (!ranges.length) { return ''; }

      let cells = 0;
      const rows = new Set();
      const columns = new Set();
      for (const range of ranges) {
        cells += (range.rowEnd - range.rowStart + 1) * (range.columnEnd - range.columnStart + 1);
        for (let column = range.columnStart; column <= range.columnEnd; column++) {
          columns.add(column);
        }
        if (range.rowEnd - range.rowStart < 10000) {
          for (let row = range.rowStart; row <= range.rowEnd; row++) {
            rows.add(row);
          }
        }
      }

      const rowText = rows.size ? rows.size.toLocaleString() : rowCount.toLocaleString();
      return rowText + ' rows × ' + columns.size.toLocaleString() + ' columns selected (' + cells.toLocaleString() + ' cells)';
    }

    function updateSelectionStatus(view) {
      const result = state.results.find(item => item.key === active);
      const node = document.querySelector('.selection-status');
      if (result && node) {
        node.textContent = selectionSummary(view, result, currentPageData?.displayRows ?? result.displayRowCount);
      }
    }

    function gridKeyDown(event, result, page, view) {
      if (!['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight', 'a', 'A', 'c', 'C'].includes(event.key)) {
        return;
      }

      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'a') {
        event.preventDefault();
        if (!page.displayRows || !view.columnOrder.length) { return; }
        view.terms = [];
        addTerm(view, 0, page.displayRows - 1, view.columnOrder, true);
        view.focus = { row: 0, column: view.columnOrder[0] };
        view.anchor = { ...view.focus };
        paintSelection(view);
        return;
      }

      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'c') {
        event.preventDefault();
        copySelection(result, view, 'tsv', false);
        return;
      }

      if (!event.key.startsWith('Arrow') || !page.displayRows || !view.columnOrder.length) {
        return;
      }

      event.preventDefault();
      const next = nextGridPosition(event, result, page, view);
      if (!next) { return; }

      if (event.shiftKey && view.anchor) {
        view.terms = [];
        addTerm(view, view.anchor.row, next.row, columnsBetween(view, view.anchor.column, next.column), true);
      } else {
        view.terms = [];
        addTerm(view, next.row, next.row, [next.column], true);
        view.anchor = { row: next.row, column: next.column };
      }

      view.focus = next;
      const targetPage = Math.floor(next.row / result.pageSize);
      if (targetPage !== currentPage) {
        changePage(targetPage);
        return;
      }

      paintSelection(view);
      document.querySelector('td[data-row="' + next.row + '"][data-column="' + next.column + '"]')?.scrollIntoView({ block: 'nearest', inline: 'nearest' });
    }

    function nextGridPosition(event, result, page, view) {
      const focus = view.focus || { row: page.start, column: view.columnOrder[0] };
      const visual = view.columnOrder.indexOf(focus.column);
      let row = focus.row;
      let column = focus.column;

      if (event.key === 'ArrowUp') { row = Math.max(0, row - 1); }
      if (event.key === 'ArrowDown') { row = Math.min(page.displayRows - 1, row + 1); }
      if (event.key === 'ArrowLeft') { column = view.columnOrder[Math.max(0, visual - 1)]; }
      if (event.key === 'ArrowRight') { column = view.columnOrder[Math.min(view.columnOrder.length - 1, visual + 1)]; }
      return { row, column };
    }

    function copySelection(result, view, format, headers) {
      const ranges = selectionRanges(view);
      if (!ranges.length) { return; }
      vscode.postMessage({
        type: 'copySelection',
        ownerUri: state.ownerUri,
        runId: state.runId,
        key: active,
        selection: { ranges, columnOrder: view.columnOrder },
        format,
        headers,
        ...spec(view)
      });
    }

    function openContext(event, result, view, row, column) {
      event.preventDefault();
      ensureContextSelection(view, row, column);
      dismissOverlays();

      const menu = makeNode('div', 'context-menu');
      positionOverlay(menu, event.clientX, event.clientY, 205, 260);

      addMenuItem(menu, 'Copy', () => copySelection(result, view, 'tsv', false));
      addMenuItem(menu, 'Copy with headers', () => copySelection(result, view, 'tsv', true));
      menu.append(makeNode('div', 'menu-separator'));
      addMenuItem(menu, 'Copy as CSV', () => copySelection(result, view, 'csv', true));
      addMenuItem(menu, 'Copy as JSON', () => copySelection(result, view, 'json', true));
      addMenuItem(menu, 'Copy as XML', () => copySelection(result, view, 'xml', true));
      addMenuItem(menu, 'Copy as Markdown', () => copySelection(result, view, 'markdown', true));
      addMenuItem(menu, 'Copy as SQL IN clause', () => copySelection(result, view, 'sqlIn', false));
      menu.append(makeNode('div', 'menu-separator'));
      addMenuItem(menu, 'Select row', () => selectContextRow(view, row));

      if (column !== undefined) {
        addMenuItem(menu, 'Select column', () => selectContextColumn(view, column));
        const value = currentCellValue(row, column);
        if (isStructuredCandidate(value)) {
          addMenuItem(menu, isDataverseRecordCandidate(value) ? 'Open Dataverse record' : 'View formatted value', () => postViewCell(row, column, value, view));
        }
      }

      document.body.append(menu);
    }

    function ensureContextSelection(view, row, column) {
      const rowSelected = view.columnOrder.every(id => isSelected(view, row, id));
      if (column !== undefined && !isSelected(view, row, column)) {
        view.terms = [];
        addTerm(view, row, row, [column], true);
        view.anchor = { row, column };
        view.focus = { row, column };
        paintSelection(view);
      } else if (column === undefined && !rowSelected) {
        view.terms = [];
        addTerm(view, row, row, view.columnOrder, true);
        view.anchor = { row, column: view.columnOrder[0] };
        view.focus = { ...view.anchor };
        paintSelection(view);
      }
    }

    function addMenuItem(menu, label, action) {
      const item = makeNode('button', 'menu-item', label);
      item.addEventListener('click', () => {
        dismissOverlays();
        action();
      });
      menu.append(item);
    }

    function selectContextRow(view, row) {
      view.terms = [];
      addTerm(view, row, row, view.columnOrder, true);
      renderResult();
    }

    function selectContextColumn(view, column) {
      view.terms = [];
      addTerm(view, 0, Math.max(0, currentPageData.displayRows - 1), [column], true);
      renderResult();
    }

    function currentCellValue(row, column) {
      const localRow = row - currentPageData.start;
      return currentPageData.rows[localRow]?.[column];
    }

    function postViewCell(row, columnIndex, text, view) {
      vscode.postMessage({
        type: 'viewCell',
        ownerUri: state.ownerUri,
        runId: state.runId,
        key: active,
        row,
        columnIndex,
        text,
        ...spec(view)
      });
    }

    function isStructuredCandidate(value) {
      const textValue = cellText(value);
      if (typeof textValue !== 'string') { return isDataverseRecordCandidate(value); }
      const text = textValue.trim();
      return (text.startsWith('{') && text.endsWith('}')) ||
        (text.startsWith('[') && text.endsWith(']')) ||
        text.startsWith('<') ||
        isDataverseRecordCandidate(value);
    }

    function cellText(value) {
      if (value && typeof value === 'object' && !Array.isArray(value) && Object.prototype.hasOwnProperty.call(value, 'text')) {
        return value.text;
      }
      return value;
    }

    function isDataverseRecordCandidate(value) {
    debugger;
      return Boolean(value && typeof value === 'object' && !Array.isArray(value) && value.isDataverseRecord === true);
    }

    function dismissOverlays() {
      document.querySelectorAll('.popover,.context-menu').forEach(element => element.remove());
    }

    function changePage(page) {
      restoreGridFocus = document.activeElement?.classList.contains('grid') || false;
      currentPage = Math.max(0, page);
      currentPageData = undefined;
      showActive();
    }

    function renderMessages() {
      content.replaceChildren();
      if (!state.messages.length) {
        content.append(makeNode('div', 'empty', state.status === 'running' ? 'Running query…' : 'No messages.'));
        return;
      }

      const list = makeNode('ol', 'messages');
      for (const message of state.messages) {
        const item = makeNode('li', 'message' + (message.isError ? ' error' : ''));
        if (message.time) {
          const time = document.createElement('time');
          const date = new Date(message.time);
          time.textContent = Number.isNaN(date.valueOf()) ? message.time : date.toLocaleTimeString();
          item.append(time);
        }
        item.append(document.createTextNode(message.message));
        list.append(item);
      }
      content.append(list);
    }

    function showError(message) {
      content.replaceChildren(makeNode('div', 'error', message));
    }

    function iconButton(label, glyph, handler, disabled) {
      const button = makeNode('button', 'icon-button', glyph);
      button.title = label;
      button.setAttribute('aria-label', label);
      button.disabled = disabled;
      button.addEventListener('click', handler);
      return button;
    }

    function makeNode(tag, className, text) {
      const node = document.createElement(tag);
      if (className) {
        node.className = className;
      }
      if (text !== undefined) {
        node.textContent = text;
      }
      return node;
    }

    function sortGlyph(direction) {
      return direction === 'asc' ? '↑' : direction === 'desc' ? '↓' : '↕';
    }

    function pageKey(key, page, version) {
      return state.runId + ':' + key + ':' + version + ':' + page;
    }
    vscode.postMessage({type:'ready'});
  </script>
</body>
</html>`;
}

function createNonce(): string {
  return randomBytes(32).toString("base64");
}
