import type { DbCellValue, DbColumn } from "../resultsProtocol";
import { exportFormats, isEntityReferenceValue, type EntityReferenceValue, type ExportFormat } from "../resultsProtocol";

export type RowFetcher = (start: number, count: number) => Promise<DbCellValue[][]>;

const ROW_HEIGHT = 24;
const PAGE_SIZE = 200;
/** Maximum number of pages of rows held in memory for each grid */
const MAX_CACHED_PAGES = 10;
/** Browsers can't render elements taller than ~33M pixels, so scale the scroll range above this */
const MAX_SCROLL_HEIGHT = 10_000_000;
const ROW_NUMBER_WIDTH = 60;
/** Approximate width of a character in the grid's monospace font */
const CHAR_WIDTH = 7.5;
const MAX_AUTO_WIDTH = 400;

interface CachedPage {
    rows: DbCellValue[][] | null;
    /** The number of rows that were available when the page was requested */
    requestedCount: number;
    pending: Promise<void> | null;
    error?: string;
}

interface CellPosition {
    row: number;
    col: number;
}

/**
 * A virtualized grid showing a single result set. Only the rows that are currently visible are rendered, and
 * only a limited number of pages of rows are held in memory. Rows are requested from the language server on
 * demand as the user scrolls.
 */
export class ResultsGrid {
    readonly element: HTMLElement;

    private readonly header: HTMLElement;
    private readonly viewport: HTMLElement;
    private readonly spacer: HTMLElement;
    private readonly body: HTMLElement;
    private readonly resizeObserver: ResizeObserver;

    private readonly columnWidths: number[];
    private readonly pages = new Map<number, CachedPage>();
    private rowCount = 0;
    private complete = false;
    private renderScheduled = false;
    private disposed = false;
    private selected: CellPosition | null = null;
    private selectionAnchor: CellPosition | null = null;
    private dragging = false;
    private dragPoint: { x: number; y: number } | null = null;
    private dragFrame: number | null = null;
    private copying = false;
    private copyError: string | null = null;
    private autoSized = false;
    private exportMenu: HTMLElement | null = null;
    private readonly exportButtons: HTMLButtonElement[] = [];
    private closeContextMenu: (() => void) | null = null;
    private exportEnabled = false;
    private exporting = false;

    constructor(
        private readonly columns: DbColumn[],
        private readonly fetchRows: RowFetcher,
        private readonly onCopy: (text: string) => void,
        private readonly onOpenRecord: (reference: EntityReferenceValue) => void,
        private readonly onStatus: (text: string, isError: boolean) => void = () => {},
        private readonly onExport?: (format: ExportFormat) => Promise<void>,
        private readonly recordActions?: {
            copyUrl(reference: EntityReferenceValue): void;
            createSelect(reference: EntityReferenceValue): void;
        },
        autoSizeColumns = true,
    ) {
        this.autoSized = !autoSizeColumns;
        this.columnWidths = columns.map((c) => Math.min(320, Math.max(90, c.columnName.length * 8 + 24)));

        this.element = document.createElement("div");
        this.element.className = "results-grid";
        this.element.tabIndex = 0;

        this.element.setAttribute("aria-haspopup", "menu");
        this.element.addEventListener("contextmenu", (event) => {
            event.preventDefault();
            const cell = event.target instanceof Element ? event.target.closest<HTMLElement>(".grid-cell") : null;
            const row = cell?.parentElement?.dataset.row;
            const col = cell?.dataset.col;
            const position = row !== undefined && col !== undefined ? { row: Number(row), col: Number(col) } : null;
            if (position) {
                const range = this.selectionRange;
                if (!range || position.row < range.firstRow || position.row > range.lastRow ||
                    position.col < range.firstCol || position.col > range.lastCol) {
                    this.selectRectangle(position, position);
                }
            }
            this.showContextMenu(event.clientX, event.clientY, position);
        });

        this.header = document.createElement("div");
        this.header.className = "grid-header";

        this.viewport = document.createElement("div");
        this.viewport.className = "grid-viewport";

        this.spacer = document.createElement("div");
        this.spacer.className = "grid-spacer";

        this.body = document.createElement("div");
        this.body.className = "grid-body";

        this.viewport.append(this.spacer, this.body);
        this.element.append(this.header, this.viewport);

        this.renderHeader();

        this.viewport.addEventListener("scroll", () => {
            this.header.scrollLeft = this.viewport.scrollLeft;
            this.scheduleRender();
        });
        this.body.addEventListener("mousedown", (e) => this.onCellClick(e));
        this.header.addEventListener("mousedown", (e) => this.onCellClick(e));
        this.element.addEventListener("keydown", (e) => this.onKeyDown(e));
        this.element.addEventListener("focusin", () => this.updateStatus());
        document.addEventListener("copy", this.onDocumentCopy);
        this.resizeObserver = new ResizeObserver(() => this.scheduleRender());
        this.resizeObserver.observe(this.viewport);
    }

    /** Updates the number of rows available, e.g. in response to `query/resultSetUpdated` */
    setRowCount(rowCount: number, complete = false): void {
        if (rowCount === this.rowCount && complete === this.complete) return;

        this.rowCount = rowCount;
        this.complete = complete;
        this.scheduleRender();
    }

    setExportEnabled(enabled: boolean): void {
        this.exportEnabled = enabled;
        for (const button of this.exportButtons) button.disabled = !enabled || this.exporting;
    }

    private showContextMenu(x: number, y: number, position: CellPosition | null = this.selected): void {
        this.closeContextMenu?.();
        if (this.disposed) return;
        const menu = document.createElement("div");
        this.exportMenu = menu;
        menu.className = "grid-context-menu";
        menu.setAttribute("role", "menu");
        menu.setAttribute("aria-label", "Result actions");
        menu.tabIndex = -1;
        const menuButtons: HTMLButtonElement[] = [];
        const addItem = (label: string, enabled: boolean, action: () => void) => {
            const button = document.createElement("button");
            button.setAttribute("role", "menuitem");
            button.textContent = label;
            button.disabled = !enabled;
            button.addEventListener("click", () => {
                this.closeContextMenu?.();
                this.element.focus();
                action();
            });
            menuButtons.push(button);
            menu.appendChild(button);
            return button;
        };
        addItem("Copy", this.selected !== null, () => void this.copySelection(false));
        addItem("Copy with Headers", this.selected !== null, () => void this.copySelection(true));
        const value = position ? this.getRow(position.row)?.[position.col] : undefined;
        const reference = value && !value.isNull && isEntityReferenceValue(value.rawObject) ? value.rawObject : null;
        addItem("Open Record", reference !== null, () => { if (reference) this.onOpenRecord(reference); });
        addItem("Copy Record Url", reference !== null && !!this.recordActions, () => {
            if (reference) this.recordActions?.copyUrl(reference);
        });
        addItem("Create SELECT Statement", reference !== null && !!this.recordActions, () => {
            if (reference) this.recordActions?.createSelect(reference);
        });
        if (this.onExport) {
            for (const { format, label } of exportFormats) {
                const button = addItem(`Export to ${label}...`, false, () => void this.exportResults(format));
                button.title = `Export all rows to ${label}`;
                this.exportButtons.push(button);
            }
        }
        this.setExportEnabled(this.exportEnabled);
        document.body.appendChild(menu);
        const bounds = menu.getBoundingClientRect();
        menu.style.left = `${Math.max(0, Math.min(x, window.innerWidth - bounds.width))}px`;
        menu.style.top = `${Math.max(0, Math.min(y, window.innerHeight - bounds.height))}px`;
        const dismiss = () => this.closeContextMenu?.();
        const outside = (event: Event) => {
            if (event.target instanceof Node && !menu.contains(event.target)) dismiss();
        };
        this.closeContextMenu = () => {
            this.closeContextMenu = null;
            this.exportMenu = null;
            this.exportButtons.length = 0;
            document.removeEventListener("pointerdown", outside, true);
            document.removeEventListener("focusin", outside);
            document.removeEventListener("scroll", dismiss, true);
            window.removeEventListener("resize", dismiss);
            window.removeEventListener("blur", dismiss);
            menu.remove();
        };
        document.addEventListener("pointerdown", outside, true);
        document.addEventListener("focusin", outside);
        document.addEventListener("scroll", dismiss, true);
        window.addEventListener("resize", dismiss);
        window.addEventListener("blur", dismiss);
        menu.addEventListener("keydown", (event) => {
            if (event.key === "Escape" || event.key === "Tab") {
                dismiss();
                this.element.focus();
                if (event.key === "Escape") event.preventDefault();
            } else if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
                event.preventDefault();
                const buttons = menuButtons.filter((button) => !button.disabled);
                const index = buttons.indexOf(document.activeElement as HTMLButtonElement);
                const next = event.key === "Home" ? 0 : event.key === "End" ? buttons.length - 1
                    : (index + (event.key === "ArrowDown" ? 1 : -1) + buttons.length) % buttons.length;
                buttons[next]?.focus();
            }
        });
        (menuButtons.find((button) => !button.disabled) ?? menu).focus();
    }

    private async exportResults(format: ExportFormat): Promise<void> {
        if (!this.onExport || !this.exportEnabled || this.exporting || this.disposed) return;
        this.exporting = true;
        this.setExportEnabled(this.exportEnabled);
        try {
            await this.onExport(format);
        } finally {
            this.exporting = false;
            if (!this.disposed) this.setExportEnabled(this.exportEnabled);
        }
    }

    dispose(): void {
        this.disposed = true;
        this.resizeObserver.disconnect();
        this.closeContextMenu?.();
        this.endDrag();
        document.removeEventListener("copy", this.onDocumentCopy);
        this.pages.clear();
        this.element.remove();
    }

    private get totalWidth(): number {
        return ROW_NUMBER_WIDTH + this.columnWidths.reduce((a, b) => a + b, 0);
    }

    private renderHeader(): void {
        this.header.replaceChildren();
        const row = document.createElement("div");
        row.className = "grid-row grid-header-row";
        row.style.width = `${this.totalWidth}px`;

        const rowNum = document.createElement("div");
        rowNum.className = "grid-cell grid-rownum";
        rowNum.style.width = `${ROW_NUMBER_WIDTH}px`;
        row.appendChild(rowNum);

        this.columns.forEach((column, index) => {
            const cell = document.createElement("div");
            cell.className = "grid-cell";
            cell.dataset.col = String(index);
            cell.style.width = `${this.columnWidths[index]}px`;
            cell.textContent = column.columnName || "(No column name)";
            cell.title = column.dataTypeName ? `${column.columnName} (${column.dataTypeName})` : column.columnName;

            const resizer = document.createElement("div");
            resizer.className = "grid-resizer";
            resizer.addEventListener("mousedown", (e) => this.startResize(e, index));
            cell.appendChild(resizer);

            row.appendChild(cell);
        });

        this.header.appendChild(row);
    }

    private startResize(e: MouseEvent, index: number): void {
        e.preventDefault();
        e.stopPropagation();
        const startX = e.clientX;
        const startWidth = this.columnWidths[index];

        const move = (ev: MouseEvent) => {
            this.autoSized = true;
            this.columnWidths[index] = Math.max(40, startWidth + ev.clientX - startX);
            this.renderHeader();
            this.header.scrollLeft = this.viewport.scrollLeft;
            this.render();
        };
        const up = () => {
            window.removeEventListener("mousemove", move);
            window.removeEventListener("mouseup", up);
        };
        window.addEventListener("mousemove", move);
        window.addEventListener("mouseup", up);
    }

    private updateStatus(): void {
        const parts: string[] = [];
        if (this.copying) parts.push("Copying…");
        if (this.copyError) parts.push(this.copyError);
        this.onStatus(parts.join(" | "), this.copyError !== null);
    }

    private get selectionRange(): { firstRow: number; lastRow: number; firstCol: number; lastCol: number } | null {
        if (!this.selected) return null;
        const anchor = this.selectionAnchor ?? this.selected;
        return {
            firstRow: Math.min(anchor.row, this.selected.row),
            lastRow: Math.max(anchor.row, this.selected.row),
            firstCol: Math.min(anchor.col, this.selected.col),
            lastCol: Math.max(anchor.col, this.selected.col),
        };
    }

    private scheduleRender(): void {
        if (this.renderScheduled || this.disposed) return;
        this.renderScheduled = true;
        requestAnimationFrame(() => {
            this.renderScheduled = false;
            if (!this.disposed) this.render();
        });
    }

    /** The number of rows that fit entirely within the viewport */
    private get fullyVisibleRows(): number {
        return Math.max(1, Math.floor(this.viewport.clientHeight / ROW_HEIGHT));
    }

    /** The highest first visible row when the scroll range is scaled, which still allows the last row to be fully shown */
    private get maxScaledFirstRow(): number {
        return Math.max(0, this.rowCount - this.fullyVisibleRows);
    }

    /** Works out the first visible row, allowing for the scroll range being scaled for very large result sets */
    private getFirstVisibleRow(): number {
        const fullHeight = this.rowCount * ROW_HEIGHT;
        if (fullHeight <= MAX_SCROLL_HEIGHT) return Math.floor(this.viewport.scrollTop / ROW_HEIGHT);

        const maxScroll = Math.max(1, MAX_SCROLL_HEIGHT - this.viewport.clientHeight);
        const maxFirstRow = this.maxScaledFirstRow;
        return Math.min(maxFirstRow, Math.floor((this.viewport.scrollTop / maxScroll) * maxFirstRow));
    }

    private render(): void {
        const scrollHeight = Math.min(this.rowCount * ROW_HEIGHT, MAX_SCROLL_HEIGHT);
        this.spacer.style.height = `${scrollHeight}px`;
        this.spacer.style.width = `${this.totalWidth}px`;

        const visibleRows = Math.ceil(this.viewport.clientHeight / ROW_HEIGHT) + 1;
        const first = this.getFirstVisibleRow();
        const last = Math.min(this.rowCount, first + visibleRows);

        // Position the rendered rows so they line up with the current scroll position
        const top = scrollHeight === this.rowCount * ROW_HEIGHT ? first * ROW_HEIGHT : this.viewport.scrollTop;
        this.body.style.transform = `translateY(${top}px)`;
        this.body.style.width = `${this.totalWidth}px`;

        const fragment = document.createDocumentFragment();
        for (let rowIndex = first; rowIndex < last; rowIndex++) {
            fragment.appendChild(this.renderRow(rowIndex));
        }
        this.body.replaceChildren(fragment);

        this.ensurePages(first, last);
    }

    private renderRow(rowIndex: number): HTMLElement {
        const row = document.createElement("div");
        row.className = "grid-row";
        row.style.width = `${this.totalWidth}px`;
        row.dataset.row = String(rowIndex);

        const rowNum = document.createElement("div");
        rowNum.className = "grid-cell grid-rownum";
        rowNum.style.width = `${ROW_NUMBER_WIDTH}px`;
        rowNum.textContent = String(rowIndex + 1);
        row.appendChild(rowNum);

        const values = this.getRow(rowIndex);
        const range = this.selectionRange;

        this.columns.forEach((_, colIndex) => {
            const cell = document.createElement("div");
            cell.className = "grid-cell";
            cell.style.width = `${this.columnWidths[colIndex]}px`;
            cell.dataset.col = String(colIndex);

            if (!values) {
                const error = this.pages.get(Math.floor(rowIndex / PAGE_SIZE))?.error;
                if (error) {
                    cell.textContent = "Error";
                    cell.title = error;
                } else {
                    cell.classList.add("loading");
                }
            } else {
                const value = values[colIndex];
                if (!value || value.isNull) {
                    cell.classList.add("null");
                    cell.textContent = "NULL";
                } else {
                    if (isEntityReferenceValue(value.rawObject)) {
                        const reference = value.rawObject;
                        const link = document.createElement("a");
                        link.className = "grid-record-link";
                        link.href = "#";
                        link.textContent = value.displayValue;
                        link.title = `Open ${reference.logicalName} record in ${reference.dataSource}`;
                        link.addEventListener("click", (e) => {
                            e.preventDefault();
                            this.onOpenRecord(reference);
                            this.scheduleRender();
                        });
                        cell.appendChild(link);
                    } else {
                        cell.textContent = value.displayValue;
                    }
                    cell.title = value.displayValue.length > 30 ? value.displayValue : "";
                }
            }

            if (range && rowIndex >= range.firstRow && rowIndex <= range.lastRow &&
                colIndex >= range.firstCol && colIndex <= range.lastCol) cell.classList.add("selected");
            row.appendChild(cell);
        });

        return row;
    }

    private getRow(rowIndex: number): DbCellValue[] | undefined {
        const pageIndex = Math.floor(rowIndex / PAGE_SIZE);
        const page = this.pages.get(pageIndex);
        if (!page?.rows) return undefined;

        // Mark the page as recently used
        this.pages.delete(pageIndex);
        this.pages.set(pageIndex, page);

        return page.rows[rowIndex - pageIndex * PAGE_SIZE];
    }

    private ensurePages(first: number, last: number): void {
        if (last <= first) return;

        const firstPage = Math.floor(first / PAGE_SIZE);
        const lastPage = Math.floor((last - 1) / PAGE_SIZE);

        for (let pageIndex = firstPage; pageIndex <= lastPage; pageIndex++) {
            const start = pageIndex * PAGE_SIZE;
            const available = Math.min(PAGE_SIZE, this.rowCount - start);
            const existing = this.pages.get(pageIndex);

            // Re-request pages that were only partially available when they were first loaded
            if (existing && (existing.pending || existing.requestedCount >= available)) continue;

            const page: CachedPage = { rows: existing?.rows ?? null, requestedCount: available, pending: null };
            page.pending = this.fetchRows(start, available)
                .then((rows) => {
                    page.rows = rows;
                    if (!this.autoSized && rows.length > 0) this.autoSizeColumns(rows);
                })
                .catch((err) => {
                    console.error("Failed to load rows", err);
                    page.error = `Failed to load rows: ${err instanceof Error ? err.message : String(err)}`;
                    if (!this.disposed) this.onStatus(page.error, true);
                })
                .finally(() => {
                    page.pending = null;
                    this.scheduleRender();
                });

            this.pages.set(pageIndex, page);
        }

        this.evictPages(firstPage, lastPage);
    }

    /** Widens columns to fit the first rows of data, unless the user has already resized them */
    private autoSizeColumns(rows: DbCellValue[][]): void {
        this.autoSized = true;
        this.columns.forEach((_, colIndex) => {
            const longest = rows.reduce((max, row) => Math.max(max, row[colIndex]?.isNull ? 4 : row[colIndex]?.displayValue.length ?? 0), 0);
            this.columnWidths[colIndex] = Math.min(MAX_AUTO_WIDTH, Math.max(this.columnWidths[colIndex], longest * CHAR_WIDTH + 16));
        });
        this.renderHeader();
    }

    private evictPages(firstVisible: number, lastVisible: number): void {
        for (const pageIndex of this.pages.keys()) {
            if (this.pages.size <= MAX_CACHED_PAGES) break;
            if (pageIndex >= firstVisible && pageIndex <= lastVisible) continue;
            this.pages.delete(pageIndex);
        }
    }

    private onCellClick(e: MouseEvent): void {
        if (e.button !== 0) return;
        if ((e.target as HTMLElement).closest(".grid-resizer")) return;
        const recordLink = (e.target as HTMLElement).closest(".grid-record-link");
        const cell = (e.target as HTMLElement).closest<HTMLElement>(".grid-cell");
        if (!cell) return;
        const row = cell?.parentElement?.dataset.row;
        const col = cell?.dataset.col;
        if (row === undefined || col === undefined) {
            e.preventDefault();
            this.selectRectangle(
                { row: row === undefined ? 0 : Number(row), col: col === undefined ? 0 : Number(col) },
                {
                    row: row === undefined ? this.rowCount - 1 : Number(row),
                    col: col === undefined ? this.columns.length - 1 : Number(col),
                },
            );
            return;
        }

        // Keep the browser's default mousedown action from moving focus after the clicked cell is replaced.
        e.preventDefault();
        this.endDrag();
        if (!e.shiftKey || !this.selected) {
            this.selectionAnchor = { row: Number(row), col: Number(col) };
        } else {
            this.selectionAnchor ??= this.selected;
        }
        this.selected = { row: Number(row), col: Number(col) };
        this.copyError = null;
        this.element.focus();
        this.updateStatus();
        // A plain link click must reach its handler; dragging can replace it once the mouse moves.
        if (!recordLink || e.shiftKey) this.render();
        this.dragging = true;
        document.addEventListener("mousemove", this.onDragMove);
        document.addEventListener("mouseup", this.endDrag);
        document.defaultView?.addEventListener("blur", this.endDrag);
    }

    private selectRectangle(anchor: CellPosition, end: CellPosition): void {
        this.endDrag();
        this.selectionAnchor = this.rowCount > 0 && this.columns.length > 0 ? anchor : null;
        this.selected = this.selectionAnchor ? end : null;
        this.copyError = null;
        this.element.focus();
        this.updateStatus();
        this.render();
    }

    private readonly onDragMove = (e: MouseEvent): void => {
        if (!(e.buttons & 1)) {
            this.endDrag();
            return;
        }
        e.preventDefault();
        this.dragPoint = { x: e.clientX, y: e.clientY };
        if (this.dragFrame !== null) cancelAnimationFrame(this.dragFrame);
        this.updateDragSelection();
    };

    private readonly updateDragSelection = (): void => {
        this.dragFrame = null;
        if (!this.dragging || !this.dragPoint || !this.selected) return;
        const rect = this.viewport.getBoundingClientRect();
        const x = this.dragPoint.x - rect.left - this.viewport.clientLeft;
        const y = this.dragPoint.y - rect.top - this.viewport.clientTop;
        const height = this.viewport.clientHeight;
        const width = this.viewport.clientWidth;
        if (height <= 0 || width <= 0) return;

        const offset = this.rowCount * ROW_HEIGHT <= MAX_SCROLL_HEIGHT ? this.viewport.scrollTop % ROW_HEIGHT : 0;
        let row = this.getFirstVisibleRow() + Math.floor((clamp(y, 0, height - 1) + offset) / ROW_HEIGHT);
        const contentX = this.viewport.scrollLeft + clamp(x, 0, width - 1);
        let col = 0;
        let right = ROW_NUMBER_WIDTH + this.columnWidths[0];
        while (col < this.columns.length - 1 && contentX >= right) right += this.columnWidths[++col];

        // Extend by one cell per frame outside the viewport, including scaled scroll ranges.
        if (y < 0) row--;
        else if (y >= height) row++;
        if (x < 0) col--;
        else if (x >= width) col++;
        row = clamp(row, 0, this.rowCount - 1);
        col = clamp(col, 0, this.columns.length - 1);
        if (this.selected.row !== row || this.selected.col !== col) {
            this.selected = { row, col };
            this.updateStatus();
            this.scheduleRender();
        }
        if (x < 0 || x >= width || y < 0 || y >= height) this.scrollIntoView(row, col);
        this.dragFrame = requestAnimationFrame(this.updateDragSelection);
    };

    private readonly endDrag = (): void => {
        this.dragging = false;
        this.dragPoint = null;
        if (this.dragFrame !== null) cancelAnimationFrame(this.dragFrame);
        this.dragFrame = null;
        document.removeEventListener("mousemove", this.onDragMove);
        document.removeEventListener("mouseup", this.endDrag);
        document.defaultView?.removeEventListener("blur", this.endDrag);
    };

    private onKeyDown(e: KeyboardEvent): void {
        if (e.defaultPrevented || e.altKey) return;
        if (e.key === "ContextMenu" || (e.shiftKey && e.key === "F10")) {
            e.preventDefault();
            const bounds = this.element.getBoundingClientRect();
            this.showContextMenu(bounds.left, bounds.top + ROW_HEIGHT);
            return;
        }

        const primary = isMac() ? e.metaKey && !e.ctrlKey : e.ctrlKey && !e.metaKey;
        if (primary && !e.shiftKey && e.key.toLowerCase() === "a") {
            e.preventDefault();
            this.selectRectangle({ row: 0, col: 0 }, { row: this.rowCount - 1, col: this.columns.length - 1 });
            return;
        }

        const copy = getCopyShortcut(e);
        if (copy) {
            if (!this.selected) return;
            e.preventDefault();
            void this.copySelection(copy === "withHeaders");
            return;
        }

        const target = this.getNavigationTarget(e);
        if (!target) return;

        e.preventDefault();
        if (this.rowCount === 0 || this.columns.length === 0) return;

        const previous = this.selected;
        this.selected = {
            row: clamp(target.row, 0, this.rowCount - 1),
            col: clamp(target.col, 0, this.columns.length - 1),
        };
        if (e.shiftKey) this.selectionAnchor ??= previous ?? this.selected;
        else this.selectionAnchor = this.selected;
        this.copyError = null;
        this.updateStatus();
        this.scrollIntoView(this.selected.row, this.selected.col);
        this.render();
    }

    /** Handles copy requests that don't come from a keyboard shortcut, e.g. from an application Edit menu */
    private readonly onDocumentCopy = (e: ClipboardEvent): void => {
        if (!this.selected || !this.element.contains(document.activeElement)) return;
        if (this.exportMenu?.contains(document.activeElement)) return;
        // Leave any text the user has explicitly selected to be copied as normal
        if (!document.getSelection()?.isCollapsed) return;

        e.preventDefault();
        void this.copySelection(false);
    };

    /** Works out which cell to move to for an SSMS-style navigation key, or null if the key isn't a navigation key */
    private getNavigationTarget(e: KeyboardEvent): { row: number; col: number } | null {
        const mac = isMac();
        const ctrl = mac ? e.metaKey : e.ctrlKey;
        if (mac ? e.ctrlKey : e.metaKey) return null;

        const lastRow = this.rowCount - 1;
        const lastCol = this.columns.length - 1;
        const page = Math.max(1, this.fullyVisibleRows - 1);

        // With nothing selected yet, the first navigation key just selects the first cell
        if (!this.selected) {
            const keys = ["ArrowUp", "ArrowDown", "ArrowLeft", "ArrowRight", "Home", "End", "PageUp", "PageDown"];
            return keys.includes(e.key) ? { row: 0, col: 0 } : null;
        }

        const { row, col } = this.selected;
        switch (e.key) {
            case "ArrowUp":
                return { row: ctrl ? 0 : row - 1, col };
            case "ArrowDown":
                return { row: ctrl ? lastRow : row + 1, col };
            case "ArrowLeft":
                return { row, col: ctrl ? 0 : col - 1 };
            case "ArrowRight":
                return { row, col: ctrl ? lastCol : col + 1 };
            case "Home":
                return { row: ctrl ? 0 : row, col: 0 };
            case "End":
                return { row: ctrl ? lastRow : row, col: lastCol };
            case "PageUp":
                return ctrl ? null : { row: row - page, col };
            case "PageDown":
                return ctrl ? null : { row: row + page, col };
            default:
                return null;
        }
    }

    private async copySelection(withHeaders: boolean): Promise<void> {
        // Snapshot the rectangle: scrolling or changing selection during a fetch must not change the copied data.
        const range = this.selectionRange;
        if (!range || this.copying || this.disposed) return;
        this.copying = true;
        this.copyError = null;
        this.updateStatus();
        try {
            const lines: string[] = [];
            if (withHeaders) {
                lines.push(this.columns.slice(range.firstCol, range.lastCol + 1).map((c) => c.columnName).join("\t"));
            }
            for (let first = range.firstRow; first <= range.lastRow;) {
                const pageIndex = Math.floor(first / PAGE_SIZE);
                const start = pageIndex * PAGE_SIZE;
                const last = Math.min(range.lastRow + 1, start + PAGE_SIZE);
                const page = this.pages.get(pageIndex);
                let rows = page?.rows;
                if ((!rows || rows.length < last - start) && page?.pending) {
                    await page.pending;
                    rows = page.rows;
                }
                if (!rows || rows.length < last - start) {
                    // Copy-only pages are not cached, so large copies don't evict visible rows or grow the cache.
                    rows = await this.fetchRows(start, Math.min(PAGE_SIZE, this.rowCount - start));
                }
                if (this.disposed) return;
                for (let rowIndex = first; rowIndex < last; rowIndex++) {
                    const values = rows[rowIndex - start];
                    const cells: string[] = [];
                    for (let col = range.firstCol; col <= range.lastCol; col++) {
                        const value = values?.[col];
                        if (!value) throw new Error(`Cell at row ${rowIndex + 1}, column ${col + 1} is unavailable`);
                        cells.push(value.isNull ? "NULL" : value.displayValue);
                    }
                    lines.push(cells.join("\t"));
                }
                first = last;
            }
            this.onCopy(lines.join("\r\n"));
        } catch (err) {
            console.error("Failed to copy selected cells", err);
            this.copyError = `Copy failed: ${err instanceof Error ? err.message : String(err)}`;
        } finally {
            this.copying = false;
            if (!this.disposed) this.updateStatus();
        }
    }

    private scrollIntoView(rowIndex: number, colIndex: number): void {
        const visibleRows = this.fullyVisibleRows;
        const fullHeight = this.rowCount * ROW_HEIGHT;

        if (fullHeight <= MAX_SCROLL_HEIGHT) {
            const top = rowIndex * ROW_HEIGHT;
            if (top < this.viewport.scrollTop) {
                this.viewport.scrollTop = top;
            } else if (top + ROW_HEIGHT > this.viewport.scrollTop + this.viewport.clientHeight) {
                this.viewport.scrollTop = top + ROW_HEIGHT - this.viewport.clientHeight;
            }
        } else {
            // The scroll range is scaled, so work in terms of the first visible row instead of pixels
            const first = this.getFirstVisibleRow();
            let newFirst = first;
            if (rowIndex < first) newFirst = rowIndex;
            else if (rowIndex >= first + visibleRows) newFirst = rowIndex - visibleRows + 1;

            if (newFirst !== first) {
                const maxScroll = Math.max(1, MAX_SCROLL_HEIGHT - this.viewport.clientHeight);
                const maxFirstRow = Math.max(1, this.maxScaledFirstRow);
                this.viewport.scrollTop = Math.min(maxScroll, Math.ceil((newFirst / maxFirstRow) * maxScroll));
            }
        }

        // The row number column scrolls with the data, so show it when moving to the first column
        const left = colIndex === 0 ? 0 : ROW_NUMBER_WIDTH + this.columnWidths.slice(0, colIndex).reduce((a, b) => a + b, 0);
        const right = ROW_NUMBER_WIDTH + this.columnWidths.slice(0, colIndex + 1).reduce((a, b) => a + b, 0);
        if (left < this.viewport.scrollLeft) {
            this.viewport.scrollLeft = left;
        } else if (right > this.viewport.scrollLeft + this.viewport.clientWidth) {
            this.viewport.scrollLeft = Math.min(left, right - this.viewport.clientWidth);
        }
        this.header.scrollLeft = this.viewport.scrollLeft;
    }
}

let isMacPlatform: boolean | undefined;

/** Whether the app is running on an Apple platform, where Cmd replaces Ctrl for standard shortcuts */
function isMac(): boolean {
    if (isMacPlatform === undefined) {
        const nav = typeof navigator === "undefined" ? undefined : (navigator as Navigator & { userAgentData?: { platform?: string } });
        isMacPlatform = /mac|iphone|ipad|ipod/i.test(nav?.userAgentData?.platform || nav?.platform || nav?.userAgent || "");
    }
    return isMacPlatform;
}

function clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, value));
}

/**
 * Checks for the platform's standard copy shortcut (Cmd+C on macOS, Ctrl+C or Ctrl+Insert elsewhere), plus the
 * SSMS Ctrl+Shift+C shortcut to copy with column headers.
 */
function getCopyShortcut(e: KeyboardEvent): "plain" | "withHeaders" | null {
    const primary = isMac() ? e.metaKey && !e.ctrlKey : e.ctrlKey && !e.metaKey;
    if (!primary) return null;

    if (e.code === "KeyC" || e.key.toLowerCase() === "c") return e.shiftKey ? "withHeaders" : "plain";
    if (!isMac() && !e.shiftKey && e.key === "Insert") return "plain";
    return null;
}
