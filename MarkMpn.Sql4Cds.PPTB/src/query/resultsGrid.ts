import type { DbCellValue, DbColumn } from "../lsp/protocol";
import { isEntityReferenceValue, type EntityReferenceValue } from "../lsp/protocol";

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
    private readonly statusBar: HTMLElement;

    private readonly columnWidths: number[];
    private readonly pages = new Map<number, CachedPage>();
    private rowCount = 0;
    private complete = false;
    private renderScheduled = false;
    private disposed = false;
    private selected: { row: number; col: number } | null = null;
    private autoSized = false;

    constructor(
        private readonly columns: DbColumn[],
        private readonly fetchRows: RowFetcher,
        private readonly onCopy: (text: string) => void,
        private readonly onOpenRecord: (reference: EntityReferenceValue) => void,
    ) {
        this.columnWidths = columns.map((c) => Math.min(320, Math.max(90, c.columnName.length * 8 + 24)));

        this.element = document.createElement("div");
        this.element.className = "results-grid";
        this.element.tabIndex = 0;

        this.header = document.createElement("div");
        this.header.className = "grid-header";

        this.viewport = document.createElement("div");
        this.viewport.className = "grid-viewport";

        this.spacer = document.createElement("div");
        this.spacer.className = "grid-spacer";

        this.body = document.createElement("div");
        this.body.className = "grid-body";

        this.statusBar = document.createElement("div");
        this.statusBar.className = "grid-status";

        this.viewport.append(this.spacer, this.body);
        this.element.append(this.header, this.viewport, this.statusBar);

        this.renderHeader();
        this.updateStatus();

        this.viewport.addEventListener("scroll", () => {
            this.header.scrollLeft = this.viewport.scrollLeft;
            this.scheduleRender();
        });
        this.body.addEventListener("mousedown", (e) => this.onCellClick(e));
        this.element.addEventListener("keydown", (e) => this.onKeyDown(e));
        new ResizeObserver(() => this.scheduleRender()).observe(this.viewport);
    }

    /** Updates the number of rows available, e.g. in response to `query/resultSetUpdated` */
    setRowCount(rowCount: number, complete = false): void {
        if (rowCount === this.rowCount && complete === this.complete) return;

        this.rowCount = rowCount;
        this.complete = complete;
        this.updateStatus();
        this.scheduleRender();
    }

    dispose(): void {
        this.disposed = true;
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
        const rows = `${this.rowCount.toLocaleString()} row${this.rowCount === 1 ? "" : "s"}`;
        this.statusBar.textContent = this.complete ? rows : `${rows} (retrieving…)`;
    }

    private scheduleRender(): void {
        if (this.renderScheduled || this.disposed) return;
        this.renderScheduled = true;
        requestAnimationFrame(() => {
            this.renderScheduled = false;
            if (!this.disposed) this.render();
        });
    }

    /** Works out the first visible row, allowing for the scroll range being scaled for very large result sets */
    private getFirstVisibleRow(visibleRows: number): number {
        const fullHeight = this.rowCount * ROW_HEIGHT;
        if (fullHeight <= MAX_SCROLL_HEIGHT) return Math.floor(this.viewport.scrollTop / ROW_HEIGHT);

        const maxScroll = Math.max(1, MAX_SCROLL_HEIGHT - this.viewport.clientHeight);
        const maxFirstRow = Math.max(0, this.rowCount - visibleRows);
        return Math.min(maxFirstRow, Math.floor((this.viewport.scrollTop / maxScroll) * maxFirstRow));
    }

    private render(): void {
        const scrollHeight = Math.min(this.rowCount * ROW_HEIGHT, MAX_SCROLL_HEIGHT);
        this.spacer.style.height = `${scrollHeight}px`;
        this.spacer.style.width = `${this.totalWidth}px`;

        const visibleRows = Math.ceil(this.viewport.clientHeight / ROW_HEIGHT) + 1;
        const first = this.getFirstVisibleRow(visibleRows);
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

        this.columns.forEach((_, colIndex) => {
            const cell = document.createElement("div");
            cell.className = "grid-cell";
            cell.style.width = `${this.columnWidths[colIndex]}px`;
            cell.dataset.col = String(colIndex);

            if (!values) {
                cell.classList.add("loading");
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
                        });
                        cell.appendChild(link);
                    } else {
                        cell.textContent = value.displayValue;
                    }
                    cell.title = value.displayValue.length > 30 ? value.displayValue : "";
                }
            }

            if (this.selected?.row === rowIndex && this.selected.col === colIndex) cell.classList.add("selected");
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
                    // Allow the page to be retried later
                    this.pages.delete(pageIndex);
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
        // Rerendering on mousedown would remove the link before its click event.
        if ((e.target as HTMLElement).closest(".grid-record-link")) return;
        const cell = (e.target as HTMLElement).closest<HTMLElement>(".grid-cell");
        const row = cell?.parentElement?.dataset.row;
        const col = cell?.dataset.col;
        if (row === undefined || col === undefined) return;

        this.selected = { row: Number(row), col: Number(col) };
        this.element.focus();
        this.render();
    }

    private onKeyDown(e: KeyboardEvent): void {
        if (!this.selected) return;

        if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "c") {
            const value = this.getRow(this.selected.row)?.[this.selected.col];
            if (value) {
                this.onCopy(value.isNull ? "" : value.displayValue);
                e.preventDefault();
            }
            return;
        }

        const moves: Record<string, [number, number]> = {
            ArrowUp: [-1, 0],
            ArrowDown: [1, 0],
            ArrowLeft: [0, -1],
            ArrowRight: [0, 1],
        };
        const move = moves[e.key];
        if (!move) return;

        e.preventDefault();
        this.selected = {
            row: Math.max(0, Math.min(this.rowCount - 1, this.selected.row + move[0])),
            col: Math.max(0, Math.min(this.columns.length - 1, this.selected.col + move[1])),
        };
        this.scrollIntoView(this.selected.row);
        this.render();
    }

    private scrollIntoView(rowIndex: number): void {
        if (this.rowCount * ROW_HEIGHT > MAX_SCROLL_HEIGHT) return;

        const top = rowIndex * ROW_HEIGHT;
        if (top < this.viewport.scrollTop) {
            this.viewport.scrollTop = top;
        } else if (top + ROW_HEIGHT > this.viewport.scrollTop + this.viewport.clientHeight) {
            this.viewport.scrollTop = top + ROW_HEIGHT - this.viewport.clientHeight;
        }
    }
}
