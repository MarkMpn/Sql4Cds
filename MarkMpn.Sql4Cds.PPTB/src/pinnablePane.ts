export interface PinnablePaneOptions {
    /** The name of the pane, used in the pin button labels */
    name: string;
    pane: HTMLElement;
    pin: HTMLButtonElement;
    /** The side tab shown in place of the pane while it is unpinned */
    tab: HTMLButtonElement;
    splitter: HTMLElement;
    /** The element containing the item to focus when the flyout is opened */
    content: HTMLElement;
    /** The side of the window the pane is docked to */
    side: "left" | "right";
    pinned: boolean;
    /** Stores the new pin state */
    savePinned: (pinned: boolean) => Promise<void>;
    /** Identifies elements outside the pane that belong to it, such as popup menus, so they don't collapse the flyout */
    isPartOfPane?: (target: Element) => boolean;
}

const MIN_WIDTH = 160;

/**
 * A side pane that can be pinned open, or unpinned so it collapses to a tab at the side of the window and opens as a
 * flyout over the rest of the content, in the same style as the tool windows in SSMS.
 */
export class PinnablePane {
    private pinnedState: boolean;
    private openState: boolean;

    constructor(private readonly options: PinnablePaneOptions) {
        this.pinnedState = options.pinned;
        this.openState = options.pinned;
        this.update();
        this.bind();
    }

    get pinned(): boolean {
        return this.pinnedState;
    }

    get open(): boolean {
        return this.openState;
    }

    private update(): void {
        const { pane, pin, tab, splitter, name } = this.options;
        pane.hidden = !this.openState;
        pane.classList.toggle("auto-hide", !this.pinnedState);
        splitter.hidden = !this.pinnedState;
        tab.hidden = this.pinnedState;
        tab.setAttribute("aria-expanded", String(this.openState && !this.pinnedState));
        pin.setAttribute("aria-pressed", String(this.pinnedState));
        const label = this.pinnedState ? `Unpin ${name}` : `Pin ${name}`;
        pin.title = label;
        pin.setAttribute("aria-label", label);
    }

    private collapse(): void {
        this.openState = false;
        this.update();
    }

    private bind(): void {
        const { pane, pin, tab, splitter, content, side, isPartOfPane } = this.options;

        pin.addEventListener("click", async () => {
            this.pinnedState = !this.pinnedState;
            this.openState = this.pinnedState;
            this.update();
            if (!this.pinnedState) tab.focus();
            pin.disabled = true;
            try {
                await this.options.savePinned(this.pinnedState);
            } finally {
                pin.disabled = false;
            }
        });
        tab.addEventListener("click", () => {
            this.openState = !this.openState;
            this.update();
            if (this.openState) content.querySelector<HTMLElement>('[tabindex="0"]')?.focus();
        });
        const collapseOutside = (event: Event) => {
            if (this.pinnedState || !this.openState || !(event.target instanceof Node)) return;
            if (pane.contains(event.target) || tab.contains(event.target)) return;
            if (event.target instanceof Element && isPartOfPane?.(event.target)) return;
            this.collapse();
        };
        document.addEventListener("pointerdown", collapseOutside);
        document.addEventListener("focusin", collapseOutside);
        document.addEventListener("keydown", (event) => {
            if (event.key !== "Escape" || this.pinnedState || !this.openState) return;
            event.preventDefault();
            this.collapse();
            tab.focus();
        });
        splitter.addEventListener("mousedown", (event) => {
            event.preventDefault();
            const startX = event.clientX;
            const width = pane.getBoundingClientRect().width;
            const direction = side === "left" ? 1 : -1;
            const move = (moveEvent: MouseEvent) => {
                const max = Math.max(MIN_WIDTH, document.body.clientWidth - 300);
                const newWidth = width + (moveEvent.clientX - startX) * direction;
                pane.style.width = `${Math.min(max, Math.max(MIN_WIDTH, newWidth))}px`;
            };
            const up = () => {
                document.body.classList.remove("resizing-pane");
                window.removeEventListener("mousemove", move);
                window.removeEventListener("mouseup", up);
            };
            document.body.classList.add("resizing-pane");
            window.addEventListener("mousemove", move);
            window.addEventListener("mouseup", up);
        });
    }
}
