import type { QueryTab } from "./query/queryTab";

interface TabStripHost {
    getTabs(): readonly QueryTab[];
    getActiveTab(): QueryTab | null;
    getConnectionName(slot: number): string | undefined;
    activateTab(tab: QueryTab): void;
    closeTab(tab: QueryTab): Promise<void>;
    focusEditor(): void;
}

export class TabStrip {
    private closeTabList: (() => void) | null = null;

    constructor(
        private readonly strip: HTMLElement,
        private readonly listButton: HTMLButtonElement,
        private readonly host: TabStripHost,
    ) {}

    render(): void {
        const fragment = document.createDocumentFragment();

        for (const tab of this.host.getTabs()) {
            const el = document.createElement("div");
            el.className = "tab";
            el.classList.toggle("active", tab === this.host.getActiveTab());
            el.classList.toggle("running", tab.running);
            el.classList.toggle("modified", tab.isDirty);
            el.title = tab.connectionSlot !== null ? `${tab.title} - ${this.host.getConnectionName(tab.connectionSlot) ?? ""}` : tab.title;
            if (tab.isDirty) el.title += " (unsaved changes)";
            el.addEventListener("click", () => this.host.activateTab(tab));
            el.addEventListener("auxclick", (e) => {
                if (e.button === 1) void this.host.closeTab(tab);
            });

            const indicator = document.createElement("span");
            indicator.className = "tab-indicator";
            indicator.classList.toggle("connected", tab.connected);
            indicator.classList.toggle("connecting", tab.connecting);

            const label = document.createElement("span");
            label.className = "tab-label";
            label.textContent = tab.title;
            label.title = tab.isDirty ? "Unsaved changes" : tab.title;

            const close = document.createElement("button");
            close.className = "tab-close";
            close.title = "Close";
            close.textContent = "\u00D7";
            close.addEventListener("click", (e) => {
                e.stopPropagation();
                void this.host.closeTab(tab);
            });

            el.append(indicator, label, close);
            fragment.appendChild(el);
        }

        this.strip.replaceChildren(fragment);
        this.scrollActiveTabIntoView();
    }

    private scrollActiveTabIntoView(): void {
        const activeTab = this.host.getActiveTab();
        const index = activeTab ? this.host.getTabs().indexOf(activeTab) : -1;
        const el = index >= 0 ? this.strip.children[index] as HTMLElement | undefined : undefined;
        if (!el) return;

        if (el.offsetLeft < this.strip.scrollLeft)
            this.strip.scrollLeft = el.offsetLeft;
        else if (el.offsetLeft + el.offsetWidth > this.strip.scrollLeft + this.strip.clientWidth)
            this.strip.scrollLeft = el.offsetLeft + el.offsetWidth - this.strip.clientWidth;
    }

    bind(): void {
        // Tabs are kept to a single line with no scroll bar, but can still be scrolled with the mouse wheel
        this.strip.addEventListener("wheel", (e) => {
            const delta = Math.abs(e.deltaX) > Math.abs(e.deltaY) ? e.deltaX : e.deltaY;
            if (!delta) return;
            this.strip.scrollLeft += delta;
            e.preventDefault();
        }, { passive: false });

        new ResizeObserver(() => this.scrollActiveTabIntoView()).observe(this.strip);

        this.listButton.addEventListener("click", () => {
            if (this.closeTabList) this.closeTabList();
            else this.showTabList();
        });
        this.listButton.addEventListener("keydown", (e) => {
            if (e.key === "ArrowDown" && !this.closeTabList) {
                e.preventDefault();
                this.showTabList();
            }
        });
    }

    private showTabList(): void {
        this.closeTabList?.();

        const menu = document.createElement("div");
        menu.className = "tab-list-menu";
        menu.setAttribute("role", "menu");
        menu.setAttribute("aria-label", "Open queries");
        menu.tabIndex = -1;

        const buttons: HTMLButtonElement[] = [];
        for (const tab of this.host.getTabs()) {
            const button = document.createElement("button");
            button.setAttribute("role", "menuitem");
            button.classList.toggle("active", tab === this.host.getActiveTab());
            button.classList.toggle("modified", tab.isDirty);
            const connectionName = tab.connectionSlot !== null ? this.host.getConnectionName(tab.connectionSlot) : undefined;
            button.title = connectionName ? `${tab.title} - ${connectionName}` : tab.title;
            if (tab.isDirty) button.title += " (unsaved changes)";

            const indicator = document.createElement("span");
            indicator.className = "tab-indicator";
            indicator.classList.toggle("connected", tab.connected);
            indicator.classList.toggle("connecting", tab.connecting);

            const label = document.createElement("span");
            label.className = "tab-label";
            label.textContent = tab.title;

            button.append(indicator, label);
            button.addEventListener("click", () => {
                this.closeTabList?.();
                this.host.activateTab(tab);
                this.host.focusEditor();
            });
            buttons.push(button);
            menu.appendChild(button);
        }

        document.body.appendChild(menu);
        const anchor = this.listButton.getBoundingClientRect();
        const bounds = menu.getBoundingClientRect();
        menu.style.left = `${Math.max(0, Math.min(anchor.right - bounds.width, window.innerWidth - bounds.width))}px`;
        menu.style.top = `${Math.max(0, Math.min(anchor.bottom, window.innerHeight - bounds.height))}px`;
        this.listButton.setAttribute("aria-expanded", "true");

        const dismiss = () => this.closeTabList?.();
        const outside = (event: Event) => {
            if (event.target instanceof Node && !menu.contains(event.target) && !this.listButton.contains(event.target)) dismiss();
        };
        const scrolled = (event: Event) => {
            if (event.target instanceof Node && !menu.contains(event.target)) dismiss();
        };
        this.closeTabList = () => {
            this.closeTabList = null;
            document.removeEventListener("pointerdown", outside, true);
            document.removeEventListener("focusin", outside);
            document.removeEventListener("scroll", scrolled, true);
            window.removeEventListener("resize", dismiss);
            window.removeEventListener("blur", dismiss);
            this.listButton.setAttribute("aria-expanded", "false");
            menu.remove();
        };
        document.addEventListener("pointerdown", outside, true);
        document.addEventListener("focusin", outside);
        document.addEventListener("scroll", scrolled, true);
        window.addEventListener("resize", dismiss);
        window.addEventListener("blur", dismiss);

        menu.addEventListener("keydown", (event) => {
            if (event.key === "Escape" || event.key === "Tab") {
                dismiss();
                this.listButton.focus();
                if (event.key === "Escape") event.preventDefault();
            } else if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
                event.preventDefault();
                const index = buttons.indexOf(document.activeElement as HTMLButtonElement);
                const next = event.key === "Home" ? 0 : event.key === "End" ? buttons.length - 1
                    : (index + (event.key === "ArrowDown" ? 1 : -1) + buttons.length) % buttons.length;
                buttons[next]?.focus();
            }
        });

        const active = buttons.find((b) => b.classList.contains("active")) ?? buttons[0];
        active?.scrollIntoView({ block: "nearest" });
        (active ?? menu).focus();
    }
}
