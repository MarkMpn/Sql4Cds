export function bindToolbarMenu(toggle: HTMLButtonElement, menu: HTMLElement): void {
    const items = () => Array.from(menu.querySelectorAll<HTMLButtonElement>("button:not(:disabled)"));
    const close = (restoreFocus = false) => {
        menu.hidden = true;
        toggle.setAttribute("aria-expanded", "false");
        if (restoreFocus) toggle.focus();
    };
    const open = () => {
        if (toggle.disabled) return;
        menu.hidden = false;
        toggle.setAttribute("aria-expanded", "true");
        items()[0]?.focus();
    };
    toggle.addEventListener("click", () => menu.hidden ? open() : close());
    toggle.addEventListener("keydown", (event) => {
        if (event.key === "ArrowDown") {
            event.preventDefault();
            open();
        }
    });
    menu.addEventListener("click", (event) => {
        if (event.target instanceof Element && event.target.closest("button")) close(true);
    });
    menu.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
            event.preventDefault();
            close(true);
        } else if (event.key === "ArrowDown" || event.key === "ArrowUp" || event.key === "Home" || event.key === "End") {
            event.preventDefault();
            const buttons = items();
            const index = buttons.indexOf(document.activeElement as HTMLButtonElement);
            const next = event.key === "Home" ? 0 : event.key === "End" ? buttons.length - 1
                : (index + (event.key === "ArrowDown" ? 1 : -1) + buttons.length) % buttons.length;
            buttons[next]?.focus();
        }
    });
    document.addEventListener("pointerdown", (event) => {
        if (event.target instanceof Node && !menu.parentElement?.contains(event.target)) close();
    });
    document.addEventListener("focusin", (event) => {
        if (event.target instanceof Node && !menu.parentElement?.contains(event.target)) close();
    });
}
