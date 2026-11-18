import { ConfirmationResult } from "./lsp/protocol";

export function showConfirmation(message: string): Promise<ConfirmationResult> {
    return new Promise((resolve) => {
        const dialog = document.createElement("dialog");
        dialog.className = "confirm-dialog";

        const form = document.createElement("form");
        form.method = "dialog";

        const title = document.createElement("h2");
        title.textContent = "Confirm";

        const text = document.createElement("p");
        text.textContent = message;

        const buttons = document.createElement("div");
        buttons.className = "dialog-buttons";

        for (const [label, value, primary] of [
            ["Yes", ConfirmationResult.Yes, true],
            ["Yes to all", ConfirmationResult.All, false],
            ["No", ConfirmationResult.No, false],
        ] as const) {
            const button = document.createElement("button");
            button.value = String(value);
            button.textContent = label;
            if (primary) button.className = "primary";
            buttons.appendChild(button);
        }

        form.append(title, text, buttons);
        dialog.appendChild(form);
        document.body.appendChild(dialog);

        dialog.addEventListener("close", () => {
            dialog.remove();
            const value = Number.parseInt(dialog.returnValue, 10);
            resolve(value === ConfirmationResult.Yes || value === ConfirmationResult.All ? value : ConfirmationResult.No);
        });

        dialog.showModal();
    });
}

export function showUnsavedChangesDialog(title: string): Promise<"save" | "discard" | "cancel"> {
    return new Promise((resolve) => {
        const dialog = document.createElement("dialog");
        dialog.className = "unsaved-dialog";

        const form = document.createElement("form");
        form.method = "dialog";

        const heading = document.createElement("h2");
        heading.textContent = "Unsaved changes";

        const message = document.createElement("p");
        message.textContent = `Save changes to ${title} before closing?`;

        const buttons = document.createElement("div");
        buttons.className = "dialog-buttons";
        for (const [value, label, primary] of [
            ["save", "Save", true],
            ["discard", "Discard", false],
            ["cancel", "Cancel", false],
        ] as const) {
            const button = document.createElement("button");
            button.value = value;
            button.textContent = label;
            if (primary) button.className = "primary";
            buttons.appendChild(button);
        }

        form.append(heading, message, buttons);
        dialog.appendChild(form);
        document.body.appendChild(dialog);

        dialog.addEventListener("close", () => {
            dialog.remove();
            const value = dialog.returnValue;
            resolve(value === "save" || value === "discard" ? value : "cancel");
        });

        dialog.showModal();
    });
}
