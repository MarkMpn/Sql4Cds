import * as vscode from "vscode";
import { StructuredValue } from "./structuredValue";

const scheme = "sql4cds-value";

/** Opens immutable, syntax-highlighted result values without creating temporary files. */
export class StructuredValueViewer implements vscode.Disposable, vscode.TextDocumentContentProvider {
  private readonly contents = new Map<string, string>();
  private readonly registration: vscode.Disposable;
  private readonly closeRegistration: vscode.Disposable;
  private nextId = 1;

  public constructor() {
    this.registration = vscode.workspace.registerTextDocumentContentProvider(scheme, this);
    this.closeRegistration = vscode.workspace.onDidCloseTextDocument(document => {
      if (document.uri.scheme === scheme) { this.contents.delete(document.uri.toString()); }
    });
  }

  public provideTextDocumentContent(uri: vscode.Uri): string {
    return this.contents.get(uri.toString()) ?? "";
  }

  public async open(value: StructuredValue, title = "Result value"): Promise<void> {
    const extension = value.language === "json" ? "json" : "xml";
    const uri = vscode.Uri.from({
      scheme,
      path: `/${this.nextId++}/${sanitizeTitle(title)}.${extension}`
    });
    this.contents.set(uri.toString(), value.formatted);
    const document = await vscode.workspace.openTextDocument(uri);
    await vscode.languages.setTextDocumentLanguage(document, value.language);
    await vscode.window.showTextDocument(document, {
      viewColumn: vscode.ViewColumn.Beside,
      preserveFocus: false,
      preview: true
    });
  }

  public dispose(): void {
    this.registration.dispose();
    this.closeRegistration.dispose();
    this.contents.clear();
  }
}

function sanitizeTitle(title: string): string {
  return title.trim().replace(/[\\/:*?"<>|]/g, "_") || "Result value";
}
