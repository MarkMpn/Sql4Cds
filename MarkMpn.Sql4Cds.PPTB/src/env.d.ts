/// <reference types="vite/client" />

declare const __SQL4CDS_VERSION__: string;

declare module "monaco-editor/esm/vs/editor/edcore.main" {
    export * from "monaco-editor/esm/vs/editor/editor.api";
}

declare module "monaco-editor/esm/vs/basic-languages/sql/sql.contribution";
