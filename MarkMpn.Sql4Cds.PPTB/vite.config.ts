import { defineConfig, type Plugin } from "vite";

/**
 * PPTB loads dist/index.html directly, so the entry script must be a classic (non-module) script at the end of
 * the body rather than the <script type="module" crossorigin> that Vite emits in the <head> by default. The
 * crossorigin attributes are also removed as the pptb-webview:// protocol doesn't send CORS headers.
 */
function classicScriptPlugin(): Plugin {
    return {
        name: "pptb-classic-script",
        enforce: "post",
        transformIndexHtml(html) {
            const scripts: string[] = [];
            html = html.replace(/<script\b[^>]*\bsrc="[^"]+"[^>]*><\/script>\s*/g, (tag) => {
                scripts.push(tag.replace(/\s+type="module"/, "").replace(/\s+crossorigin(="[^"]*")?/, "").trim());
                return "";
            });
            return html
                .replace(/(<link\b[^>]*?)\s+crossorigin(="[^"]*")?/g, "$1")
                .replace("</body>", `    ${scripts.join("\n    ")}\n</body>`);
        },
    };
}

export default defineConfig((configEnv) => {
    return {
        base: "./",
        root: "./src",
        publicDir: "../public",
        plugins: [classicScriptPlugin()],
        worker: {
            format: "iife",
        },
        build: {
            outDir: "../dist",
            emptyOutDir: true,
            assetsDir: "assets",
            cssCodeSplit: false,
            sourcemap: configEnv.mode === "development",
            chunkSizeWarningLimit: 5000,
            rollupOptions: {
                output: {
                    // Use IIFE format for compatibility with iframe srcdoc loading
                    // ES modules can have issues when loaded via file:// URLs in iframes
                    format: "iife",
                    // Bundle everything into a single file to avoid module loading issues
                    inlineDynamicImports: true,
                    // Disable chunking since we're bundling everything
                    manualChunks: undefined,
                    entryFileNames: "app.js",
                    assetFileNames: "assets/[name][extname]",
                },
            },
        },
    };
});
