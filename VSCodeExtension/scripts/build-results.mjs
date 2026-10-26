import { build } from "esbuild";
import { cp, mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const shared = join(dirname(root), "MarkMpn.Sql4Cds.PPTB", "src");
const images = join(dirname(root), "MarkMpn.Sql4Cds.Controls", "Images");
const imageNames = (await readdir(images)).filter(name => name.endsWith(".ico"));
await build({
  entryPoints: [join(root, "src", "webview", "results.ts")],
  bundle: true, platform: "browser", format: "iife", target: "es2022",
  outfile: join(root, "dist", "results.js"), sourcemap: true,
  define: { PLAN_IMAGE_NAMES: JSON.stringify(imageNames.map(name => name.slice(0, -4))) }
});
const styles = await Promise.all([
  readFile(join(shared, "styles.css"), "utf8"),
  readFile(join(root, "src", "webview", "results.css"), "utf8")
]);
await writeFile(join(root, "dist", "results.css"), styles.join("\n"));
await mkdir(join(root, "dist", "plan-images"), { recursive: true });
await Promise.all(imageNames.map(name => cp(join(images, name), join(root, "dist", "plan-images", name))));
