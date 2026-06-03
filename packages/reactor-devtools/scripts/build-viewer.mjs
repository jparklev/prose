// Bundle the Observable reactive viewer (viewer/main.mjs) into a single
// vendored, offline-clean ESM file served by the devtools http server.
// Outputs to BOTH src/public (package source of truth) and dist/public (what
// the built server serves), so a Chrome reload picks up changes with no server
// restart (the server reads public files per-request).
//
// Usage:  node scripts/build-viewer.mjs [--watch]
import * as esbuild from "esbuild";
import { cpSync, mkdirSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const src = join(root, "src", "public");
const dist = join(root, "dist", "public");
const watch = process.argv.includes("--watch");

const options = {
  entryPoints: [join(root, "viewer", "main.mjs")],
  bundle: true,
  format: "esm",
  platform: "browser",
  target: ["es2022"],
  sourcemap: true,
  logLevel: "info",
  outfile: join(src, "viewer.bundle.js"),
};

function syncToDist() {
  mkdirSync(dist, { recursive: true });
  for (const f of ["viewer.bundle.js", "viewer.bundle.js.map", "viewer.html"]) {
    const from = join(src, f);
    if (existsSync(from)) cpSync(from, join(dist, f));
  }
}

if (watch) {
  const ctx = await esbuild.context({
    ...options,
    plugins: [
      {
        name: "sync-to-dist",
        setup(build) {
          build.onEnd((r) => {
            if (r.errors.length === 0) {
              syncToDist();
              console.log("[viewer] rebuilt + synced to dist/public");
            }
          });
        },
      },
    ],
  });
  await ctx.watch();
  console.log("[viewer] watching viewer/main.mjs …");
} else {
  await esbuild.build(options);
  syncToDist();
  console.log("[viewer] built + synced to dist/public");
}
