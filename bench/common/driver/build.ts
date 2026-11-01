/**
 * Builds dist/: the driver (its fixtures' sources embedded), the store's probe and its S3
 * client, each with its dependencies in one file, the same bytes for every system, which a
 * machine without this package's modules runs with Node alone (alasio runs them in its pods).
 */
import { readFileSync } from "node:fs";

import { build } from "esbuild";

const fixtures = ["bench.bun.js", "fan-out.py"];
const sources = Object.fromEntries(fixtures.map((name) => [name, readFileSync(new URL(`./fixtures/${name}`, import.meta.url), "utf8")]));
const path = (relative: string) => new URL(relative, import.meta.url).pathname;

await build({
  entryPoints: { driver: path("./driver.ts"), probe: path("../store/probe.ts"), s3: path("../store/s3.ts") },
  outdir: path("../dist"),
  outExtension: { ".js": ".mjs" },
  bundle: true,
  platform: "node",
  target: "node24",
  format: "esm",
  define: { FIXTURE_SOURCES: JSON.stringify(sources) },
  // The SDKs' CommonJS dependencies require node's modules.
  banner: { js: "import { createRequire } from 'node:module'; const require = createRequire(import.meta.url);" },
  legalComments: "none",
});
