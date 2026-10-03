// @ts-nocheck
/**
 * Installs the lake's DuckDB extensions into a directory, at image build time, and
 * loads each to prove it works, so the running lake never fetches one.
 *
 *   node install-extensions.ts <directory>
 */
import { DuckDBInstance } from "@duckdb/node-api";

import { EXTENSIONS } from "./extensions.ts";

const [directory] = process.argv.slice(2);
if (!directory) {
  console.error("usage: node install-extensions.ts <directory>");
  process.exit(2);
}
const instance = await DuckDBInstance.create(":memory:", { extension_directory: directory });
const db = await instance.connect();
for (const extension of EXTENSIONS) {
  await db.run(`install ${extension}`);
  await db.run(`load ${extension}`);
}
const installed = (await db.runAndReadAll("select extension_name, extension_version from duckdb_extensions() where installed and install_path <> '(BUILT-IN)'")).getRowObjectsJS();
console.log(installed.map(({ extension_name, extension_version }) => `${extension_name} ${extension_version}`).join("\n"));
db.closeSync();
instance.closeSync();
