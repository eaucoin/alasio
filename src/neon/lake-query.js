/**
 * Queries the analytics lake, read-only, from this machine (neon/lake/README.md):
 *
 *   npm run lake -- [--format table|csv|json] "<SQL>"
 *
 * The query runs in the lake's own container, which reaches the lake's catalog and
 * files on the stack's private network, so nothing of the lake is published here.
 */
import "dotenv/config";
import { spawn } from "node:child_process";

import { neonLayout } from "../../neon/control/setup.js";
import { resolveStateDir } from "../config.js";
import { composeCommand, NEON_PROJECT } from "./stack.js";

const layout = neonLayout(resolveStateDir(process.env));
const query = spawn(
  "docker",
  [...composeCommand(layout, NEON_PROJECT), "exec", "-T", "lake", "node", "src/query.js", ...process.argv.slice(2)],
  { stdio: "inherit" },
);
query.on("error", (error) => {
  console.error(`cannot run the query: ${error.message}`);
  process.exit(1);
});
query.on("exit", (code) => process.exit(code ?? 1));
