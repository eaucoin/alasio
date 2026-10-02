/**
 * Queries the analytics lake, read-only (neon/lake/README.md):
 *
 *   npm run lake -- [--format table|csv|json] "<SQL>"
 *
 * The query runs in the lake's own container, which reaches the lake's catalog and
 * files, so nothing of the lake is published here: on Kubernetes through `kubectl exec`
 * into its Deployment (ALASIO_NAMESPACE and ALASIO_LAKE_DEPLOYMENT, alasio and alasio-lake
 * unless set), on Docker through `docker compose exec` on the stack's private network.
 */
import "dotenv/config";
import { spawn } from "node:child_process";

import { neonLayout } from "../../neon/control/setup.js";
import { resolveStateDir } from "../config.js";
import { onKubernetes } from "../kube/config.js";
import { composeCommand, NEON_PROJECT } from "./stack.js";

const args = ["node", "src/query.js", ...process.argv.slice(2)];
const [command, commandArgs] = onKubernetes()
  ? ["kubectl", [
    "--namespace", process.env.ALASIO_NAMESPACE?.trim() || "alasio",
    "exec", `deployment/${process.env.ALASIO_LAKE_DEPLOYMENT?.trim() || "alasio-lake"}`, "--", ...args,
  ]]
  : ["docker", [...composeCommand(neonLayout(resolveStateDir(process.env)), NEON_PROJECT), "exec", "-T", "lake", ...args]];

const query = spawn(command, commandArgs, { stdio: "inherit" });
query.on("error", (error) => {
  console.error(`cannot run the query: ${error.message}`);
  process.exit(1);
});
query.on("exit", (code) => process.exit(code ?? 1));
