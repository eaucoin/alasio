/**
 * Queries the analytics lake, read-only (neon/lake/README.md):
 *
 *   npm run lake -- [--format table|csv|json] "<SQL>"
 *
 * The query runs in the lake's own pod, which reaches the lake's catalog and files, so
 * nothing of the lake is published: through `kubectl exec` into its Deployment
 * (ALASIO_NAMESPACE and ALASIO_LAKE_DEPLOYMENT, alasio and alasio-lake unless set), as the
 * cluster's current kubectl context reaches it.
 */
import { spawn } from "node:child_process";

const query = spawn("kubectl", [
  "--namespace", process.env.ALASIO_NAMESPACE?.trim() || "alasio",
  "exec", `deployment/${process.env.ALASIO_LAKE_DEPLOYMENT?.trim() || "alasio-lake"}`, "--",
  "node", "src/query.js", ...process.argv.slice(2),
], { stdio: "inherit" });
query.on("error", (error) => {
  console.error(`cannot run the query: ${error.message}`);
  process.exit(1);
});
query.on("exit", (code) => process.exit(code ?? 1));
