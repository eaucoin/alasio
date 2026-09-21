import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { preflightConfiguredMcpServers } from "./preflight.js";

function argumentValue(name) {
  const index = process.argv.indexOf(name);
  if (index === -1) {
    return null;
  }
  const value = process.argv[index + 1];
  if (!value || value.startsWith("--")) {
    throw new Error(`${name} requires a value`);
  }
  return value;
}

const workingDirectory = resolve(
  argumentValue("--workspace")
  ?? process.env.WORKING_DIRECTORY
  ?? "/home/operator/monorepo",
);
const temporaryRoot = await mkdtemp(
  join(tmpdir(), "alasio-breadbutter-doctor-"),
);

try {
  await preflightConfiguredMcpServers(
    process.env,
    {
      mcp_servers: {
        bayma_repl: {
          command: process.env.BAYMA_REPL_COMMAND ?? "bayma-repl",
          args: [
            "mcp-stdio",
            "--state-dir",
            join(temporaryRoot, "runtime-state"),
          ],
          startup_timeout_sec: 60,
        },
      },
    },
    workingDirectory,
  );
  process.stdout.write(
    `ok: Bayma REPL Python and Rust loaded monorepo Breadbutter from ${workingDirectory}\n`,
  );
} finally {
  await rm(temporaryRoot, { recursive: true, force: true });
}
