/**
 * bayma, the MCP server alasio itself gives its agents, next to whatever
 * servers the operator has configured for each harness on this machine.
 *
 * bayma runs from its image, pinned by digest, as a container beside alasio's
 * own through the host's Docker. Its container keeps what alasio's keeps (see
 * container/run.sh), so its REPL sessions can do what alasio's agents can: the
 * same user, /home and /tmp at the same paths, the host's network, Docker,
 * Tailscale, Stripe, Google Cloud, and systemd, and the environment it is
 * given. It
 * keeps its own processes, though, which is what lets bayma snapshot idle
 * REPL sessions as it stops and restore them whole in the next container.
 * Each harness adapter turns `baymaLaunch` into its own MCP config shape.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { statSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { resolveStateDir } from "../config.js";
import { createLogger } from "../shared/log.js";

export const BAYMA_SERVER_NAME = "bayma";

export const BAYMA_IMAGE =
  "ghcr.io/eaucoin/bayma:0.7.0@sha256:88f473c02f861712299e3b2d69674b6c114b8a07959f2ac4cd5dc525ad334c38";

/**
 * A server that follows one on the same state directory waits for it to
 * snapshot its REPL sessions and stop, so allow well beyond a warm start.
 */
export const BAYMA_STARTUP_TIMEOUT_MS = 60_000;

const DOCKER_SOCKET = "/var/run/docker.sock";

/** The container label that names the state directory a bayma serves. */
const STATE_LABEL = "alasio.bayma.state-dir";

// Mounted from the host at the same path, read-only unless bayma's sessions
// write through them.
const HOST_MOUNTS = [
  "/etc/passwd:/etc/passwd:ro",
  "/etc/group:/etc/group:ro",
  "/etc/localtime:/etc/localtime:ro",
  "/home:/home",
  "/tmp:/tmp",
  `${DOCKER_SOCKET}:${DOCKER_SOCKET}`,
  "/usr/bin/docker:/usr/bin/docker:ro",
  "/usr/libexec/docker/cli-plugins:/usr/libexec/docker/cli-plugins:ro",
  "/var/run/tailscale:/var/run/tailscale",
  "/usr/bin/tailscale:/usr/bin/tailscale:ro",
  "/usr/bin/stripe:/usr/bin/stripe:ro",
  "/usr/lib/google-cloud-sdk:/usr/lib/google-cloud-sdk:ro",
  // The host's systemd, which restart-alasio-standalone.sh restarts alasio
  // through, and the host's systemctl to reach it with.
  "/run/dbus/system_bus_socket:/run/dbus/system_bus_socket",
  "/run/systemd:/run/systemd:ro",
  "/usr/bin/systemctl:/usr/bin/systemctl:ro",
  "/usr/lib/x86_64-linux-gnu/systemd:/usr/lib/x86_64-linux-gnu/systemd:ro",
];

// What bayma's container needs of its own: the image's PATH, which finds
// CRIU and bayma's runtimes, and bayma's own settings.
const OWN_VARIABLE = /^(PATH|HOSTNAME|BAYMA_.*)$/u;

// A bayma stopping snapshots its REPL sessions and holds its state directory
// until it has; the next one on that directory waits for it, then starts.
const WAIT_THEN_RUN = `ids=$(docker ps --quiet --filter "label=${STATE_LABEL}=$0"); [ -z "$ids" ] || docker wait $ids >/dev/null; exec docker run --label "${STATE_LABEL}=$0" "$@"`;

const log = createLogger("bayma");

function sanitizePathToken(value) {
  return String(value).replace(/[^A-Za-z0-9_.-]+/g, "-").replace(/^[-.]+|-+$/g, "") || "default";
}

/**
 * Sessions are checkpointed, bayma's default, stated so a alasio restart can
 * never take them with it: a checkpointed session outlives the server,
 * restored whole where its processes were snapshotted, and otherwise resumed
 * with its history and what its code checkpointed.
 */
function stdioCommand(stateDir, env) {
  const environment = Object.keys(env)
    .filter((name) => env[name] !== undefined && !OWN_VARIABLE.test(name))
    .sort()
    .flatMap((name) => ["--env", name]);
  return {
    command: "/bin/sh",
    args: [
      "-c",
      WAIT_THEN_RUN,
      stateDir,
      "--interactive",
      "--rm",
      "--log-driver",
      "none",
      "--user",
      `${process.getuid()}:${process.getgid()}`,
      "--group-add",
      String(statSync(DOCKER_SOCKET).gid),
      "--network",
      "host",
      // What CRIU needs to snapshot and restore the container's processes.
      "--cap-add",
      "CHECKPOINT_RESTORE",
      "--cap-add",
      "SYS_PTRACE",
      "--security-opt",
      "seccomp=unconfined",
      // As alasio's own container: Docker's AppArmor profile keeps a container
      // off the host's D-Bus, and so from its systemd.
      "--security-opt",
      "apparmor=unconfined",
      ...HOST_MOUNTS.flatMap((mount) => ["--volume", mount]),
      ...environment,
      BAYMA_IMAGE,
      "mcp-stdio",
      "--default-durability",
      "checkpointed",
      "--state-dir",
      stateDir,
    ],
  };
}

/**
 * The command that serves bayma to one conversation under one harness.
 *
 * bayma leases its state directory to a single server, and a Codex thread
 * keeps its server alive after the conversation switches to Claude, so the
 * directory is keyed by harness as well as conversation. It lives under
 * alasio's state directory and is stable across restarts, so the next server
 * finds the previous one's sessions there.
 */
export function baymaLaunch({ harness, threadKey, env = process.env }) {
  return stdioCommand(
    join(resolveStateDir(env), "bayma", sanitizePathToken(harness), sanitizePathToken(threadKey)),
    env,
  );
}

async function checkBayma(env) {
  const stateDir = await mkdtemp(join(tmpdir(), "alasio-bayma-check-"));
  const client = new Client({ name: "alasio-bayma-check", version: "1.0.0" });
  try {
    const { command, args } = stdioCommand(stateDir, env);
    const timeout = { timeout: BAYMA_STARTUP_TIMEOUT_MS };
    await client.connect(new StdioClientTransport({ command, args, env, stderr: "ignore" }), timeout);
    const { tools } = await client.listTools(undefined, timeout);
    if (tools.length === 0) {
      throw new Error("bayma started but exposed no tools");
    }
    log.info(`ready (tools=${tools.length})`);
  } finally {
    await client.close().catch(() => {});
    await rm(stateDir, { recursive: true, force: true });
  }
}

let readiness = null;

/**
 * Prove once per alasio process that bayma starts and serves its tools, so a
 * harness never begins a turn believing it has a REPL it cannot reach. The
 * check runs against a throwaway state directory; a failure is retried on the
 * next turn.
 */
export function ensureBaymaReady(env) {
  readiness ??= checkBayma(env).catch((error) => {
    readiness = null;
    throw new Error(`bayma is unavailable: ${error instanceof Error ? error.message : String(error)}`);
  });
  return readiness;
}
