/**
 * A session host: the privileged container alasio starts per session filesystem
 * (sandbox/session-host/). It mounts the volume, builds the agent's network, and runs
 * bayma and every process the agent starts inside one gVisor sandbox whose root is the
 * agent image. The harness is not in it: Claude Code and Codex run in alasio and reach the
 * sandbox only through bayma, over `connect` (see ./bayma-forward.js). So no model login
 * and no harness state is ever inside a session (session-fs-research E5, E8).
 */
import { setTimeout as sleep } from "node:timers/promises";
import { createLogger } from "../shared/log.js";
import { inSpan } from "../telemetry/index.js";
import { overLimitNote } from "../telegram/rich-media.js";
import { sessionHostName } from "./names.js";

const log = createLogger("session-host");
// Covers the entrypoint's own wait for bayma (BAYMA_READY_TIMEOUT_S, 90s) and the mount,
// network, and sandbox before it, so a host that is slow but coming up is not given up on.
const READY_TIMEOUT_MS = 150_000;

export class SessionHost {
  constructor({ docker, config }) {
    this.docker = docker;
    this.config = config;
  }

  /** The `docker run` argv for a session host on `volumeId`, given its mount env. Pure, for tests. */
  runArgs(volumeId, { mountEnv, netMode }) {
    const c = this.config;
    const env = {
      ...mountEnv,
      META_PASSWORD_FILE: c.metadataPasswordFile,
      NET_MODE: netMode === "full" ? "full" : "none",
      HOST_PUBLIC_IP: c.hostPublicIp ?? "",
      // A stop checkpoints the sandbox onto the volume, and the next start restores it,
      // so the agent's processes and bayma's REPL sessions come back as they were.
      CHECKPOINT_ON_STOP: "1",
      RESTORE: "1",
      // What bayma exports its telemetry with, to the drain the entrypoint then starts
      // beside it (./telemetry.js); absent when alasio exports none.
      ...(Object.keys(c.telemetryEnv ?? {}).length > 0 ? { SANDBOX_TELEMETRY: JSON.stringify(c.telemetryEnv) } : {}),
    };
    return [
      "run", "--detach", "--name", sessionHostName(volumeId),
      "--network", c.network, "--privileged",
      "--memory", `${c.memoryMb ?? 2048}m`, "--cpus", String(c.cpus ?? 2),
      "--pids-limit", String(c.pidsLimit ?? 512),
      "--mount", `type=image,source=${c.agentImage},target=/agent-root`,
      "-v", `${c.metadataPasswordFile}:${c.metadataPasswordFile}:ro`,
      ...Object.entries(env).flatMap(([k, v]) => ["-e", `${k}=${v}`]),
      c.sessionHostImage,
    ];
  }

  /**
   * Start the session host and wait until bayma answers inside its sandbox. Any earlier
   * container of the name is removed first; its checkpoint, if it left one, is on the
   * volume, where the new container restores it. The start is the span
   * `alasio.session_host.start`, with an event for each step the entrypoint stamps.
   */
  async start(volumeId, { mountEnv, netMode }) {
    const name = sessionHostName(volumeId);
    await inSpan("alasio.session_host.start", { attributes: { "alasio.volume.id": volumeId, "alasio.sandbox.net_mode": netMode } }, async (span) => {
      await this.docker.cli(["rm", "--force", name]).catch(() => {});
      await this.docker.cli(this.runArgs(volumeId, { mountEnv, netMode }));
      const runAt = Date.now();
      const deadline = runAt + READY_TIMEOUT_MS;
      while (Date.now() < deadline) {
        const { stdout, stderr = "" } = await this.docker.cli(["logs", name]).catch(() => ({ stdout: "" }));
        if (/session-host ready/.test(stdout)) {
          // Each step is stamped with the time since the entrypoint began, just after
          // `docker run` returned.
          for (const [, step, ms] of stderr.matchAll(/^session-host (\w+) (\d+)ms$/gmu)) {
            span.addEvent(step, runAt + Number(ms));
          }
          log.info(`session host ${name} ready`);
          return;
        }
        if (!(await this.docker.isRunning(name))) {
          throw new Error(`session host ${name} exited before becoming ready:\n${stdout.slice(-2000)}`);
        }
        await sleep(200);
      }
      throw new Error(`session host ${name} did not become ready within ${READY_TIMEOUT_MS / 1000}s`);
    });
  }

  isRunning(volumeId) {
    return this.docker.isRunning(sessionHostName(volumeId));
  }

  /**
   * A connection to `port` on the sandbox's own loopback, as a child process whose stdin
   * and stdout are its two directions (`agent-connect`, made from inside as the agent).
   * The caller owns the child; it exits when either side closes.
   */
  connect(volumeId, port) {
    return this.docker.spawn(["exec", "-i", sessionHostName(volumeId), "agent-connect", String(port)], {
      stdio: ["pipe", "pipe", "ignore"],
    });
  }

  /**
   * A file as the sandboxed agent sees it: `{ bytes }`, or `{ note }` saying why not.
   * The read runs inside the sandbox as the agent, so the path (relative to /workspace,
   * symlinks included) reaches only what the agent itself can, never the host. Files
   * over `maxBytes` are not read.
   */
  async readFile(volumeId, path, maxBytes) {
    const script = 'f="$1"; [ -f "$f" ] || exit 3; s=$(stat -L -c %s -- "$f") || exit 3; [ "$s" -le "$2" ] || { printf %s "$s" >&2; exit 4; }; exec cat -- "$f"';
    try {
      const { stdout } = await this.docker.cli(
        ["exec", sessionHostName(volumeId), "agent-exec", "sh", "-c", script, "sh", path, String(maxBytes)],
        { encoding: "buffer", maxBuffer: maxBytes + 1024 * 1024 },
      );
      return { bytes: stdout };
    } catch (error) {
      if (error?.code === 3) return { note: "file not found" };
      if (error?.code === 4) {
        return { note: overLimitNote(Number(String(error.stderr ?? "").trim()), maxBytes) };
      }
      throw error;
    }
  }

  /** Stop the session host; its entrypoint checkpoints the sandbox and unmounts the volume. */
  async stop(volumeId) {
    await this.docker.cli(["stop", "--time", "30", sessionHostName(volumeId)]).catch(() => {});
  }

  async remove(volumeId) {
    await this.docker.cli(["rm", "--force", sessionHostName(volumeId)]).catch(() => {});
  }
}
