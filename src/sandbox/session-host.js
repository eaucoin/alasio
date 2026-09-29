/**
 * A session host: the privileged container alasio starts per session-filesystem session
 * (sandbox/session-host/). It mounts the volume, builds the agent's network, and runs
 * the agent, its harness, and bayma inside one gVisor sandbox whose root is the agent
 * image. alasio adds the harness later with `docker exec agent-exec` (which runs
 * `runsc exec` into the sandbox), so the harness speaks its stdio protocol to alasio as
 * it does today; the credential is a per-session gateway bearer written into the
 * sandbox's env, never a real login (session-fs-research E5, E8).
 */
import { setTimeout as sleep } from "node:timers/promises";
import { createLogger } from "../shared/log.js";
import { sessionHostName } from "./names.js";

const log = createLogger("session-host");
const READY_TIMEOUT_MS = 60_000;

export class SessionHost {
  constructor({ docker, config }) {
    this.docker = docker;
    this.config = config;
  }

  /** The `docker run` argv for a session host on `volumeId`, given its mount env. Pure, for tests. */
  runArgs(volumeId, { mountEnv, netMode }) {
    const c = this.config;
    const gateway = c.gateway ?? {};
    const env = {
      ...mountEnv,
      META_PASSWORD_FILE: c.metadataPasswordFile,
      NET_MODE: netMode === "full" ? "full" : "none",
      GATEWAY_IP: gateway.ip ?? "",
      GATEWAY_PORT: gateway.port ? String(gateway.port) : "",
      HOST_PUBLIC_IP: c.hostPublicIp ?? "",
      CHECKPOINT_ON_STOP: "1",
      RESTORE: "1", // restore a prior checkpoint if one exists; a fresh volume has none
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

  /** Start the session host and wait until its sandbox is ready. */
  async start(volumeId, { mountEnv, netMode }) {
    const name = sessionHostName(volumeId);
    await this.docker.cli(["rm", "--force", name]).catch(() => {});
    await this.docker.cli(this.runArgs(volumeId, { mountEnv, netMode }));
    const deadline = Date.now() + READY_TIMEOUT_MS;
    while (Date.now() < deadline) {
      const { stdout } = await this.docker.cli(["logs", name]).catch(() => ({ stdout: "" }));
      if (/session-host ready/.test(stdout)) {
        log.info(`session host ${name} ready`);
        return;
      }
      if (!(await this.docker.isRunning(name))) {
        throw new Error(`session host ${name} exited before becoming ready:\n${stdout.slice(-2000)}`);
      }
      await sleep(200);
    }
    throw new Error(`session host ${name} did not become ready within ${READY_TIMEOUT_MS / 1000}s`);
  }

  /** Write the env the harness runs with inside the sandbox (the gateway bearer, CODEX_HOME, ...). */
  async writeAgentEnv(volumeId, vars) {
    const lines = Object.entries(vars).map(([k, v]) => `${k}=${v}`).join("\n") + "\n";
    await this.docker.cli(["exec", "-i", sessionHostName(volumeId), "sh", "-c", "cat > /run/agent-env"], { input: lines });
  }

  /** The argv to spawn the harness inside the sandbox; the caller owns the child (the SDK, the app-server). */
  execCommand(volumeId, argv) {
    return this.docker.spawnArgs(["exec", "-i", sessionHostName(volumeId), "agent-exec", ...argv]);
  }

  isRunning(volumeId) {
    return this.docker.isRunning(sessionHostName(volumeId));
  }

  /** Stop the session host; its entrypoint checkpoints the sandbox and unmounts the volume. */
  async stop(volumeId) {
    await this.docker.cli(["stop", "--time", "30", sessionHostName(volumeId)]).catch(() => {});
  }

  async remove(volumeId) {
    await this.docker.cli(["rm", "--force", sessionHostName(volumeId)]).catch(() => {});
  }
}
