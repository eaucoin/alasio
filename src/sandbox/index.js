/**
 * The session-filesystem subsystem, assembled from configuration (config.js): the
 * metadata engine, the volume manager, the session-host launcher, and the forwards that
 * carry harnesses to bayma inside each session. Off unless configured; `createSandbox`
 * returns null then, and callers treat a null sandbox as "session filesystems are
 * unavailable, offer only folders".
 *
 * A session is the workspace only. The harness runs in alasio with its own login and
 * state, and reaches the session through one door, bayma (./bayma-forward.js), so the
 * sandbox holds no credential and, in "none" mode, reaches nothing at all.
 *
 * The pieces and the evidence behind them live in session-fs-research; the atlas is in
 * ./README.md.
 */
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { startBaymaForward, SANDBOX_BAYMA_PORT } from "./bayma-forward.js";
import { createDocker } from "./docker.js";
import { createMetadataEngine } from "./metadata-engine.js";
import { SessionHost } from "./session-host.js";
import { assertValidVolumeId } from "./names.js";
import { SessionVolumeManager } from "./volume.js";

/**
 * `stateDir` is alasio's state directory, under which each session's harness directory
 * lives (`harnessDirectory`).
 */
export function createSandbox({ config, store, stateDir, docker = createDocker(), startForward = startBaymaForward } = {}) {
  if (!config) return null;
  const engine = createMetadataEngine(config.metadata);
  const volumes = new SessionVolumeManager({
    engine,
    store: store.sessionVolumes,
    docker,
    config: {
      s3Endpoint: config.s3.endpoint,
      s3Bucket: config.s3.bucket,
      s3AccessKey: config.s3.accessKey,
      s3SecretKey: config.s3.secretKey,
      cacheMb: config.host.cacheMb,
      network: config.host.network,
      metadataPasswordFile: config.metadata.passwordFile,
      sessionHostImage: config.host.sessionHostImage,
    },
  });
  const host = new SessionHost({
    docker,
    config: {
      network: config.host.network,
      agentImage: config.host.agentImage,
      sessionHostImage: config.host.sessionHostImage,
      memoryMb: config.host.memoryMb,
      cpus: config.host.cpus,
      pidsLimit: config.host.pidsLimit,
      hostPublicIp: config.host.hostPublicIp,
      metadataPasswordFile: config.metadata.passwordFile,
    },
  });

  // Per volume, the start in flight (so two harnesses asking at once share one start
  // rather than race a second `docker run` against it) and the forward to its bayma.
  const starting = new Map();
  const forwards = new Map();

  async function ensureRunning(volumeId) {
    const record = store.sessionVolumes.getVolume(volumeId) ?? volumes.create(volumeId);
    if (await host.isRunning(volumeId)) return;
    await host.start(volumeId, { mountEnv: volumes.mountEnv(volumeId), netMode: record.netMode ?? "none" });
    volumes.markFormatted(volumeId);
  }

  function forwardFor(volumeId) {
    if (!forwards.has(volumeId)) {
      const forward = startForward({ connect: () => host.connect(volumeId, SANDBOX_BAYMA_PORT) });
      forward.catch(() => forwards.delete(volumeId));
      forwards.set(volumeId, forward);
    }
    return forwards.get(volumeId);
  }

  async function closeForward(volumeId) {
    const forward = forwards.get(volumeId);
    forwards.delete(volumeId);
    await (await forward?.catch(() => null))?.close();
  }

  return {
    enabled: true,
    engine,
    volumes,
    host,

    /**
     * The directory on this machine a session's harness runs in: empty, the session's
     * own, and none of the workspace's (which is only in the sandbox). Its path keys the
     * harness's sessions to the workspace (Claude Code's project, Codex's thread list).
     */
    harnessDirectory(volumeId) {
      const directory = join(stateDir, "sessionfs", "workspaces", assertValidVolumeId(volumeId));
      mkdirSync(directory, { recursive: true });
      return directory;
    },

    /**
     * Make sure the session host for `volumeId` is running and bayma answers inside it,
     * creating the volume on first use and starting the container if it is down (a host
     * that survived a alasio restart is adopted as it is). Returns `{ bayma: { url,
     * headers } }`: the MCP endpoint a harness reaches the session through.
     */
    async ensureSession(volumeId) {
      if (!starting.has(volumeId)) {
        starting.set(volumeId, ensureRunning(volumeId).finally(() => starting.delete(volumeId)));
      }
      await starting.get(volumeId);
      const { url, headers } = await forwardFor(volumeId);
      return { bayma: { url, headers } };
    },

    /**
     * A file from a session's sandbox, as its agent sees it (see SessionHost.readFile):
     * `{ bytes }` or `{ note }`. Only a running session is read; one is running while
     * its turns deliver their responses.
     */
    async readFile(volumeId, path, maxBytes) {
      if (!(await host.isRunning(volumeId))) return { note: "the session is not running" };
      return await host.readFile(volumeId, path, maxBytes);
    },

    /**
     * alasio is shutting down: close every forward. Session hosts keep running, with the
     * agent's processes and bayma's REPL sessions, for the next alasio to adopt; nothing of
     * the harness is in them to lose.
     */
    async close() {
      await Promise.all([...forwards.keys()].map(closeForward));
    },
  };
}
