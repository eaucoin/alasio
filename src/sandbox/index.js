/**
 * The session-filesystem subsystem, assembled from configuration (config.js): the
 * metadata engine, the volume manager, the session-host launcher, and the forwards that
 * carry harnesses to bayma inside each session. Off unless configured; `createSandbox`
 * returns null then, and callers treat a null sandbox as "session filesystems are
 * unavailable, offer only folders".
 *
 * When alasio exports telemetry, bayma inside each session exports its own through the
 * session's telemetry drain, which alasio relays where it exports (./telemetry.js).
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
import { createLogger } from "../shared/log.js";
import { startBaymaForward, SANDBOX_BAYMA_PORT } from "./bayma-forward.js";
import { createDocker } from "./docker.js";
import { createMetadataEngine } from "./metadata-engine.js";
import { SessionHost } from "./session-host.js";
import { assertValidVolumeId } from "./names.js";
import { DRAIN_READ_PORT, sandboxBaymaTelemetryEnv, sandboxResource, startTelemetryRelay } from "./telemetry.js";
import { SessionVolumeManager } from "./volume.js";

const log = createLogger("sandbox");

/** The forwarder relayed telemetry is exported through, loaded only once it is needed. */
async function loadForwarder(env) {
  const { createOtlpForwarder } = await import("../telemetry/forward.js");
  return createOtlpForwarder(env, { warn: (message) => log.warn(message) });
}

/**
 * `stateDir` is alasio's state directory, under which each session's harness directory
 * lives (`harnessDirectory`). `env` holds the standard OpenTelemetry variables that
 * say whether and where sessions' telemetry is exported.
 */
export function createSandbox({
  config,
  store,
  stateDir,
  env = process.env,
  docker = createDocker(),
  startForward = startBaymaForward,
  startRelay = startTelemetryRelay,
  createForwarder = loadForwarder,
} = {}) {
  if (!config) return null;
  const telemetryEnv = sandboxBaymaTelemetryEnv(env);
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
      telemetryEnv,
    },
  });

  // Per volume, the start in flight (so two harnesses asking at once share one start
  // rather than race a second `docker run` against it) and the forward to its bayma.
  const starting = new Map();
  const forwards = new Map();
  // Per volume, the relay of its telemetry, while one runs, and the forwarder they share.
  const relays = new Map();
  let forwarder = null;

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

  /** Relays the session's telemetry, unless alasio exports none or a relay already runs. */
  function relayFor(volumeId) {
    if (Object.keys(telemetryEnv).length === 0 || relays.has(volumeId)) return;
    forwarder ??= createForwarder(env).catch((error) => {
      forwarder = null; // tried again for the next relay
      throw error;
    });
    const relay = forwarder.then((loaded) => startRelay({
      volumeId,
      connect: () => host.connect(volumeId, DRAIN_READ_PORT),
      isRunning: () => host.isRunning(volumeId),
      forwarder: loaded,
      stamp: sandboxResource(volumeId, env),
      onEnd: () => relays.delete(volumeId),
    }));
    relay.catch((error) => {
      relays.delete(volumeId);
      log.warn(`${volumeId}: cannot relay the session's telemetry: ${error.message}`);
    });
    relays.set(volumeId, relay);
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
     * that survived a alasio restart is adopted as it is), and relay its telemetry while
     * it runs. Returns `{ bayma: { url, headers } }`: the MCP endpoint a harness reaches
     * the session through.
     */
    async ensureSession(volumeId) {
      if (!starting.has(volumeId)) {
        starting.set(volumeId, ensureRunning(volumeId).finally(() => starting.delete(volumeId)));
      }
      await starting.get(volumeId);
      relayFor(volumeId);
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
     * alasio is shutting down: close every forward and relay. Session hosts keep running,
     * with the agent's processes and bayma's REPL sessions, for the next alasio to adopt;
     * nothing of the harness is in them to lose, and their drains hold what bayma records
     * until the next alasio relays it.
     */
    async close() {
      await Promise.all([
        ...[...forwards.keys()].map(closeForward),
        ...[...relays.values()].map(async (relay) => (await relay.catch(() => null))?.close()),
      ]);
      (await forwarder?.catch(() => null))?.close();
    },
  };
}
