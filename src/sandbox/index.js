/**
 * The session-filesystem subsystem, assembled from configuration (config.js): the
 * metadata engine, the volume manager, the session-host launcher, and the credential
 * gateway. Off unless configured; `createSandbox` returns null then, and callers treat
 * a null sandbox as "session filesystems are unavailable, offer only folders".
 *
 * The pieces and the evidence behind them live in session-fs-research; the atlas is in
 * ./README.md.
 */
import { readFileSync } from "node:fs";
import { createLogger } from "../shared/log.js";
import { createDocker } from "./docker.js";
import { SessionGateway } from "./gateway.js";
import { createMetadataEngine } from "./metadata-engine.js";
import { SessionHost } from "./session-host.js";
import { SessionVolumeManager } from "./volume.js";

const log = createLogger("sandbox");

/** A credential read fresh from its file each call, so a token added while running is picked up; null when absent. */
function tokenFromFile(path) {
  return () => {
    if (!path) return null;
    try {
      return readFileSync(path, "utf8").trim() || null;
    } catch {
      return null;
    }
  };
}

/**
 * The access token from a local `claude` credentials JSON (`claudeAiOauth.accessToken`),
 * read fresh each call so a token Claude Code refreshes on disk is picked up; null when
 * absent or unreadable. The real login is never copied, only sourced at request time.
 */
function oauthTokenFromFile(path) {
  return () => {
    if (!path) return null;
    try {
      const token = JSON.parse(readFileSync(path, "utf8"))?.claudeAiOauth?.accessToken;
      return typeof token === "string" && token ? token : null;
    } catch {
      return null;
    }
  };
}

export function createSandbox({ config, store, docker = createDocker() } = {}) {
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
      gateway: { ip: config.gateway.ip, port: config.gateway.port },
    },
  });
  // A subscription OAuth login (anthropicOAuthFile) takes precedence over an API-key file
  // and is injected as a Bearer; either is read fresh per request, neither is copied.
  const anthropic = config.gateway.anthropicOAuthFile
    ? { upstream: config.gateway.anthropicUpstream, credentialSource: oauthTokenFromFile(config.gateway.anthropicOAuthFile), oauth: true }
    : { upstream: config.gateway.anthropicUpstream, credentialSource: tokenFromFile(config.gateway.anthropicTokenFile) };
  const gateway = new SessionGateway({
    providers: {
      anthropic,
      openai: { upstream: config.gateway.openaiUpstream, credentialSource: tokenFromFile(config.gateway.openaiTokenFile) },
    },
  });

  const gatewayUrl = `http://${config.gateway.ip}:${config.gateway.port}`;

  return {
    enabled: true,
    engine,
    volumes,
    host,
    gateway,
    gatewayUrl,

    /** Start the credential gateway; the app calls this once at startup. */
    async startGateway() {
      const { port } = await gateway.listen(config.gateway.port);
      log.info(`session filesystems enabled; gateway on ${config.gateway.port} (bound ${port})`);
    },

    /**
     * Make sure the session host for `volumeId` is running and ready for a harness,
     * creating the volume on first use and starting the container if it is down, then
     * issue a fresh gateway bearer and write the harness's env inside the sandbox.
     * `buildEnv({ bearer, gatewayUrl })` returns the env the harness runs with (where
     * the bearer goes is the harness's business). Returns `execCommand(argv)`, the argv
     * to spawn the harness inside the sandbox.
     */
    async ensureSession(volumeId, buildEnv) {
      const record = store.sessionVolumes.getVolume(volumeId) ?? volumes.create(volumeId);
      if (!(await host.isRunning(volumeId))) {
        await host.start(volumeId, { mountEnv: volumes.mountEnv(volumeId), netMode: record.netMode ?? "none" });
        volumes.markFormatted(volumeId);
      }
      const bearer = gateway.issueBearer(volumeId);
      await host.writeAgentEnv(volumeId, buildEnv({ bearer, gatewayUrl }));
      return {
        bearer,
        gatewayUrl,
        baymaHttpUrl: "http://127.0.0.1:7290/mcp",
        execCommand: (argv, env) => host.execCommand(volumeId, argv, env),
        spawn: (argv, env) => host.spawn(volumeId, argv, env),
        transcriptExists: (sessionId) => host.transcriptExists(volumeId, sessionId),
      };
    },

    /** A turn or session ended: revoke its bearer, and optionally stop the host (checkpointing it). */
    async releaseSession(volumeId, { stop = false } = {}) {
      gateway.revokeSession(volumeId);
      if (stop) await host.stop(volumeId);
    },
  };
}
