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
  const gateway = new SessionGateway({
    providers: {
      anthropic: { upstream: config.gateway.anthropicUpstream, credentialSource: tokenFromFile(config.gateway.anthropicTokenFile) },
      openai: { upstream: config.gateway.openaiUpstream, credentialSource: tokenFromFile(config.gateway.openaiTokenFile) },
    },
  });

  return {
    enabled: true,
    engine,
    volumes,
    host,
    gateway,
    /** Start the credential gateway; the app calls this once at startup. */
    async startGateway() {
      const { port } = await gateway.listen(config.gateway.port);
      log.info(`session filesystems enabled; gateway on ${config.gateway.port} (bound ${port})`);
    },
  };
}
