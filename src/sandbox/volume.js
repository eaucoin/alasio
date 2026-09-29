/**
 * A session filesystem's volume: an empty JuiceFS volume of the session's own, metadata
 * in Valkey (one logical DB per volume) and data in an S3 bucket under the volume's
 * prefix (session-fs-research E2, E12). One volume per session, never shared — JuiceFS's
 * control file reaches a whole volume, so sharing would let one session reach another's
 * files (notes/juicefs.md).
 *
 * Creating a volume only reserves its namespace and records it; the JuiceFS `format`
 * happens on the first session-host start (JFS_FORMAT), so no throwaway container is
 * needed to create one. Destroying a volume runs `juicefs destroy` and purges the S3
 * prefix in a throwaway session-host container, since destroy needs the JuiceFS binary
 * and, alone, orphans objects if the metadata is already gone (E10).
 */
import { createLogger } from "../shared/log.js";
import { volumeS3Prefix } from "./names.js";

const log = createLogger("session-volume");

export class SessionVolumeManager {
  /**
   * `engine` builds metadata URLs (metadata-engine.js). `store` reserves and frees a
   * volume's DB namespace and keeps its record (persistence). `docker` runs the
   * throwaway destroy container. `config` carries the S3 and image settings.
   */
  constructor({ engine, store, docker, config }) {
    this.engine = engine;
    this.store = store;
    this.docker = docker;
    this.config = config;
  }

  /** Reserve a namespace and record a new, empty volume. It is formatted on first start. */
  create(volumeId) {
    const { dbIndex } = this.store.reserveVolume(volumeId, this.engine.firstNamespace, this.engine.namespaceCount);
    log.info(`created session volume ${volumeId} in metadata namespace ${dbIndex}`);
    return { volumeId, dbIndex };
  }

  /**
   * The environment a session host needs to mount this volume. `JFS_FORMAT` is 1 only
   * until the first successful start marks it formatted (markFormatted).
   */
  mountEnv(volumeId) {
    const record = this.store.getVolume(volumeId);
    if (!record) throw new Error(`no such session volume: ${volumeId}`);
    const { s3Endpoint, s3Bucket, s3AccessKey, s3SecretKey, cacheMb } = this.config;
    return {
      JFS_META: this.engine.metaUrl(record.dbIndex),
      JFS_NAME: volumeId,
      JFS_FORMAT: record.formatted ? "0" : "1",
      JFS_STORAGE: "s3",
      JFS_BUCKET: `${s3Endpoint.replace(/\/+$/, "")}/${s3Bucket}/${volumeS3Prefix(volumeId)}`,
      ACCESS_KEY: s3AccessKey,
      SECRET_KEY: s3SecretKey,
      JFS_CACHE_MB: String(cacheMb ?? 1024),
    };
  }

  markFormatted(volumeId) {
    this.store.setVolumeFormatted(volumeId, true);
  }

  /**
   * Destroy the volume: its JuiceFS metadata and every object under its S3 prefix, then
   * free its namespace and record. The caller must have stopped any session host first
   * (juicefs destroy refuses while a mount is active, E12).
   */
  async destroy(volumeId) {
    const record = this.store.getVolume(volumeId);
    if (!record) return;
    const env = this.mountEnv(volumeId);
    // A throwaway session-host container: it has juicefs, the network, and the password.
    const script =
      'set -e; ' +
      'uuid=$(juicefs status "$JFS_META" 2>/dev/null | grep -o \'"UUID": "[^"]*"\' | head -1 | cut -d\'"\' -f4); ' +
      '[ -n "$uuid" ] && juicefs destroy --yes --force "$JFS_META" "$uuid" 2>&1 | tail -1 || echo "no metadata to destroy"; ' +
      // Purge the S3 prefix directly too, so nothing is orphaned if metadata was already gone (E10).
      'juicefs rmr "$JFS_META" / 2>/dev/null || true';
    try {
      await this.docker.cli([
        "run", "--rm", "--network", this.config.network,
        "--entrypoint", "sh",
        ...Object.entries(env).flatMap(([k, v]) => ["-e", `${k}=${v}`]),
        "-e", `META_PASSWORD_FILE=${this.config.metadataPasswordFile}`,
        "-v", `${this.config.metadataPasswordFile}:${this.config.metadataPasswordFile}:ro`,
        this.config.sessionHostImage, "-c", script,
      ]);
    } catch (error) {
      log.warn(`juicefs destroy for ${volumeId} did not complete cleanly: ${error instanceof Error ? error.message : String(error)}`);
    }
    await this.#purgeS3Prefix(volumeId).catch((error) =>
      log.warn(`purging the S3 prefix of ${volumeId} failed: ${error instanceof Error ? error.message : String(error)}`));
    this.store.deleteVolume(volumeId);
    log.info(`destroyed session volume ${volumeId}`);
  }

  /** Delete every object under the volume's S3 prefix, the belt to destroy's braces. */
  async #purgeS3Prefix(volumeId) {
    if (!this.config.s3) return; // an injected S3 client; absent in unit tests
    const prefix = volumeS3Prefix(volumeId);
    for (;;) {
      const listed = await this.config.s3.list({ prefix, maxKeys: 1000 });
      const keys = listed.contents ?? [];
      if (keys.length === 0) break;
      await Promise.all(keys.map((k) => this.config.s3.delete(k.key)));
      if (!listed.isTruncated) break;
    }
  }
}
