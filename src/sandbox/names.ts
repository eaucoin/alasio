/**
 * Naming for session filesystems. A workspace is one of two kinds (see
 * ../workspace/kind.ts): a folder, or a session filesystem identified by a volume id.
 * A volume id is what alasio generates and stores, and its session's Sandbox, Service and
 * volume claim are named for it.
 */
import { Schema } from "effect";

/**
 * A volume id is a DNS label of 3–63 characters from `[a-z0-9-]` that starts and ends
 * with a letter or digit, as the names of the Sandbox, its Service and its pod must be.
 * alasio generates ids that already satisfy this, and this guards against any that would
 * not.
 */
const VOLUME_ID_PATTERN = /^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/;

/** A volume id, as what comes from outside alasio is decoded. */
const VolumeId = Schema.String.check(Schema.isPattern(VOLUME_ID_PATTERN));

/**
 * A session's token, `<volumeId>.<random>` (../kube/sandboxes.ts newToken), decoded into
 * the session it names and the rest.
 */
export const SessionToken = Schema.TemplateLiteralParser([VolumeId, ".", Schema.NonEmptyString]);

export function isValidVolumeId(volumeId: unknown): volumeId is string {
  return typeof volumeId === "string" && VOLUME_ID_PATTERN.test(volumeId);
}

export function assertValidVolumeId(volumeId: unknown): string {
  if (!isValidVolumeId(volumeId)) {
    throw new Error(`invalid session volume id: ${JSON.stringify(volumeId)} (need 3–63 chars of [a-z0-9-], starting and ending with a letter or digit)`);
  }
  return volumeId;
}

/** A fresh volume id, e.g. `fs-1a2b3c4d5e`, valid by construction. */
export function newVolumeId(random: () => string = () => crypto.randomUUID()): string {
  return `fs-${random().replace(/-/g, "").slice(0, 12)}`;
}
