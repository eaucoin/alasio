/**
 * Naming for session filesystems. A workspace is one of two kinds (see
 * ../workspace/kind.ts): a folder, or a session filesystem identified by a volume id.
 * A volume id is what alasio generates and stores, and its session's workspace, which the
 * sandbox gateway holds, is named for it (sessionKey).
 */
import { Schema } from "effect";

/** The longest a volume id is: a DNS label's 63 characters, with room to spare, as the ids alasio has stored are. */
export const VOLUME_ID_MAX = 58;

/**
 * A volume id is a DNS label of 3–58 characters from `[a-z0-9-]` that starts and ends
 * with a letter or digit, as the names of what is made of it must be, a workspace's
 * directory among them, of VOLUME_ID_MAX at most. alasio generates ids that already
 * satisfy this, and this guards against any that would not.
 */
const VOLUME_ID_PATTERN = new RegExp(`^[a-z0-9][a-z0-9-]{1,${VOLUME_ID_MAX - 2}}[a-z0-9]$`, "u");

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
    throw new Error(`invalid session volume id: ${JSON.stringify(volumeId)} (need 3–${VOLUME_ID_MAX} chars of [a-z0-9-], starting and ending with a letter or digit)`);
  }
  return volumeId;
}

/** A fresh volume id, e.g. `fs-1a2b3c4d5e`, valid by construction. */
export function newVolumeId(random: () => string = () => crypto.randomUUID()): string {
  return `fs-${random().replace(/-/g, "").slice(0, 12)}`;
}

/** The key a session filesystem's workspace is bound by in the sandbox service: `sessionfs:<volumeId>`. */
export const sessionKey = (volumeId: string): string => `sessionfs:${volumeId}`;

/** The volume id of the session filesystem `key` binds, where it binds one; else null. */
export function volumeIdOfKey(key: string): string | null {
  const volumeId = key.startsWith("sessionfs:") ? key.slice("sessionfs:".length) : "";
  return isValidVolumeId(volumeId) ? volumeId : null;
}
