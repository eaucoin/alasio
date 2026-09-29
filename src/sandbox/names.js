/**
 * Naming for session filesystems. A workspace is one of two kinds (see
 * ../workspace/kind.js): a folder on the host, or a session filesystem identified by a
 * volume id. A volume id is what alasio generates and stores; the JuiceFS volume name,
 * the S3 prefix, and the metadata namespace all derive from it.
 */

/**
 * JuiceFS requires a volume name of 3–63 characters from `[A-Za-z0-9-]` (verified in
 * juicefs format; see session-fs-research E12/E1). alasio generates ids that already
 * satisfy this, and this guards against any that would not.
 */
const VOLUME_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9-]{2,62}$/;

export function isValidVolumeId(volumeId) {
  return typeof volumeId === "string" && VOLUME_ID_PATTERN.test(volumeId);
}

export function assertValidVolumeId(volumeId) {
  if (!isValidVolumeId(volumeId)) {
    throw new Error(`invalid session volume id: ${JSON.stringify(volumeId)} (need 3–63 chars of [A-Za-z0-9-])`);
  }
  return volumeId;
}

/** A fresh volume id, e.g. `fs-1a2b3c4d5e`, valid by construction. */
export function newVolumeId(random = () => crypto.randomUUID()) {
  return `fs-${random().replace(/-/g, "").slice(0, 12)}`;
}

/** The container name alasio gives a session host, one per volume. */
export function sessionHostName(volumeId) {
  return `alasio-session-${assertValidVolumeId(volumeId)}`;
}

/** The S3 prefix (a bucket subpath) a volume's data lives under. */
export function volumeS3Prefix(volumeId) {
  return `${assertValidVolumeId(volumeId)}/`;
}
