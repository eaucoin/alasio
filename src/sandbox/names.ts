/**
 * Naming for session filesystems. A workspace is one of two kinds (see
 * ../workspace/kind.ts): a folder, or a session filesystem identified by a volume id.
 * A volume id is what alasio generates and stores, and its session's Sandbox, Service and
 * volume claim are named for it.
 */
import { Schema } from "effect";

/**
 * The longest a volume id is: a DNS label's 63 characters, less a claim template's name
 * of up to CLAIM_TEMPLATE_MAX and the hyphen after it, so its volume claims' names,
 * `<template>-<id>` (../kube/sandboxes.ts claimName), are DNS labels too, as alasio's
 * admission policy of its clones holds them to (cli/src/manifests/admission.ts).
 */
export const VOLUME_ID_MAX = 58;

/** The longest a claim template's name is, of which a volume id's claims are named. */
export const CLAIM_TEMPLATE_MAX = 63 - 1 - VOLUME_ID_MAX;

/**
 * A volume id is a DNS label of 3–58 characters from `[a-z0-9-]` that starts and ends
 * with a letter or digit, as the names of the Sandbox, its Service and its pod must be,
 * and short enough for its claims' (VOLUME_ID_MAX). alasio generates ids that already
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
