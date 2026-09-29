/**
 * A workspace is one of two kinds, and both are carried in the one `working_directory`
 * string alasio already threads through conversations, parked sessions, and the harness
 * registry, so the whole of that plumbing keeps working unchanged:
 *
 * - a **folder**: an absolute host path (today's behaviour), a trusted session with the
 *   host's own access;
 * - a **session filesystem**: the sentinel `sessionfs:<volumeId>`, an empty isolated
 *   volume in a gVisor sandbox (see ../sandbox/ and session-fs-research).
 *
 * Every place that must tell them apart parses the string here rather than testing the
 * prefix itself, so the vocabulary lives in one module.
 */
import { assertValidVolumeId, isValidVolumeId } from "../sandbox/names.js";

const SESSION_FS_PREFIX = "sessionfs:";

/** The `working_directory` value for a session-filesystem workspace on `volumeId`. */
export function sessionFsWorkspace(volumeId) {
  return `${SESSION_FS_PREFIX}${assertValidVolumeId(volumeId)}`;
}

/**
 * Parse a `working_directory` into `{ kind: "folder", path }` or
 * `{ kind: "sessionfs", volumeId }`. A malformed sentinel is rejected rather than
 * mistaken for a folder path.
 */
export function parseWorkspace(workingDirectory) {
  if (typeof workingDirectory !== "string" || workingDirectory === "") {
    return null;
  }
  if (workingDirectory.startsWith(SESSION_FS_PREFIX)) {
    const volumeId = workingDirectory.slice(SESSION_FS_PREFIX.length);
    if (!isValidVolumeId(volumeId)) {
      throw new Error(`malformed session-filesystem workspace: ${JSON.stringify(workingDirectory)}`);
    }
    return { kind: "sessionfs", volumeId };
  }
  return { kind: "folder", path: workingDirectory };
}

/** True when the workspace is a session filesystem (a sandboxed, isolated volume). */
export function isSessionFs(workingDirectory) {
  return typeof workingDirectory === "string" && workingDirectory.startsWith(SESSION_FS_PREFIX);
}
