// @ts-nocheck
/**
 * Mirroring a rollout file: one whose size or place changed since the store
 * last kept it is kept again. Codex only appends to a
 * rollout, so a file that grew with the same first line gets only its new
 * bytes; any other change, as when Codex's migration rewrites a file, gets
 * all of them. A file moved by archiving only has its place updated.
 *
 * Files Codex has compressed are not mirrored: the copy of such a file is
 * the one kept before it was compressed.
 */
import { createHash } from "node:crypto";
import { open } from "node:fs/promises";
import { join } from "node:path";

import { parseRolloutName } from "./files.ts";

/** How much of a file one read for its first line takes. */
const HEAD_READ_BYTES = 64 * 1024;

/** A file's first line, without its newline, or null while it has none. */
async function readHead(handle) {
  const pieces = [];
  for (let position = 0; ; ) {
    const buffer = Buffer.alloc(HEAD_READ_BYTES);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, position);
    if (bytesRead === 0) return null;
    const piece = buffer.subarray(0, bytesRead);
    const end = piece.indexOf(0x0a);
    if (end >= 0) {
      pieces.push(piece.subarray(0, end));
      return Buffer.concat(pieces);
    }
    pieces.push(piece);
    position += bytesRead;
  }
}

/** The bytes from `start` to `end`, or fewer if the file is shorter now. */
async function readRange(handle, start, end) {
  const buffer = Buffer.alloc(end - start);
  let read = 0;
  while (read < buffer.length) {
    const { bytesRead } = await handle.read(buffer, read, buffer.length - read, start + read);
    if (bytesRead === 0) break;
    read += bytesRead;
  }
  return buffer.subarray(0, read);
}

/**
 * The rollout id the history of a file starts in: its first line is Codex's
 * `session_meta`, whose `history_base` names it for a fork or a revert.
 */
function historyBaseOf(head) {
  try {
    return JSON.parse(head.toString("utf8")).payload?.history_base?.thread_id ?? null;
  } catch {
    return null;
  }
}

/**
 * Mirrors one rollout file, as files.ts gives it, into `store`. `known` maps
 * each name the store keeps to its `{ path, size, headDigest }`, as
 * `store.list()` gives them, and is kept up to date. Returns whether any of
 * its bytes were mirrored.
 */
export async function mirrorRollout({ store, home, known, file }) {
  if (file.compressed) return false;
  const kept = known.get(file.name);
  if (kept?.size === file.size) {
    if (kept.path !== file.path) {
      await store.move(file.name, file.path);
      kept.path = file.path;
    }
    return false;
  }
  let handle;
  try {
    handle = await open(join(home, file.path), "r");
  } catch (error) {
    // Moved or removed since it was listed: it is seen where it went.
    if (error.code === "ENOENT") return false;
    throw error;
  }
  try {
    const head = await readHead(handle);
    if (!head) return false;
    const headDigest = createHash("sha256").update(head).digest("hex");
    const start = kept && file.size > kept.size && kept.headDigest === headDigest ? kept.size : 0;
    const bytes = await readRange(handle, start, file.size);
    // Shorter than it was listed: rewritten meanwhile, and seen again.
    if (start + bytes.length !== file.size) return false;
    const rollout = {
      name: file.name,
      path: file.path,
      ...parseRolloutName(file.name),
      historyBase: historyBaseOf(head),
      size: file.size,
      headDigest,
      modifiedMs: file.modifiedMs,
    };
    await store.save(rollout, { start, bytes });
    known.set(file.name, { path: file.path, size: file.size, headDigest });
    return true;
  } finally {
    await handle.close();
  }
}
