/**
 * Resolves the media a final response shows (`![caption](path)`, see
 * telegram/rich-media.ts) into files delivered with it.
 *
 * Paths resolve against the conversation's workspace. In a folder workspace they are
 * read on this host, where the agent already had the same access. In a session
 * filesystem they are read through the session's own sandbox, so a path, symlinks
 * included, can reach only what the sandboxed agent itself can, never a host file.
 * Each file is identified by its bytes, held to Telegram's upload limits and alasio's
 * per-reply caps, and read as the reply is queued: the outbox keeps the bytes with the
 * reply until it is sent, so a delivery retried later, by this alasio or the next, sends
 * exactly what the agent showed even if the original is gone by then.
 */
import { readFile, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";

import { Effect, Schema } from "effect";

import type { StoreError } from "../persistence/sql.ts";
import type { FileRead, SessionSandboxes } from "../sandbox/index.ts";
import type { ChatId, MediaAttachment, SendMessageOptions } from "../telegram/client.ts";
import { MEDIA_LIMITS, type ResolvedEmbed, findMediaEmbeds, overLimitNote, placeMedia, sniffMedia } from "../telegram/rich-media.ts";
import { parseWorkspace, type Workspace } from "../workspace/kind.ts";

const SANDBOX_HOME = "/home/agent";

/** What makeReplyMedia is given; see there. */
export interface ReplyMediaOptions {
  readonly workspaceForChat: (chatId: ChatId) => Effect.Effect<string | null, StoreError>;
  readonly sandbox?: Pick<SessionSandboxes["Service"], "readFile"> | null;
}

/** A file a reply shows could not be read: the file system, or the session's sandbox, failed to. */
export class ReplyMediaReadError extends Schema.TaggedError<ReplyMediaReadError>()("ReplyMediaReadError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** A reply ready to enqueue: its text with its embeds placed, and how it is sent. */
export interface PreparedReply {
  readonly text: string;
  readonly options: SendMessageOptions;
}

/** Reads a file in a folder workspace: `{ bytes }`, or `{ note }` when it cannot be attached. */
async function readFolderFile(workingDirectory: string, path: string, maxBytes: number): Promise<FileRead> {
  const expanded = path === "~" || path.startsWith("~/") ? join(homedir(), path.slice(1)) : path;
  const absolute = isAbsolute(expanded) ? expanded : resolve(workingDirectory, expanded);
  let info;
  try {
    info = await stat(absolute);
  } catch {
    return { note: "file not found" };
  }
  if (!info.isFile()) return { note: "not a file" };
  if (info.size > maxBytes) return { note: overLimitNote(info.size, maxBytes) };
  return { bytes: await readFile(absolute) };
}

/** A response's media, resolved and read for delivery with it. */
export interface ReplyMedia {
  /** A file the response shows, read from the workspace: `{ bytes }`, or `{ note }` when it cannot be attached. */
  readonly read: (workspace: Workspace | null, path: string, maxBytes: number) => Effect.Effect<FileRead, ReplyMediaReadError>;
  /**
   * The response ready to enqueue: `{ text, options }`, where `text` has its embeds
   * placed and `options` carries the rich format and the media read, if any.
   */
  readonly prepare: (reply: { readonly chatId: ChatId; readonly text: string }) => Effect.Effect<PreparedReply, StoreError>;
}

/**
 * The reply media of the workspaces `workspaceForChat(chatId)` gives (a conversation's
 * working directory: a folder or a session-filesystem sentinel); `sandbox` reads
 * session-filesystem files and is null when session filesystems are off.
 */
export function makeReplyMedia({ workspaceForChat, sandbox = null }: ReplyMediaOptions): ReplyMedia {
  const read = (workspace: Workspace | null, path: string, maxBytes: number): Effect.Effect<FileRead, ReplyMediaReadError> => {
    if (workspace?.kind === "folder") {
      return Effect.tryPromise({ try: () => readFolderFile(workspace.path, path, maxBytes), catch: (cause) => new ReplyMediaReadError({ cause }) });
    }
    if (workspace?.kind === "sessionfs" && sandbox) {
      const inSandbox = path === "~" || path.startsWith("~/") ? SANDBOX_HOME + path.slice(1) : path;
      return sandbox.readFile(workspace.volumeId, inSandbox, maxBytes).pipe(Effect.mapError((cause) => new ReplyMediaReadError({ cause })));
    }
    return Effect.succeed({ note: "no workspace to read it from" });
  };

  const prepare = Effect.fnUntraced(function*({ chatId, text }: { readonly chatId: ChatId; readonly text: string }): Effect.fn.Return<PreparedReply, StoreError> {
    const embeds = findMediaEmbeds(text);
    if (!embeds.length) return { text, options: { format: "rich" } };
    const workspace = parseWorkspace(yield* workspaceForChat(chatId));
    const media: MediaAttachment[] = [];
    const shown = new Map<string, string>(); // path as written -> id
    const resolved: ResolvedEmbed[] = [];
    let totalBytes = 0;
    for (const embed of embeds) {
      const duplicateOf = shown.get(embed.path);
      if (duplicateOf !== undefined) {
        resolved.push({ duplicateOf });
        continue;
      }
      if (media.length >= MEDIA_LIMITS.perReply) {
        resolved.push({ note: `more than ${MEDIA_LIMITS.perReply} in one reply` });
        continue;
      }
      const file = yield* read(workspace, embed.path, MEDIA_LIMITS.videoBytes).pipe(
        Effect.catchTag("ReplyMediaReadError", (error) =>
          Effect.logWarning(`reply media ${embed.path} could not be read: ${error.message}`).pipe(Effect.as<FileRead>({ note: "could not be read" }))),
      );
      if (!file.bytes) {
        resolved.push({ note: file.note });
        continue;
      }
      const bytes = file.bytes;
      const type = sniffMedia(bytes.subarray(0, 16));
      if (!type) {
        resolved.push({ note: "not an image or video" });
        continue;
      }
      const limit = type.kind === "photo" ? MEDIA_LIMITS.photoBytes : MEDIA_LIMITS.videoBytes;
      if (bytes.length > limit) {
        resolved.push({ note: overLimitNote(bytes.length, limit, type.kind) });
        continue;
      }
      if (totalBytes + bytes.length > MEDIA_LIMITS.totalBytes) {
        resolved.push({ note: `over the ${MEDIA_LIMITS.totalBytes / (1024 * 1024)} MB a reply may carry` });
        continue;
      }
      totalBytes += bytes.length;
      const id = `m${media.length + 1}`;
      media.push({ id, kind: type.kind, animation: Boolean(type.animation), fileName: `${id}.${type.ext}`, content: bytes });
      shown.set(embed.path, id);
      resolved.push({ id, kind: type.kind, caption: embed.caption });
    }
    const options: SendMessageOptions = media.length ? { format: "rich", media } : { format: "rich" };
    return { text: placeMedia(text, resolved), options };
  });

  return { read, prepare };
}
