/**
 * Resolves the media a final response shows (`![caption](path)`, see
 * telegram/rich-media.ts) into files delivered with it.
 *
 * Paths resolve against the conversation's workspace. In a folder workspace they are
 * read on this host, where the agent already had the same access. In a session
 * filesystem they are read through the session's own sandbox, so a path, symlinks
 * included, can reach only what the sandboxed agent itself can, never a host file.
 * Each file is identified by its bytes, held to Telegram's upload limits and alasio's
 * per-reply caps, and copied into alasio's state directory, so a delivery retried later
 * sends exactly what the agent showed even if the original is gone by then. The outbox
 * deletes the copies once the reply is sent.
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import type { FileRead, SessionFilesystems } from "../sandbox/index.ts";
import type { Logger } from "../shared/log.ts";
import type { ChatId, MediaAttachment, SendMessageOptions } from "../telegram/client.ts";
import { MEDIA_LIMITS, type ResolvedEmbed, findMediaEmbeds, overLimitNote, placeMedia, sniffMedia } from "../telegram/rich-media.ts";
import { parseWorkspace, type Workspace } from "../workspace/kind.ts";

const SANDBOX_HOME = "/home/agent";

/** What ReplyMedia is given; see its constructor. */
export interface ReplyMediaOptions {
  readonly stateDir: string;
  readonly workspaceForChat: (chatId: ChatId) => string | null;
  readonly sandbox?: Pick<SessionFilesystems, "readFile"> | null;
  readonly log?: Logger | undefined;
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

export class ReplyMedia {
  private readonly root: string;
  private readonly workspaceForChat: (chatId: ChatId) => string | null;
  private readonly sandbox: Pick<SessionFilesystems, "readFile"> | null;
  private readonly log: Logger | undefined;

  /**
   * `workspaceForChat(chatId)` returns the conversation's working directory (a folder or
   * a session-filesystem sentinel); `sandbox` reads session-filesystem files and is null
   * when session filesystems are off.
   */
  constructor({ stateDir, workspaceForChat, sandbox = null, log }: ReplyMediaOptions) {
    this.root = join(stateDir, "reply-media");
    this.workspaceForChat = workspaceForChat;
    this.sandbox = sandbox;
    this.log = log;
  }

  async read(workspace: Workspace | null, path: string, maxBytes: number): Promise<FileRead> {
    if (workspace?.kind === "folder") return await readFolderFile(workspace.path, path, maxBytes);
    if (workspace?.kind === "sessionfs" && this.sandbox) {
      const inSandbox = path === "~" || path.startsWith("~/") ? SANDBOX_HOME + path.slice(1) : path;
      return await this.sandbox.readFile(workspace.volumeId, inSandbox, maxBytes);
    }
    return { note: "no workspace to read it from" };
  }

  /**
   * The response ready to enqueue: `{ text, options }`, where `text` has its embeds
   * placed and `options` carries the rich format and the copied media, if any.
   */
  async prepare({ chatId, text, key }: { readonly chatId: ChatId; readonly text: string; readonly key: string }): Promise<PreparedReply> {
    const embeds = findMediaEmbeds(text);
    if (!embeds.length) return { text, options: { format: "rich" } };
    const workspace = parseWorkspace(this.workspaceForChat(chatId));
    const dir = join(this.root, key);
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
      let read: FileRead;
      try {
        read = await this.read(workspace, embed.path, MEDIA_LIMITS.videoBytes);
      } catch (error) {
        this.log?.warn?.(`reply media ${embed.path} could not be read: ${error instanceof Error ? error.message : String(error)}`);
        read = { note: "could not be read" };
      }
      if (!read.bytes) {
        resolved.push({ note: read.note });
        continue;
      }
      const type = sniffMedia(read.bytes.subarray(0, 16));
      if (!type) {
        resolved.push({ note: "not an image or video" });
        continue;
      }
      const limit = type.kind === "photo" ? MEDIA_LIMITS.photoBytes : MEDIA_LIMITS.videoBytes;
      if (read.bytes.length > limit) {
        resolved.push({ note: overLimitNote(read.bytes.length, limit, type.kind) });
        continue;
      }
      if (totalBytes + read.bytes.length > MEDIA_LIMITS.totalBytes) {
        resolved.push({ note: `over the ${MEDIA_LIMITS.totalBytes / (1024 * 1024)} MB a reply may carry` });
        continue;
      }
      totalBytes += read.bytes.length;
      const id = `m${media.length + 1}`;
      mkdirSync(dir, { recursive: true, mode: 0o700 });
      const file = join(dir, `${id}.${type.ext}`);
      writeFileSync(file, read.bytes, { mode: 0o600 });
      media.push({ id, kind: type.kind, animation: Boolean(type.animation), file });
      shown.set(embed.path, id);
      resolved.push({ id, kind: type.kind, caption: embed.caption });
    }
    const options: SendMessageOptions = media.length ? { format: "rich", media, mediaDir: dir } : { format: "rich" };
    return { text: placeMedia(text, resolved), options };
  }
}
