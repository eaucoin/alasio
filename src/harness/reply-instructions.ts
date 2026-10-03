// @ts-nocheck
/**
 * What alasio tells each agent about its replies, on top of the harness's own prompt:
 * how to show the operator an image or video (telegram/rich-media.ts). Claude Code gets
 * it appended to its system prompt, Codex as developer instructions after any the
 * operator set in their own Codex config.
 */
import { MEDIA_LIMITS } from "../telegram/rich-media.ts";

const mb = (bytes) => bytes / (1024 * 1024);

export const REPLY_INSTRUCTIONS = [
  "Your replies reach the operator in Telegram through alasio.",
  "To show the operator an image or video file, embed it on a line of its own with Markdown image syntax",
  "and a local path, `![short caption](path)`, absolute or relative to the working directory: alasio sends",
  "the file itself in its place, several embeds in one paragraph as a collage.",
  "Naming a path any other way, such as in backticks, only refers to the file.",
  `Photos up to ${mb(MEDIA_LIMITS.photoBytes)} MB and videos up to ${mb(MEDIA_LIMITS.videoBytes)} MB, at most ${MEDIA_LIMITS.perReply} per reply.`,
].join(" ");

/** `existing` instructions followed by alasio's, or alasio's alone. */
export function withReplyInstructions(existing) {
  const own = typeof existing === "string" ? existing.trim() : "";
  return own ? `${own}\n\n${REPLY_INSTRUCTIONS}` : REPLY_INSTRUCTIONS;
}
