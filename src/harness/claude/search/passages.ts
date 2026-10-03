/**
 * What of a Claude Code transcript entry is searched: its passages.
 *
 * Every string an entry holds is text to search except three kinds, which are
 * told apart by the entry's own structure, never by hand:
 *
 * - machine values: ids, digests, signatures, timestamps, base64 data, and
 *   labels such as a model's name;
 * - copies: a tool's result is kept both in the message the model read and
 *   in `toolUseResult`, and only the message's is searched;
 * - repeats: each distinct text is one passage, however often it is written,
 *   by its `digest`; where it was written is kept beside it.
 *
 * A passage's kind is the entry's type, and for a message the content block's
 * too, as Claude Code writes them: `user.text`, `assistant.thinking`,
 * `assistant.tool_use`, `user.tool_result`, `attachment`, `queue-operation`.
 * Prose is searched with English stemming; tool calls and their output,
 * code and paths more than words, as they are.
 */
import type { SessionStoreEntry } from "@anthropic-ai/claude-agent-sdk";
import { createHash } from "node:crypto";

import { storable } from "../session-store.ts";

/** The most characters one passage holds; longer text is split at a boundary. */
export const PASSAGE_CHARS = 2000;

/** A piece of an entry's text to search, as passagesOf gives it. */
export interface Passage {
  /** Where it is in the entry: its place among the entry's passages. */
  readonly part: number;
  readonly kind: string;
  /** Whether it is searched as English. */
  readonly prose: boolean;
  readonly text: string;
  readonly digest: string;
}

/**
 * A content block of an entry's message, as found: Claude Code writes the
 * Messages API's, and anything else is read by its strings.
 */
type ContentBlock = Readonly<Record<string, unknown>>;

/** Kinds read as words; the rest are searched as written. */
const PROSE_KINDS = new Set([
  "user.text",
  "assistant.text",
  "assistant.thinking",
  "queue-operation",
  "summary",
  "ai-title",
  "custom-title",
  "last-prompt",
  "attachment",
  "system",
]);

/** Fields that hold machine values wherever they appear. */
const MACHINE_FIELDS = new Set([
  "agentId",
  "base64",
  "cwd",
  "data",
  "entrypoint",
  "gitBranch",
  "id",
  "isMeta",
  "isSidechain",
  "leafUuid",
  "logicalParentUuid",
  "media_type",
  "messageId",
  "parentUuid",
  "promptId",
  "requestId",
  "sessionId",
  "signature",
  "slug",
  "sourceToolAssistantUUID",
  "sourceToolUseID",
  "timestamp",
  "tool_use_id",
  "toolUseID",
  "type",
  "userType",
  "uuid",
  "version",
]);

/** Fields holding copies of what the entry's message already has. */
const COPY_FIELDS = new Set(["toolUseResult", "mcpMeta"]);

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;
const HEX = /^[0-9a-f]{32,}$/iu;
const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/u;
/** A long unbroken run of base64 or similar: encoded data, not text. */
const ENCODED = /^[A-Za-z0-9+/=_-]{120,}$/u;
/** A short single word, such as `end_turn` or a model's name: a label. */
const LABEL = /^\S{1,40}$/u;
const URL_TEXT = /^[a-z][a-z0-9+.-]*:\/\//iu;

function isMachineValue(text: string): boolean {
  return UUID.test(text) || HEX.test(text) || TIMESTAMP.test(text) || ENCODED.test(text);
}

/**
 * The strings of `value` worth searching, in order. Outside a message's own
 * content (`labels: false`), short single words are labels and are skipped.
 */
function stringsOf(value: unknown, { labels }: { readonly labels: boolean }, out: string[] = []): string[] {
  if (typeof value === "string") {
    const text = value.trim();
    if (text && !isMachineValue(text) && (labels || !LABEL.test(text) || URL_TEXT.test(text))) out.push(text);
  } else if (Array.isArray(value)) {
    const items: readonly unknown[] = value;
    for (const item of items) stringsOf(item, { labels }, out);
  } else if (value !== null && typeof value === "object") {
    const fields: [string, unknown][] = Object.entries(value);
    for (const [name, item] of fields) {
      if (!MACHINE_FIELDS.has(name) && !COPY_FIELDS.has(name)) stringsOf(item, { labels }, out);
    }
  }
  return out;
}

/** The text of one block of a message's content, or null for none. */
function blockText(block: ContentBlock): unknown {
  switch (block["type"]) {
    case "text":
      return block["text"];
    case "thinking":
      return block["thinking"];
    case "tool_use":
      return [block["name"], ...stringsOf(block["input"], { labels: true })].join("\n");
    case "tool_result":
      return typeof block["content"] === "string" ? block["content"] : stringsOf(block["content"], { labels: true }).join("\n");
    case "image":
    case "redacted_thinking":
      return null;
    default:
      return stringsOf(block, { labels: true }).join("\n");
  }
}

/** The texts of an entry, each with its kind, before splitting. */
function textsOf(entry: SessionStoreEntry): Array<{ kind: string; text: unknown }> {
  const type = typeof entry.type === "string" ? entry.type : "unknown";
  const message = entry["message"];
  if ((type === "user" || type === "assistant") && message) {
    const content = typeof message === "object" && "content" in message ? message.content : undefined;
    if (typeof content === "string") return [{ kind: `${type}.text`, text: content }];
    if (!Array.isArray(content)) return [];
    const blocks: readonly unknown[] = content;
    return blocks
      .filter((block): block is ContentBlock => block !== null && typeof block === "object")
      .map((block) => ({ kind: `${type}.${block["type"] ?? "unknown"}`, text: blockText(block) }));
  }
  return [{ kind: type, text: stringsOf(entry, { labels: false }).join("\n") }];
}

/** Splits text into pieces of at most `max` characters, at the widest boundary it can. */
export function split(text: string, max = PASSAGE_CHARS): string[] {
  const pieces: string[] = [];
  let rest = text;
  while (rest.length > max) {
    const window = rest.slice(0, max);
    let cut = -1;
    for (const boundary of ["\n\n", "\n", " "]) {
      const at = window.lastIndexOf(boundary);
      if (at >= max / 2) {
        cut = at + boundary.length;
        break;
      }
    }
    if (cut < 0) {
      // Never between the halves of a surrogate pair.
      const code = rest.charCodeAt(max - 1);
      cut = code >= 0xd800 && code <= 0xdbff ? max - 1 : max;
    }
    pieces.push(rest.slice(0, cut).trim());
    rest = rest.slice(cut);
  }
  pieces.push(rest.trim());
  return pieces.filter(Boolean);
}

/** The digest that makes each distinct text one passage. */
export function digest(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

/**
 * An entry's passages, `[{ part, kind, prose, text, digest }]`, in the order
 * its text appears. `prose` passages are searched as English.
 */
export function passagesOf(entry: SessionStoreEntry): Passage[] {
  const passages: Passage[] = [];
  for (const { kind, text } of textsOf(entry)) {
    if (typeof text !== "string") continue;
    for (const piece of split(storable(text))) {
      passages.push({ part: passages.length, kind, prose: PROSE_KINDS.has(kind), text: piece, digest: digest(piece) });
    }
  }
  return passages;
}
