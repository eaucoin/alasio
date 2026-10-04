/**
 * Prepares an agent's Markdown for Telegram rich messages (Bot API 10.1+, `sendRichMessage`
 * with `rich_message.markdown`), which render tables, headings, lists, and code natively.
 *
 * Telegram's rich Markdown is GitHub-flavoured Markdown plus syntax an agent does not mean
 * when it writes ordinary prose: `$…$` is a formula, `||…||` a spoiler, `==…==` a
 * highlight, and anything shaped like an HTML tag is taken as one and dropped if unknown
 * (`List<String>` loses `<String>`). Outside code, those are escaped so the text reads as
 * written; code spans and fenced blocks are passed through untouched, since Telegram shows
 * them literally. The escapes are the ones the Bot API was seen to honour: a backslash
 * before `$`, `|`, `=`, and `*`, and the `&lt;` entity for `<` (a backslash is shown
 * literally there).
 */

import { MEDIA_LINE } from "./rich-media.ts";

/** Rich messages hold 32768 characters; this leaves room for the escapes. */
const RICH_MESSAGE_MAX_CHARS = 30_000;
/** Rich messages hold 500 blocks (rows, list items, paragraphs, ...); this bounds a part's lines. */
const RICH_MESSAGE_MAX_LINES = 450;

/** How large each part `splitRichMarkdown` makes may be. */
export interface RichMarkdownLimits {
  maxChars?: number;
  maxLines?: number;
}

const FENCE = /^ {0,3}(`{3,}|~{3,})/;
const TABLE_ROW = /^\s*\|/;
// A line's block-structure prefix: blockquote markers, then a list marker if any.
const BLOCK_PREFIX = /^(\s*(?:>\s?)*)((?:[*+-]|\d{1,9}[.)])\s+)?/;

/** The fence run a line opens with, or null. */
function openingFence(line: string) {
  return FENCE.exec(line)?.[1] ?? null;
}

/** Whether `line` closes a block opened by `opener`: the same character, at least as long, nothing after. */
function closesFence(line: string, opener: string) {
  const run = openingFence(line);
  return !!run && run[0] === opener[0] && run.length >= opener.length && line.trim() === run;
}

function escapeProse(text: string, { tableRow }: { tableRow: boolean }) {
  let out = text
    // An entity the agent wrote is shown as written, not decoded.
    .replace(/&(?=#?[A-Za-z0-9]+;)/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/\$/g, "\\$")
    .replace(/==/g, "\\=\\=")
    // A star with space on both sides is arithmetic, not emphasis.
    .replace(/(^|\s)\*(?=\s)/g, "$1\\*");
  // Table rows keep their pipes: there `||` is an empty cell, as in any Markdown table.
  if (!tableRow) out = out.replace(/\|\|/g, "\\|\\|");
  return out;
}

/** Escapes one line of prose, leaving its block prefix (quote, list marker) and inline code spans as written. */
function escapeLine(line: string) {
  const tableRow = TABLE_ROW.test(line);
  // Always matches: every part of BLOCK_PREFIX is optional.
  const prefix = BLOCK_PREFIX.exec(line)![0];
  let out = prefix;
  let i = prefix.length;
  while (i < line.length) {
    const tick = line.indexOf("`", i);
    if (tick < 0) {
      out += escapeProse(line.slice(i), { tableRow });
      break;
    }
    out += escapeProse(line.slice(i, tick), { tableRow });
    // Always matches: the slice starts at a backtick.
    const run = /^`+/.exec(line.slice(tick))![0];
    const close = line.indexOf(run, tick + run.length);
    if (close < 0) {
      // An unmatched backtick run is ordinary text.
      out += run;
      i = tick + run.length;
      continue;
    }
    out += line.slice(tick, close + run.length);
    i = close + run.length;
  }
  return out;
}

/**
 * The agent's Markdown with the syntax it did not mean escaped, ready for
 * `rich_message.markdown`. Media lines alasio placed itself (rich-media.ts) pass through.
 */
export function toRichMarkdown(markdown: string): string {
  let fence: string | null = null;
  return String(markdown)
    .split("\n")
    .map((line) => {
      if (fence) {
        if (closesFence(line, fence)) fence = null;
        return line;
      }
      fence = openingFence(line);
      if (fence || MEDIA_LINE.test(line)) return line;
      return escapeLine(line);
    })
    .join("\n");
}

/**
 * Splits prepared rich Markdown into parts that each fit one rich message, preferring
 * breaks at blank lines between blocks. A block too large on its own (a long code block)
 * is split by line, its fence closed at the end of one part and reopened in the next.
 */
export function splitRichMarkdown(markdown: string, { maxChars = RICH_MESSAGE_MAX_CHARS, maxLines = RICH_MESSAGE_MAX_LINES }: RichMarkdownLimits = {}): string[] {
  const lines = String(markdown).split("\n");
  const parts: string[] = [];
  let current: string[] = [];
  let chars = 0;
  let fence: string | null = null; // the opening fence line while inside a fenced block
  let lastBreak = -1; // a length of `current` that ends on a blank line between blocks

  const flush = (upTo = current.length) => {
    const part = current.slice(0, upTo);
    const rest = current.slice(upTo);
    while (part.length && !part.at(-1)?.trim()) part.pop();
    if (part.length) parts.push(part.join("\n"));
    current = rest;
    while (current.length && !current[0]?.trim()) current.shift();
    chars = current.reduce((n, l) => n + l.length + 1, 0);
    lastBreak = -1;
  };

  for (const line of lines) {
    const size = line.length + 1;
    if (chars + size > maxChars || current.length + 1 > maxLines) {
      if (!fence && lastBreak > 0) {
        flush(lastBreak);
      } else if (fence) {
        // Mid-fence with nowhere better to break: close it here, reopen in the next part.
        // `fence` is a line that opened a fence, so it has a fence run.
        current.push(openingFence(fence)!);
        flush();
        current.push(fence);
        chars = fence.length + 1;
      } else {
        flush();
      }
    }
    current.push(line);
    chars += size;
    if (fence) {
      if (closesFence(line, openingFence(fence)!)) fence = null;
    } else if (openingFence(line)) {
      fence = line;
    } else if (!line.trim()) {
      lastBreak = current.length;
    }
  }
  flush();
  return parts.length ? parts : [""];
}
