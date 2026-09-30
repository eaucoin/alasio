/**
 * Media an agent shows in a reply, embedded in the Telegram rich message itself.
 *
 * An agent shows an image or video by writing Markdown image syntax with a local path,
 * `![caption](path)`; naming a path any other way (in backticks, say) only refers to it.
 * Each shown file is placed as a media block directly below the block (paragraph, list
 * item, table) that shows it, one block per file, or a collage when one block shows
 * several, so the reply stays a single message however many files it shows. The text
 * keeps the caption where the image syntax stood.
 *
 * This module is pure: finding embeds, planning their placement, and identifying a file
 * by its bytes. Reading files and uploading them are the caller's (codex/reply-media.js,
 * telegram/client.js).
 */

/** Telegram Bot API upload limits by kind, and alasio's own per-reply caps. */
export const MEDIA_LIMITS = Object.freeze({
  photoBytes: 10 * 1024 * 1024,
  videoBytes: 50 * 1024 * 1024,
  perReply: 10,
  totalBytes: 100 * 1024 * 1024,
});

/** Why a file over a limit was not attached, e.g. "10.4 MB, over Telegram's 10 MB photo limit". */
export function overLimitNote(bytes, limitBytes, kind = null) {
  const mb = (n) => n / (1024 * 1024);
  return `${mb(bytes).toFixed(1)} MB, over Telegram's ${Math.round(mb(limitBytes))} MB ${kind ? `${kind} ` : ""}limit`;
}

const FENCE = /^ {0,3}(`{3,}|~{3,})/;
// `![caption](destination "optional title")`, the destination without spaces or parens.
const IMAGE = /!\[([^\]\n]*)\]\(\s*([^\s)]+)(?:\s+"[^"\n]*")?\s*\)/g;
const REMOTE = /^[A-Za-z][A-Za-z0-9+.-]*:/; // http:, https:, tg:, data:, ...

/** A line alasio generated for a media block, which the rich-Markdown escaper passes through. */
export const MEDIA_LINE = /^!\[[^\]\n]*\]\(tg:\/\/(?:photo|video)\?id=[A-Za-z0-9_-]+(?: "[^"\n]*")?\)$|^<\/?tg-collage>$/;

/** Identifies an image or video by its first bytes, not its name. */
export function sniffMedia(bytes) {
  const b = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const ascii = (from, to) => String.fromCharCode(...b.subarray(from, to));
  if (b[0] === 0x89 && ascii(1, 4) === "PNG") return { kind: "photo", ext: "png" };
  if (b[0] === 0xff && b[1] === 0xd8 && b[2] === 0xff) return { kind: "photo", ext: "jpg" };
  if (ascii(0, 4) === "RIFF" && ascii(8, 12) === "WEBP") return { kind: "photo", ext: "webp" };
  if (ascii(0, 4) === "GIF8") return { kind: "video", ext: "gif", animation: true };
  if (ascii(4, 8) === "ftyp") return { kind: "video", ext: ascii(8, 11) === "qt " ? "mov" : "mp4" };
  if (b[0] === 0x1a && b[1] === 0x45 && b[2] === 0xdf && b[3] === 0xa3) return { kind: "video", ext: "webm" };
  return null;
}

/** Splits a line into prose and inline-code segments, so embeds are looked for in prose only. */
function segments(line) {
  const out = [];
  let i = 0;
  while (i < line.length) {
    const tick = line.indexOf("`", i);
    if (tick < 0) {
      out.push({ code: false, text: line.slice(i) });
      break;
    }
    out.push({ code: false, text: line.slice(i, tick) });
    const run = /^`+/.exec(line.slice(tick))[0];
    const close = line.indexOf(run, tick + run.length);
    if (close < 0) {
      out.push({ code: false, text: run });
      i = tick + run.length;
      continue;
    }
    out.push({ code: true, text: line.slice(tick, close + run.length) });
    i = close + run.length;
  }
  return out;
}

/**
 * The local-file embeds in `markdown`, in order: `{ caption, path }` for each
 * `![caption](path)` outside code whose destination is a path, not a URL.
 */
export function findMediaEmbeds(markdown) {
  const found = [];
  let fence = null;
  for (const line of String(markdown).split("\n")) {
    const run = FENCE.exec(line)?.[1];
    if (fence) {
      if (run && run[0] === fence[0] && run.length >= fence.length && line.trim() === run) fence = null;
      continue;
    }
    if (run) {
      fence = run;
      continue;
    }
    for (const seg of segments(line)) {
      if (seg.code) continue;
      for (const m of seg.text.matchAll(IMAGE)) {
        if (!REMOTE.test(m[2])) found.push({ caption: m[1].trim(), path: m[2] });
      }
    }
  }
  return found;
}

function cleanCaption(caption) {
  return caption.replace(/["\]\n]/g, "").trim();
}

function mediaLine(item) {
  const link = `tg://${item.kind}?id=${item.id}`;
  const caption = cleanCaption(item.caption ?? "");
  return caption ? `![](${link} "${caption}")` : `![](${link})`;
}

function basename(path) {
  return path.replace(/\/+$/, "").split("/").pop() || path;
}

/**
 * Rewrites `markdown` with its embeds resolved. `resolved` maps each embed (by its index
 * in `findMediaEmbeds` order) to `{ id, kind, caption }` when attached, `{ duplicateOf }`
 * when the same file was already shown above, or `{ note }` when it was not attached
 * (the reason). Each image syntax becomes its caption in the text (or the file name, in
 * code, when it has none); attached files follow their block as media blocks, a collage
 * when a block shows several; a block that was nothing but embeds is replaced by its media.
 */
export function placeMedia(markdown, resolved) {
  const lines = String(markdown).split("\n");
  const out = [];
  let fence = null;
  let embedIndex = 0;
  let block = []; // the current block's rewritten lines
  let blockMedia = []; // media attached in the current block

  const endBlock = () => {
    out.push(...block);
    if (blockMedia.length) {
      if (block.length) out.push("");
      if (blockMedia.length === 1) out.push(mediaLine(blockMedia[0]));
      else out.push("<tg-collage>", ...blockMedia.map(mediaLine), "</tg-collage>");
    }
    block = [];
    blockMedia = [];
  };

  for (const line of lines) {
    const run = FENCE.exec(line)?.[1];
    if (fence) {
      block.push(line);
      if (run && run[0] === fence[0] && run.length >= fence.length && line.trim() === run) fence = null;
      continue;
    }
    if (run) {
      fence = run;
      block.push(line);
      continue;
    }
    if (!line.trim()) {
      endBlock();
      out.push(line);
      continue;
    }
    const segs = segments(line);
    // A line that is nothing but embeds (and list or quote markers) gives way to its media.
    const bare = segs.map((seg) => (seg.code ? seg.text : seg.text.replace(IMAGE, (w, c, path) => (REMOTE.test(path) ? w : "")))).join("");
    const onlyEmbeds = !bare.replace(/[\s>*+-]|\d+[.)]/g, "").length;
    let rewritten = "";
    let kept = false; // whether an embed left text behind (a caption or a not-attached note)
    for (const seg of segs) {
      if (seg.code) {
        rewritten += seg.text;
        continue;
      }
      rewritten += seg.text.replace(IMAGE, (whole, caption, path) => {
        if (REMOTE.test(path)) return whole;
        const r = resolved[embedIndex++] ?? { note: "not attached" };
        const label = caption.trim() || `\`${basename(path)}\``;
        if (r.id) {
          blockMedia.push(r);
          return onlyEmbeds ? "" : caption.trim();
        }
        kept = true;
        if (r.duplicateOf) return label;
        return `${label} *(not attached: ${r.note})*`;
      });
    }
    if (onlyEmbeds && !kept) continue;
    block.push(rewritten.replace(/[ \t]+$/, ""));
  }
  endBlock();
  return out.join("\n");
}

/** The media ids a prepared part references, in order. */
export function mediaIdsIn(markdown) {
  return [...String(markdown).matchAll(/tg:\/\/(?:photo|video)\?id=([A-Za-z0-9_-]+)/g)].map((m) => m[1]);
}

/** A prepared part with its media lines removed, for the classic (non-rich) fallback. */
export function withoutMediaLines(markdown) {
  return String(markdown)
    .split("\n")
    .filter((line) => !MEDIA_LINE.test(line))
    .join("\n")
    .replace(/\n{3,}/g, "\n\n");
}
