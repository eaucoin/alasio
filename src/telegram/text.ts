// @ts-nocheck
export function splitTelegramText(text) {
  const limit = 4096;
  if (text.length <= limit) {
    return [text || " "];
  }
  const chunks = [];
  let remaining = text;
  while (remaining.length > limit) {
    let cut = remaining.lastIndexOf("\n\n", limit);
    if (cut < 1000) {
      cut = remaining.lastIndexOf("\n", limit);
    }
    if (cut < 1000) {
      cut = remaining.lastIndexOf(" ", limit);
    }
    if (cut < 1000) {
      cut = limit;
    }
    chunks.push(remaining.slice(0, cut).trimEnd());
    remaining = remaining.slice(cut).trimStart();
  }
  if (remaining) {
    chunks.push(remaining);
  }
  return chunks;
}
