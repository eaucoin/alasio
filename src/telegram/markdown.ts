const PLACEHOLDER_START = "";
const PLACEHOLDER_END = "";

function escapeHtml(value: string) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function escapeAttribute(value: string) {
  return escapeHtml(value).replaceAll('"', "&quot;");
}

function makePlaceholder(index: number) {
  return `${PLACEHOLDER_START}${index}${PLACEHOLDER_END}`;
}

function restorePlaceholders(value: string, placeholders: readonly string[]) {
  return value.replace(new RegExp(`${PLACEHOLDER_START}(\\d+)${PLACEHOLDER_END}`, "g"), (_: string, index: string) => placeholders[Number(index)] ?? "");
}

function stashLinks(value: string, placeholders: string[]) {
  return value.replace(/\[([^\]\n]+)\]\((https?:\/\/[^)\s]+)\)/g, (_: string, label: string, href: string) => {
    const html = `<a href="${escapeAttribute(href)}">${renderInline(label)}</a>`;
    placeholders.push(html);
    return makePlaceholder(placeholders.length - 1);
  });
}

function stashCodeSpans(value: string, placeholders: string[]) {
  return value.replace(/`([^`\n]+)`/g, (_: string, code: string) => {
    placeholders.push(`<code>${escapeHtml(code)}</code>`);
    return makePlaceholder(placeholders.length - 1);
  });
}

function renderInline(value: string): string {
  const placeholders: string[] = [];
  const protectedValue = stashLinks(stashCodeSpans(String(value), placeholders), placeholders);
  const escaped = escapeHtml(protectedValue)
    .replace(/\*\*([^*\n]+)\*\*/g, "<b>$1</b>")
    .replace(/(^|[\s(])_([^_\n]+)_($|[\s.,;:!?)])/g, "$1<i>$2</i>$3");
  return restorePlaceholders(escaped, placeholders);
}

function renderFence(buffer: readonly string[]) {
  return `<pre>${escapeHtml(buffer.join("\n"))}</pre>`;
}

export function renderTelegramHtml(markdown: string): string {
  const lines = String(markdown).split("\n");
  const rendered: string[] = [];
  let fenceBuffer: string[] | null = null;

  for (const line of lines) {
    if (fenceBuffer) {
      if (/^```/.test(line.trim())) {
        rendered.push(renderFence(fenceBuffer));
        fenceBuffer = null;
      } else {
        fenceBuffer.push(line);
      }
      continue;
    }

    const fenceStart = line.match(/^```/);
    if (fenceStart) {
      fenceBuffer = [];
      continue;
    }

    const heading = line.match(/^(#{1,6})\s+(.+)$/);
    if (heading) {
      // Both groups are mandatory, so a match always has its text.
      rendered.push(`<b>${renderInline(heading[2]!)}</b>`);
      continue;
    }

    rendered.push(renderInline(line));
  }

  if (fenceBuffer) {
    rendered.push(renderFence(fenceBuffer));
  }

  return rendered.join("\n");
}
