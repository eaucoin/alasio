const PLACEHOLDER_START = "\uE000";
const PLACEHOLDER_END = "\uE001";

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function escapeAttribute(value) {
  return escapeHtml(value).replaceAll('"', "&quot;");
}

function makePlaceholder(index) {
  return `${PLACEHOLDER_START}${index}${PLACEHOLDER_END}`;
}

function restorePlaceholders(value, placeholders) {
  return value.replace(new RegExp(`${PLACEHOLDER_START}(\\d+)${PLACEHOLDER_END}`, "g"), (_, index) => placeholders[Number(index)] ?? "");
}

function stashLinks(value, placeholders) {
  return value.replace(/\[([^\]\n]+)\]\((https?:\/\/[^)\s]+)\)/g, (_, label, href) => {
    const html = `<a href="${escapeAttribute(href)}">${renderInline(label)}</a>`;
    placeholders.push(html);
    return makePlaceholder(placeholders.length - 1);
  });
}

function stashCodeSpans(value, placeholders) {
  return value.replace(/`([^`\n]+)`/g, (_, code) => {
    placeholders.push(`<code>${escapeHtml(code)}</code>`);
    return makePlaceholder(placeholders.length - 1);
  });
}

function renderInline(value) {
  const placeholders = [];
  const protectedValue = stashLinks(stashCodeSpans(String(value), placeholders), placeholders);
  const escaped = escapeHtml(protectedValue)
    .replace(/\*\*([^*\n]+)\*\*/g, "<b>$1</b>")
    .replace(/(^|[\s(])_([^_\n]+)_($|[\s.,;:!?)])/g, "$1<i>$2</i>$3");
  return restorePlaceholders(escaped, placeholders);
}

function renderFence(buffer) {
  return `<pre>${escapeHtml(buffer.join("\n"))}</pre>`;
}

export function renderTelegramHtml(markdown) {
  const lines = String(markdown).split("\n");
  const rendered = [];
  let fenceBuffer = null;

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
      rendered.push(`<b>${renderInline(heading[2])}</b>`);
      continue;
    }

    rendered.push(renderInline(line));
  }

  if (fenceBuffer) {
    rendered.push(renderFence(fenceBuffer));
  }

  return rendered.join("\n");
}
