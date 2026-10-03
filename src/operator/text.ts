// @ts-nocheck
export function truncateText(value, maxLength = 120) {
  const text = String(value ?? "").replace(/\s+/g, " ").trim();
  if (text.length <= maxLength) {
    return text;
  }
  if (maxLength <= 3) {
    return text.slice(0, maxLength);
  }
  return `${text.slice(0, maxLength - 3).trimEnd()}...`;
}

export function formatCommandListRows(rows, emptyText) {
  if (rows.length === 0) {
    return emptyText;
  }
  return rows.join("\n");
}
