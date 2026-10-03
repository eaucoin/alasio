export function truncateText(value: string | null | undefined, maxLength = 120): string {
  const text = String(value ?? "").replace(/\s+/g, " ").trim();
  if (text.length <= maxLength) {
    return text;
  }
  if (maxLength <= 3) {
    return text.slice(0, maxLength);
  }
  return `${text.slice(0, maxLength - 3).trimEnd()}...`;
}

export function formatCommandListRows(rows: readonly string[], emptyText: string): string {
  if (rows.length === 0) {
    return emptyText;
  }
  return rows.join("\n");
}
