// @ts-nocheck
/** A session's one-line label in the session panels: its text, whitespace folded, at most `maxChars`. */
export function sessionLabel(text, maxChars = 40, suffix = "...") {
  const normalized = String(text ?? "").replace(/\s+/g, " ").trim();
  if (normalized.length <= maxChars) {
    return normalized;
  }
  if (maxChars <= suffix.length) {
    return suffix.slice(0, maxChars);
  }
  return `${normalized.slice(0, maxChars - suffix.length).trimEnd()}${suffix}`;
}

/** A session's date in the session panels, YYYY-MM-DD, or "-" without one. */
export function dateLabel(epochMs) {
  const value = Number(epochMs);
  if (!Number.isFinite(value) || value <= 0) {
    return "-";
  }
  return new Date(value).toISOString().slice(0, 10);
}
