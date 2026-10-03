// @ts-nocheck
import { formatCommandListRows, truncateText } from "./text.ts";

export function formatSessionsForTelegram(sessions, page, totalPages) {
  const startNumber = (page - 1) * 5 + 1;
  const rows = sessions.map((session, index) => {
    return `${startNumber + index}. ${session.timestamp || "-"} - ${truncateText(session.label, 72)}`;
  });
  return `Recent sessions (page ${page}/${totalPages})\n\n${formatCommandListRows(rows, "No sessions found.")}\n\nUse !resume <#> to resume a session.\nUse !sessions new to start a new session.`;
}

export function formatRewindForTelegram(messages, page, totalPages) {
  const start = (page - 1) * 5;
  const pageMessages = messages.slice(start, start + 5);
  const rows = pageMessages.map((message) => `${message.index}. ${truncateText(message.text, 92)}`);
  return `Rewind points (page ${page}/${totalPages})\n\n${formatCommandListRows(rows, "No messages found in session.")}\n\nUse !rewind <#> to rewind before that message.`;
}
