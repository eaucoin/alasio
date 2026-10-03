import { formatCommandListRows, truncateText } from "./text.ts";

/** A session as a harness lists it for !sessions: its date label and its one-line label. */
interface ListedSession {
  readonly timestamp: string;
  readonly label: string;
}

/** A message of a session that !rewind can rewind to, by its index. */
interface RewindMessage {
  readonly index: number;
  readonly text: string;
}

export function formatSessionsForTelegram(sessions: readonly ListedSession[], page: number, totalPages: number): string {
  const startNumber = (page - 1) * 5 + 1;
  const rows = sessions.map((session, index) => {
    return `${startNumber + index}. ${session.timestamp || "-"} - ${truncateText(session.label, 72)}`;
  });
  return `Recent sessions (page ${page}/${totalPages})\n\n${formatCommandListRows(rows, "No sessions found.")}\n\nUse !resume <#> to resume a session.\nUse !sessions new to start a new session.`;
}

export function formatRewindForTelegram(messages: readonly RewindMessage[], page: number, totalPages: number): string {
  const start = (page - 1) * 5;
  const pageMessages = messages.slice(start, start + 5);
  const rows = pageMessages.map((message) => `${message.index}. ${truncateText(message.text, 92)}`);
  return `Rewind points (page ${page}/${totalPages})\n\n${formatCommandListRows(rows, "No messages found in session.")}\n\nUse !rewind <#> to rewind before that message.`;
}
