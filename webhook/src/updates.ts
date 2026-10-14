/**
 * What the intake reads of an update from Telegram, and whether it passes it on: alasio's
 * own rule (src/telegram/authorizer.ts), applied at the edge, so nothing but what alasio
 * would act on reaches the log. An update passes when it is a message, or a button pressed
 * under one, in the private chat of a user the environment allows, the chat's id the
 * user's own. Everything else, a group's message, a channel's post, a user not allowed, an
 * update of a type the bot is not sent, is answered and dropped, so Telegram does not send
 * it again.
 */

/** Who sent an update, and in which chat: what the rule reads. */
export interface Sender {
  readonly userId: string;
  readonly chatId: string;
  readonly chatType: string;
}

/** What is read of an object, which may be anything: its field `key`, as an object, if it is one. */
function field(value: unknown, key: string): Record<string, unknown> | undefined {
  const found = typeof value === "object" && value !== null ? (value as Record<string, unknown>)[key] : undefined;
  return typeof found === "object" && found !== null ? (found as Record<string, unknown>) : undefined;
}

/** An id, as Telegram gives one, a number, as the string alasio keeps it as; none for anything else. */
function idOf(value: Record<string, unknown> | undefined): string | undefined {
  const id = value?.["id"];
  return typeof id === "number" && Number.isSafeInteger(id) ? String(id) : undefined;
}

/**
 * The sender of `update`, and its chat: a message's own, or the message a pressed
 * button is under; none for an update of another type, or one that says neither.
 */
export function senderOf(update: unknown): Sender | undefined {
  const message = field(update, "message");
  const callbackQuery = field(update, "callback_query");
  const from = message ? field(message, "from") : field(callbackQuery, "from");
  const chat = message ? field(message, "chat") : field(field(callbackQuery, "message"), "chat");
  const userId = idOf(from);
  const chatId = idOf(chat);
  const chatType = chat?.["type"];
  if (userId === undefined || chatId === undefined || typeof chatType !== "string") return undefined;
  return { userId, chatId, chatType };
}

/** Whether `sender` is a user `allowed` names, in their private chat with the bot. */
export function admitted(sender: Sender | undefined, allowed: ReadonlySet<string>): sender is Sender {
  return sender !== undefined && sender.chatType === "private" && sender.chatId === sender.userId && allowed.has(sender.userId);
}
