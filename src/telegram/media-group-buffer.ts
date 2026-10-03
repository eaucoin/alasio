import type { TurnController } from "../codex/turn-controller.ts";
import type { SqliteStore } from "../persistence/store.ts";
import type { Logger } from "../shared/log.ts";

const DEFAULT_FLUSH_MS = 1_500;

/** The store's media groups, and the messages and files they gather. */
type MediaGroupStore = Pick<
  SqliteStore,
  "upsertMediaGroup" | "getPendingMediaGroupsDue" | "getMediaGroupMessages" | "getFilesForMessages" | "markMediaGroupFlushed"
>;

/** What the buffer calls on the turn controller. */
export type MediaGroupTurns = Pick<TurnController, "processPrompt">;

export interface MediaGroupBufferOptions {
  store: MediaGroupStore;
  turns: MediaGroupTurns;
  log: Logger;
  flushMs?: number;
}

/** One message of a media group, as it arrives. */
export interface MediaGroupMessage {
  mediaGroupId: string;
  conversationId: string;
  updateId: number;
  chatId: number;
}

export class MediaGroupBuffer {
  private readonly store: MediaGroupStore;
  private readonly turns: MediaGroupTurns;
  private readonly log: Logger;
  private readonly flushMs: number;
  private readonly timers: Map<string, ReturnType<typeof setTimeout>>;

  constructor({ store, turns, log, flushMs = DEFAULT_FLUSH_MS }: MediaGroupBufferOptions) {
    this.store = store;
    this.turns = turns;
    this.log = log;
    this.flushMs = flushMs;
    this.timers = new Map();
  }

  stop(): void {
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();
  }

  buffer({ mediaGroupId, conversationId, updateId, chatId }: MediaGroupMessage): void {
    this.store.upsertMediaGroup({
      mediaGroupId,
      conversationId,
      updateId,
      flushAfterMs: this.flushMs,
    });
    this.schedule(mediaGroupId, conversationId, chatId);
  }

  schedule(mediaGroupId: string, conversationId: string, chatId: number | string): void {
    const existing = this.timers.get(mediaGroupId);
    if (existing) {
      clearTimeout(existing);
    }
    const timer = setTimeout(() => {
      this.timers.delete(mediaGroupId);
      this.flush(mediaGroupId, conversationId, chatId).catch((error: unknown) => {
        this.log.error(`Failed to flush media group ${mediaGroupId}: ${error}`);
      });
    }, this.flushMs);
    this.timers.set(mediaGroupId, timer);
  }

  async flushDue(): Promise<void> {
    const groups = this.store.getPendingMediaGroupsDue(this.flushMs);
    for (const group of groups) {
      await this.flush(group.id, group.conversation_id, group.conversation_id.replace(/^telegram:/, ""));
    }
  }

  async flush(mediaGroupId: string, conversationId: string, chatId: number | string): Promise<void> {
    const messages = this.store.getMediaGroupMessages(mediaGroupId);
    const files = this.store.getFilesForMessages(messages.map((message) => message.id));
    const text = messages.map((message) => String(message.text ?? "").trim()).find(Boolean) ?? "";
    const messageId = Number(messages[0]?.transport_message_id ?? 0) || Date.now();
    const filePaths = files.map((file) => file.local_path).filter((path): path is string => Boolean(path));
    this.store.markMediaGroupFlushed(mediaGroupId);
    await this.turns.processPrompt({ conversationId, chatId, messageId, text, filePaths });
  }
}
