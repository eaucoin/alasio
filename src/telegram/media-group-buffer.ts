const DEFAULT_FLUSH_MS = 1_500;

// The rows and calls the buffer uses from the store and the turn controller, narrowed
// to what it reads (persistence/telegram-content-repository.ts, codex/turn-controller.ts).
interface MediaGroupRow {
  id: string;
  conversation_id: string;
}

interface MediaGroupMessageRow {
  id: string;
  text: string | null;
  transport_message_id: string | null;
}

interface MediaGroupFileRow {
  local_path: string | null;
}

interface MediaGroupStore {
  upsertMediaGroup(group: { mediaGroupId: string; conversationId: string; updateId: number; flushAfterMs: number }): void;
  getPendingMediaGroupsDue(ageMs: number): MediaGroupRow[];
  getMediaGroupMessages(mediaGroupId: string): MediaGroupMessageRow[];
  getFilesForMessages(messageIds: string[]): MediaGroupFileRow[];
  markMediaGroupFlushed(mediaGroupId: string): void;
}

interface MediaGroupPrompt {
  conversationId: string;
  chatId: number | string;
  messageId: number;
  text: string;
  filePaths: string[];
}

interface MediaGroupTurns {
  processPrompt(prompt: MediaGroupPrompt): Promise<void>;
}

interface MediaGroupLog {
  error(message: string): void;
}

export interface MediaGroupBufferOptions {
  store: MediaGroupStore;
  turns: MediaGroupTurns;
  log: MediaGroupLog;
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
  private readonly log: MediaGroupLog;
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
