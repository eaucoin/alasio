// @ts-nocheck
const DEFAULT_FLUSH_MS = 1_500;

export class MediaGroupBuffer {
  constructor({ store, turns, log, flushMs = DEFAULT_FLUSH_MS }) {
    this.store = store;
    this.turns = turns;
    this.log = log;
    this.flushMs = flushMs;
    this.timers = new Map();
  }

  stop() {
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();
  }

  buffer({ mediaGroupId, conversationId, updateId, chatId }) {
    this.store.upsertMediaGroup({
      mediaGroupId,
      conversationId,
      updateId,
      flushAfterMs: this.flushMs,
    });
    this.schedule(mediaGroupId, conversationId, chatId);
  }

  schedule(mediaGroupId, conversationId, chatId) {
    const existing = this.timers.get(mediaGroupId);
    if (existing) {
      clearTimeout(existing);
    }
    const timer = setTimeout(() => {
      this.timers.delete(mediaGroupId);
      this.flush(mediaGroupId, conversationId, chatId).catch((error) => {
        this.log.error(`Failed to flush media group ${mediaGroupId}: ${error}`);
      });
    }, this.flushMs);
    this.timers.set(mediaGroupId, timer);
  }

  async flushDue() {
    const groups = this.store.getPendingMediaGroupsDue(this.flushMs);
    for (const group of groups) {
      await this.flush(group.id, group.conversation_id, group.conversation_id.replace(/^telegram:/, ""));
    }
  }

  async flush(mediaGroupId, conversationId, chatId) {
    const messages = this.store.getMediaGroupMessages(mediaGroupId);
    const files = this.store.getFilesForMessages(messages.map((message) => message.id));
    const text = messages.map((message) => String(message.text ?? "").trim()).find(Boolean) ?? "";
    const messageId = Number(messages[0]?.transport_message_id ?? 0) || Date.now();
    const filePaths = files.map((file) => file.local_path).filter(Boolean);
    this.store.markMediaGroupFlushed(mediaGroupId);
    await this.turns.processPrompt({ conversationId, chatId, messageId, text, filePaths });
  }
}
