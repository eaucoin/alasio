import { isHarnessName } from "../harness/names.js";
import { newId } from "../shared/ids.js";

function mapRow(row) {
  return row ? { ...row, filePaths: JSON.parse(row.file_paths_json || "[]") } : null;
}

export class SqlitePromptJobRepository {
  constructor(db) {
    this.db = db;
  }

  enqueue({ conversationId, chatId, messageId, prompt, filePaths = [], state = "pending", priority = 0, harness = null }) {
    if (!isHarnessName(harness)) {
      throw new Error(`Cannot queue a prompt for ${conversationId}: no service is mounted`);
    }
    const id = newId();
    this.db.prepare(`
      insert into prompt_jobs
        (id, conversation_id, chat_id, message_id, prompt, file_paths_json, harness, state, priority, created_at)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      on conflict(conversation_id, message_id) do nothing
    `).run(id, conversationId, String(chatId), String(messageId), prompt, JSON.stringify(filePaths), harness, state, priority, Date.now() / 1000);
    return mapRow(this.db.prepare("select * from prompt_jobs where conversation_id = ? and message_id = ?").get(conversationId, String(messageId)));
  }

  get(id) {
    return mapRow(this.db.prepare("select * from prompt_jobs where id = ?").get(id));
  }

  claimNext(conversationId) {
    const claim = this.db.transaction(() => {
      const row = this.db.prepare(`
        select * from prompt_jobs
        where conversation_id = ? and state = 'pending'
        order by priority desc, created_at asc
        limit 1
      `).get(conversationId);
      if (!row) {
        return null;
      }
      this.db.prepare(`
        update prompt_jobs
        set state = 'running', attempts = attempts + 1, started_at = ?, last_error = null
        where id = ? and state = 'pending'
      `).run(Date.now() / 1000, row.id);
      return this.db.prepare("select * from prompt_jobs where id = ?").get(row.id);
    });
    return mapRow(claim());
  }

  setDisposition(id, state, priority = 0) {
    this.db.prepare(`
      update prompt_jobs set state = ?, priority = ?, completed_at = case when ? in ('completed', 'cancelled') then ? else null end
      where id = ?
    `).run(state, priority, state, Date.now() / 1000, id);
  }

  complete(id) {
    this.setDisposition(id, "completed");
  }

  markUpstreamStarted(id, sessionId, turnId) {
    this.db.prepare(`
      update prompt_jobs
      set upstream_session_id = ?, upstream_turn_id = ?, upstream_started_at = ?
      where id = ? and state = 'running'
    `).run(sessionId ?? null, turnId ?? null, Date.now() / 1000, id);
  }

  markUpstreamCompleted(id, sessionId, turnId) {
    this.db.prepare(`
      update prompt_jobs
      set upstream_session_id = coalesce(?, upstream_session_id),
          upstream_turn_id = coalesce(?, upstream_turn_id),
          upstream_completed_at = ?
      where id = ? and state = 'running'
    `).run(sessionId ?? null, turnId ?? null, Date.now() / 1000, id);
  }

  fail(id, error) {
    this.db.prepare(`
      update prompt_jobs set state = 'failed', last_error = ?, completed_at = ? where id = ?
    `).run(String(error).slice(0, 2000), Date.now() / 1000, id);
  }

  hasOpenJobs(conversationId) {
    const row = this.db.prepare(`
      select count(*) as count from prompt_jobs
      where conversation_id = ? and state in ('pending', 'running', 'awaiting_choice')
    `).get(conversationId);
    return Number(row?.count ?? 0) > 0;
  }

  listPendingConversations() {
    return this.db.prepare(`
      select distinct conversation_id from prompt_jobs where state = 'pending' order by conversation_id
    `).all().map((row) => row.conversation_id);
  }

  recoverAfterRestart() {
    const completed = this.db.prepare(`
      select distinct conversation_id from prompt_jobs
      where state = 'running' and upstream_completed_at is not null
    `).all().map((row) => row.conversation_id);
    this.db.prepare(`
      update prompt_jobs set state = 'completed', completed_at = coalesce(completed_at, ?)
      where state = 'running' and upstream_completed_at is not null
    `).run(Date.now() / 1000);
    this.db.prepare(`
      update prompt_jobs set state = 'pending', started_at = null
      where state = 'running' and upstream_started_at is null and upstream_completed_at is null
    `).run();
    this.db.prepare(`
      update prompt_jobs set state = 'interrupted', completed_at = ?
      where state = 'running' and upstream_started_at is not null and upstream_completed_at is null
    `).run(Date.now() / 1000);
    return completed;
  }
}
