/**
 * The end-to-end run's stand-in for Claude Code (test/e2e/claude-stand-in.ts), run by the
 * Agent SDK with the options alasio runs Claude Code with: it answers as the CLI does, so
 * a run that has no Claude login can still run Claude's turns through alasio.
 */
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { query, type SDKMessage, type SDKUserMessage, type SessionKey, type SessionStoreEntry } from "@anthropic-ai/claude-agent-sdk";

import { buildClaudeUserMessage } from "../src/harness/claude/prompt-channel.ts";
import { buildClaudeQueryOptions } from "../src/harness/claude/runtime.ts";
import { ANSWER, projectKey } from "./e2e/claude-stand-in.ts";

const STAND_IN = join(import.meta.dirname, "e2e", "claude-stand-in.ts");

/** A prompt channel: each prompt pushed is the SDK's next, until it is ended. */
function prompts() {
  const queued: SDKUserMessage[] = [];
  let wake: (() => void) | null = null;
  let ended = false;
  return {
    push(message: SDKUserMessage) {
      queued.push(message);
      wake?.();
    },
    end() {
      ended = true;
      wake?.();
    },
    async *[Symbol.asyncIterator](): AsyncGenerator<SDKUserMessage> {
      for (;;) {
        const next = queued.shift();
        if (next) yield next;
        else if (ended) return;
        else await new Promise<void>((resolve) => (wake = resolve));
      }
    },
  };
}

test("the stand-in for Claude Code answers each prompt as the CLI does, naming it, its transcript written and mirrored to the session store", async (t) => {
  const root = realpathSync(mkdtempSync(join(tmpdir(), "alasio-claude-stand-in-")));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const configDir = join(root, "claude-config");
  const appended: [SessionKey, SessionStoreEntry[]][] = [];
  const sessionId = "11111111-1111-4111-8111-111111111111";
  const options = buildClaudeQueryOptions({
    workingDirectory: root,
    claudeEnv: { PATH: process.env["PATH"] ?? "", CLAUDE_CONFIG_DIR: configDir },
    mcpServers: {},
    resumeSession: sessionId,
    resumeExists: false,
    controller: new AbortController(),
    hooks: {},
    env: { ALASIO_CLAUDE_BIN: STAND_IN },
    sessionStore: { append: async (key, entries) => void appended.push([key, entries]), load: async () => null },
  });
  const channel = prompts();
  const first = buildClaudeUserMessage("hello");
  const second = buildClaudeUserMessage("wait 1");
  channel.push(first);
  const messages: SDKMessage[] = [];
  for await (const message of query({ prompt: channel, options })) {
    messages.push(message);
    if (message.type !== "result") continue;
    if (message.user_message_uuids?.includes(first.uuid ?? "")) channel.push(second);
    else channel.end();
  }
  const results = messages.flatMap((message) => (message.type === "result" && message.subtype === "success" ? [[message.result, message.user_message_uuids, message.session_id]] : []));
  assert.deepEqual(results, [[ANSWER, [first.uuid], sessionId], [ANSWER, [second.uuid], sessionId]]);
  assert.deepEqual(messages.filter((message) => message.type === "system" && message.subtype === "init").map((message) => "session_id" in message && message.session_id), [sessionId]);
  // The transcript, as the CLI writes it, and the same entries in the session store.
  const written = readFileSync(join(configDir, "projects", projectKey(root), `${sessionId}.jsonl`), "utf8").trim().split("\n").map((line) => JSON.parse(line) as { type: string; uuid: string });
  assert.deepEqual(written.map(({ type }) => type), ["user", "assistant", "user", "assistant"]);
  assert.deepEqual(written.filter(({ type }) => type === "user").map(({ uuid }) => uuid), [first.uuid, second.uuid]);
  assert.deepEqual(appended.flatMap(([, entries]) => entries.map((entry) => (entry as { uuid?: string }).uuid)), written.map(({ uuid }) => uuid));
  assert.deepEqual([...new Set(appended.map(([key]) => `${key.projectKey}/${key.sessionId}`))], [`${projectKey(root)}/${sessionId}`]);
});
