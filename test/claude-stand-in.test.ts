/**
 * The end-to-end run's stand-in for Claude Code (test/e2e/claude-stand-in.ts), run by the
 * Agent SDK with the options alasio runs Claude Code with: it answers as the CLI does, so
 * a run that has no Claude login can still run Claude's turns through alasio.
 */
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { query, type SDKMessage, type SDKUserMessage, type SessionKey, type SessionStoreEntry } from "@anthropic-ai/claude-agent-sdk";

import { buildClaudeUserMessage } from "../src/harness/claude/prompt-channel.ts";
import { buildClaudeQueryOptions } from "../src/harness/claude/runtime.ts";
import { ANSWER, BACKGROUND_REPORT, projectKey } from "./e2e/claude-stand-in.ts";

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

test("the stand-in reports a background task's work as a turn of its own, answering no prompt, and says how it was started, the operator's memory and skills, and the files it edits and reads", async (t) => {
  const root = realpathSync(mkdtempSync(join(tmpdir(), "alasio-claude-stand-in-")));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const configDir = join(root, "claude-config");
  mkdirSync(join(configDir, "skills", "review"), { recursive: true });
  writeFileSync(join(configDir, "CLAUDE.md"), "Remember the operator.\nMore.\n");
  const options = buildClaudeQueryOptions({
    workingDirectory: root,
    claudeEnv: { PATH: process.env["PATH"] ?? "", CLAUDE_CONFIG_DIR: configDir },
    mcpServers: {},
    resumeSession: "22222222-2222-4222-8222-222222222222",
    resumeExists: false,
    controller: new AbortController(),
    hooks: {},
    env: { ALASIO_CLAUDE_BIN: STAND_IN },
    sessionStore: { append: async () => {}, load: async () => null },
  });
  const said = ["background 1", "options", "memory", "edit note.txt written by the stand-in", "read note.txt"].map((text) => buildClaudeUserMessage(text));
  const channel = prompts();
  for (const prompt of said) channel.push(prompt);
  const results: [string | undefined, readonly string[] | undefined][] = [];
  const tasks: number[] = [];
  for await (const message of query({ prompt: channel, options })) {
    if (message.type === "system" && message.subtype === "background_tasks_changed") tasks.push(message.tasks.length);
    if (message.type !== "result" || message.subtype !== "success") continue;
    results.push([message.result, message.user_message_uuids]);
    if (results.length === said.length + 1) channel.end();
  }
  const answerTo = (text: string) => results.find(([, uuids]) => uuids?.[0] === said.find((prompt) => buildText(prompt) === text)?.uuid)?.[0];
  assert.equal(answerTo("background 1"), ANSWER);
  // Its report a turn of its own, answering no prompt, its task told live, then ended.
  assert.deepEqual(results.filter(([, uuids]) => uuids?.length === 0).map(([result]) => result), [BACKGROUND_REPORT]);
  assert.deepEqual(tasks, [1, 0]);
  const started = JSON.parse((answerTo("options") ?? "").split("options: ")[1] ?? "{}") as { readonly disallowedTools?: readonly string[] };
  assert.ok(started.disallowedTools?.includes("Bash"), JSON.stringify(started));
  assert.equal(answerTo("memory"), `${ANSWER}\n\nmemory: Remember the operator.; skills: review`);
  assert.equal(answerTo("edit note.txt written by the stand-in"), `${ANSWER}\n\nedited note.txt`);
  assert.equal(readFileSync(join(root, "note.txt"), "utf8"), "written by the stand-in");
  assert.equal(answerTo("read note.txt"), `${ANSWER}\n\nread note.txt: written by the stand-in`);
});

/** The text of `prompt`, as the SDK sends it. */
const buildText = (prompt: SDKUserMessage): string => {
  const { content } = prompt.message;
  return typeof content === "string" ? content : content.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("\n");
};
