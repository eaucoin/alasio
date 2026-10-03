// @ts-nocheck
import assert from "node:assert/strict";
import { test } from "node:test";

import { PASSAGE_CHARS, digest, passagesOf, split } from "../src/harness/claude/search/passages.ts";

const texts = (entry) => passagesOf(entry).map(({ kind, prose, text }) => ({ kind, prose, text }));
const UUID = "0b4fd7d3-5d6b-4c1e-9a55-2f0e1c7d3a11";

test("a prompt and an answer are prose, each block of a message its own passage", () => {
  assert.deepEqual(texts({ type: "user", uuid: UUID, message: { role: "user", content: "where are the transcripts kept?" } }), [
    { kind: "user.text", prose: true, text: "where are the transcripts kept?" },
  ]);
  assert.deepEqual(
    texts({
      type: "assistant",
      uuid: UUID,
      message: {
        model: "claude-opus-5-5",
        stop_reason: "end_turn",
        content: [
          { type: "thinking", thinking: "They mean Neon.", signature: "CAQS2woKEAgSGAI4AUIIdGhpbmtpbmc" },
          { type: "redacted_thinking", data: "EqQBCkgIAxABGAIiQL" },
          { type: "text", text: "In Neon, mirrored as they are written." },
        ],
      },
    }),
    [
      { kind: "assistant.thinking", prose: true, text: "They mean Neon." },
      { kind: "assistant.text", prose: true, text: "In Neon, mirrored as they are written." },
    ],
  );
});

test("a tool call keeps its name and every input, single words included; its result is searched as written", () => {
  assert.deepEqual(
    texts({
      type: "assistant",
      message: { content: [{ type: "tool_use", id: "toolu_01", name: "Read", input: { file_path: "/home/operator/alasio/neon/compose.yml", limit: 40 } }] },
    }),
    [{ kind: "assistant.tool_use", prose: false, text: "Read\n/home/operator/alasio/neon/compose.yml" }],
  );
  assert.deepEqual(
    texts({
      type: "user",
      toolUseResult: { stdout: "listed twice", stderr: "" },
      message: {
        content: [
          { type: "tool_result", tool_use_id: "toolu_01", content: [{ type: "text", text: "compose.yml\ncontrol" }, { type: "image", source: { type: "base64", data: "iVBORw0KGgo" } }] },
          { type: "tool_result", tool_use_id: "toolu_02", content: "exit 0" },
        ],
      },
    }),
    [
      { kind: "user.tool_result", prose: false, text: "compose.yml\ncontrol" },
      { kind: "user.tool_result", prose: false, text: "exit 0" },
    ],
  );
});

test("outside a message, ids, timestamps, encoded data, and one-word labels are not text, but URLs are", () => {
  assert.deepEqual(
    texts({
      type: "attachment",
      uuid: UUID,
      timestamp: "2026-09-27T01:12:27.073Z",
      attachment: {
        type: "hook_success",
        hookName: "PostToolUse",
        digest: "e0e743251c7566e2b1e4f5ad091c681a700d7d7a3d85541ea56ca3acf43d1afa",
        blob: "A".repeat(200),
        content: "127 pass, 0 fail across 29 files",
      },
    }),
    [{ kind: "attachment", prose: true, text: "127 pass, 0 fail across 29 files" }],
  );
  assert.deepEqual(texts({ type: "pr-link", sessionId: UUID, prNumber: 12, prUrl: "https://github.com/eaucoin/bayma/pull/12" }), [
    { kind: "pr-link", prose: false, text: "https://github.com/eaucoin/bayma/pull/12" },
  ]);
  assert.deepEqual(texts({ type: "cost-state", sessionId: UUID, totalCostUSD: 1070.59 }), []);
});

test("NUL and half a surrogate pair are stored as U+FFFD", () => {
  const half = "😀".slice(0, 1);
  assert.deepEqual(texts({ type: "user", message: { content: [{ type: "tool_result", content: `a\u0000b ${half}` }] } }), [
    { kind: "user.tool_result", prose: false, text: "a\ufffdb \ufffd" },
  ]);
});

test("long text is split at the widest boundary, never past the limit or between a surrogate pair", () => {
  const paragraphs = [`${"a ".repeat(700)}`, `${"b ".repeat(700)}`, `${"c ".repeat(700)}`].join("\n\n");
  const pieces = split(paragraphs);
  assert.ok(pieces.every((piece) => piece.length <= PASSAGE_CHARS));
  // Each paragraph is 1,400 characters: no two fit in one passage.
  assert.deepEqual(pieces.map((piece) => piece[0]), ["a", "b", "c"]);
  assert.equal(pieces.join(" ").replace(/\s+/gu, ""), paragraphs.replace(/\s+/gu, ""));

  const unbroken = `${"x".repeat(PASSAGE_CHARS - 1)}😀${"y".repeat(10)}`;
  const [first, second] = split(unbroken);
  assert.equal(first, "x".repeat(PASSAGE_CHARS - 1));
  assert.equal(second, `😀${"y".repeat(10)}`);
});

test("parts number an entry's passages in order, and the same text has the same digest wherever it is", () => {
  const passages = passagesOf({ type: "user", message: { content: [{ type: "text", text: "one" }, { type: "text", text: "two" }] } });
  assert.deepEqual(passages.map((passage) => passage.part), [0, 1]);
  assert.equal(passages[0].digest, digest("one"));
  assert.equal(passagesOf({ type: "queue-operation", content: "one more time" })[0].digest, digest("one more time"));
});
