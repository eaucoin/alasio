// @ts-nocheck
import assert from "node:assert/strict";
import { test } from "node:test";

import { workingStatusHtml } from "../src/codex/status-reporter.ts";
import { Client } from "../src/telegram/client.ts";
import { splitRichMarkdown, toRichMarkdown } from "../src/telegram/rich-markdown.ts";

test("prose is escaped where Telegram's rich Markdown would read syntax the agent did not mean", () => {
  assert.equal(toRichMarkdown("Costs $5 to $10."), "Costs \\$5 to \\$10.");
  assert.equal(toRichMarkdown("a || b and a == b"), "a \\|\\| b and a \\=\\= b");
  assert.equal(toRichMarkdown("List<String> and x < y"), "List&lt;String> and x &lt; y");
  assert.equal(toRichMarkdown("an &lt; entity, AT&T"), "an &amp;lt; entity, AT&T");
  assert.equal(toRichMarkdown("a * b * c"), "a \\* b \\* c");
});

test("Markdown the agent did mean is left alone", () => {
  const kept = [
    "**bold**, *italic*, 2*3*4, ~~gone~~, [a link](https://example.com)",
    "# Heading",
    "* a bullet",
    "- another",
    "1. numbered",
    "> a quote",
    "| a | b |",
    "|---|---|",
    "| 1 || 3 |", // an empty table cell, as in any Markdown table
  ];
  for (const line of kept) assert.equal(toRichMarkdown(line), line, line);
});

test("code is passed through as written, inline and fenced", () => {
  assert.equal(toRichMarkdown("Run `echo $HOME || true` then $x"), "Run `echo $HOME || true` then \\$x");
  assert.equal(toRichMarkdown("``a ` b`` and <c>"), "``a ` b`` and &lt;c>");
  const fenced = "```bash\nexport A=$HOME\ntest a == b || echo <x>\n```\nafter $1";
  assert.equal(toRichMarkdown(fenced), "```bash\nexport A=$HOME\ntest a == b || echo <x>\n```\nafter \\$1");
  // A fence closes only on the same character, at least as long, with nothing after it.
  const nested = "````md\n```js\n$a\n```\n$b\n````\n$c";
  assert.equal(toRichMarkdown(nested), "````md\n```js\n$a\n```\n$b\n````\n\\$c");
  // An unmatched backtick is ordinary text around which prose is still escaped.
  assert.equal(toRichMarkdown("a ` stray $1"), "a ` stray \\$1");
});

test("long Markdown splits between blocks, and a long code block is closed and reopened", () => {
  const paragraphs = Array.from({ length: 6 }, (_, i) => `para ${i} ${"x".repeat(30)}`).join("\n\n");
  const parts = splitRichMarkdown(paragraphs, { maxChars: 100 });
  assert.ok(parts.length > 1);
  assert.equal(parts.join("\n\n"), paragraphs); // nothing lost, broken only at blank lines
  for (const part of parts) assert.ok(part.length <= 100, part);

  const code = ["intro", "", "```js", ...Array.from({ length: 12 }, (_, i) => `line${i}();`), "```", "", "outro"].join("\n");
  const codeParts = splitRichMarkdown(code, { maxChars: 1000, maxLines: 6 });
  assert.ok(codeParts.length > 2);
  for (const part of codeParts) {
    const fences = part.split("\n").filter((l) => l.startsWith("```")).length;
    assert.equal(fences % 2, 0, `unbalanced fences in:\n${part}`);
  }
  const bodyLines = codeParts.join("\n").split("\n").filter((l) => /^line\d+\(\);$/.test(l));
  assert.equal(bodyLines.length, 12);

  assert.deepEqual(splitRichMarkdown("short"), ["short"]);
});

test("a rich part Telegram rejects is sent the classic way; other failures still throw", async () => {
  const client = new Client("test-token");
  const calls = [];
  client.call = async (method, payload) => {
    calls.push({ method, payload });
    if (method === "sendRichMessage" && payload.rich_message.markdown.includes("reject")) {
      const error = new Error("Bad Request");
      error.status = 400;
      throw error;
    }
    return { message_id: calls.length };
  };
  const sent = await client.sendMessage(1, "| a |\n|---|\n| $5 |", { format: "rich" });
  assert.equal(sent.length, 1);
  assert.equal(calls[0].method, "sendRichMessage");
  assert.equal(calls[0].payload.rich_message.markdown, "| a |\n|---|\n| \\$5 |");
  assert.equal(calls[0].payload.format, undefined); // alasio's own option is not sent to Telegram

  calls.length = 0;
  await client.sendMessage(1, "please reject **this**", { format: "rich" });
  assert.deepEqual(calls.map((c) => c.method), ["sendRichMessage", "sendMessage"]);
  assert.equal(calls[1].payload.parse_mode, "HTML");
  assert.match(calls[1].payload.text, /<b>this<\/b>/);

  client.call = async () => {
    const error = new Error("Too Many Requests");
    error.status = 429;
    throw error;
  };
  await assert.rejects(client.sendMessage(1, "x", { format: "rich" }), /Too Many Requests/); // the outbox retries
});

test("the status line carries a relative start time the app keeps current", () => {
  const html = workingStatusHtml({ harnessName: "Claude", startedAtMs: 1_790_000_000_500 });
  assert.equal(html, 'Claude is working · started <tg-time unix="1790000000" format="r">just now</tg-time>');
  const waiting = workingStatusHtml({
    harnessName: "Codex",
    startedAtMs: 1_790_000_000_000,
    workflowWait: { runId: "wf_<1>", waitType: "agent" },
  });
  assert.match(waiting, /^Codex is watching workflow wf_&lt;1&gt; \(agent\) · started <tg-time/);
});
