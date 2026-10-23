/** What `alasio status` says of an environment's bound harnesses (cli/src/bound-harnesses.ts). */
import assert from "node:assert/strict";
import { test } from "node:test";

import { describeHarnesses, type ListedHarness } from "../src/bound-harnesses.ts";

const NOW = Date.parse("2026-10-23T12:00:00.000Z");
const bound = (conversation: string, harness: string, host: ListedHarness["host"]): ListedHarness => ({
  conversation,
  harness,
  pool: `harness-${harness}`,
  pod: `harness-${harness}-abc`,
  boundAt: "2026-10-23T11:55:00.000Z",
  host,
});

test("each bound harness is said with its pod, pool and time bound, and what its host says it does, or why it was not reached", () => {
  assert.deepEqual(
    describeHarnesses("main", {
      listed: [
        bound("telegram:1", "codex", { running: true, background: false, idleSince: null }),
        bound("telegram:2", "claude", { running: false, background: true, idleSince: null }),
        bound("telegram:3", "codex", { running: false, background: false, idleSince: "2026-10-23T11:59:00.000Z" }),
        { ...bound("telegram:4", "claude", { unreached: "it did not answer within 5 s" }), boundAt: null },
      ],
    }, NOW),
    [
      "main's bound harnesses:",
      "  telegram:1 under Codex: bound to harness-codex-abc of harness-codex for 5m, running a turn",
      "  telegram:2 under Claude: bound to harness-claude-abc of harness-claude for 5m, working in the background",
      "  telegram:3 under Codex: bound to harness-codex-abc of harness-codex for 5m, idle for 60s",
      "  telegram:4 under Claude: bound to harness-claude-abc of harness-claude, its host not reached: it did not answer within 5 s",
    ],
  );
});

test("an environment with none bound says so, and one the gateway did not list says why", () => {
  assert.deepEqual(describeHarnesses("try", { listed: [] }, NOW), ["try's bound harnesses: none"]);
  assert.deepEqual(describeHarnesses("main", { unread: "it answered 503: no" }, NOW), ["main's bound harnesses: not seen, as the sandbox gateway did not list them: it answered 503: no"]);
});
