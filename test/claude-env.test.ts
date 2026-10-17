/**
 * The environment Claude Code is started with (src/harness/claude/env.ts).
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { buildClaudeEnv, CLAUDEAI_CONNECTORS } from "../src/harness/claude/env.ts";

test("Claude Code is started with the login's claude.ai connectors switched off, whatever alasio's own environment says", () => {
  const before = process.env[CLAUDEAI_CONNECTORS];
  try {
    for (const set of [undefined, "true", "1"]) {
      if (set === undefined) delete process.env[CLAUDEAI_CONNECTORS];
      else process.env[CLAUDEAI_CONNECTORS] = set;
      assert.equal(buildClaudeEnv()[CLAUDEAI_CONNECTORS], "false");
    }
  } finally {
    if (before === undefined) delete process.env[CLAUDEAI_CONNECTORS];
    else process.env[CLAUDEAI_CONNECTORS] = before;
  }
});
