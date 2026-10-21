/**
 * The environments a harness's process is started with (src/harness/claude/env.ts,
 * src/codex/env.ts): alasio's, but for its own variables, its secrets among them, which
 * nothing a harness runs may have.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { buildCodexEnv } from "../src/codex/env.ts";
import { buildClaudeEnv, CLAUDE_LOGIN } from "../src/harness/claude/env.ts";

/** alasio's own variables, as its deployment gives them, beside Claude Code's login and a variable of the operator's configuration. */
const GIVEN: Readonly<Record<string, string>> = {
  TELEGRAM_BOT_TOKEN: "123:the-bot",
  TELEGRAM_ALLOWED_USER_IDS: "1",
  ALASIO_GATEWAY_CALLER_KEY: "the-caller-key",
  ALASIO_LOG_USERNAME: "alasio-main",
  ALASIO_DATABASE_URL_FILE: "/run/alasio/database/url",
  WORKING_DIRECTORY: "/home/operator/repo",
  [CLAUDE_LOGIN]: "the-login",
  OPERATORS_OWN: "kept",
};

/** What `build` makes with GIVEN in alasio's environment: which of GIVEN it holds. */
function heldOf(build: () => Readonly<Record<string, string>>): string[] {
  const before = Object.fromEntries(Object.keys(GIVEN).map((name) => [name, process.env[name]]));
  try {
    Object.assign(process.env, GIVEN);
    const env = build();
    return Object.keys(GIVEN).filter((name) => env[name] === GIVEN[name]);
  } finally {
    for (const [name, value] of Object.entries(before)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

test("Claude Code is started with none of alasio's own variables, its bot's token and the gateway's caller key among them, but with its login and what the operator's configuration adds", () => {
  assert.deepEqual(heldOf(buildClaudeEnv), [CLAUDE_LOGIN, "OPERATORS_OWN"]);
});

test("Codex is started with none of alasio's own variables, nor Claude Code's login, but with what the operator's configuration adds", () => {
  assert.deepEqual(heldOf(buildCodexEnv), ["OPERATORS_OWN"]);
});
