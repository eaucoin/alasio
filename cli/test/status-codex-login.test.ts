/** What `alasio status` says of alasio's Codex login (cli/src/commands/status.ts), as alasio's pod reads its store (src/codex/login-state.ts). */
import assert from "node:assert/strict";
import { test } from "node:test";

import { describeCodexLogin } from "../src/commands/status.ts";

test("status says a Codex login kept, none kept, or refused as alasio refreshed it, telling the operator to log in again where Codex runs no turn", () => {
  assert.equal(describeCodexLogin({ kept: true, refusedAt: null }), "Codex's login: kept");
  assert.equal(describeCodexLogin({ kept: false, refusedAt: null }), "Codex's login: none kept, so Codex runs no turn until it is logged in: run `alasio login codex`");
  assert.equal(
    describeCodexLogin({ kept: true, refusedAt: "2026-10-23T10:00:00.000Z" }),
    "Codex's login: refused as alasio refreshed it, at 2026-10-23T10:00:00.000Z, so Codex runs no turn until it is logged in again: run `alasio login codex`",
  );
});
