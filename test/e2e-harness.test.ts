/**
 * The shard of the end-to-end run's suites (test/e2e/harness.ts) its environment selects,
 * as CI's end-to-end jobs select one each.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { shardOf } from "./e2e/harness.ts";

test("ALASIO_E2E_SHARD names a shard of the suites, and every suite unset or empty", () => {
  assert.equal(shardOf("sessions"), "sessions");
  assert.equal(shardOf("neon"), "neon");
  assert.equal(shardOf(undefined), undefined);
  assert.equal(shardOf(""), undefined);
});

test("a shard there is not is refused, with those there are", () => {
  assert.throws(() => shardOf("kubernetes"), { message: "ALASIO_E2E_SHARD is kubernetes, not one of sessions, neon" });
});
