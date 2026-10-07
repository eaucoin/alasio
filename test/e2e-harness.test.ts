/**
 * The shard of the end-to-end run's suites (test/e2e/harness.ts) its environment selects,
 * and where it makes its cluster, as CI's end-to-end jobs select each.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { shardOf, targetOf } from "./e2e/harness.ts";

test("ALASIO_E2E_SHARD names a shard of the suites, and every suite unset or empty", () => {
  assert.equal(shardOf("sessions"), "sessions");
  assert.equal(shardOf("neon"), "neon");
  assert.equal(shardOf("telemetry"), "telemetry");
  assert.equal(shardOf(undefined), undefined);
  assert.equal(shardOf(""), undefined);
});

test("a shard there is not is refused, with those there are", () => {
  assert.throws(() => shardOf("kubernetes"), { message: "ALASIO_E2E_SHARD is kubernetes, not one of sessions, neon, telemetry" });
});

test("ALASIO_E2E_TARGET makes the cluster in Docker unless it names the host, and refuses another", () => {
  assert.equal(targetOf(undefined), "docker");
  assert.equal(targetOf(""), "docker");
  assert.equal(targetOf("docker"), "docker");
  assert.equal(targetOf("host"), "host");
  assert.throws(() => targetOf("local"), { message: "ALASIO_E2E_TARGET is local, not host or docker" });
});
