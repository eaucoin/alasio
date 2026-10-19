import assert from "node:assert/strict";
import { test } from "node:test";

import { decodeEvent, type EventInput, eventRecord } from "../src/events/events.ts";
import { eventsTopic } from "../src/events/names.ts";

// An environment's events, as their writers make them and their readers take them.

const lost: EventInput = {
  kind: "workspace.lost",
  environment: "try",
  source: "sandbox-gateway",
  subject: { kind: "workspace", key: "sessionfs:fs-abc123" },
  binding: "uid-1",
  pod: "sandboxes-none-7d9f-abcde",
  reason: "its workspace's mount is not connected",
};

test("an event is written on its environment's topic, keyed by its subject, in the envelope of its version, an id of its own", () => {
  const record = eventRecord(lost, 1_792_000_000_000);
  assert.deepEqual([record.topic, record.key, eventsTopic("main")], ["events.try", "sessionfs:fs-abc123", "events.main"]);
  const value = JSON.parse(record.value) as Record<string, unknown>;
  assert.deepEqual({ ...value, id: typeof value["id"] }, { v: 1, id: "string", at: 1_792_000_000_000, ...lost });
  assert.notEqual(JSON.parse(eventRecord(lost, 0).value).id, JSON.parse(eventRecord(lost, 0).value).id);
  assert.deepEqual(decodeEvent(record.value), { _tag: "Known", event: value });
});

test("what a harness pool's host tells of its conversation is read as it was told", () => {
  for (const kind of ["harness.background-response", "harness.idle"] as const) {
    const told: EventInput = { kind, environment: "main", source: "harness", subject: { kind: "conversation", key: "telegram:41" } };
    const record = eventRecord(told, 0);
    assert.deepEqual([record.topic, record.key], ["events.main", "telegram:41"]);
    assert.deepEqual(decodeEvent(record.value), { _tag: "Known", event: JSON.parse(record.value) });
  }
});

test("a record of a higher version, of a kind not known, or not as its kind says, is skipped, saying why, and none fails", () => {
  const record = (fields: Record<string, unknown>) => JSON.stringify({ ...JSON.parse(eventRecord(lost, 0).value), ...fields });
  const skipped = (value: string) => {
    const decoded = decodeEvent(value);
    return decoded._tag === "Skipped" ? decoded.reason : "known";
  };
  assert.equal(skipped(record({ v: 2 })), "of version 2, past 1");
  assert.equal(skipped(record({ kind: "workspace.forked" })), "of a kind not known, workspace.forked");
  assert.match(skipped(record({ pod: 7 })), /^a workspace\.lost not as its kind says/u);
  assert.match(skipped(record({ source: "elsewhere" })), /^a workspace\.lost not as its kind says/u);
  assert.equal(skipped("{"), "not JSON");
  assert.equal(skipped("[]"), "of version undefined, past 1");
  assert.equal(skipped("null"), "not an object");
});
