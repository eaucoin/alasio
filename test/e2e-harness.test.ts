/**
 * The shard of the end-to-end run's suites (test/e2e/harness.ts) its environment selects,
 * and where it makes its cluster, as CI's end-to-end jobs select each; and the Telegram
 * stand-in's reading of what is sent it as a form, as Grafana sends its alerts.
 */
import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { test } from "node:test";

import { shardOf, targetOf } from "./e2e/harness.ts";
import { createTelegramStub } from "./e2e/telegram-stub.ts";

test("ALASIO_E2E_SHARD names a shard of the suites, and every suite unset or empty", () => {
  assert.equal(shardOf("sessions"), "sessions");
  assert.equal(shardOf("workspaces"), "workspaces");
  assert.equal(shardOf("neon"), "neon");
  assert.equal(shardOf("telemetry"), "telemetry");
  assert.equal(shardOf(undefined), undefined);
  assert.equal(shardOf(""), undefined);
});

test("a shard there is not is refused, with those there are", () => {
  assert.throws(() => shardOf("kubernetes"), { message: "ALASIO_E2E_SHARD is kubernetes, not one of sessions, workspaces, neon, telemetry" });
});

test("ALASIO_E2E_TARGET makes the cluster in Docker unless it names the host, and refuses another", () => {
  assert.equal(targetOf(undefined), "docker");
  assert.equal(targetOf(""), "docker");
  assert.equal(targetOf("docker"), "docker");
  assert.equal(targetOf("host"), "host");
  assert.throws(() => targetOf("local"), { message: "ALASIO_E2E_TARGET is local, not host or docker" });
});

test("the Telegram stand-in records a form's fields as text, and its files by their size", async () => {
  const stub = createTelegramStub();
  await new Promise<void>((resolve) => stub.server.listen(0, "127.0.0.1", resolve));
  try {
    // A server listening on a TCP port has an address of its own.
    const base = `http://127.0.0.1:${(stub.server.address() as AddressInfo).port}`;
    const alert = new FormData();
    alert.set("chat_id", "1001");
    alert.set("text", "**Firing**: turns are failing");
    alert.set("parse_mode", "Markdown");
    assert.equal((await fetch(`${base}/bot123:token/sendMessage`, { method: "POST", body: alert })).status, 200);
    const photo = new FormData();
    photo.set("chat_id", "1002");
    photo.set("photo", new Blob([new Uint8Array(5)]), "graph.png");
    assert.equal((await fetch(`${base}/bot123:token/sendPhoto`, { method: "POST", body: photo })).status, 200);
    assert.deepEqual(stub.calls().calls.map(({ method, payload }) => [method, payload]), [
      ["sendMessage", { chat_id: "1001", text: "**Firing**: turns are failing", parse_mode: "Markdown" }],
      ["sendPhoto", { chat_id: "1002", multipartBytes: 5 }],
    ]);
  } finally {
    stub.server.close();
  }
});
