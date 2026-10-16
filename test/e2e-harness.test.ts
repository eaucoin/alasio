/**
 * The shard of the end-to-end run's suites (test/e2e/harness.ts) its environment selects,
 * and where it makes its cluster, as CI's end-to-end jobs select each; the lines of a node's
 * logs its failure dump keeps as errors; the configuration the
 * upgrade shard upgrades with; and the Telegram
 * stand-in's reading of what is sent it as a form, as Grafana sends its alerts.
 */
import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { test } from "node:test";

import { LOGGED_ERROR, runsShard, shardOf, SHARDS, targetOf } from "./e2e/harness.ts";
import { overlaid } from "./e2e/release-config.ts";
import { createTelegramStub } from "./e2e/telegram-stub.ts";

test("ALASIO_E2E_SHARD names a shard of the suites, and every suite unset or empty", () => {
  assert.equal(shardOf("sessions"), "sessions");
  assert.equal(shardOf("workspaces"), "workspaces");
  assert.equal(shardOf("neon"), "neon");
  assert.equal(shardOf("telemetry"), "telemetry");
  assert.equal(shardOf("branches"), "branches");
  assert.equal(shardOf("scaling"), "scaling");
  assert.equal(shardOf("snapshots"), "snapshots");
  assert.equal(shardOf("upgrade"), "upgrade");
  assert.equal(shardOf(undefined), undefined);
  assert.equal(shardOf(""), undefined);
});

test("a run of a shard runs its suites alone, and a run of none every suite but the snapshots and upgrade shards'", () => {
  assert.deepEqual(SHARDS.filter((shard) => runsShard("neon", shard)), ["neon"]);
  assert.deepEqual(SHARDS.filter((shard) => runsShard("snapshots", shard)), ["snapshots"]);
  assert.deepEqual(SHARDS.filter((shard) => runsShard("upgrade", shard)), ["upgrade"]);
  assert.deepEqual(SHARDS.filter((shard) => runsShard(undefined, shard)), ["sessions", "workspaces", "neon", "telemetry", "branches", "scaling"]);
});

test("a node's logs are read for its errors as k3s, containerd and kubelet log them, and for no other line", () => {
  const errors = [
    `time="2026-10-16T13:20:01Z" level=error msg="failed to get CA certs: Get \\"https://172.30.0.2:6443/cacerts\\": dial tcp 172.30.0.2:6443: connect: connection refused"`,
    `time="2026-10-16T13:20:01Z" level=fatal msg="Error: failed to start containerd"`,
    "E1016 13:20:01.123456     118 kubelet.go:2412] \"Container runtime network not ready\"",
    "F1016 13:20:01.123456     118 server.go:310] failed to run Kubelet",
  ];
  const others = [
    `time="2026-10-16T13:20:01Z" level=info msg="Waiting for control-plane node to register"`,
    `time="2026-10-16T13:20:01Z" level=warning msg="Error ensuring node password secret"`,
    "I1016 13:20:01.123456     118 kubelet.go:2412] \"Starting kubelet\"",
    "W1016 13:20:01.123456     118 sysinfo.go:203] Nodes topology is not available",
    "I1016 13:20:01.123456     118 kuberuntime_manager.go:1050] \"Pulled\" image=\"registry.local/app:E2024\"",
  ];
  assert.deepEqual(errors.filter((line) => !LOGGED_ERROR.test(line)), []);
  assert.deepEqual(others.filter((line) => LOGGED_ERROR.test(line)), []);
});

test("a shard there is not is refused, with those there are", () => {
  assert.throws(() => shardOf("kubernetes"), { message: "ALASIO_E2E_SHARD is kubernetes, not one of sessions, workspaces, neon, telemetry, branches, scaling, snapshots, upgrade" });
});

test("the upgrade shard's install configuration is laid over the one alasio init wrote, keeping what only that has", () => {
  const written = { host: { user: "1000:1000", home: "/home/operator" }, images: { alasio: { repository: "old", tag: "6.0.0" }, pullPolicy: "IfNotPresent" }, alasio: { env: { A: "1" } } };
  const release = { images: { alasio: { repository: "new", tag: "e2e" } }, alasio: { env: { B: "2" } }, sessions: { storage: { size: "1Gi" } } };
  assert.deepEqual(overlaid(written, release), {
    host: { user: "1000:1000", home: "/home/operator" },
    images: { alasio: { repository: "new", tag: "e2e" }, pullPolicy: "IfNotPresent" },
    alasio: { env: { A: "1", B: "2" } },
    sessions: { storage: { size: "1Gi" } },
  });
  assert.deepEqual(overlaid({ list: [1, 2] }, { list: [3] }), { list: [3] });
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
