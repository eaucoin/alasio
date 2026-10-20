/**
 * The shard of the end-to-end run's suites (test/e2e/harness.ts) its environment selects,
 * and where it makes its cluster, as CI's end-to-end jobs select each; the lines of a node's
 * logs its failure dump keeps as errors; the configuration the
 * upgrade shard upgrades with; and the Telegram
 * stand-in's reading of what is sent it as a form, as Grafana sends its alerts.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { boxConfiguration, LOGGED_ERROR, runsShard, shardOf, SHARDS, targetOf } from "./e2e/harness.ts";
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

test("the cut-over's configuration step makes the configuration as the box's does: the folders' bayma unpinned, folders let go after four hours, alasio's and the log's memory as the box's need", () => {
  const written = {
    version: 1,
    install: {
      host: { user: "1000:1000", baymaImage: { repository: "ghcr.io/eaucoin/bayma", tag: "0.14.3", digest: "sha256:0" }, resources: { requests: { cpu: "20m" } } },
      alasio: { env: { A: "1" }, resources: { requests: { cpu: "50m", memory: "256Mi" } } },
    },
  };
  assert.deepEqual(boxConfiguration(written), {
    version: 1,
    install: {
      host: { user: "1000:1000", resources: { requests: { cpu: "20m" } }, releaseAfter: "4h" },
      alasio: { env: { A: "1" }, resources: { requests: { cpu: "50m", memory: "2560Mi" } } },
      log: { resources: { requests: { memory: "1Gi" } } },
    },
  });
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

test("the Telegram stand-in hands a release before the webhook's its updates by getUpdates, and refuses that once a webhook is registered, as Telegram does", async () => {
  const stub = createTelegramStub();
  await new Promise<void>((resolve) => stub.server.listen(0, "127.0.0.1", resolve));
  try {
    // A server listening on a TCP port has an address of its own.
    const base = `http://127.0.0.1:${(stub.server.address() as AddressInfo).port}`;
    const call = async (method: string, body: object) => {
      const answer = await fetch(`${base}/bot123:token/${method}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
      return { status: answer.status, body: (await answer.json()) as { result?: { update_id: number; message?: { text?: string } }[]; description?: string } };
    };
    await fetch(`${base}/control/message`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ text: "polled" }) });
    const polled = await call("getUpdates", { offset: 0, timeout: 1 });
    assert.deepEqual(polled.body.result?.map(({ message }) => message?.text), ["polled"]);
    // Confirmed by the next offset, and gone.
    assert.deepEqual((await call("getUpdates", { offset: (polled.body.result?.[0]?.update_id ?? 0) + 1, timeout: 0 })).body.result, []);
    // Unrecorded, as alasio's calls are what the tests read.
    assert.deepEqual(stub.calls().calls.map(({ method }) => method), []);
    assert.equal((await call("setWebhook", { url: "https://127.0.0.1:9/telegram/main/route", secret_token: "s" })).status, 200);
    const refused = await call("getUpdates", { offset: 0, timeout: 0 });
    assert.deepEqual([refused.status, refused.body.description], [409, "Conflict: can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first"]);
  } finally {
    stub.server.close();
  }
});

test("the Telegram stand-in, started again on what it kept, has the webhook, the updates and the calls it had, as Telegram forgets nothing of a bot's", async () => {
  const dir = mkdtempSync(join(tmpdir(), "telegram-stub-"));
  const state = join(dir, "held.json");
  const started = async () => {
    const stub = createTelegramStub({ state });
    await new Promise<void>((resolve) => stub.server.listen(0, "127.0.0.1", resolve));
    // A server listening on a TCP port has an address of its own.
    const base = `http://127.0.0.1:${(stub.server.address() as AddressInfo).port}`;
    const call = async (method: string, body: object) => (await (await fetch(`${base}/bot123:token/${method}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) })).json()) as { result: { url?: string; pending_update_count?: number } };
    const queued = async (text: string) => ((await (await fetch(`${base}/control/message`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ text }) })).json()) as { message: { message_id: number } }).message;
    return { stub, call, queued };
  };
  try {
    const first = await started();
    // A webhook no one answers at, so what is queued waits.
    await first.call("setWebhook", { url: "https://127.0.0.1:9/telegram/main/route", secret_token: "s" });
    const before = await first.queued("before");
    first.stub.server.close();
    const again = await started();
    try {
      const info = await again.call("getWebhookInfo", {});
      assert.deepEqual([info.result.url, info.result.pending_update_count], ["https://127.0.0.1:9/telegram/main/route", 1]);
      assert.deepEqual(again.stub.calls().calls.map(({ method }) => method), ["setWebhook", "getWebhookInfo"]);
      assert.ok((await again.queued("after")).message_id > before.message_id);
    } finally {
      again.stub.server.close();
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
