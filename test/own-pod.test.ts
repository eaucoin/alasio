/**
 * alasio told to stop as its own pod is deleted (src/kube/own-pod.ts), on a real API server
 * (./support/kube-apiserver.ts) and no kubelet, so no SIGTERM: as one lost, which kubelet sends
 * once. alasio's start, run as its image runs it (src/index.ts), held as it waits for a Neon
 * that never answers, ends within seconds of its pod's deletion; and a pod gone is deleted.
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import { KubeConfig } from "@kubernetes/client-node";
import { Duration, Effect, Layer } from "effect";

import { scramVerifier } from "../neon/control/scram.ts";
import { makeKubeClient, KubeClient } from "../src/kube/client.ts";
import { deleted, READ_EVERY } from "../src/kube/own-pod.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const NAMESPACE = "own-pod";

/** The API server, its client, and a namespace of the test's on it. */
async function onApiServer() {
  const api = await startKubeApiServer();
  const kubeConfig = new KubeConfig();
  kubeConfig.loadFromFile(api.kubeconfig);
  const kube = makeKubeClient(kubeConfig);
  await Effect.runPromise(kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: NAMESPACE } }).pipe(Effect.ignore));
  return { api, kube };
}

/** How long a stop may take once alasio's pod is deleted: a read of it, and the stop of a start that connected to nothing. */
const STOPPED_WITHIN_MS = Duration.toMillis(Duration.fromInputUnsafe(READ_EVERY)) + 8_000;

test("alasio, its start held, stops within seconds of its pod's deletion, as told by the SIGTERM that never came", { timeout: 120_000 }, async () => {
  const { api, kube } = await onApiServer();
  // Bound to a node, as kubelet would run it: deleted, it is held, its deletionTimestamp set, until its kubelet, of none here, ends it.
  const pod = "alasio-held";
  await Effect.runPromise(kube.create({ apiVersion: "v1", kind: "Pod", metadata: { name: pod, namespace: NAMESPACE }, spec: { nodeName: "nowhere", containers: [{ name: "alasio", image: "alasio" }] } }));
  const home = mkdtempSync(join(tmpdir(), "alasio-own-pod-"));
  const password = join(home, "password");
  writeFileSync(password, "password");
  const alasio = spawn(process.execPath, ["src/index.ts"], {
    cwd: ROOT,
    env: {
      PATH: process.env["PATH"],
      HOME: home,
      KUBECONFIG: api.kubeconfig,
      ALASIO_POD: pod,
      ALASIO_NAMESPACE: NAMESPACE,
      TELEGRAM_BOT_TOKEN: "123:token",
      ALASIO_LOG_BROKERS: "127.0.0.1:9",
      ALASIO_LOG_USERNAME: "alasio",
      ALASIO_LOG_PASSWORD_FILE: password,
      ALASIO_TELEGRAM_TOPIC: "telegram.main",
      ALASIO_TELEGRAM_GROUP: "telegram.main",
      ALASIO_GATEWAY_URL: "http://127.0.0.1:9",
      ALASIO_GATEWAY_ENVIRONMENT: "main",
      ALASIO_GATEWAY_CALLER_KEY: "key",
      ALASIO_CODEX_LOGIN_URL: "http://127.0.0.1:9",
      // A Neon that never answers, which alasio's start waits for.
      ALASIO_DATABASE_URL: "postgres://alasio:password@127.0.0.1:9/alasio",
      ALASIO_LAKE_VERIFIER: scramVerifier("lake"),
      ALASIO_HARNESS_VERIFIER: scramVerifier("harness"),
      ALASIO_CODEX_LOGIN_VERIFIER: scramVerifier("codex-login"),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let said = "";
  for (const stream of [alasio.stdout, alasio.stderr]) stream.on("data", (chunk: Buffer) => void (said += chunk.toString("utf8")));
  const ended = once(alasio, "exit") as Promise<[number | null, NodeJS.Signals | null]>;
  try {
    const waiting = Date.now();
    while (!said.includes("waiting for Neon")) {
      assert.equal(alasio.exitCode, null, `alasio ended before it waited for Neon:\n${said}`);
      assert.ok(Date.now() - waiting < 60_000, `alasio did not wait for Neon within a minute:\n${said}`);
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    // Its pod read, and not deleted: it waits on.
    await new Promise((resolve) => setTimeout(resolve, STOPPED_WITHIN_MS));
    assert.equal(alasio.exitCode, null, `alasio ended, its pod not deleted:\n${said}`);
    const deleting = Date.now();
    await Effect.runPromise(kube.remove("v1", "Pod", NAMESPACE, pod));
    const [code] = await Promise.race([
      ended,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`alasio ran on ${STOPPED_WITHIN_MS} ms after its pod's deletion:\n${said}`)), STOPPED_WITHIN_MS)),
    ]);
    console.error(`# alasio stopped ${Date.now() - deleting} ms after its pod's deletion`);
    // Stopped as told to, not failed: what a SIGTERM's stop exits with.
    assert.equal(code, 0, said);
    assert.match(said, /its pod own-pod\/alasio-held is deleted, and no SIGTERM came: it stops as told by one/u);
  } finally {
    alasio.kill("SIGKILL");
    rmSync(home, { recursive: true, force: true });
  }
});

test("a pod already gone is deleted, as one deleted after", { timeout: 60_000 }, async () => {
  const { kube } = await onApiServer();
  await Effect.runPromise(deleted({ namespace: NAMESPACE, name: "never-made" }).pipe(Effect.provide(Layer.succeed(KubeClient, kube)), Effect.timeout("5 seconds")));
});
