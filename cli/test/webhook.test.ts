/**
 * The routes Secret (cli/src/webhook.ts), as environments made and deleted at once write
 * it, against a fake Kubernetes API: each keeps the routes of the others.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import { Effect } from "effect";

import { KubeApi } from "../src/kube/api.ts";
import { ROUTES_SECRET } from "../src/manifests/webhook.ts";
import { ensureRoute, removeRoute, type Route } from "../src/webhook.ts";
import { type FakeKube, serveFakeKube } from "./support/fake-kube.ts";

const ROUTES = `/api/v1/namespaces/alasio/secrets/${ROUTES_SECRET}`;

interface Rig {
  readonly kube: FakeKube;
  readonly run: <A, E>(effect: Effect.Effect<A, E, KubeApi>) => Promise<A>;
}

async function rig(t: TestContext): Promise<Rig> {
  const kube = await serveFakeKube();
  const directory = mkdtempSync(join(tmpdir(), "alasio-webhook-"));
  t.after(async () => {
    await kube.close();
    rmSync(directory, { recursive: true, force: true });
  });
  const kubeconfig = join(directory, "kubeconfig");
  writeFileSync(kubeconfig, kube.kubeconfig);
  return { kube, run: (effect) => Effect.runPromise(Effect.provide(effect, KubeApi.layer({ path: kubeconfig }))) };
}

/** The environments the routes Secret holds a route of. */
const routed = (kube: FakeKube): readonly string[] => Object.keys((kube.get(ROUTES)?.["data"] ?? {}) as object).sort();

/** The route the routes Secret holds of `environment`. */
const routeOf = (kube: FakeKube, environment: string): Route =>
  JSON.parse(Buffer.from((kube.get(ROUTES)?.["data"] as Record<string, string> | undefined)?.[environment] ?? "", "base64").toString("utf8")) as Route;

test("routes made at once, the Secret not there yet, are each kept", async (t) => {
  const { kube, run } = await rig(t);
  await run(Effect.all([ensureRoute("main", ["42"]), ensureRoute("try", ["42"]), ensureRoute("next", ["42"])], { concurrency: "unbounded" }));
  assert.deepEqual(routed(kube), ["main", "next", "try"]);
  assert.deepEqual(kube.get(ROUTES)?.metadata.labels, { "app.kubernetes.io/managed-by": "alasio", "app.kubernetes.io/part-of": "alasio" });
});

test("a route made as another is taken out keeps the rest, and the one taken out stays out", async (t) => {
  const { kube, run } = await rig(t);
  await run(Effect.all([ensureRoute("main", ["42"]), ensureRoute("try", ["42"])]));
  const main = routeOf(kube, "main");
  await run(Effect.all([ensureRoute("next", ["42"]), removeRoute("try"), ensureRoute("main", ["42", "43"])], { concurrency: "unbounded" }));
  assert.deepEqual(routed(kube), ["main", "next"]);
  // main's keeps its path and secret token, its users as given now.
  assert.deepEqual(routeOf(kube, "main"), { ...main, allowedUserIds: ["42", "43"] });
});
