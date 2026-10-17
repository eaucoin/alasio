import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { createServer, type IncomingHttpHeaders } from "node:http";
import type { AddressInfo } from "node:net";
import { before, test } from "node:test";

import type { V1Pod } from "@kubernetes/client-node";
import { Effect, Exit, Layer, Scope } from "effect";

import { Binder } from "../src/binder/binder.ts";
import { activeAt, freeLabels, type Pool, readLease } from "../src/binder/bindings.ts";
import { type BinderServer, PlacementRefused, serveBinder } from "../src/binder/server.ts";
import { mintToken, verifyToken } from "../src/binder/tokens.ts";
import { KubeClient } from "../src/kube/client.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

// The binder's front, on a real API server, its pool's pods each at the loopback's address,
// where a stand-in for every pod records what it is sent.

const callerKey = randomBytes(32);
const bindingKey = randomBytes(32);

const withKube = <A, E>(use: (kube: KubeClient["Service"]) => Effect.Effect<A, E>): Effect.Effect<A, E, KubeClient> => Effect.flatMap(KubeClient, use);
const run = <A, E>(effect: Effect.Effect<A, E, KubeClient>): Promise<A> => Effect.runPromise(Effect.provide(effect, Layer.fresh(KubeClient.layer)));

let namespaces = 0;
async function pool(): Promise<Pool> {
  const name = `front-${(namespaces += 1)}`;
  await run(withKube((kube) => kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name } })));
  return { name: "sandboxes", namespace: name };
}

/** `count` free pods of `of`, serving ready at `address`, a loopback's, named from `first` on. */
async function freePods(of: Pool, count: number, first = 0, address = "127.0.0.1"): Promise<void> {
  await Promise.all(Array.from({ length: count }, (_, n) =>
    run(withKube((kube) =>
      Effect.gen(function*() {
        const name = `free-${first + n}`;
        yield* kube.create<V1Pod>({ apiVersion: "v1", kind: "Pod", metadata: { name, namespace: of.namespace, labels: { ...freeLabels(of) } }, spec: { containers: [{ name: "bayma", image: "agent" }] } });
        yield* kube.patchPodStatus(of.namespace, name, { status: { phase: "Running", podIP: address, conditions: [{ type: "Ready", status: "True" }] } });
      })
    ))
  ));
}

/** A stand-in for every bound pod: the headers of what it was sent, in order. */
async function pods(): Promise<{ port: number; sent: IncomingHttpHeaders[]; close: () => void }> {
  const sent: IncomingHttpHeaders[] = [];
  const server = createServer((incoming, outgoing) => {
    sent.push(incoming.headers);
    incoming.resume();
    incoming.on("end", () => outgoing.writeHead(200, { "content-type": "application/json" }).end("{}"));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return { port: (server.address() as AddressInfo).port, sent, close: () => server.close() };
}

/** A front on its own client and Binder, binds waiting up to `waitMs`, every key in `of`; and how to stop it. */
async function front(
  of: Pool,
  podPort: number,
  { waitMs = 5_000, peers = [] as string[], placed = [] as Readonly<Record<string, string>>[] } = {},
): Promise<BinderServer & { close: () => Promise<void> }> {
  const scope = await Effect.runPromise(Scope.make());
  const layer = Layer.provideMerge(Binder.layer({ waitMs }), Layer.fresh(KubeClient.layer));
  const served = await Effect.runPromise(
    serveBinder({ port: 0, host: "127.0.0.1", callerKey, bindingKey, podPort, poolOf: (_key, options) =>
      options["environment"] === "nowhere" ? Effect.fail(new PlacementRefused({ reason: "there is no environment nowhere" })) : Effect.sync(() => (placed.push(options), { pool: of })), peers: () => Effect.succeed(peers) }).pipe(
      Effect.provide(layer),
      Scope.provide(scope),
    ),
  );
  return { ...served, close: () => Effect.runPromise(Scope.close(scope, Exit.void)) };
}

const caller = (key: string, expires = Date.now() + 60_000, options?: Record<string, string>) => `Bearer ${mintToken(callerKey, { key, expires, ...(options ? { options } : {}) })}`;

before(async () => {
  process.env["KUBECONFIG"] = (await startKubeApiServer()).kubeconfig;
});

test("a caller's request binds its key and reaches the key's pod with the binding's token, and the next goes the same way", async () => {
  const of = await pool();
  await freePods(of, 3);
  const stand = await pods();
  const served = await front(of, stand.port);
  try {
    const key = "sessionfs:workspace";
    const stamps: (number | null)[] = [];
    for (const _ of [1, 2]) {
      const response = await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key) }, body: "{}" });
      assert.equal(response.status, 200);
      const held = await run(readLease(of, key));
      stamps.push(held ? activeAt(held) : null);
    }
    const lease = await run(readLease(of, key));
    assert.ok(lease?.spec?.holderIdentity);
    // Stamped active as the first came, and not again within the stamp's interval.
    assert.ok(stamps[0] !== null && Date.now() - (stamps[0] ?? 0) < 30_000, String(stamps));
    assert.equal(stamps[1], stamps[0]);
    const tokens = stand.sent.map(({ authorization }) => verifyToken(bindingKey, String(authorization).slice("Bearer ".length), Date.now()));
    assert.deepEqual(tokens, [{ key, binding: lease.metadata?.uid }, { key, binding: lease.metadata?.uid }]);
    assert.equal(served.inFlight(key), 0);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a request without a caller's token, with a binding's, or with one stopped, is refused, and binds nothing", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  const served = await front(of, stand.port);
  try {
    const key = "sessionfs:workspace";
    const refused = [
      undefined,
      "Bearer not-a-token",
      `Bearer ${mintToken(bindingKey, { key, binding: "uid", expires: Date.now() + 60_000 })}`,
      `Bearer ${mintToken(callerKey, { key, binding: "uid", expires: Date.now() + 60_000 })}`,
      `Bearer ${mintToken(callerKey, { key })}`,
      caller(key, Date.now() - 1),
    ];
    for (const authorization of refused) {
      const response = await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: authorization ? { authorization } : {}, body: "{}" });
      assert.equal(response.status, 401, String(authorization));
    }
    assert.equal(await run(readLease(of, key)), null);
    assert.deepEqual(stand.sent, []);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a bind asked ahead binds the key; one whose pool has no pod in time is answered 503, to be asked again", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  const served = await front(of, stand.port, { waitMs: 1_000 });
  try {
    const bound = await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller("sessionfs:first") } });
    assert.equal(bound.status, 204);
    assert.ok((await run(readLease(of, "sessionfs:first")))?.spec?.holderIdentity);
    assert.deepEqual(stand.sent, []);
    const late = await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller("sessionfs:second") } });
    assert.deepEqual([late.status, late.headers.get("retry-after")], [503, "1"]);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a key whose pod is not reached is answered 503, and its next request binds it anew", async () => {
  const of = await pool();
  // At an address of the loopback nothing listens at.
  await freePods(of, 1, 0, "127.0.0.2");
  const stand = await pods();
  const served = await front(of, stand.port);
  try {
    const key = "sessionfs:workspace";
    const unreached = await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key) }, body: "{}" });
    assert.deepEqual([unreached.status, unreached.headers.get("retry-after")], [503, "1"]);
    // Its pod gone, as one evicted, and another free.
    await run(withKube((kube) => kube.remove("v1", "Pod", of.namespace, "free-0")));
    await freePods(of, 1, 1);
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key) }, body: "{}" })).status, 200);
    assert.equal((await run(readLease(of, key)))?.spec?.holderIdentity, "free-1");
    assert.equal(stand.sent.length, 1);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a pool's counts are its free pods, its bindings, and the binds waiting in every replica", async () => {
  const of = await pool();
  const stand = await pods();
  const [one, two] = [await front(of, stand.port, { waitMs: 3_000 }), await front(of, stand.port, { waitMs: 3_000 })];
  const counting = await front(of, stand.port, { peers: [`127.0.0.1:${one.port}`, `127.0.0.1:${two.port}`, "127.0.0.1:1"] });
  try {
    const waiting = [one, two].map((each, n) => fetch(`http://127.0.0.1:${each.port}/bind`, { method: "POST", headers: { authorization: caller(`sessionfs:waits-${n}`) } }));
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    const counts = await (await fetch(`http://127.0.0.1:${counting.port}/counts/${of.namespace}/${of.name}`)).json();
    assert.deepEqual(counts, { free: 0, bound: 0, waiting: 2 });
    await freePods(of, 3);
    assert.deepEqual((await Promise.all(waiting)).map(({ status }) => status), [204, 204]);
    assert.deepEqual(await (await fetch(`http://127.0.0.1:${counting.port}/counts/${of.namespace}/${of.name}`)).json(), { free: 1, bound: 2, waiting: 0 });
  } finally {
    await Promise.all([one.close(), two.close(), counting.close()]);
    stand.close();
  }
});

test("a key is bound where the gateway places it, by what its caller's token says it is bound with, and refused where it is placed nowhere", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  const placed: Readonly<Record<string, string>>[] = [];
  const served = await front(of, stand.port, { placed });
  try {
    const bound = await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller("sessionfs:workspace", Date.now() + 60_000, { environment: "main", mode: "none" }) } });
    assert.equal(bound.status, 204);
    assert.deepEqual(placed, [{ environment: "main", mode: "none" }]);
    const nowhere = await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller("sessionfs:other", Date.now() + 60_000, { environment: "nowhere" }) } });
    assert.equal(nowhere.status, 403);
    assert.equal(await run(readLease(of, "sessionfs:other")), null);
  } finally {
    await served.close();
    stand.close();
  }
});
