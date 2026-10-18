import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { createServer, type IncomingHttpHeaders } from "node:http";
import type { AddressInfo } from "node:net";
import { before, test } from "node:test";

import type { V1Pod } from "@kubernetes/client-node";
import { Context, Effect, Exit, Layer, Scope } from "effect";

import { Binder } from "../src/binder/binder.ts";
import { activeAt, readLease } from "../src/binder/bindings.ts";
import { LiveBindings } from "../src/binder/live.ts";
import { environmentKey, freeLabels, type Pool } from "../src/binder/names.ts";
import { type BinderServer, MAX_OPERATION_BYTES, NO_BIND_HEADER, type Operation, type Placement, PlacementRefused, serveBinder } from "../src/binder/server.ts";
import { bindingToken, mintToken, type TokenClaims, verifyToken } from "../src/binder/tokens.ts";
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
  {
    waitMs = 5_000,
    peers = [] as string[],
    placed = [] as Readonly<Record<string, string>>[],
    release = undefined as Placement["release"],
    keyOf = callerKey as Buffer | ((claims: TokenClaims) => Buffer | null),
    operate = undefined as ((claims: TokenClaims, operation: Operation) => Effect.Effect<{ status: number; json?: unknown; bytes?: Uint8Array }>) | undefined,
    watched = false,
  } = {},
): Promise<BinderServer & { close: () => Promise<void> }> {
  const scope = await Effect.runPromise(Scope.make());
  const layer = Layer.provideMerge(Binder.layer({ waitMs }), Layer.fresh(KubeClient.layer));
  // Its routes checked against the bindings' Leases as they are watched, where it is told to.
  const live = watched
    ? await Effect.runPromise(Effect.map(Layer.build(LiveBindings.layer({ namespaces: { current: Effect.succeed([of.namespace]), known: (namespace) => Effect.succeed(namespace === of.namespace) } })), (built) => Context.get(built, LiveBindings)).pipe(
      Effect.provide(Layer.fresh(KubeClient.layer)),
      Scope.provide(scope),
    ))
    : null;
  const served = await Effect.runPromise(
    serveBinder({ port: 0, host: "127.0.0.1", callerKey: keyOf, bindingKey, podPort, ...(operate ? { operate } : {}), ...(live ? { routeHeld: live.holds } : {}), poolOf: (_key, options) =>
      options["environment"] === "nowhere" ? Effect.fail(new PlacementRefused({ reason: "there is no environment nowhere" })) : Effect.sync(() => (placed.push(options), { pool: of, release })), peers: () => Effect.succeed(peers) }).pipe(
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
    assert.deepEqual(tokens, Array(2).fill({ key, binding: lease.metadata?.uid, namespace: lease.metadata?.namespace }));
    assert.equal(served.inFlight(key), 0);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a no-bind request for an unbound key takes no Lease and claims no pod; one for a bound key is served, the header its own", async () => {
  const of = await pool();
  await freePods(of, 2);
  const stand = await pods();
  const served = await front(of, stand.port);
  const free = () => run(withKube((kube) => kube.list("v1", "Pod", of.namespace, "alasio.dev/free=true"))).then((listed) => listed.length);
  try {
    const key = "sessionfs:workspace";
    const ask = (method: string, path: string) => fetch(`http://127.0.0.1:${served.port}${path}`, { method, headers: { authorization: caller(key), [NO_BIND_HEADER]: "never" }, ...(method === "POST" ? { body: "{}" } : {}) });
    for (const [method, path] of [["GET", "/status"], ["POST", "/mcp"], ["POST", "/bind"]] as const) {
      const unbound = await ask(method, path);
      assert.deepEqual([unbound.status, await unbound.json()], [409, { bound: false }], `${method} ${path}`);
    }
    assert.equal(await run(readLease(of, key)), null);
    assert.deepEqual([await free(), stand.sent.length], [2, 0]);
    // Bound by a request that may bind: then served, without the header.
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key) }, body: "{}" })).status, 200);
    assert.equal((await ask("GET", "/status")).status, 200);
    assert.deepEqual(stand.sent.map((headers) => headers[NO_BIND_HEADER]), [undefined, undefined]);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a caller's release runs what its placement says comes before, then lets its binding go; one whose step fails keeps it; one of no binding is nothing", async () => {
  const of = await pool();
  await freePods(of, 2);
  const stand = await pods();
  const before: string[] = [];
  let failing = true;
  const served = await front(of, stand.port, {
    release: ({ pod }) => Effect.suspend(() => (before.push(pod), failing ? Effect.fail({ message: "its sandbox was not saved, and runs on" }) : Effect.void)),
  });
  try {
    const key = "sessionfs:released";
    const call = (path: string) => fetch(`http://127.0.0.1:${served.port}${path}`, { method: "POST", headers: { authorization: caller(key) } });
    assert.equal((await call("/release")).status, 204);
    assert.deepEqual(before, []);
    assert.equal((await call("/bind")).status, 204);
    const bound = await run(readLease(of, key));
    const failed = await call("/release");
    assert.deepEqual([failed.status, failed.headers.get("retry-after")], [503, "1"]);
    assert.equal((await run(readLease(of, key)))?.metadata?.uid, bound?.metadata?.uid);
    failing = false;
    assert.equal((await call("/release")).status, 204);
    assert.deepEqual(before, [bound?.spec?.holderIdentity, bound?.spec?.holderIdentity]);
    assert.equal(await run(readLease(of, key)), null);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a no-bind request, of a front that never bound its key, is served by the binding another front made", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  const [binding, reading] = [await front(of, stand.port), await front(of, stand.port)];
  try {
    const key = "sessionfs:elsewhere";
    assert.equal((await fetch(`http://127.0.0.1:${binding.port}/bind`, { method: "POST", headers: { authorization: caller(key) } })).status, 204);
    const read = await fetch(`http://127.0.0.1:${reading.port}/status`, { headers: { authorization: caller(key), [NO_BIND_HEADER]: "never" } });
    assert.equal(read.status, 200);
  } finally {
    await binding.close();
    await reading.close();
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
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/healthz`)).status, 200);
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

test("a bind asked ahead, and a no-bind request, of a key whose binding another let go since, as an idle or broken one is, ask how it is bound now, not the route last known", async () => {
  const of = await pool();
  await freePods(of, 2);
  const stand = await pods();
  const served = await front(of, stand.port);
  const other = await front(of, stand.port);
  try {
    const key = "sessionfs:let-go";
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller(key) } })).status, 204);
    const first = (await run(readLease(of, key)))?.spec?.holderIdentity;
    assert.ok(first);
    // Let go through another front, as the gateway's sweep, or its look at broken bindings, lets one go.
    assert.equal((await fetch(`http://127.0.0.1:${other.port}/release`, { method: "POST", headers: { authorization: caller(key) } })).status, 204);
    const unbound = await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key), [NO_BIND_HEADER]: "never" }, body: "{}" });
    assert.deepEqual([unbound.status, await unbound.json()], [409, { bound: false }]);
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller(key) } })).status, 204);
    const again = (await run(readLease(of, key)))?.spec?.holderIdentity;
    assert.ok(again && again !== first, `${first} then ${again}`);
  } finally {
    stand.close();
    await served.close();
    await other.close();
  }
});

test("a front that watches the bindings forgets a route it knew of a binding another front let go, and binds its key anew before it passes a request by it", async () => {
  const of = await pool();
  await freePods(of, 2);
  const stand = await pods();
  const served = await front(of, stand.port, { watched: true });
  const other = await front(of, stand.port);
  try {
    const key = "sessionfs:watched";
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key) }, body: "{}" })).status, 200);
    const first = await run(readLease(of, key));
    assert.ok(first?.spec?.holderIdentity);
    // Let go through another front, as the gateway's sweep lets one go, and seen gone by the first's watch.
    assert.equal((await fetch(`http://127.0.0.1:${other.port}/release`, { method: "POST", headers: { authorization: caller(key) } })).status, 204);
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key) }, body: "{}" })).status, 200);
    // Bound anew, its request passed with the new binding's token, not the one let go.
    const again = await run(readLease(of, key));
    assert.ok(again?.spec?.holderIdentity && again.metadata?.uid !== first.metadata?.uid, `${first.metadata?.uid} then ${again?.metadata?.uid}`);
    assert.equal(stand.sent.at(-1)?.authorization, `Bearer ${bindingToken(bindingKey, { key, binding: again.metadata?.uid ?? "", namespace: of.namespace })}`);
  } finally {
    stand.close();
    await served.close();
    await other.close();
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

test("a request under /workspace or /snapshots is answered by the front itself, as its operate says, of its caller's token's claims, binding nothing; one past the most it takes is refused", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  const asked: { claims: TokenClaims; method: string; path: string; query: string; body: string }[] = [];
  const served = await front(of, stand.port, {
    operate: (claims, { method, path, query, body }) =>
      Effect.sync(() => {
        asked.push({ claims, method, path, query: query.toString(), body: Buffer.from(body).toString() });
        return path === "/workspace/files" ? { status: 200, bytes: Buffer.from("bytes of a file") } : { status: 201, json: { made: true } };
      }),
  });
  try {
    const key = "sessionfs:workspace";
    const made = await fetch(`http://127.0.0.1:${served.port}/workspace`, { method: "POST", headers: { authorization: caller(key, Date.now() + 60_000, { environment: "main" }) }, body: "{\"a\":1}" });
    assert.deepEqual([made.status, await made.json()], [201, { made: true }]);
    const read = await fetch(`http://127.0.0.1:${served.port}/workspace/files?path=workspace%2Fa`, { headers: { authorization: caller(key) } });
    assert.deepEqual([read.status, read.headers.get("content-type"), await read.text()], [200, "application/octet-stream", "bytes of a file"]);
    const releases = await fetch(`http://127.0.0.1:${served.port}/snapshots/releases`, { headers: { authorization: caller(key) } });
    assert.deepEqual([releases.status, await releases.json()], [201, { made: true }]);
    assert.deepEqual(asked.map(({ claims, ...rest }) => ({ ...rest, key: claims.key, options: claims.options })), [
      { method: "POST", path: "/workspace", query: "", body: "{\"a\":1}", key, options: { environment: "main" } },
      { method: "GET", path: "/workspace/files", query: "path=workspace%2Fa", body: "", key, options: undefined },
      { method: "GET", path: "/snapshots/releases", query: "", body: "", key, options: undefined },
    ]);
    const over = await fetch(`http://127.0.0.1:${served.port}/workspace/files`, { method: "PUT", headers: { authorization: caller(key) }, body: Buffer.alloc(MAX_OPERATION_BYTES + 1) });
    assert.equal(over.status, 413);
    assert.equal(asked.length, 3);
    // Without a caller's token, nothing is asked.
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/workspace`, { method: "POST" })).status, 401);
    assert.deepEqual([await run(readLease(of, key)), stand.sent.length, asked.length], [null, 0, 3]);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a front that checks each environment's callers by its own key takes a token signed by the key of the environment it names alone", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  const served = await front(of, stand.port, { keyOf: ({ options }) => (options?.["environment"] === undefined ? null : environmentKey(callerKey, options["environment"])) });
  try {
    const key = "sessionfs:workspace";
    const signed = (signer: Buffer, environment: string) => `Bearer ${mintToken(signer, { key, expires: Date.now() + 60_000, options: { environment, mode: "none" } })}`;
    const status = async (authorization: string) => (await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization }, body: "{}" })).status;
    assert.equal(await status(signed(environmentKey(callerKey, "try"), "main")), 401);
    assert.equal(await status(signed(callerKey, "main")), 401);
    assert.equal(await status(caller(key)), 401);
    assert.equal(stand.sent.length, 0);
    assert.equal(await status(signed(environmentKey(callerKey, "main"), "main")), 200);
  } finally {
    await served.close();
    stand.close();
  }
});
