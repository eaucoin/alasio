import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { createServer, type IncomingHttpHeaders } from "node:http";
import type { AddressInfo } from "node:net";
import { before, test } from "node:test";

import type { V1Pod } from "@kubernetes/client-node";
import { Context, Effect, Exit, Layer, Scope } from "effect";

import { Binder } from "../src/binder/binder.ts";
import { ACTIVE_ANNOTATION, activeAt, readLease, takeLease, touchLease } from "../src/binder/bindings.ts";
import { baymaIdleness } from "../src/binder/sweep.ts";
import { LiveBindings } from "../src/binder/live.ts";
import { environmentKey, freeLabels, NO_BIND_HEADER, NOT_PASSED_HEADER, PINNED_HEADER, type Pool } from "../src/binder/names.ts";
import { type BinderServer, type BinderServerOptions, FIRST_TOLD_KEPT, MAX_CALLER_TOKEN_MS, MAX_OPERATION_BYTES, type Operation, type Placement, PlacementRefused, serveBinder, TOLD_BYTES } from "../src/binder/server.ts";
import { bindingToken, mintToken, type TokenClaims, verifyToken } from "../src/binder/tokens.ts";
import { heldToHarness } from "../src/gateway/names.ts";
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

/** A front on its own client and Binder, binds waiting up to `waitMs`, every key in `of` but those of an environment `elsewhere` names; and how to stop it. */
async function front(
  of: Pool,
  podPort: number,
  {
    waitMs = 5_000,
    peers = [] as string[],
    placed = [] as Readonly<Record<string, string>>[],
    release = undefined as Placement["release"],
    told = {} as Pick<Placement, "held" | "settled" | "reset" | "port">,
    keyOf = callerKey as Buffer | ((claims: TokenClaims) => Buffer | null),
    operate = undefined as ((claims: TokenClaims, operation: Operation) => Effect.Effect<{ status: number; json?: unknown; bytes?: Uint8Array; headers?: Record<string, string> }>) | undefined,
    watched = false,
    holds = undefined as BinderServerOptions["routeHeld"],
    grants = undefined as BinderServerOptions["grants"],
    relay = undefined as BinderServerOptions["relay"],
    elsewhere = {} as Readonly<Record<string, Pool>>,
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
  const routeHeld = holds ?? live?.holds;
  const served = await Effect.runPromise(
    serveBinder({ port: 0, host: "127.0.0.1", callerKey: keyOf, bindingKey, podPort, ...(operate ? { operate } : {}), ...(routeHeld ? { routeHeld } : {}), ...(grants ? { grants } : {}), ...(relay ? { relay } : {}), poolOf: (_key, options) =>
      options["environment"] === "nowhere" ? Effect.fail(new PlacementRefused({ reason: "there is no environment nowhere" })) : Effect.sync(() => (placed.push(options), { pool: elsewhere[options["environment"] ?? ""] ?? of, release, ...told })), peers: () => Effect.succeed(peers) }).pipe(
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
    assert.equal(served.inFlight(of, key), 0);
  } finally {
    await served.close();
    stand.close();
  }
});

/** The namespace of the binding whose token the stand-in for its pods was last sent. */
const reachedNamespace = (sent: readonly IncomingHttpHeaders[]): string | undefined =>
  verifyToken(bindingKey, String(sent.at(-1)?.authorization).slice("Bearer ".length), Date.now())?.namespace;

test("a key whose placement says its pod serves on a port of its own is passed on there, not on the front's", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  // Nothing serves on the front's port of pods: its placement's alone is reached.
  const served = await front(of, 1, { told: { port: stand.port } });
  try {
    const response = await fetch(`http://127.0.0.1:${served.port}/turns`, { method: "POST", headers: { authorization: caller("harness:telegram:41") }, body: "{}" });
    assert.equal(response.status, 200);
    assert.equal(stand.sent.length, 1);
  } finally {
    await served.close();
    stand.close();
  }
});

test("environments sharing a key each reach their own binding of it, whichever binds first, their requests interleaved, as a branch's forks of main's sessions keep their ids", async () => {
  const [main, branch] = [await pool(), await pool()];
  await freePods(main, 2);
  await freePods(branch, 2);
  const stand = await pods();
  const served = await front(main, stand.port, { elsewhere: { branch } });
  try {
    const key = "sessionfs:inherited";
    const ask = async (environment: string) => {
      const response = await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key, Date.now() + 60_000, { environment }) }, body: "{}" });
      assert.equal(response.status, 200, environment);
      return reachedNamespace(stand.sent);
    };
    // The branch first, then main, then each again, in turn.
    for (const environment of ["branch", "main", "branch", "main", "main", "branch"]) {
      assert.equal(await ask(environment), environment === "main" ? main.namespace : branch.namespace, environment);
    }
    // Each binding of its own, each stamped active by its own requests.
    for (const each of [main, branch]) {
      const lease = await run(readLease(each, key));
      assert.ok(lease?.spec?.holderIdentity && activeAt(lease) !== null, each.namespace);
    }
  } finally {
    await served.close();
    stand.close();
  }
});

test("a branch's requests of a key stamp its own binding of it alone, never main's, which goes idle on its own requests' time", async () => {
  const [main, branch] = [await pool(), await pool()];
  await freePods(main, 1);
  await freePods(branch, 1);
  const stand = await pods();
  const served = await front(main, stand.port, { elsewhere: { branch } });
  try {
    const key = "sessionfs:inherited";
    const as = (environment: string) => ({ authorization: caller(key, Date.now() + 60_000, { environment }) });
    // Main's binding, known to the front, stamped as it was bound, which no request of main's has stamped since.
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: as("main") })).status, 200);
    const bound = await run(readLease(main, key));
    const boundAt = bound ? activeAt(bound) : null;
    assert.ok(boundAt !== null, "main's binding stamped as it was bound");
    for (let n = 0; n < 3; n += 1) assert.equal((await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: as("branch"), body: "{}" })).status, 200);
    assert.equal(reachedNamespace(stand.sent), branch.namespace);
    const [mains, branchs] = [await run(readLease(main, key)), await run(readLease(branch, key))];
    assert.deepEqual([mains?.spec?.holderIdentity !== undefined, mains ? activeAt(mains) : "gone"], [true, boundAt]);
    assert.ok(branchs && activeAt(branchs) !== null, "the branch's own binding stamped");
  } finally {
    await served.close();
    stand.close();
  }
});

test("a caller whose placement refuses its key is refused, though the front knows another environment's route to it", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  const served = await front(of, stand.port);
  try {
    const key = "folder:codex:telegram:1001";
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key, Date.now() + 60_000, { environment: "main" }) }, body: "{}" })).status, 200);
    const sent = stand.sent.length;
    // As a branch's harness would ask of main's folder: placed nowhere for it, so refused, by any request.
    for (const [method, path] of [["POST", "/mcp"], ["GET", "/status"]] as const) {
      const refused = await fetch(`http://127.0.0.1:${served.port}${path}`, { method, headers: { authorization: caller(key, Date.now() + 60_000, { environment: "nowhere" }) }, ...(method === "POST" ? { body: "{}" } : {}) });
      assert.equal(refused.status, 403, `${method} ${path}`);
    }
    assert.equal(stand.sent.length, sent);
  } finally {
    await served.close();
    stand.close();
  }
});

test("an environment's release lets go of its own binding of a key alone, another's of it bound and reached still", async () => {
  const [main, branch] = [await pool(), await pool()];
  await freePods(main, 1);
  await freePods(branch, 1);
  const stand = await pods();
  const served = await front(main, stand.port, { elsewhere: { branch } });
  try {
    const key = "sessionfs:shared";
    const as = (environment: string) => ({ authorization: caller(key, Date.now() + 60_000, { environment }) });
    for (const environment of ["main", "branch"]) assert.equal((await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: as(environment), body: "{}" })).status, 200);
    const held = await run(readLease(main, key));
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/release`, { method: "POST", headers: as("branch") })).status, 204);
    assert.equal(await run(readLease(branch, key)), null);
    assert.equal((await run(readLease(main, key)))?.metadata?.uid, held?.metadata?.uid);
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: as("main"), body: "{}" })).status, 200);
    assert.equal(reachedNamespace(stand.sent), main.namespace);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a caller's token good for longer than alasio's, as a key leaked would mint, is refused; one good for as long is taken", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  const served = await front(of, stand.port);
  try {
    const key = "sessionfs:lasting";
    const ask = (expires: number) => fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key, expires) }, body: "{}" });
    assert.equal((await ask(Date.now() + MAX_CALLER_TOKEN_MS + 60_000)).status, 401);
    assert.equal((await ask(Date.now() + 365 * 24 * 60 * 60_000)).status, 401);
    assert.equal(stand.sent.length, 0);
    assert.equal((await ask(Date.now() + 5 * 60_000)).status, 200);
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

test("a no-bind request reads what a binding has, and keeps it no longer: its Lease is stamped active by the requests that may bind alone", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  const key = "harness:telegram:42";
  const bound = await front(of, stand.port);
  // Another replica, which has stamped nothing yet, so stamps whatever request it may.
  const other = await front(of, stand.port);
  try {
    assert.equal((await fetch(`http://127.0.0.1:${bound.port}/turns`, { method: "POST", headers: { authorization: caller(key) }, body: "{}" })).status, 200);
    await run(touchLease(of, key, 1_000));
    const read = await fetch(`http://127.0.0.1:${other.port}/status`, { method: "POST", headers: { authorization: caller(key), [NO_BIND_HEADER]: "never" }, body: "{}" });
    assert.equal(read.status, 200);
    assert.equal(activeAt((await run(readLease(of, key)))!), 1_000);
    assert.equal((await fetch(`http://127.0.0.1:${other.port}/turns`, { method: "POST", headers: { authorization: caller(key) }, body: "{}" })).status, 200);
    assert.ok((activeAt((await run(readLease(of, key)))!) ?? 0) > 1_000);
  } finally {
    await bound.close();
    await other.close();
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
    assert.equal((await call("/bind")).status, 200);
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

test("a caller's release resets what its placement keeps of its binds, bound or not; one of a key a bind is under way of releases none, and says so", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  let resets = 0;
  const served = await front(of, stand.port, { told: { reset: Effect.sync(() => void (resets += 1)) } });
  try {
    const call = (key: string) => fetch(`http://127.0.0.1:${served.port}/release`, { method: "POST", headers: { authorization: caller(key) } });
    assert.equal((await call("sessionfs:unbound")).status, 204);
    assert.equal(resets, 1);
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller("sessionfs:bound") } })).status, 200);
    assert.equal((await call("sessionfs:bound")).status, 204);
    assert.deepEqual([resets, await run(readLease(of, "sessionfs:bound"))], [2, null]);
    // A bind under way, as another replica's.
    await run(takeLease(of, "sessionfs:under-way", "another"));
    const busy = await call("sessionfs:under-way");
    assert.deepEqual([busy.status, busy.headers.get("retry-after"), ((await busy.json()) as { busy?: boolean }).busy, resets], [409, "1", true, 3]);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a bind that waited out another's bind of its key in vain tells of no failure of its own", async () => {
  const of = await pool();
  const stand = await pods();
  const told: (string | null)[] = [];
  const served = await front(of, stand.port, { waitMs: 1_000, told: { settled: (failure) => Effect.sync(() => void told.push(failure)) } });
  try {
    await run(takeLease(of, "sessionfs:under-way", "another"));
    const bind = await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller("sessionfs:under-way") } });
    assert.equal(bind.status, 503);
    assert.match(((await bind.json()) as { reason: string }).reason, /another binder's binding of it/u);
    assert.deepEqual(told, []);
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
    assert.equal((await fetch(`http://127.0.0.1:${binding.port}/bind`, { method: "POST", headers: { authorization: caller(key) } })).status, 200);
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
    assert.equal(bound.status, 200);
    assert.ok((await run(readLease(of, "sessionfs:first")))?.spec?.holderIdentity);
    assert.deepEqual(stand.sent, []);
    const late = await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller("sessionfs:second") } });
    assert.deepEqual([late.status, late.headers.get("retry-after")], [503, "1"]);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a bind tells its placement how it ended, and the 503 of one that failed says why; one its placement holds is refused, saying why, with no Retry-After, binding nothing", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  const settled: (string | null)[] = [];
  let holding: string | null = null;
  const served = await front(of, stand.port, { waitMs: 1_000, told: { held: Effect.sync(() => holding), settled: (failure) => Effect.sync(() => void settled.push(failure)) } });
  try {
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller("sessionfs:first") } })).status, 200);
    const late = await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller("sessionfs:second") } });
    const said = (await late.json()) as { reason: string };
    assert.equal(late.status, 503);
    assert.match(said.reason, /^sessionfs:second was not bound in pool sandboxes in time: it waited for a free pod$/u);
    assert.deepEqual(settled, [null, said.reason]);
    holding = "its last 3 binds failed, the last as its pod is pending";
    const held = await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller("sessionfs:third") } });
    assert.deepEqual([held.status, held.headers.get("retry-after"), await held.json()], [503, null, { error: "sessionfs:third is bound again only as its caller asks", reason: holding, held: true }]);
    assert.equal(await run(readLease(of, "sessionfs:third")), null);
    assert.equal(settled.length, 2);
    // A request that binds nothing is not held: it asks how the key is bound.
    const unbound = await fetch(`http://127.0.0.1:${served.port}/status`, { headers: { authorization: caller("sessionfs:third"), [NO_BIND_HEADER]: "never" } });
    assert.equal(unbound.status, 409);
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
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller(key) } })).status, 200);
    const first = (await run(readLease(of, key)))?.spec?.holderIdentity;
    assert.ok(first);
    // Let go through another front, as the gateway's sweep, or its look at broken bindings, lets one go.
    assert.equal((await fetch(`http://127.0.0.1:${other.port}/release`, { method: "POST", headers: { authorization: caller(key) } })).status, 204);
    const unbound = await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key), [NO_BIND_HEADER]: "never" }, body: "{}" });
    assert.deepEqual([unbound.status, await unbound.json()], [409, { bound: false }]);
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller(key) } })).status, 200);
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
    // Its connection never made, nothing of it sent: to be sent again, as the front says.
    assert.deepEqual([unreached.status, unreached.headers.get("retry-after"), unreached.headers.get(NOT_PASSED_HEADER)], [503, "1", "true"]);
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
    assert.deepEqual((await Promise.all(waiting)).map(({ status }) => status), [200, 200]);
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
    assert.equal(bound.status, 200);
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
        if (path === "/workspace/files") return method === "PUT" ? { status: 503, json: { error: "not yet" }, headers: { "retry-after": "2" } } : { status: 200, bytes: Buffer.from("bytes of a file") };
        return { status: 201, json: { made: true } };
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
    // Refused for a moment, saying when to ask again, as its operate says.
    const later = await fetch(`http://127.0.0.1:${served.port}/workspace/files?path=workspace%2Fb`, { method: "PUT", headers: { authorization: caller(key) }, body: "b" });
    assert.deepEqual([later.status, later.headers.get("retry-after"), await later.json()], [503, "2", { error: "not yet" }]);
    assert.deepEqual(asked.map(({ claims, ...rest }) => ({ ...rest, key: claims.key, options: claims.options })), [
      { method: "POST", path: "/workspace", query: "", body: "{\"a\":1}", key, options: { environment: "main" } },
      { method: "GET", path: "/workspace/files", query: "path=workspace%2Fa", body: "", key, options: undefined },
      { method: "GET", path: "/snapshots/releases", query: "", body: "", key, options: undefined },
      { method: "PUT", path: "/workspace/files", query: "path=workspace%2Fb", body: "b", key, options: undefined },
    ]);
    const over = await fetch(`http://127.0.0.1:${served.port}/workspace/files`, { method: "PUT", headers: { authorization: caller(key) }, body: Buffer.alloc(MAX_OPERATION_BYTES + 1) });
    assert.equal(over.status, 413);
    assert.equal(asked.length, 4);
    // Without a caller's token, nothing is asked.
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/workspace`, { method: "POST" })).status, 401);
    assert.deepEqual([await run(readLease(of, key)), stand.sent.length, asked.length], [null, 0, 4]);
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

/** A stand-in for a bound pod at `host`, on `port` or one of its own: the bodies it was sent, in order, and how to stop it. */
async function podAt(host: string, port = 0): Promise<{ port: number; bodies: string[]; close: () => Promise<void> }> {
  const bodies: string[] = [];
  const server = createServer((incoming, outgoing) => {
    let body = "";
    incoming.on("data", (chunk: Buffer) => (body += chunk.toString()));
    incoming.on("end", () => {
      bodies.push(body);
      outgoing.writeHead(200, { "content-type": "application/json" }).end("{}");
    });
  });
  server.listen(port, host);
  await once(server, "listening");
  return { port: (server.address() as AddressInfo).port, bodies, close: () => new Promise<void>((resolve) => server.close(() => resolve())) };
}

test("a binding is stamped active as its bind is through, so one whose bind took longer than the wait to release it, as a restore's may, is not idle as its first request comes", async () => {
  const of = await pool();
  await freePods(of, 1);
  // Its pod's bayma, idle since long before, as a sandbox restored from a snapshot says it is.
  const status = createServer((_incoming, outgoing) => void outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 300_000 })));
  status.listen(0, "127.0.0.1");
  await once(status, "listening");
  const statusPort = (status.address() as AddressInfo).port;
  const served = await front(of, statusPort);
  try {
    const key = "sessionfs:restored";
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller(key) } })).status, 200);
    const lease = await run(readLease(of, key));
    assert.ok(lease);
    const boundAt = activeAt(lease);
    assert.ok(boundAt !== null && Math.abs(Date.now() - boundAt) < 10_000, `stamped as it was bound: ${boundAt}`);
    const pod = await run(withKube((kube) => kube.read("v1", "Pod", of.namespace, lease.spec?.holderIdentity ?? ""))) as V1Pod;
    // Its pod made two minutes before its bind was through, as a restore's is, which takes that long.
    const madeEarlier: V1Pod = { ...pod, metadata: { ...pod.metadata, creationTimestamp: new Date(boundAt - 120_000) } };
    const idleness = baymaIdleness({ podPort: statusPort, releaseAfterMs: 60_000, bindingKey });
    const asked = await Effect.runPromise(idleness(lease, madeEarlier, boundAt + 10_000));
    assert.equal(asked.idle, false, "released as its first request comes");
    // Unstamped, as before, it would have been.
    const unstamped = { ...lease, metadata: { ...lease.metadata, annotations: Object.fromEntries(Object.entries(lease.metadata?.annotations ?? {}).filter(([name]) => name !== ACTIVE_ANNOTATION)) } };
    assert.equal(activeAt(unstamped), null);
    assert.equal((await Effect.runPromise(idleness(unstamped, madeEarlier, boundAt + 10_000))).idle, true);
  } finally {
    await served.close();
    status.close();
  }
});

test("a request on a route the front knew of, whose binding was released before its watch said so, binds its key anew and is passed there, its caller never told the pod was not reached", async () => {
  const of = await pool();
  const gone = await podAt("127.0.0.2");
  const next = await podAt("127.0.0.1", gone.port);
  await freePods(of, 1, 0, "127.0.0.2");
  const served = await front(of, gone.port);
  try {
    const key = "sessionfs:released";
    const asked = (body: string) => fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key) }, body });
    assert.equal((await asked("first")).status, 200);
    assert.deepEqual(gone.bodies, ["first"]);
    // Released by another, its pod gone with it; another pod free to bind it in.
    await Effect.runPromise(Effect.provide(Effect.flatMap(Binder, (binder) => binder.release(of, key)), Layer.provideMerge(Binder.layer({ waitMs: 5_000 }), Layer.fresh(KubeClient.layer))));
    await gone.close();
    await freePods(of, 1, 1, "127.0.0.1");
    const second = await asked("second");
    assert.equal(second.status, 200, await second.text());
    assert.deepEqual(next.bodies, ["second"]);
    const lease = await run(readLease(of, key));
    assert.equal(lease?.spec?.holderIdentity, "free-1");
  } finally {
    await served.close();
    await next.close();
  }
});

test("a request of no stated length on a route of a binding gone is streamed to its pod once, never read whole to pass again: its caller is told it was passed to no pod, to send it again itself", async () => {
  const of = await pool();
  const gone = await podAt("127.0.0.2");
  const next = await podAt("127.0.0.1", gone.port);
  await freePods(of, 1, 0, "127.0.0.2");
  const served = await front(of, gone.port);
  try {
    const key = "sessionfs:streamed";
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key) }, body: "first" })).status, 200);
    await Effect.runPromise(Effect.provide(Effect.flatMap(Binder, (binder) => binder.release(of, key)), Layer.provideMerge(Binder.layer({ waitMs: 5_000 }), Layer.fresh(KubeClient.layer))));
    await gone.close();
    await freePods(of, 1, 1, "127.0.0.1");
    // Its body as it comes, chunked, as a stream's is.
    const chunked = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("sec"));
        controller.enqueue(new TextEncoder().encode("ond"));
        controller.close();
      },
    });
    const second = await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key) }, body: chunked, duplex: "half" } as RequestInit);
    assert.deepEqual([second.status, second.headers.get("retry-after"), second.headers.get(NOT_PASSED_HEADER)], [503, "1", "true"]);
    assert.deepEqual(next.bodies, [], "read whole and passed again");
  } finally {
    await served.close();
    await next.close();
  }
});

test("a request on a route of a binding gone that reached its pod before its connection was lost is not passed again, as its pod may have run it: its caller is told the pod was not reached", async () => {
  const of = await pool();
  // Its pod takes the request, then drops its connection unanswered, as one ending under it would.
  let dropping = false;
  const taken: string[] = [];
  const old = createServer((incoming, outgoing) => {
    let body = "";
    incoming.on("data", (chunk: Buffer) => (body += chunk.toString()));
    incoming.on("end", () => {
      taken.push(body);
      if (dropping) incoming.socket.destroy();
      else outgoing.writeHead(200, { "content-type": "application/json" }).end("{}");
    });
  });
  old.listen(0, "127.0.0.2");
  await once(old, "listening");
  const port = (old.address() as AddressInfo).port;
  const next = await podAt("127.0.0.1", port);
  await freePods(of, 1, 0, "127.0.0.2");
  const served = await front(of, port);
  try {
    const key = "sessionfs:dropped";
    const asked = (body: string) => fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key) }, body });
    assert.equal((await asked("first")).status, 200);
    await Effect.runPromise(Effect.provide(Effect.flatMap(Binder, (binder) => binder.release(of, key)), Layer.provideMerge(Binder.layer({ waitMs: 5_000 }), Layer.fresh(KubeClient.layer))));
    await freePods(of, 1, 1, "127.0.0.1");
    dropping = true;
    const second = await asked("second");
    // Its pod may have run it: not to be sent again, as the front says by neither header.
    assert.deepEqual([second.status, second.headers.get("retry-after"), second.headers.get(NOT_PASSED_HEADER)], [503, null, null]);
    assert.deepEqual(await second.json(), { error: `the pod of ${key} was not reached` });
    assert.deepEqual(taken, ["first", "second"]);
    assert.deepEqual(next.bodies, [], "passed again");
  } finally {
    await served.close();
    await next.close();
    old.close();
  }
});

test("a request that may bind, of a key another replica bound anew within the stamp's interval, stamps the new binding, whose stamps are its own", async () => {
  const of = await pool();
  await freePods(of, 2);
  const stand = await pods();
  const served = await front(of, stand.port);
  const elsewhere = Layer.provideMerge(Binder.layer({ waitMs: 5_000 }), Layer.fresh(KubeClient.layer));
  try {
    const key = "sessionfs:rebound";
    // Bound and stamped by this front.
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key) }, body: "{}" })).status, 200);
    const first = await run(readLease(of, key));
    assert.ok(first && activeAt(first) !== null);
    // Released and bound anew by another replica, which stamps nothing of it, within this one's stamp's interval.
    await Effect.runPromise(Effect.provide(Effect.flatMap(Binder, (binder) => Effect.andThen(binder.release(of, key), binder.bind(of, key))), elsewhere));
    const rebound = await run(readLease(of, key));
    assert.ok(rebound && rebound.metadata?.uid !== first.metadata?.uid, "bound anew");
    assert.equal(activeAt(rebound), null);
    // A request that binds nothing asks how the key is bound now, and so routes it to the new binding, stamping nothing.
    const read = await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key), [NO_BIND_HEADER]: "never" }, body: "{}" });
    assert.equal(read.status, 200);
    assert.equal(activeAt((await run(readLease(of, key)))!), null, "a request that binds nothing stamped");
    // The next, which may bind, on that route, within the stamp's interval of the binding gone.
    const passed = await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key) }, body: "{}" });
    assert.equal(passed.status, 200);
    const stamped = await run(readLease(of, key));
    assert.equal(stamped?.metadata?.uid, rebound.metadata?.uid);
    assert.ok(stamped && activeAt(stamped) !== null, "the new binding's first request stamped");
  } finally {
    await served.close();
    stand.close();
  }
});

test("a caller's token that stops at no time is good only held to a binding of another key the gateway lets it be, for as long as that binding is held; one held to none is no caller's", async () => {
  const of = await pool();
  await freePods(of, 3);
  const stand = await pods();
  const served = await front(of, stand.port, { watched: true, grants: (key, held) => heldToHarness(key, held.key) });
  try {
    const at = (path: string, authorization: string) => fetch(`http://127.0.0.1:${served.port}${path}`, { method: "POST", headers: { authorization }, body: "{}" });
    const harness = "harness:telegram:1";
    const bound = await at("/bind", caller(harness));
    assert.equal(bound.status, 200);
    const { binding, namespace } = (await bound.json()) as { binding: string; namespace: string };
    assert.deepEqual([binding, namespace], [(await run(readLease(of, harness)))?.metadata?.uid, of.namespace]);
    const heldTo = (key: string, held = { key: harness, binding, namespace }) => `Bearer ${mintToken(callerKey, { key, held, options: { environment: "main", mode: "none" } })}`;
    // Its conversation's session's, and its own folder instance's: passed on.
    assert.equal((await at("/mcp", heldTo("sessionfs:workspace"))).status, 200);
    assert.equal((await at("/mcp", heldTo("folder:claude:telegram:1"))).status, 200);
    // Another conversation's folder instance's, another key's than a workspace's, a binding not held, or held to none: refused.
    assert.equal((await at("/mcp", heldTo("folder:claude:telegram:2"))).status, 401);
    assert.equal((await at("/mcp", heldTo("harness:telegram:2"))).status, 401);
    assert.equal((await at("/mcp", heldTo("sessionfs:workspace", { key: harness, binding: "another-binding", namespace }))).status, 401);
    // Held to a binding in another namespace, the watch knowing none of it: refused.
    assert.equal((await at("/mcp", heldTo("sessionfs:workspace", { key: harness, binding, namespace: "another-namespace" }))).status, 401);
    // Held to one not heard of yet, minted since the watch last heard anything: not known yet, to be asked again.
    const unseen = `Bearer ${mintToken(callerKey, { key: "sessionfs:workspace", held: { key: harness, binding: "not-heard-of-yet", namespace }, issued: Date.now() + 60_000, options: { environment: "main", mode: "none" } })}`;
    const notYet = await at("/mcp", unseen);
    assert.deepEqual([notYet.status, notYet.headers.get("retry-after")], [503, "1"]);
    // Minted a minute before the front's clock says, as a clock of alasio's behind the front's: not known yet either.
    const behind = `Bearer ${mintToken(callerKey, { key: "sessionfs:workspace", held: { key: harness, binding: "not-heard-of-yet", namespace }, issued: Date.now() - 60_000, options: { environment: "main", mode: "none" } })}`;
    assert.equal((await at("/mcp", behind)).status, 503);
    // Minted well before the watch last heard of its namespace, its binding not held: refused.
    const stale = `Bearer ${mintToken(callerKey, { key: "sessionfs:workspace", held: { key: harness, binding: "not-heard-of-yet", namespace }, issued: Date.now() - 60 * 60_000, options: { environment: "main", mode: "none" } })}`;
    assert.equal((await at("/mcp", stale)).status, 401);
    assert.equal((await at("/mcp", `Bearer ${mintToken(callerKey, { key: "sessionfs:workspace", options: { environment: "main", mode: "none" } })}`)).status, 401);
    // Its binding released: refused once its Lease's deletion is seen.
    assert.equal((await at("/release", caller(harness))).status, 204);
    for (const deadline = Date.now() + 10_000; (await at("/mcp", heldTo("sessionfs:workspace"))).status !== 401; await new Promise((resolve) => setTimeout(resolve, 100))) {
      assert.ok(Date.now() < deadline, "a token held to a binding released was taken still");
    }
  } finally {
    await served.close();
    stand.close();
  }
});

test("a caller's token held to a binding whether held is not known yet is answered 503, to be asked again, nothing passed on", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  const served = await front(of, stand.port, { holds: () => Effect.succeed("unknown"), grants: () => true });
  try {
    const held = `Bearer ${mintToken(callerKey, { key: "sessionfs:workspace", held: { key: "harness:telegram:1", binding: "binding-1", namespace: of.namespace } })}`;
    const answered = await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: held }, body: "{}" });
    assert.deepEqual([answered.status, answered.headers.get("retry-after"), answered.headers.get(NOT_PASSED_HEADER), await answered.json()], [503, "1", "true", { error: "whether harness:telegram:1's binding is held is not known yet" }]);
    assert.deepEqual(stand.sent, []);
    assert.equal(await run(readLease(of, "sessionfs:workspace")), null, "nothing bound");
  } finally {
    await served.close();
    stand.close();
  }
});

test("a caller's token held to a binding asks its key's pod and its workspace's files alone, nothing else the front answers itself", async () => {
  const of = await pool();
  await freePods(of, 1);
  const stand = await pods();
  const operated: string[] = [];
  const served = await front(of, stand.port, {
    holds: () => Effect.succeed("held"),
    grants: () => true,
    operate: (_claims, { method, path }) => Effect.sync(() => (operated.push(`${method} ${path}`), { status: 200, json: {} })),
  });
  try {
    const held = `Bearer ${mintToken(callerKey, { key: "sessionfs:workspace", held: { key: "harness:telegram:1", binding: "binding-1", namespace: of.namespace }, options: { environment: "main", mode: "none" } })}`;
    const at = (method: string, path: string) => fetch(`http://127.0.0.1:${served.port}${path}`, { method, headers: { authorization: held }, ...(method === "GET" || method === "DELETE" ? {} : { body: "{}" }) });
    for (const [method, path] of [["DELETE", "/workspace"], ["POST", "/workspace"], ["POST", "/workspace/forks"], ["GET", "/workspace/all"], ["GET", "/workspace/quota"], ["POST", "/bind"], ["POST", "/release"], ["GET", "/snapshots"], ["GET", "/folders/starting"], ["DELETE", "/workspace/files?path=workspace%2Fa"]] as const) {
      assert.equal((await at(method, path)).status, 403, `${method} ${path}`);
    }
    assert.deepEqual([operated, stand.sent.length], [[], 0], "none answered, none passed on");
    assert.equal((await at("GET", "/workspace/files?path=workspace%2Fa")).status, 200);
    assert.equal((await at("PUT", "/workspace/files?path=workspace%2Fa")).status, 200);
    assert.equal((await at("POST", "/mcp")).status, 200);
    assert.deepEqual([operated, stand.sent.length], [["GET /workspace/files", "PUT /workspace/files"], 1]);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a pod's answer that says the front passed its request to no pod is passed on without saying so, as the front alone says it", async () => {
  const of = await pool();
  await freePods(of, 1);
  // A pod that answers as the front answers a request it passed nowhere.
  const pretending = createServer((incoming, outgoing) => {
    incoming.resume();
    incoming.on("end", () => outgoing.writeHead(503, { "retry-after": "1", [NOT_PASSED_HEADER]: "true" }).end("not yet"));
  });
  pretending.listen(0, "127.0.0.1");
  await once(pretending, "listening");
  const served = await front(of, (pretending.address() as AddressInfo).port);
  try {
    const answered = await fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller("sessionfs:pretends") }, body: "{}" });
    assert.deepEqual([answered.status, answered.headers.get("retry-after"), answered.headers.get(NOT_PASSED_HEADER), await answered.text()], [503, "1", null, "not yet"]);
  } finally {
    await served.close();
    pretending.close();
  }
});

test("a pod tells the front with its binding's token alone, while its binding is held: a caller's token, a binding gone, and a telling past its bound are refused, and one whose binding is not known yet is told again", async () => {
  const of = await pool();
  const stand = await pods();
  const key = "harness:telegram:41";
  /** What each binding's watch says, by its Lease's uid: gone where it says nothing. */
  const held = new Map<string, "held" | "unknown">([["binding-1", "held"], ["binding-2", "unknown"]]);
  const asked: { readonly binding: string; readonly since: number | undefined }[] = [];
  const told: { readonly bound: unknown; readonly said: string }[] = [];
  const served = await front(of, stand.port, {
    holds: (_key, binding, _namespace, since) => Effect.sync(() => (asked.push({ binding, since }), held.get(binding) ?? "gone")),
    relay: (bound, body) => Effect.sync(() => (told.push({ bound, said: Buffer.from(body).toString("utf8") }), { status: 202 })),
  });
  try {
    const tell = (authorization: string, body = '{"kind":"harness.idle"}', method = "POST") =>
      fetch(`http://127.0.0.1:${served.port}/events`, { method, headers: { authorization }, ...(method === "POST" ? { body } : {}) });
    const of1 = (binding: string) => `Bearer ${bindingToken(bindingKey, { key, binding, namespace: of.namespace })}`;
    // Its binding held: told on, as the pod's binding, whole.
    const before = Date.now();
    assert.equal((await tell(of1("binding-1"))).status, 202);
    assert.deepEqual(told, [{ bound: { key, binding: "binding-1", namespace: of.namespace }, said: '{"kind":"harness.idle"}' }]);
    // Of a binding not seen, not known until the watch has heard of its namespace since the telling.
    assert.ok((asked[0]?.since ?? 0) >= before);
    // Not known yet: told again after a while, known since it was first told of, not since each telling, so one gone is found gone; gone: refused.
    const unknown = await tell(of1("binding-2"));
    assert.deepEqual([unknown.status, unknown.headers.get("retry-after")], [503, "1"]);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal((await tell(of1("binding-2"))).status, 503);
    const sinces = asked.filter(({ binding }) => binding === "binding-2").map(({ since }) => since);
    assert.deepEqual([sinces.length, sinces[1]], [2, sinces[0]]);
    assert.equal((await tell(of1("binding-3"))).status, 401);
    // A caller's token, the key's own, or one held to the binding, is no binding's; nor is none.
    assert.equal((await tell(caller(key))).status, 401);
    assert.equal((await tell(`Bearer ${mintToken(callerKey, { key: "session:fs-a", held: { key, binding: "binding-1", namespace: of.namespace }, issued: Date.now() })}`)).status, 401);
    assert.equal((await tell("Bearer forged")).status, 401);
    // A telling past its bound, and one that is no POST.
    assert.equal((await tell(of1("binding-1"), "x".repeat(TOLD_BYTES + 1))).status, 413);
    assert.equal((await tell(of1("binding-1"), "", "GET")).status, 405);
    assert.equal(told.length, 1);
    // Nor does a pod reach anything else of the front's, a wake of the lake's loader among it: no path but /events takes a binding's token.
    for (const path of ["/wake", "/lake", "/bind"]) assert.equal((await fetch(`http://127.0.0.1:${served.port}${path}`, { method: "POST", headers: { authorization: of1("binding-1") }, body: '{"lsn":"FFFFFFFF/FFFFFFFF"}' })).status, 401, path);
    // None of it reached a pod, nor bound one.
    assert.deepEqual(stand.sent, []);
  } finally {
    await served.close();
    stand.close();
  }
});

test("the front remembers when bindings not known yet were first told of, of so many at most, the oldest let go of first, so no pod's tellings grow it", { timeout: 60_000 }, async () => {
  const of = await pool();
  const stand = await pods();
  const key = "harness:telegram:41";
  const asked = new Map<string, number[]>();
  const served = await front(of, stand.port, {
    holds: (_key, binding, _namespace, since) => Effect.sync(() => (asked.set(binding, [...(asked.get(binding) ?? []), since ?? 0]), "unknown" as const)),
    relay: () => Effect.succeed({ status: 202 }),
  });
  try {
    const tell = (binding: string) =>
      fetch(`http://127.0.0.1:${served.port}/events`, { method: "POST", headers: { authorization: `Bearer ${bindingToken(bindingKey, { key, binding, namespace: of.namespace })}` }, body: '{"kind":"harness.idle"}' });
    assert.equal((await tell("binding-0")).status, 503);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal((await tell("binding-0")).status, 503);
    // Remembered while it is among the latest.
    assert.equal(asked.get("binding-0")?.[1], asked.get("binding-0")?.[0]);
    for (let n = 1; n <= FIRST_TOLD_KEPT; n += 1) assert.equal((await tell(`binding-${n}`)).status, 503);
    // Let go of, as so many were told of since: asked of anew.
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal((await tell("binding-0")).status, 503);
    const sinces = asked.get("binding-0") ?? [];
    assert.ok((sinces[2] ?? 0) > (sinces[0] ?? 0), "the oldest was not let go of");
    // The latest remembered still.
    assert.equal((await tell(`binding-${FIRST_TOLD_KEPT}`)).status, 503);
    const latest = asked.get(`binding-${FIRST_TOLD_KEPT}`) ?? [];
    assert.equal(latest[1], latest[0]);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a front no pod tells anything answers a telling 404", async () => {
  const of = await pool();
  const stand = await pods();
  const served = await front(of, stand.port);
  try {
    const answered = await fetch(`http://127.0.0.1:${served.port}/events`, { method: "POST", headers: { authorization: `Bearer ${bindingToken(bindingKey, { key: "harness:telegram:41", binding: "binding-1", namespace: of.namespace })}` }, body: "{}" });
    assert.equal(answered.status, 404);
    assert.deepEqual(stand.sent, []);
  } finally {
    await served.close();
    stand.close();
  }
});

test("a request for one binding alone is passed to its pod while its key is bound to it, and refused, passed to none, saying the binding now, once the key is bound anew", async () => {
  const of = await pool();
  await freePods(of, 2);
  const stand = await pods();
  const served = await front(of, stand.port, { watched: true });
  try {
    const key = "session:fs-pin";
    const bind = async () => (await (await fetch(`http://127.0.0.1:${served.port}/bind`, { method: "POST", headers: { authorization: caller(key) } })).json()) as { readonly binding: string; readonly namespace: string };
    const ask = (pinned: string) => fetch(`http://127.0.0.1:${served.port}/mcp`, { method: "POST", headers: { authorization: caller(key), [PINNED_HEADER]: pinned }, body: "{}" });
    const first = await bind();
    assert.equal((await ask(first.binding)).status, 200);
    // The pod is sent no word of the binding asked for.
    assert.equal(stand.sent.at(-1)?.[PINNED_HEADER], undefined);
    const sent = stand.sent.length;
    // Let go of, and bound anew to another pod.
    assert.equal((await fetch(`http://127.0.0.1:${served.port}/release`, { method: "POST", headers: { authorization: caller(key) } })).status, 204);
    const second = await bind();
    assert.notEqual(second.binding, first.binding);
    const refused = await ask(first.binding);
    assert.deepEqual([refused.status, refused.headers.get(NOT_PASSED_HEADER), ((await refused.json()) as { readonly moved?: unknown }).moved], [409, "true", { binding: second.binding, namespace: of.namespace }]);
    assert.equal(stand.sent.length, sent);
    assert.equal((await ask(second.binding)).status, 200);
  } finally {
    await served.close();
    stand.close();
  }
});
