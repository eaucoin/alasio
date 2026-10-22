/**
 * A stand-in for a deployment's harness pools, as alasio's tests run its services on them:
 * alasio's own pooled host (src/harness/pooled-host.ts) reaching each conversation's host
 * through the sandbox gateway's real front (src/binder/server.ts serveBinder), which binds the
 * conversation's harness key to a pod of its own, `alasio harness` itself (src/harness/
 * program.ts startHarness) run in the test's process on the harnesses a test stands in, its
 * store reached as the harness host's role, row-level security holding it to the rows of the
 * conversation its grant is of; what a pod tells the front is relayed as the gateway relays it
 * (src/gateway/harnesses.ts relayed) onto the environment's events topic, held in memory, which
 * alasio reads as it reads the log's (src/events/consume.ts, src/harness/host-events.ts).
 *
 * Under the front, what binds a key stands in for Kubernetes: a bind writes the pod's binding
 * beside it and starts its host, as a pool's free pod is readied and its node's agent writes its
 * binding; a release ends it; and what the front stamps on a binding's Lease is refused, as of
 * a Lease there is not, which the front lets be. The pods' Codex home, and alasio's, is one of the
 * test process's own (CODEX_HOME), so no test writes the operator's.
 *
 * Each store a test runs its services on is of a database of its own (pooledStore), alasio's
 * state in its own schema and the stores of Claude Code's transcripts and Codex's rollouts beside
 * it, as alasio makes them, the harness host's role granted its part of them: the stores' tables,
 * which every state of one database would share, are held to one state's grants alone.
 */
import { randomBytes, randomUUID } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer as createHttpServer } from "node:http";
import { type AddressInfo, createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after } from "node:test";

import { Clock, ConfigProvider, Context, type Duration, Effect, Exit, Fiber, FiberSet, Layer, Scope } from "effect";
import pg from "pg";

import { HARNESS_ROLE } from "../../neon/control/secrets.ts";
import { scramVerifier } from "../../neon/control/scram.ts";
import { Binder, type Bound, type Released } from "../../src/binder/binder.ts";
import { type HeldTo, bindingToken } from "../../src/binder/tokens.ts";
import { PlacementRefused, serveBinder } from "../../src/binder/server.ts";
import type { Pool as BoundPool } from "../../src/binder/names.ts";
import type { Held } from "../../src/binder/live.ts";
import { CodexAppServer } from "../../src/codex/app-server/client.ts";
import { ActiveTurns } from "../../src/harness/active-turns.ts";
import { NeonRolloutStore, SESSION_FS_SCHEMA } from "../../src/codex/rollouts/store.ts";
import { EnvironmentEvents, EventsConsumer } from "../../src/events/consume.ts";
import { eventRecord } from "../../src/events/events.ts";
import { eventsTopic } from "../../src/events/names.ts";
import { relayed } from "../../src/gateway/harnesses.ts";
import { conversationOfHarnessKey, folderOfKey, heldToHarness } from "../../src/gateway/names.ts";
import { NeonSessionStore } from "../../src/harness/claude/session-store.ts";
import { HarnessHost } from "../../src/harness/host.ts";
import type { HarnessHostError } from "../../src/harness/index.ts";
import type { StoreError } from "../../src/persistence/sql.ts";
import { HarnessEvents } from "../../src/harness/host-events.ts";
import { type HarnessStandIns, startHarness } from "../../src/harness/program.ts";
import { pooledHarnessHost } from "../../src/harness/pooled-host.ts";
import { KubeApiError, KubeClient } from "../../src/kube/client.ts";
import { ensureHarnessRole } from "../../src/neon/harness.ts";
import { Store } from "../../src/persistence/store.ts";
import { startPostgres } from "./postgres.ts";
import { type MemoryTopic, memoryTopic } from "./topics.ts";

/** The harness host's role's password, as the stand-in's databases keep its verifier. */
const HARNESS_PASSWORD = "harness-password";
/** The environment the stand-in's alasio is, main. */
export const ENVIRONMENT = "main";
/** Where the stand-in's alasio tells its pods its Codex login is relayed: nowhere, as the harnesses its pods' hosts make call no model. */
export const CODEX_LOGIN_URL = "http://127.0.0.1:9/v1";
/** The namespace its pools are in, as main's are. */
const NAMESPACE = "alasio";

// The test process's own Codex home, its pods' and alasio's, so that no test writes the operator's:
// removed as the test file's tests end, where it is made here.
if (process.env["CODEX_HOME"] === undefined) {
  const home = mkdtempSync(join(tmpdir(), "alasio-pools-codex-"));
  process.env["CODEX_HOME"] = home;
  after(() => rmSync(home, { recursive: true, force: true }));
}

/** Where a pooled store is: its database, as alasio owns it, a pool of its, and as the harness host's role reaches it. */
export interface PooledDatabase {
  readonly url: string;
  readonly pool: pg.Pool;
  readonly harnessUrl: string;
}

/** The schema a pooled store's state is in, alasio's own. */
export const POOLED_SCHEMA = "state";

/** Where a store of pooledStore's is, kept on it as a property of its own, so a store a test makes of one, spreading it, keeps it too. */
const DATABASE = Symbol("alasio/test/pooled-database");
/** The database of `store`, where it is one of pooledStore's, or one a test made of one. */
const databaseOf = (store: Store["Service"]): PooledDatabase | undefined => (store as Store["Service"] & { readonly [DATABASE]?: PooledDatabase })[DATABASE];
/** The pools pooledStore opened, ended as the test file's tests end. */
const opened: pg.Pool[] = [];
after(async () => {
  await Promise.all(opened.splice(0).map((pool) => pool.end()));
});

/** What a pooled store is made with, as testStore's are (./store.ts). */
export interface PooledStoreOptions {
  readonly workingDirectory?: string | null | undefined;
  readonly branch?: string | null | undefined;
  readonly handlingLockTimeout?: Duration.Input | undefined;
  readonly chatStatementTimeout?: Duration.Input | undefined;
}

/**
 * A database of its own, as alasio's Neon is made for it: the harness host's role, and the
 * stores of the transcripts and rollouts, made before alasio's state, as alasio makes them, for
 * the role to be granted theirs; its pool ended as the test file's tests end.
 */
export async function pooledDatabase(): Promise<PooledDatabase> {
  const database = await startPostgres();
  const pool = new pg.Pool({ connectionString: database.url, max: 4 });
  opened.push(pool);
  await ensureHarnessRole(pool, scramVerifier(HARNESS_PASSWORD));
  for (const store of [new NeonSessionStore(pool), new NeonRolloutStore(pool), new NeonRolloutStore(pool, { schema: SESSION_FS_SCHEMA })]) await store.ensureSchema();
  const harnessUrl = new URL(database.url);
  Object.assign(harnessUrl, { username: HARNESS_ROLE, password: HARNESS_PASSWORD });
  return { url: database.url, pool, harnessUrl: harnessUrl.toString() };
}

/** A store of a database of its own (pooledDatabase), as the module says: alasio's state, made as alasio makes it, the harness host's role granted its part. */
export async function pooledStore({ workingDirectory = null, branch = null, handlingLockTimeout, chatStatementTimeout }: PooledStoreOptions = {}): Promise<Store["Service"]> {
  const database = await pooledDatabase();
  const store = await Effect.runPromise(Effect.provide(Store, Store.layer({ pool: database.pool, workingDirectory, branch, handlingLockTimeout, chatStatementTimeout, harnessRole: HARNESS_ROLE })));
  return Object.assign(store, { [DATABASE]: database });
}

/** The pool, as alasio, of the database of `store`, where it is one of pooledStore's (or a store a test made of one), and null otherwise. */
export function pooledPoolOf(store: Store["Service"]): pg.Pool | null {
  return databaseOf(store)?.pool ?? null;
}

/** The pool, as alasio, of the database of `store`, one of pooledStore's, for a test to read or change it as no store does. */
export function poolOf(store: Store["Service"]): pg.Pool {
  const database = databaseOf(store);
  if (!database) throw new Error("not a store of pooledStore's");
  return database.pool;
}

/** The holds `store`, one of pooledStore's, keeps (conversation_holds), as [conversation, reason], in the order held. */
export async function holdsOf(store: Store["Service"]): Promise<(readonly [string, string])[]> {
  const { rows } = await poolOf(store).query<{ conversation_id: string; reason: string }>(`select conversation_id, reason from ${POOLED_SCHEMA}.conversation_holds order by held_at`);
  return rows.map(({ conversation_id, reason }) => [conversation_id, reason] as const);
}

/** A port of the loopback no one listens on now, which a pod is to serve on. */
async function freePort(): Promise<number> {
  const probe = createServer();
  await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
  // A server listening on a TCP port has an address of its own.
  const { port } = probe.address() as AddressInfo;
  await new Promise((resolve) => probe.close(resolve));
  return port;
}

/**
 * Every folder instance's bayma, as the front passes a folder workspace's harness's requests on
 * to it: what each asked, its path and its bearer, answered as a bayma that has nothing to say
 * would answer; closed as the scope is.
 */
const folderBaymaStandIn: Effect.Effect<{ readonly port: number; readonly asked: readonly { readonly path: string; readonly authorization: string | undefined }[] }, never, Scope.Scope> =
  Effect.acquireRelease(
    Effect.promise(async () => {
      const asked: { path: string; authorization: string | undefined }[] = [];
      const server = createHttpServer((incoming, outgoing) => {
        asked.push({ path: incoming.url ?? "", authorization: incoming.headers.authorization });
        incoming.resume();
        outgoing.writeHead(200, { "content-type": "application/json" }).end("{}");
      });
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      return { server, port: (server.address() as AddressInfo).port, asked };
    }),
    ({ server }) => Effect.promise(() => new Promise((resolve) => server.close(resolve))),
  );

/** A pod of a key's: the port it serves on; its bind, while one is under way; and, once bound, its binding, and the scope its host runs in. */
interface Pod {
  readonly name: string;
  readonly port: number;
  binding: Fiber.Fiber<Bound> | null;
  bound: { readonly binding: string; readonly scope: Scope.Closeable; readonly stateDir?: string } | null;
}

/**
 * What the stand-in is made with: the harnesses its pods' hosts make in place of the
 * deployment's, the branch environment it is, none for main, and what alasio's host is made
 * into, for a test that stands in for part of it.
 */
export interface PoolsOptions {
  readonly standIns?: HarnessStandIns | undefined;
  readonly branch?: string | undefined;
  readonly host?: ((host: HarnessHost["Service"]) => HarnessHost["Service"]) | undefined;
}

/** What the stand-in says of its pods, for a test to look at: the conversations whose harness is bound now, each to its binding, those whose harness is being bound, and where their pods serve. */
export class Pools extends Context.Service<Pools, {
  readonly bound: () => ReadonlyMap<string, string>;
  readonly binding: () => ReadonlySet<string>;
  /** The port each conversation's pod serves on, or is to. */
  readonly ports: () => ReadonlyMap<string, number>;
  /**
   * The state directory of the conversation's pod bound now, where it writes the files a prompt
   * names: a pool's pod's is alasio's own path, where the prompt names them, on a filesystem of its
   * own, which the stand-in's pods, on the test's one, each have a directory of their own for.
   */
  readonly stateDir: (conversationId: string) => string | undefined;
  /** Tells of what the conversation's harness did on its own, as its pod tells the front and the gateway relays it, once each time. */
  readonly tell: (kind: "harness.idle" | "harness.background-response", conversationId: string) => Effect.Effect<void>;
  /**
   * Ends the conversation's pod bound now, as a node lost with it, or its pod deleted, ends one,
   * what it runs with it, and tells of its binding let go of so, as the gateway does
   * (harness.released, of `reason`, deleted or broken; or harness.lost): its binding's.
   */
  readonly lose: (conversationId: string, reason: "deleted" | "broken" | "lost") => Effect.Effect<string>;
  /** Ends the conversation's pod bound now as lose does, telling of it only as `tell` runs, as an event heard late is: its binding, and the telling. */
  readonly loseUntold: (conversationId: string, reason: "deleted" | "broken" | "lost") => Effect.Effect<{ readonly binding: string; readonly tell: Effect.Effect<void> }>;
}>()("alasio/test/Pools") {}

/** The stand-in's pools: the front's address and the key alasio's calls to it are made with, the environment's events topic its pods' events are relayed onto, and what it says of its pods. */
export interface StandInPools {
  readonly url: string;
  readonly callerKey: Buffer;
  readonly events: MemoryTopic;
  readonly pools: Pools["Service"];
}

/** What the stand-in's pools are made with: the database their pods reach as the harness host's role, where they bind any, and what their hosts are made with. */
export interface StandInPoolsOptions {
  readonly database?: PooledDatabase | undefined;
  readonly standIns?: HarnessStandIns | undefined;
  readonly branch?: string | undefined;
}

/**
 * The stand-in's pools, as the module says, for as long as the scope is open, their pods' hosts
 * ended as it closes: for alasio's services in the test's process (pooledHost), or for an alasio
 * of its own process (./alasio.ts), which they outlast.
 */
export const standInPools = ({ database, standIns = {}, branch }: StandInPoolsOptions): Effect.Effect<StandInPools, never, Scope.Scope> =>
  Effect.gen(function*() {
    const callerKey = randomBytes(32);
    const bindingKey = randomBytes(32);
    const events = memoryTopic(eventsTopic(ENVIRONMENT));
    const pods = new Map<string, Pod>();
    // The pods' directories, removed once their hosts have ended, as the scope closes.
    const root = mkdtempSync(join(tmpdir(), "alasio-pools-"));
    yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(root, { recursive: true, force: true })));
    let made = 0;
    const folderBayma = yield* folderBaymaStandIn;
    // The pods' hosts, ended as the scope is.
    yield* Effect.addFinalizer(() => Effect.forEach([...pods.values()], (pod) => (pod.bound ? Scope.close(pod.bound.scope, Exit.void) : Effect.void), { discard: true }));
    /** The pod of `key`, made where it has none: of the conversation alone, on a port of its own. */
    const podOf = (key: string): Effect.Effect<Pod> =>
      Effect.gen(function*() {
        const known = pods.get(key);
        if (known) return known;
        made += 1;
        // A folder instance's serves on the stand-in's folder bayma's port.
        const port = folderOfKey(key) ? folderBayma.port : yield* Effect.promise(freePort);
        const pod: Pod = { name: folderOfKey(key) ? `host-${made}` : `harness-${made}`, port, binding: null, bound: null };
        pods.set(key, pod);
        return pod;
      });

    /** `pod` of `key` bound: its binding written beside it, as its node's agent writes it, and its host started, which serves once bound. */
    const bindPod = (pool: BoundPool, key: string, pod: Pod): Effect.Effect<Bound> =>
      Effect.gen(function*() {
        if (pod.bound) return { binding: pod.bound.binding, pod: pod.name, address: "127.0.0.1" };
        // A folder instance's: bound as its bayma is there, which the front passes its requests on to.
        if (folderOfKey(key)) {
          pod.bound = { binding: randomUUID(), scope: yield* Scope.make() };
          return { binding: pod.bound.binding, pod: pod.name, address: "127.0.0.1" };
        }
        // Its pod reaches the store's database as the harness host's role: of a store of pooledStore's alone, which a test that binds none needs not make.
        if (!database) return yield* Effect.die(new Error("the stand-in of the harness pools binds a pod only on a store of pooledStore's, as the pod reaches its database as the harness host's role"));
        const conversationId = conversationOfHarnessKey(key) ?? "";
        const binding = randomUUID();
        const directory = join(root, `${pod.name}-${binding}`);
        const [attach, stateDir] = [join(directory, "attach"), join(directory, "state")];
        for (const made of [attach, stateDir]) mkdirSync(made, { recursive: true });
        const bindingFile = join(attach, "binding.json");
        writeFileSync(bindingFile, JSON.stringify({ conversationId, token: bindingToken(bindingKey, { key, binding, namespace: pool.namespace }) }));
        const scope = yield* Scope.make();
        const env = ConfigProvider.layer(ConfigProvider.fromEnv({ env: { ALASIO_DATABASE_URL: database.harnessUrl } }));
        // Its host's scope closed where it does not start, as one ended as it starts.
        yield* startHarness({ bindingFile, port: pod.port, stateDir, branch: branch ?? null, gatewayUrl: front.url }, standIns).pipe(
          Effect.provide(env),
          Scope.provide(scope),
          Effect.onExit((exit) => (Exit.isSuccess(exit) ? Effect.void : Scope.close(scope, exit))),
          Effect.orDie,
        );
        pod.bound = { binding, scope, stateDir };
        return { binding, pod: pod.name, address: "127.0.0.1" };
      });

    // Each bind, the front's request's own no more, in the stand-in's scope, so it ends as the services do, its host with it.
    const binds = yield* FiberSet.make<Bound>();
    const binder = Binder.of({
      // One bind of a key at a time, as a binder holds its Lease: one under way is waited for.
      bind: (pool, key) =>
        Effect.flatMap(podOf(key), (pod) =>
          Effect.gen(function*() {
            if (pod.bound) return { binding: pod.bound.binding, pod: pod.name, address: "127.0.0.1" };
            pod.binding ??= yield* FiberSet.run(binds, bindPod(pool, key, pod).pipe(Effect.ensuring(Effect.sync(() => void (pod.binding = null)))));
            return yield* Fiber.join(pod.binding);
          })),
      bound: (_pool, key) => Effect.sync(() => {
        const pod = pods.get(key);
        return pod?.bound ? { binding: pod.bound.binding, pod: pod.name, address: "127.0.0.1" } : null;
      }),
      release: (_pool, key, before, only) =>
        Effect.gen(function*() {
          const pod = pods.get(key);
          if (!pod?.bound || (only !== undefined && only !== pod.name)) return "unbound" satisfies Released;
          if (before) yield* before({ pod: pod.name, binding: pod.bound.binding });
          yield* Scope.close(pod.bound.scope, Exit.void);
          pods.delete(key);
          return "released" satisfies Released;
        }),
      hold: (_pool, _key, effect) => Effect.map(effect, (value) => ({ value })),
      waiting: () => Effect.succeed(0),
    });

    /** Whether the binding `binding` of `key` is held now, as the bindings' Leases are watched. */
    const routeHeld = (key: string, binding: string): Effect.Effect<Held> => Effect.succeed(pods.get(key)?.bound?.binding === binding ? "held" : "gone");

    const front = yield* serveBinder({
      port: 0,
      host: "127.0.0.1",
      callerKey,
      bindingKey,
      // Each pod serves on a port of its own, as its placement says.
      podPort: 0,
      poolOf: (key, options) =>
        Effect.gen(function*() {
          const folder = folderOfKey(key);
          if (folder) return { pool: { name: `host-${folder.harness}`, namespace: NAMESPACE }, port: (yield* podOf(key)).port };
          if (conversationOfHarnessKey(key) === null) return yield* new PlacementRefused({ reason: `${key} is no conversation's harness nor folder instance` });
          const pod = yield* podOf(key);
          const ofFolder = options["workspace"] === "folder" ? "folder-" : "";
          return { pool: { name: `harness-${ofFolder}${options["harness"] ?? ""}`, namespace: NAMESPACE }, port: pod.port };
        }),
      routeHeld,
      grants: (key: string, held: HeldTo) => heldToHarness(key, held.key),
      relay: (bound, body) =>
        Effect.gen(function*() {
          const conversation = conversationOfHarnessKey(bound.key);
          if (!conversation) return { status: 403, json: { error: `${bound.key} is no harness's` } };
          return yield* relayed(conversation, body, ({ kind, subject }) =>
            Effect.map(Clock.currentTimeMillis, (at) => void events.write(eventRecord({ kind, environment: ENVIRONMENT, source: "harness", subject }, at).value)));
        }),
    }).pipe(
      Effect.provideService(Binder, binder),
      Effect.provideService(KubeClient, leaseless),
      Effect.map((served) => ({ ...served, url: `http://127.0.0.1:${served.port}` })),
    );

    /** The conversation's pod bound now ended, and the telling of its binding let go of so, as Pools.loseUntold says. */
    const loseUntold = (conversationId: string, reason: "deleted" | "broken" | "lost") =>
      Effect.gen(function*() {
        const key = [...pods.keys()].find((each) => conversationOfHarnessKey(each) === conversationId);
        const pod = key === undefined ? undefined : pods.get(key);
        if (key === undefined || !pod?.bound) return yield* Effect.die(new Error(`${conversationId} has no pod bound`));
        const { binding, scope } = pod.bound;
        yield* Scope.close(scope, Exit.void);
        pods.delete(key);
        const subject = { kind: "conversation", key: conversationId } as const;
        const tell = Effect.map(Clock.currentTimeMillis, (at) =>
          void events.write(eventRecord(
            reason === "lost"
              ? { kind: "harness.lost", environment: ENVIRONMENT, source: "sandbox-gateway", subject, binding, pod: pod.name, reason: "its pod went while it was bound" }
              : { kind: "harness.released", environment: ENVIRONMENT, source: "sandbox-gateway", subject, binding, reason },
            at,
          ).value));
        return { binding, tell };
      });

    /** The conversations' harnesses' pods, by their conversations, of all the stand-in binds. */
    const harnessPods = () =>
      [...pods].flatMap(([key, pod]) => {
        const conversation = conversationOfHarnessKey(key);
        return conversation ? [[conversation, pod] as const] : [];
      });
    const pools = Pools.of({
      bound: () => new Map(harnessPods().flatMap(([conversation, pod]) => (pod.bound ? [[conversation, pod.bound.binding] as const] : []))),
      binding: () => new Set(harnessPods().flatMap(([conversation, pod]) => (pod.binding && !pod.bound ? [conversation] : []))),
      ports: () => new Map(harnessPods().map(([conversation, pod]) => [conversation, pod.port] as const)),
      stateDir: (conversationId) => harnessPods().find(([conversation]) => conversation === conversationId)?.[1].bound?.stateDir,
      tell: (kind, conversationId) =>
        Effect.map(Clock.currentTimeMillis, (at) => void events.write(eventRecord({ kind, environment: ENVIRONMENT, source: "harness", subject: { kind: "conversation", key: conversationId } }, at).value)),
      lose: (conversationId, reason) => Effect.flatMap(loseUntold(conversationId, reason), ({ binding, tell }) => Effect.as(tell, binding)),
      loseUntold: (conversationId, reason) => loseUntold(conversationId, reason),
    });
    return { url: front.url, callerKey, events, pools };
  });

/**
 * alasio's host where its harnesses run in pools, on the stand-in, as the module says, and where
 * it hears what they do on their own; and the stand-in, for a test to look at.
 */
export const pooledHost = ({ standIns = {}, branch, host = (made) => made }: PoolsOptions = {}): Layer.Layer<HarnessHost | HarnessEvents | EventsConsumer | Pools, never, Store | CodexAppServer | ActiveTurns> =>
  Layer.unwrap(Effect.gen(function*() {
    // The running turns its pods keep, the test's, for a test to say a turn runs in a conversation as a pod's harness would.
    const activeTurns = yield* ActiveTurns;
    const { url, callerKey, events, pools } = yield* standInPools({ database: databaseOf(yield* Store), standIns: { ...standIns, activeTurns }, branch });
    const home = process.env["CODEX_HOME"] ?? "";
    const pooled = pooledHarnessHost({ url, environment: ENVIRONMENT, callerKey, codexLogin: { url: CODEX_LOGIN_URL, home } });
    return Layer.mergeAll(
      Layer.effect(HarnessHost, Effect.map(HarnessHost, host)).pipe(Layer.provide(pooled)),
      HarnessEvents.fromLog().pipe(Layer.provideMerge(EventsConsumer.layer.pipe(Layer.provide(Layer.succeed(EnvironmentEvents, EnvironmentEvents.of({ join: events.join })))))),
      Layer.succeed(Pools, pools),
    );
  }));

/**
 * The Kubernetes the front stamps a binding's Lease active on, which has no Lease: a stamp found
 * of none, as the front lets one be (../../src/binder/bindings.ts touchLease); what else the
 * front asks of it, it asks of none of the stand-in's.
 */
const leaseless = new Proxy({} as KubeClient["Service"], {
  get: (_target, method) =>
    method === "patch"
      ? () => Effect.fail(new KubeApiError({ status: 404, cause: new Error("the stand-in keeps no Lease") }))
      : () => Effect.die(new Error(`the stand-in front asked Kubernetes for ${String(method)}`)),
});

/**
 * The conversation's harness bound to a pod of its pool, as alasio binds it ahead of a turn,
 * held for none: for a test to say a turn runs in it, as a pod's harness would, on the running
 * turns its pods keep (ActiveTurns), the conversation's mount placing it.
 */
export const bindHarness = (conversationId: string): Effect.Effect<void, StoreError | HarnessHostError, HarnessHost> =>
  Effect.asVoid(Effect.scoped(Effect.flatMap(HarnessHost, (host) => host.reserve(conversationId))));
