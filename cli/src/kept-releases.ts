/**
 * The gVisor releases alasio keeps: those it pins (cluster/node/pins.json, the current
 * one first, which new sandboxes run), and every other one installed that a snapshot
 * still names, as only the release that took a snapshot restores it
 * (src/gateway/snapshots.ts). The sandbox gateway says which those are, of every
 * environment's workspaces, the snapshot each was released into and its current one, as
 * main's token reads it (GET /snapshots/releases, src/gateway/sessions.ts), through a
 * port-forward to a gateway pod of its own.
 *
 * Where the gateway cannot be read (the cluster is down, no gateway runs, or what it
 * answers is not what it says), every release installed is kept, and it is said so. A
 * release no snapshot names, and that is not pinned, is removed, its RuntimeClass with it.
 * On a cluster in Docker, the releases are its node image's, the pinned ones: a snapshot
 * naming another falls back, as one whose release is not kept does. On a cluster of a
 * kubeconfig, the operator keeps the releases. A release before
 * gvisor.restoresFromBucketSince, as the fork's .6, does not end a restore it refuses,
 * which stalls the next pod's teardown for minutes before the fallback
 * (progress/live-lazy.md); its snapshots, released again under a later release, leave it
 * unkept over time.
 */
import { request } from "node:http";
import { type AddressInfo, createServer, type Server, type Socket } from "node:net";

import type { V1RuntimeClass, V1Secret } from "@kubernetes/client-node";
import { Clock, Effect, FiberSet, Schema } from "effect";

import { MAIN_ENVIRONMENT_NAME } from "../../src/branch/names.ts";
import { mintToken } from "../../src/binder/tokens.ts";
import { GVISOR_RELEASE_ANNOTATION } from "./cluster/k3s.ts";
import { runningPodIn } from "./commands/common.ts";
import { kind, KubeApi } from "./kube/api.ts";
import { NAMESPACE, SESSIONS_CALLER_SECRET } from "./manifests/common.ts";
import { GATEWAY_COMPONENT, GATEWAY_PORT } from "./manifests/sandbox-gateway.ts";

/** The snapshots a workspace may be restored from, by the gVisor release that took each, as the gateway names them. */
export type SnapshotReleases = ReadonlyMap<string, readonly string[]>;

/** What the gateway answers of the releases its snapshots name. */
const ReleasesAnswer = Schema.Struct({ releases: Schema.Record(Schema.String, Schema.Array(Schema.String)) });

/** How long reading what the gateway says may take. */
const READ_TIMEOUT = "30 seconds";

/**
 * What the snapshots name, as `asked`, what the gateway answered, says; null where it
 * failed, or its answer is not one, which is said so, as every release installed is then
 * kept.
 */
export const snapshotReleasesFrom = <R>(asked: Effect.Effect<unknown, { readonly message: string }, R>): Effect.Effect<SnapshotReleases | null, never, R> =>
  asked.pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(ReleasesAnswer)),
    Effect.map(({ releases }): SnapshotReleases => new Map(Object.entries(releases))),
    Effect.timeout(READ_TIMEOUT),
    Effect.catch((error) =>
      Effect.as(Effect.logWarning(`what the snapshots name could not be read of the sandbox gateway, so every gVisor release installed is kept: ${error.message}`), null)
    ),
  );

/** The gateway refused to say what the snapshots name, or main's caller key is not there to ask it with. */
class GatewayUnread extends Schema.TaggedError<GatewayUnread>()("GatewayUnread", { reason: Schema.String }) {
  override get message(): string {
    return this.reason;
  }
}

/** A server on a free port of the loopback, open while the scope is, each connection forwarded by `forward`. */
const loopback = (forward: (socket: Socket) => void) =>
  Effect.acquireRelease(
    Effect.callback<Server, GatewayUnread>((resume) => {
      const server = createServer(forward);
      server.once("error", (error) => resume(Effect.fail(new GatewayUnread({ reason: `no port of this machine could be listened on: ${error.message}` }))));
      server.listen(0, "127.0.0.1", () => resume(Effect.succeed(server)));
    }),
    (server) => Effect.sync(() => server.close()),
  );

/** What the gateway answers of the releases its snapshots name, asked by main's token, through a port-forward to one of its pods. */
const askGateway = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const secret = yield* kube.get<V1Secret>({ ...kind("Secret"), namespace: NAMESPACE, name: SESSIONS_CALLER_SECRET });
  const callerKey = secret?.data?.["caller-key"];
  if (!callerKey) return yield* new GatewayUnread({ reason: `${SESSIONS_CALLER_SECRET} holds no caller key of main's` });
  const pod = yield* runningPodIn(NAMESPACE, GATEWAY_COMPONENT);
  const run = yield* FiberSet.makeRuntime<never>();
  const server = yield* loopback((socket) => {
    socket.on("error", () => socket.destroy());
    run(kube.portForward({ namespace: NAMESPACE, pod }, GATEWAY_PORT, socket).pipe(Effect.catch(() => Effect.sync(() => socket.destroy()))));
  });
  // A server listening on a TCP port has an address of its own.
  const { port } = server.address() as AddressInfo;
  const token = mintToken(Buffer.from(callerKey, "base64"), { key: "snapshots", expires: (yield* Clock.currentTimeMillis) + 60_000, options: { environment: MAIN_ENVIRONMENT_NAME } });
  // Asked on a connection of its own, closed once answered, so its forward ends with it.
  return yield* Effect.tryPromise({
    try: (signal) =>
      new Promise<unknown>((resolve, reject) => {
        const asked = request({ host: "127.0.0.1", port, path: "/snapshots/releases", headers: { authorization: `Bearer ${token}` }, agent: false, signal }, (response) => {
          const chunks: Buffer[] = [];
          response.on("data", (chunk: Buffer) => chunks.push(chunk));
          response.on("error", reject);
          response.on("end", () => {
            const text = Buffer.concat(chunks).toString("utf8");
            if (response.statusCode !== 200) return reject(new Error(`it answered ${response.statusCode}: ${text}`));
            try {
              resolve(JSON.parse(text));
            } catch (error) {
              reject(error);
            }
          });
        });
        asked.on("error", reject);
        asked.end();
      }),
    catch: (error) => new GatewayUnread({ reason: error instanceof Error ? error.message : String(error) }),
  });
}).pipe(Effect.scoped);

/** What the snapshots name, as the sandbox gateway says, by snapshotReleasesFrom; null where it cannot be read. */
export const snapshotReleases: Effect.Effect<SnapshotReleases | null, never, KubeApi> = snapshotReleasesFrom(askGateway);

/**
 * The releases kept, of those `installed`, beside the `pinned` ones, as what the
 * snapshots name (`named`, or null where it is not known) says: the pinned first, in
 * their order, then those installed that a snapshot names, in theirs. Pure, for tests.
 */
export function keptReleases(pinned: readonly string[], installed: readonly string[], named: SnapshotReleases | null): string[] {
  return [...new Set([...pinned, ...installed.filter((release) => named === null || named.has(release))])];
}

/**
 * The releases the cluster keeps, as its RuntimeClasses of alasio's say (the ones
 * HostCluster and DockerCluster apply), beside the `pinned` ones, in keptReleases' order;
 * the pinned alone where it has none, or they cannot be read, as on a cluster of a
 * kubeconfig.
 */
export const clusterReleases = (pinned: readonly string[]): Effect.Effect<string[], never, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) =>
    kube.list<V1RuntimeClass>(kind("RuntimeClass")).pipe(
      Effect.map((classes) => keptReleases(pinned, classes.flatMap(({ metadata }) => metadata?.annotations?.[GVISOR_RELEASE_ANNOTATION] ?? []), null)),
      Effect.catch(() => Effect.succeed([...pinned])),
    ));

/** `count` snapshots, as a sentence says it. */
const snapshotsOf = (count: number): string => (count === 1 ? "1 snapshot names it" : `${count} snapshots name it`);

/**
 * What `alasio status` says of the gVisor releases `kept` (the current one first) and of
 * those the snapshots name, `named` (null where they cannot be read), a line each: each
 * release kept, with how many snapshots name it, then each release named but not kept,
 * whose snapshots fall back. Pure, for tests.
 */
export function describeKept(kept: readonly string[], named: SnapshotReleases | null): string[] {
  const count = (release: string) => (named === null ? null : named.get(release)?.length ?? 0);
  const said = (release: string) => {
    const counted = count(release);
    return counted === null ? "what snapshots name it is not seen" : snapshotsOf(counted);
  };
  return [
    ...kept.map((release, index) => `gVisor ${release}, kept${index === 0 ? ", which new sessions run" : ""}: ${said(release)}`),
    ...[...(named?.keys() ?? [])].filter((release) => !kept.includes(release)).map((release) =>
      `gVisor ${release}, not kept: ${said(release)}, restored from bayma's checkpoints instead`
    ),
  ];
}
