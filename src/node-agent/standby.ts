/**
 * Restores into standby pods on the node. A standby is a pod of a sandbox pool the gateway
 * makes ahead of a restore (../gateway/standbys.ts), annotated so (STANDBY_ANNOTATION) that
 * the fork's shim boots its sandbox with its containers created and none started: kubelet
 * sees it running, with its address, while runsc runs nothing of it. The shim listens on the
 * Unix socket the annotation names, under the node's STANDBY_SOCKETS, for the one restore it
 * takes: `POST /restore`, `{ imagePath, paused }`, answered `{ containers, milliseconds }`
 * once each of its containers is restored from the image, in the order they were created,
 * paused, until the agent resumes them as it resumes a restore's pod (./agent.ts resume). A
 * second request is refused (409), and the socket goes once its request is answered; a
 * restore that fails ends the sandbox, its containers with it.
 *
 * Where each container has a shim of its own, as containerd runs runsc's without shim
 * grouping, the pod's socket restores its sandbox's own container alone, and each other
 * container's shim listens on `<pod>.<container>.sock` beside it for its own: each is asked
 * in turn, the pod's first. Every one of them listens once kubelet has started its container,
 * which a standby claimed has had, as it is claimed only once each runs.
 */
import { readdir } from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { join } from "node:path";

import { Clock, Effect, Schema } from "effect";

import { NAME, SNAPSHOT_CACHE_DIR, SNAPSHOT_ID, STANDBY_SOCKETS } from "../kube/sandbox-shape.ts";
import { withLogScope } from "../shared/log.ts";
import { meter, withAlasioSpan } from "../telemetry/index.ts";

const Name = Schema.String.check(Schema.isPattern(NAME));

/** A request to restore a workspace's image, as this node's agent prepared it, into the standby pod `namespace`/`pod` on this node. */
export const StandbyRestoreRequest = Schema.Struct({
  environment: Name,
  workspace: Name,
  namespace: Name,
  /** A standby's name, a restore's pod's (./names.ts RESTORE_POD). */
  pod: Name,
  /** The image, as the agent prepared it: the workspace's in the snapshot cache, by its snapshot's id (./agent.ts prepare). */
  imagePath: Schema.String,
});
export type StandbyRestoreRequest = typeof StandbyRestoreRequest.Type;

/** What the shims of a standby said of its restore: its containers, in the order restored, and how long each took, in milliseconds. */
export const StandbyRestored = Schema.Struct({
  containers: Schema.Array(Schema.String),
  milliseconds: Schema.Array(Schema.Number),
});
export type StandbyRestored = typeof StandbyRestored.Type;

/**
 * A request to restore into a standby whose image path is not one of its workspace's own in the snapshot cache,
 * `<cache>/<environment>/<workspace>/<snapshot>`, as the agent prepares an image there: why. Whether an image is there is the
 * shim's to find, as it opens it.
 */
export class StandbyImageRefused extends Schema.TaggedError<StandbyImageRefused>()("StandbyImageRefused", {
  imagePath: Schema.String,
}) {
  override get message(): string {
    return `${this.imagePath} is not a path of the workspace's own images in the snapshot cache`;
  }
}

/** A standby that takes no restore: it has no shim listening, or its shim took one already. Nothing of it ran. */
export class StandbyUnavailable extends Schema.TaggedError<StandbyUnavailable>()("StandbyUnavailable", {
  namespace: Schema.String,
  pod: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `the standby ${this.namespace}/${this.pod} takes no restore: ${this.reason}`;
  }
}

/** A restore into a standby that failed, which ends its sandbox: which shim said what. */
export class StandbyRestoreFailed extends Schema.TaggedError<StandbyRestoreFailed>()("StandbyRestoreFailed", {
  namespace: Schema.String,
  pod: Schema.String,
  socket: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `the restore into the standby ${this.namespace}/${this.pod} failed at ${this.socket}: ${this.reason}`;
  }
}

/** How a restore into a standby fails. */
export type StandbyRestoreError = StandbyImageRefused | StandbyUnavailable | StandbyRestoreFailed;

/** How long a shim is given to restore its containers: as long as a restore's pod is waited for to run. */
const RESTORE_TIMEOUT_MS = 120_000;

const standbyRestores = meter.createCounter("alasio.node_agent.standby_restores", {
  description: "Restores into standby pods on the node, by outcome: restored, or why not",
  unit: "{restore}",
});

/** What a shim answered a request with: its status and its body. */
interface ShimAnswer {
  readonly status: number;
  readonly text: string;
}

/** Posts `body` to the shim listening on `socket`, as the module says. */
const post = (socket: string, body: string): Promise<ShimAnswer> =>
  new Promise((resolve, reject) => {
    const asked = httpRequest(
      { socketPath: socket, path: "/restore", method: "POST", headers: { "content-type": "application/json", "content-length": Buffer.byteLength(body) }, timeout: RESTORE_TIMEOUT_MS },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk: Buffer) => chunks.push(chunk));
        response.on("end", () => resolve({ status: response.statusCode ?? 0, text: Buffer.concat(chunks).toString("utf8").trim() }));
        response.on("error", reject);
      },
    );
    asked.on("timeout", () => asked.destroy(new Error(`it did not answer within ${RESTORE_TIMEOUT_MS / 1000} s`)));
    asked.on("error", reject);
    asked.end(body);
  });

/** Restores into standby pods on this node, whose shims listen under `sockets`; see the module. */
export interface StandbyRestores {
  /** Restores `request`'s image into its standby, paused: its containers, and how long each took. */
  readonly restore: (request: StandbyRestoreRequest) => Effect.Effect<StandbyRestored, StandbyRestoreError>;
}

/** The standby restores of the node whose shims listen under `sockets`, STANDBY_SOCKETS unless given. */
export const makeStandbyRestores = (sockets: string = STANDBY_SOCKETS): StandbyRestores => {
  /** The sockets of the standby `namespace`/`pod`: its own, then each of its containers' that has a shim of its own, by name; none where it has no socket. */
  const socketsOf = (namespace: string, pod: string): Effect.Effect<readonly string[]> =>
    Effect.promise(async () => {
      const names = await readdir(join(sockets, namespace)).catch((): string[] => []);
      if (!names.includes(`${pod}.sock`)) return [];
      const own = names.filter((name) => name.startsWith(`${pod}.`) && name.endsWith(".sock") && name !== `${pod}.sock`).sort();
      return [`${pod}.sock`, ...own].map((name) => join(sockets, namespace, name));
    });

  /** Asks the shim on `socket` to restore `body`: what it says it restored. */
  const ask = (request: StandbyRestoreRequest, socket: string, body: string): Effect.Effect<StandbyRestored, StandbyUnavailable | StandbyRestoreFailed> =>
    Effect.tryPromise({
      try: () => post(socket, body),
      catch: (error) => new StandbyRestoreFailed({ namespace: request.namespace, pod: request.pod, socket, reason: error instanceof Error ? error.message : String(error) }),
    }).pipe(
      Effect.flatMap(({ status, text }): Effect.Effect<StandbyRestored, StandbyUnavailable | StandbyRestoreFailed> => {
        // Taken already, by a restore whose answer was lost: nothing of this one ran.
        if (status === 409) return Effect.fail(new StandbyUnavailable({ namespace: request.namespace, pod: request.pod, reason: `its shim at ${socket} took a restore already: ${text}` }));
        if (status !== 200) return Effect.fail(new StandbyRestoreFailed({ namespace: request.namespace, pod: request.pod, socket, reason: `its shim answered ${status}: ${text}` }));
        return Schema.decodeUnknownEffect(Schema.fromJsonString(StandbyRestored))(text).pipe(
          Effect.mapError((error) => new StandbyRestoreFailed({ namespace: request.namespace, pod: request.pod, socket, reason: `its shim's answer is not one: ${error.message}` })),
        );
      }),
    );

  const restore = (request: StandbyRestoreRequest): Effect.Effect<StandbyRestored, StandbyRestoreError> =>
    Effect.gen(function*() {
      const own = `${SNAPSHOT_CACHE_DIR}/${request.environment}/${request.workspace}/`;
      if (!request.imagePath.startsWith(own) || !new RegExp(`^${SNAPSHOT_ID}$`, "u").test(request.imagePath.slice(own.length))) {
        return yield* new StandbyImageRefused({ imagePath: request.imagePath });
      }
      const found = yield* socketsOf(request.namespace, request.pod);
      if (found.length === 0) return yield* new StandbyUnavailable({ namespace: request.namespace, pod: request.pod, reason: `no shim of it listens under ${sockets}` });
      const started = yield* Clock.currentTimeMillis;
      const body = JSON.stringify({ imagePath: request.imagePath, paused: true });
      const restored = yield* Effect.forEach(found, (socket) => ask(request, socket, body));
      const containers = restored.flatMap((one) => one.containers);
      const milliseconds = restored.flatMap((one) => one.milliseconds);
      yield* Effect.annotateCurrentSpan({ "alasio.standby.containers": containers.length, "alasio.standby.restore_ms": (yield* Clock.currentTimeMillis) - started });
      return { containers, milliseconds };
    }).pipe(
      Effect.tap(({ containers, milliseconds }) =>
        Effect.sync(() => standbyRestores.add(1, { "alasio.node_agent.standby_restore.outcome": "restored" })).pipe(
          Effect.andThen(Effect.logInfo(
            `restored ${request.environment}/${request.workspace} into the standby ${request.namespace}/${request.pod}, paused: ${containers.map((id, index) => `${id} in ${milliseconds[index]} ms`).join(", ")}`,
          )),
        )
      ),
      Effect.tapError((error) => Effect.sync(() => standbyRestores.add(1, { "alasio.node_agent.standby_restore.outcome": error._tag })).pipe(Effect.andThen(Effect.logWarning(error.message)))),
      withAlasioSpan("alasio.node_agent.standby_restore", {
        attributes: { "alasio.environment": request.environment, "alasio.workspace": request.workspace, "k8s.namespace.name": request.namespace, "k8s.pod.name": request.pod },
      }),
      withLogScope("node-agent"),
    );

  return { restore };
};
