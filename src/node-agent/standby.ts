/**
 * Spike, not for main (the kernel-standby validation's E15): restores into standby pods. A
 * standby is a pod of a sandbox pool whose sandbox the fork's shim booted with its containers
 * created and none started (gvisor-fork spike/val-standby,
 * `dev.gvisor.internal.restore.standby`): kubelet sees it running, with its address, while
 * runsc runs nothing of it. Its shim listens on a Unix socket under the node's /run, which
 * the agent mounts, for the one restore it takes: `POST /restore`, `{ imagePath, paused }`,
 * answered `{ containers, milliseconds }`, each of its containers restored from the image in
 * the order they were created, paused, until the agent resumes them as it resumes a restore's
 * pod (./agent.ts resume). A second request is refused (409), as the socket goes once one is
 * taken.
 */
import { access } from "node:fs/promises";
import { request as httpRequest } from "node:http";

import { Effect, Schema } from "effect";

import { NAME, SNAPSHOT_CACHE_DIR, STANDBY_SOCKETS } from "../kube/sandbox-shape.ts";

/** A request to restore an image into a standby pod on this node. */
export const StandbyRestoreRequest = Schema.Struct({
  namespace: Schema.String.check(Schema.isPattern(NAME)),
  pod: Schema.String.check(Schema.isPattern(NAME)),
  /** The image, as the agent prepared it under the snapshot cache (./agent.ts prepare). */
  imagePath: Schema.String.check(Schema.makeFilter((path) => (path.startsWith(`${SNAPSHOT_CACHE_DIR}/`) && !path.includes("/../")) || "must be under the snapshot cache")),
});
export type StandbyRestoreRequest = typeof StandbyRestoreRequest.Type;

/** What the shim says of a restore into its standby: its containers, in the order restored, and how long each took. */
export const StandbyRestored = Schema.Struct({
  containers: Schema.Array(Schema.String),
  milliseconds: Schema.Array(Schema.Number),
});
export type StandbyRestored = typeof StandbyRestored.Type;

/** A restore into a standby that failed: why, with the shim's status where it answered. */
export class StandbyRestoreFailed extends Schema.TaggedError<StandbyRestoreFailed>()("StandbyRestoreFailed", {
  message: Schema.String,
  status: Schema.optional(Schema.Number),
}) {}

/** The socket of the standby pod `namespace`/`pod`'s shim. */
export const standbySocket = (namespace: string, pod: string): string => `${STANDBY_SOCKETS}/${namespace}/${pod}.sock`;

/** How long the agent waits for a standby's socket, which its shim makes as its pod starts. */
const SOCKET_WAIT_MS = 10_000;
/** How long a restore into a standby may take. */
const RESTORE_TIMEOUT_MS = 120_000;

/** Restores `request`'s image into its standby pod, paused, as the module says. */
export const restoreIntoStandby = (request: StandbyRestoreRequest): Effect.Effect<StandbyRestored, StandbyRestoreFailed> =>
  Effect.tryPromise({
    try: async () => {
      const socket = standbySocket(request.namespace, request.pod);
      for (const deadline = Date.now() + SOCKET_WAIT_MS; ; await new Promise((resolve) => setTimeout(resolve, 50))) {
        try {
          await access(socket);
          break;
        } catch {
          if (Date.now() > deadline) throw new StandbyRestoreFailed({ message: `the standby ${request.namespace}/${request.pod} has no socket ${socket}` });
        }
      }
      const body = JSON.stringify({ imagePath: request.imagePath, paused: true });
      const { status, text } = await new Promise<{ status: number; text: string }>((resolve, reject) => {
        const asked = httpRequest(
          { socketPath: socket, path: "/restore", method: "POST", headers: { "content-type": "application/json", "content-length": Buffer.byteLength(body) }, timeout: RESTORE_TIMEOUT_MS },
          (response) => {
            const chunks: Buffer[] = [];
            response.on("data", (chunk: Buffer) => chunks.push(chunk));
            response.on("end", () => resolve({ status: response.statusCode ?? 0, text: Buffer.concat(chunks).toString("utf8") }));
            response.on("error", reject);
          },
        );
        asked.on("timeout", () => asked.destroy(new Error(`it did not answer within ${RESTORE_TIMEOUT_MS / 1000} s`)));
        asked.on("error", reject);
        asked.end(body);
      });
      if (status !== 200) throw new StandbyRestoreFailed({ message: `the standby ${request.namespace}/${request.pod} refused the restore (${status}): ${text.trim()}`, status });
      return Schema.decodeUnknownSync(StandbyRestored)(JSON.parse(text));
    },
    catch: (cause) =>
      cause instanceof StandbyRestoreFailed
        ? cause
        : new StandbyRestoreFailed({ message: `the restore into the standby ${request.namespace}/${request.pod} failed: ${cause instanceof Error ? cause.message : String(cause)}` }),
  }).pipe(
    Effect.tap(({ containers, milliseconds }) =>
      Effect.logInfo(`restored ${request.imagePath} into the standby ${request.namespace}/${request.pod}: ${containers.map((id, index) => `${id} in ${milliseconds[index]} ms`).join(", ")}`)
    ),
  );
