/**
 * alasio's one door to the Kubernetes API (decision 001 of the Kubernetes design): a
 * narrow client over @kubernetes/client-node, so the modules that drive workloads take
 * this small service and their tests a fake of it.
 *
 * In a pod it authenticates as the pod's ServiceAccount; elsewhere (tests, a developer's
 * machine) as the current context of `KUBECONFIG` or ~/.kube/config.
 */
import { PassThrough } from "node:stream";
import { finished } from "node:stream/promises";
import { isDeepStrictEqual } from "node:util";

import { Exec, KubeConfig, type KubernetesObject, KubernetesObjectApi, PatchStrategy, type V1Status } from "@kubernetes/client-node";
import { Context, Effect, Layer, Predicate, Result, Schema } from "effect";

/** What `exec` is given beyond the command. */
export interface ExecOptions {
  /** The most stdout may hold; the command is ended past it. */
  readonly maxBytes?: number;
}

/** What a command run by `exec` came to. */
export interface ExecResult {
  readonly exitCode: number;
  readonly stdout: Buffer;
  readonly stderr: string;
}

/** The API refused a call, or could not be reached: the status it answered with, when it answered. */
export class KubeApiError extends Schema.TaggedError<KubeApiError>()("KubeApiError", {
  /** The API's HTTP status (404 for absent, 409 for a conflict), when it answered. */
  status: Schema.optional(Schema.Number),
  cause: Schema.Defect(),
}) {
  /** What a call failed with, read for the status the API answered with (the client's ApiException carries it as `code`). */
  static of(cause: unknown): KubeApiError {
    return new KubeApiError({ status: Predicate.hasProperty(cause, "code") && Predicate.isNumber(cause.code) ? cause.code : undefined, cause });
  }

  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** A command `exec` ran in a container came to no exit code: its output overflowed, or the exec itself failed. */
export class KubeExecError extends Schema.TaggedError<KubeExecError>()("KubeExecError", {
  message: Schema.String,
}) {}

/** Whether `error` is the API's answer `status`. */
export const hasStatus = (status: number) => (error: KubeApiError): boolean => error.status === status;

/** Whether `value` is a JSON object, which a merge patch merges key by key. */
const isJsonObject = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/**
 * The JSON merge patch (RFC 7386) that makes `from` into `to`: objects patched key by key,
 * with null for each key `to` has no value for, and anything else, arrays included,
 * given whole. A merge patch cannot set a null, so a null in `to` is a key it lacks.
 */
export function mergePatch(from: unknown, to: unknown): unknown {
  if (!isJsonObject(from) || !isJsonObject(to)) return to;
  const patch: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(from)) {
    if (value !== undefined && to[key] === undefined) patch[key] = null;
  }
  for (const [key, value] of Object.entries(to)) {
    if (value === undefined || isDeepStrictEqual(from[key], value)) continue;
    patch[key] = mergePatch(from[key], value);
  }
  return patch;
}

/** The socket a `pods/exec` runs over. */
type ExecSocket = Awaited<ReturnType<Exec["exec"]>>;

const DEFAULT_EXEC_MAX_BYTES = 16 * 1024 * 1024;

/** The kubeconfig alasio runs with: its ServiceAccount in a pod, the default elsewhere. */
function loadKubeConfig(): KubeConfig {
  const kubeConfig = new KubeConfig();
  kubeConfig.loadFromDefault();
  return kubeConfig;
}

const ref = (apiVersion: string, kind: string, namespace: string, name: string) => ({ apiVersion, kind, metadata: { namespace, name } });

/**
 * What a merge patch of the object sends: `patch`, addressed to the object, whose name
 * and namespace sit in its metadata beside what `patch` changes there.
 */
export function patchBody(apiVersion: string, kind: string, namespace: string, name: string, patch: object): KubernetesObject {
  const target = ref(apiVersion, kind, namespace, name);
  const metadata = Predicate.hasProperty(patch, "metadata") && isJsonObject(patch.metadata) ? patch.metadata : {};
  return { ...patch, ...target, metadata: { ...metadata, ...target.metadata } };
}

/** A call to the API, its rejection a KubeApiError. */
const call = <A>(request: () => Promise<A>): Effect.Effect<A, KubeApiError> =>
  Effect.tryPromise({ try: request, catch: KubeApiError.of });

/**
 * The Kubernetes API, as alasio calls it. Objects are addressed by `apiVersion`, `kind`,
 * namespace and name, so one set of calls serves core objects and agent-sandbox's alike.
 */
export class KubeClient extends Context.Service<KubeClient, {
  /** The object, or null when there is none. */
  readonly read: (apiVersion: string, kind: string, namespace: string, name: string) => Effect.Effect<KubernetesObject | null, KubeApiError>;
  /** The objects of `kind` in the namespace whose labels `labelSelector` selects. */
  readonly list: (apiVersion: string, kind: string, namespace: string, labelSelector: string) => Effect.Effect<readonly KubernetesObject[], KubeApiError>;
  /** The created object; fails with status 409 when it exists. */
  readonly create: <T extends KubernetesObject>(object: T) => Effect.Effect<T, KubeApiError>;
  /** Replaces it whole, at its `metadata.resourceVersion`. */
  readonly replace: <T extends KubernetesObject>(object: T) => Effect.Effect<T, KubeApiError>;
  /** A JSON merge patch. */
  readonly patch: (apiVersion: string, kind: string, namespace: string, name: string, patch: object) => Effect.Effect<KubernetesObject, KubeApiError>;
  /** Deletes it, with its dependents in the background; one already gone is not an error. */
  readonly remove: (apiVersion: string, kind: string, namespace: string, name: string) => Effect.Effect<void, KubeApiError>;
  /**
   * Runs `command` in the container: `{ exitCode, stdout, stderr }`, stdout of at most
   * `maxBytes` (the command is ended past it). Interrupting it closes the exec.
   */
  readonly exec: (
    namespace: string,
    pod: string,
    container: string,
    command: readonly string[],
    options?: ExecOptions,
  ) => Effect.Effect<ExecResult, KubeApiError | KubeExecError>;
}>()("alasio/kube/KubeClient") {
  /** The API alasio's kubeconfig (loadKubeConfig) reaches. */
  static readonly layer: Layer.Layer<KubeClient> = Layer.sync(KubeClient, () => makeKubeClient(loadKubeConfig()));
}

/** The client over `kubeConfig`. */
function makeKubeClient(kubeConfig: KubeConfig): KubeClient["Service"] {
  const objects = KubernetesObjectApi.makeApiClient(kubeConfig);
  const executor = new Exec(kubeConfig);

  const exec = Effect.fnUntraced(function*(
    namespace: string,
    pod: string,
    container: string,
    command: readonly string[],
    { maxBytes = DEFAULT_EXEC_MAX_BYTES }: ExecOptions = {},
  ): Effect.fn.Return<ExecResult, KubeApiError | KubeExecError> {
    const stdout = new PassThrough();
    const stderr = new PassThrough();
    const out: Buffer[] = [];
    const err: Buffer[] = [];
    let size = 0;
    let socket: ExecSocket | null = null;
    let overflowed = false;
    stdout.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > maxBytes) {
        overflowed = true;
        socket?.close();
        return;
      }
      out.push(chunk);
    });
    stderr.on("data", (chunk: Buffer) => err.push(chunk));
    // Held in an object: the status callback sets it where control flow cannot see.
    const reported: { status: V1Status | null } = { status: null };
    // The status arrives on its own channel before the socket closes; the output is
    // whole once the socket has closed and both streams have drained.
    yield* Effect.callback<void, KubeApiError>((resume) => {
      executor
        .exec(namespace, pod, container, [...command], stdout, stderr, null, false, (status) => { reported.status = status; })
        .then((opened) => {
          socket = opened;
          opened.on("close", () => resume(Effect.void));
          opened.on("error", (error) => resume(Effect.fail(KubeApiError.of(error))));
        }, (error: unknown) => resume(Effect.fail(KubeApiError.of(error))));
      return Effect.sync(() => socket?.close());
    });
    for (const stream of [stdout, stderr]) {
      if (!stream.writableEnded) stream.end();
    }
    // Both were ended just above, so both finish.
    yield* Effect.promise(() => Promise.all([finished(stdout), finished(stderr)]));
    if (overflowed) {
      return yield* new KubeExecError({ message: `the output of ${JSON.stringify(command[0])} in ${namespace}/${pod} is over ${maxBytes} bytes` });
    }
    if (!reported.status) return yield* new KubeExecError({ message: `exec in ${namespace}/${pod} ended without a status` });
    return {
      exitCode: yield* Effect.fromResult(exitCodeOf(reported.status)),
      stdout: Buffer.concat(out),
      stderr: Buffer.concat(err).toString("utf8"),
    };
  });

  return KubeClient.of({
    read: (apiVersion, kind, namespace, name) =>
      call(() => objects.read(ref(apiVersion, kind, namespace, name))).pipe(
        Effect.catchIf(hasStatus(404), () => Effect.succeed(null)),
      ),
    list: (apiVersion, kind, namespace, labelSelector) =>
      call(() => objects.list(apiVersion, kind, namespace, undefined, undefined, undefined, undefined, labelSelector)).pipe(Effect.map(({ items }) => items)),
    create: (object) => call(() => objects.create(object)),
    replace: (object) => call(() => objects.replace(object)),
    patch: (apiVersion, kind, namespace, name, patch) =>
      call(() =>
        objects.patch(patchBody(apiVersion, kind, namespace, name, patch), undefined, undefined, "alasio", undefined, PatchStrategy.MergePatch)
      ),
    remove: (apiVersion, kind, namespace, name) =>
      call(() => objects.delete(ref(apiVersion, kind, namespace, name), undefined, undefined, undefined, undefined, "Background")).pipe(
        Effect.asVoid,
        Effect.catchIf(hasStatus(404), () => Effect.void),
      ),
    exec,
  });
}

/** The exit code a `pods/exec` status reports: 0 on success, the process's own otherwise. */
export function exitCodeOf(status: V1Status): Result.Result<number, KubeExecError> {
  if (status.status === "Success") return Result.succeed(0);
  const cause = status.details?.causes?.find((entry) => entry.reason === "ExitCode");
  if (cause) return Result.succeed(Number(cause.message));
  return Result.fail(new KubeExecError({ message: `exec failed: ${status.message ?? status.reason ?? "unknown error"}` }));
}
