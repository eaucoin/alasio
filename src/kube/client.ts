/**
 * alasio's one door to the Kubernetes API (decision 001 of the Kubernetes design): a
 * narrow client over @kubernetes/client-node, so the modules that drive workloads take
 * this small interface and their tests a fake of it.
 *
 * In a pod it authenticates as the pod's ServiceAccount; elsewhere (tests, a developer's
 * machine) as the current context of `KUBECONFIG` or ~/.kube/config.
 */
import { PassThrough } from "node:stream";
import { finished } from "node:stream/promises";
import { Exec, KubeConfig, type KubernetesObject, KubernetesObjectApi, PatchStrategy, type V1Status } from "@kubernetes/client-node";

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

/** The client; see createKubeClient. */
export interface KubeClient {
  read(apiVersion: string, kind: string, namespace: string, name: string): Promise<KubernetesObject | null>;
  create<T extends KubernetesObject>(object: T): Promise<T>;
  replace<T extends KubernetesObject>(object: T): Promise<T>;
  patch(apiVersion: string, kind: string, namespace: string, name: string, patch: object): Promise<KubernetesObject>;
  remove(apiVersion: string, kind: string, namespace: string, name: string): Promise<void>;
  exec(namespace: string, pod: string, container: string, command: string[], options?: ExecOptions): Promise<ExecResult>;
}

/** The socket a `pods/exec` runs over. */
type ExecSocket = Awaited<ReturnType<Exec["exec"]>>;

/** The kubeconfig alasio runs with: its ServiceAccount in a pod, the default elsewhere. */
export function loadKubeConfig(): KubeConfig {
  const kubeConfig = new KubeConfig();
  kubeConfig.loadFromDefault();
  return kubeConfig;
}

const ref = (apiVersion: string, kind: string, namespace: string, name: string) => ({ apiVersion, kind, metadata: { namespace, name } });

/** Whether `error` is the API's answer `code` (404 for absent, 409 for a conflict). */
export function isStatus(error: unknown, code: number): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === code;
}

/**
 * The client. Objects are addressed by `apiVersion`, `kind`, namespace and name, so one
 * set of calls serves core objects and agent-sandbox's alike.
 *
 * - `read(apiVersion, kind, namespace, name)`: the object, or null when there is none.
 * - `create(object)`: the created object; throws with `code` 409 when it exists.
 * - `replace(object)`: replaces it whole, at its `metadata.resourceVersion`.
 * - `patch(apiVersion, kind, namespace, name, patch)`: a JSON merge patch.
 * - `remove(apiVersion, kind, namespace, name)`: deletes it, with its dependents in the
 *   background; one already gone is not an error.
 * - `exec(namespace, pod, container, command, { maxBytes })`: runs `command` in the
 *   container and resolves `{ exitCode, stdout, stderr }`, stdout a Buffer of at most
 *   `maxBytes` (the command is ended past it).
 */
export function createKubeClient({ kubeConfig = loadKubeConfig() }: { readonly kubeConfig?: KubeConfig } = {}): KubeClient {
  const objects = KubernetesObjectApi.makeApiClient(kubeConfig);
  const executor = new Exec(kubeConfig);

  return {
    async read(apiVersion, kind, namespace, name) {
      try {
        return await objects.read(ref(apiVersion, kind, namespace, name));
      } catch (error) {
        if (isStatus(error, 404)) return null;
        throw error;
      }
    },

    async create(object) {
      return await objects.create(object);
    },

    async replace(object) {
      return await objects.replace(object);
    },

    async patch(apiVersion, kind, namespace, name, patch) {
      return await objects.patch(
        { ...ref(apiVersion, kind, namespace, name), ...patch },
        undefined,
        undefined,
        "alasio",
        undefined,
        PatchStrategy.MergePatch,
      );
    },

    async remove(apiVersion, kind, namespace, name) {
      try {
        await objects.delete(ref(apiVersion, kind, namespace, name), undefined, undefined, undefined, undefined, "Background");
      } catch (error) {
        if (!isStatus(error, 404)) throw error;
      }
    },

    async exec(namespace, pod, container, command, { maxBytes = 16 * 1024 * 1024 } = {}) {
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
      await new Promise((resolve, reject) => {
        executor
          .exec(namespace, pod, container, command, stdout, stderr, null, false, (status) => { reported.status = status; })
          .then((opened) => {
            socket = opened;
            opened.on("close", resolve);
            opened.on("error", reject);
          }, reject);
      });
      for (const stream of [stdout, stderr]) {
        if (!stream.writableEnded) stream.end();
      }
      await Promise.all([finished(stdout), finished(stderr)]);
      if (overflowed) throw new Error(`the output of ${JSON.stringify(command[0])} in ${namespace}/${pod} is over ${maxBytes} bytes`);
      if (!reported.status) throw new Error(`exec in ${namespace}/${pod} ended without a status`);
      return {
        exitCode: exitCodeOf(reported.status),
        stdout: Buffer.concat(out),
        stderr: Buffer.concat(err).toString("utf8"),
      };
    },
  };
}

/** The exit code a `pods/exec` status reports: 0 on success, the process's own otherwise. */
export function exitCodeOf(status: V1Status): number {
  if (status.status === "Success") return 0;
  const cause = status.details?.causes?.find((entry) => entry.reason === "ExitCode");
  if (cause) return Number(cause.message);
  throw new Error(`exec failed: ${status.message ?? status.reason ?? "unknown error"}`);
}
