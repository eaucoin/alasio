/**
 * Valkey, as alasio runs it, once for each store of metadata it keeps there: workspace
 * storage's file system's (./workspace-storage.ts), and the bundled object store's
 * filers' (./object-store.ts). One server each, configured as JuiceFS's Redis best
 * practices (administration/metadata/redis_best_practices.md at juicedata/juicefs v1.4.1)
 * say, which suit both:
 *
 * - durability: snapshots and an append-only file together, the file fsynced before each
 *   write is answered (`appendfsync always`), as a client's fsync, JuiceFS's of a file
 *   among them, is answered only once its metadata is written, and a server killed then
 *   loses none of it, where once a second loses what it answered in the second before; and
 *   written with a snapshot as its preamble, so a rewrite is a snapshot; at a start the file
 *   is loaded, as the more complete, and the server is ready only once it has been;
 * - one server on its volume at a time (oneAtATime): a pod deleted outright, as
 *   `kubectl delete --force` deletes one, is made anew by its StatefulSet at once while its
 *   server still runs, answering the clients it holds connections from, and one that loaded
 *   the files then would lose what the other wrote after;
 * - memory: a `maxmemory` under the container's limit, leaving room for the fork that
 *   writes snapshots and for deletions once it is reached, and the policy `noeviction`, as
 *   an evicted key is lost metadata.
 *
 * Its clients log in as its default user, with the password in a Secret of its name, and,
 * beside it, any user of its own as narrow as its rules say; or, of one with users of its own
 * alone, as each of those, with the password in a Secret of that
 * user's, its default user turned off. The stack's setup makes each (./neon.ts). None is of
 * the data stack, whose pods reach each other: each one's NetworkPolicy
 * (./network-policies.ts) admits its own clients alone, and the collector.
 */
import type { KubernetesObject, V1ConfigMap, V1Container, V1EnvVar, V1EnvVarSource, V1PodSpec, V1Service, V1StatefulSet } from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { claimSpec, imagePullSecrets, labels, NAMESPACE, ONE, replicasOf, restrictedContainer, restrictedPod, selectorLabels, sha256 } from "./common.ts";
import type { Image, InstallConfig } from "./config.ts";
import { priority } from "./priorities.ts";
import { verticalPodAutoscaler } from "./sizing.ts";

export const VALKEY_PORT = 6379;

/** The uid and gid of the image's `valkey` user. */
const VALKEY_UID = 999;

/** The lock of a Valkey's volume, which its server holds as long as it runs. */
const VALKEY_LOCK = "/data/valkey.lock";

/**
 * The script a Valkey's server is started by, the server's command its arguments: once no
 * other server holds `lock`, the lock of its volume, which it then holds as long as it runs,
 * on a descriptor it keeps across its exec, so the kernel lets it go as it ends however it
 * ends. Where the lock cannot be opened or taken (flock failing, or killed as it waits), the
 * script ends, and with it the container, starting no server. The lock is on the volume
 * itself, which one node alone mounts: that holds of local-path and of ReadWriteOnce block
 * storage, not of a shared file system such as NFS, which two nodes may mount at once. A
 * server waiting on it listens on nothing, so its pod is not ready and its Service routes
 * nothing to it.
 */
export const oneAtATime = (lock: string): string => [
  `exec 9>>${lock} || exit 1`,
  "if ! flock -n 9; then",
  `  echo "waiting for the Valkey that holds ${lock} to end" >&2`,
  `  flock 9 || { echo "could not take ${lock}" >&2; exit 1; }`,
  "fi",
  'exec "$0" "$@"',
].join("\n");

/**
 * A user of a Valkey's own: its name, the Secret its password is in (as `password`), what
 * it may do, as Valkey's ACL rules say it, and whether the readiness probe logs in as it,
 * which needs INFO alone.
 */
export interface ValkeyUser {
  readonly name: string;
  readonly secret: string;
  readonly rules: readonly string[];
  readonly probes?: boolean;
}

/** A Valkey of alasio's: what tells it from the other, and how it runs. */
export interface ValkeyInstance {
  /** Its Service's, its StatefulSet's and its password's Secret's name. */
  readonly name: string;
  /** Its pods' component. */
  readonly component: string;
  /** Labels its objects carry besides its component's. */
  readonly labels?: Readonly<Record<string, string>>;
  readonly image: Image;
  readonly storage: { readonly size: string; readonly storageClassName: string };
  readonly resources: { readonly requests: Readonly<Record<string, string>>; readonly limits: Readonly<Record<string, string>> };
  /** What it may hold, as Valkey says an amount: `384mb`. */
  readonly maxmemory: string;
  /** Its users, its default user turned off; none, its clients log in as its default user, with the password of the Secret of its name. */
  readonly users?: readonly ValkeyUser[];
  /** Of one whose clients log in as its default user, its users beside it, each as narrow as its rules say. */
  readonly besideDefault?: readonly ValkeyUser[];
}

/** The configuration file of a Valkey that may hold `maxmemory`. */
export function valkeyConf(maxmemory: string): string {
  return [
    "bind * -::*",
    `port ${VALKEY_PORT}`,
    "dir /data",
    "appendonly yes",
    "appendfsync always",
    "aof-use-rdb-preamble yes",
    "save 3600 1 300 100 60 10000",
    // Writes stop, rather than go unsaved, when a snapshot cannot be written.
    "stop-writes-on-bgsave-error yes",
    `maxmemory ${maxmemory}`,
    "maxmemory-policy noeviction",
    "",
  ].join("\n");
}

/** What a pod that runs the Valkey image on a Valkey's volumes has in its spec: the image's pull Secrets, and its user. */
export function valkeyPod(config: InstallConfig): Pick<V1PodSpec, "imagePullSecrets" | "securityContext"> {
  return { ...imagePullSecrets(config), securityContext: restrictedPod(VALKEY_UID, VALKEY_UID) };
}

/** The password in the Secret `name`, a Valkey's or a Valkey user's, as a variable's source. */
export const valkeyPassword = (name: string): V1EnvVarSource => ({ secretKeyRef: { name, key: "password" } });

/** The variable a Valkey's server is given the password of its user `name` in. */
const userVariable = (name: string) => `VALKEY_${name.toUpperCase().replaceAll("-", "_")}_PASSWORD`;

/**
 * How the Valkey `instance` is started, and logged in to by its readiness probe: with its
 * default user's password; or its users, each with its password and rules, the default
 * user turned off, and the probe as the user that probes.
 */
function access({ name, users = [], besideDefault = [] }: ValkeyInstance): { readonly args: string[]; readonly env: V1EnvVar[]; readonly cli: string } {
  const userArgs = (each: readonly ValkeyUser[]) => each.flatMap(({ name: user, rules }) => ["--user", user, "on", `>$(${userVariable(user)})`, ...rules]);
  const userEnv = (each: readonly ValkeyUser[]) => each.map(({ name: user, secret }) => ({ name: userVariable(user), valueFrom: valkeyPassword(secret) }));
  if (users.length === 0) {
    const password = valkeyPassword(name);
    return {
      args: ["--requirepass", "$(VALKEY_PASSWORD)", ...userArgs(besideDefault)],
      env: [{ name: "VALKEY_PASSWORD", valueFrom: password }, { name: "VALKEYCLI_AUTH", valueFrom: password }, ...userEnv(besideDefault)],
      cli: "valkey-cli",
    };
  }
  const probing = users.find(({ probes }) => probes) ?? users[0];
  return {
    cli: probing ? `valkey-cli --user ${probing.name}` : "valkey-cli",
    args: [
      "--user",
      "default",
      "off",
      ...userArgs(users),
    ],
    env: [
      ...userEnv(users),
      ...(probing ? [{ name: "VALKEYCLI_AUTH", valueFrom: valkeyPassword(probing.secret) }] : []),
    ],
  };
}

/**
 * The Valkey `instance`: its configuration, its Service, and its StatefulSet, with a volume
 * of its own, and `sidecars` beside the server in its pod, which is ready once each of
 * them is.
 */
export function valkeyObjects(config: InstallConfig, instance: ValkeyInstance, sidecars: readonly V1Container[] = []): KubernetesObject[] {
  const { name, component } = instance;
  const conf = valkeyConf(instance.maxmemory);
  const login = access(instance);
  const metadata = { name, namespace: NAMESPACE, labels: { ...labels(component), ...instance.labels } };
  const configMap: V1ConfigMap = { apiVersion: "v1", kind: "ConfigMap", metadata, data: { "valkey.conf": conf } };
  const service: V1Service = {
    apiVersion: "v1",
    kind: "Service",
    metadata,
    spec: { selector: selectorLabels(component), ports: [{ name: "valkey", port: VALKEY_PORT }] },
  };
  const statefulSet: V1StatefulSet = {
    apiVersion: "apps/v1",
    kind: "StatefulSet",
    metadata,
    spec: {
      serviceName: name,
      ...replicasOf(ONE),
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: labels(component), annotations: { "checksum/config": sha256(conf) } },
        spec: {
          ...priority("bound"),
          ...valkeyPod(config),
          // Long enough for its last snapshot and fsync as it stops.
          terminationGracePeriodSeconds: 60,
          containers: [{
            name: "valkey",
            image: imageReference(instance.image),
            imagePullPolicy: "IfNotPresent",
            command: ["/bin/sh", "-c", oneAtATime(VALKEY_LOCK), "valkey-server", "/etc/valkey/valkey.conf", ...login.args],
            // VALKEYCLI_AUTH is valkey-cli's password, for the readiness probe.
            env: login.env,
            ports: [{ name: "valkey", containerPort: VALKEY_PORT }],
            // Ready once its append-only file is loaded, which it reads as off until then.
            readinessProbe: {
              exec: {
                command: [
                  "/bin/sh",
                  "-c",
                  `persistence=$(${login.cli} info persistence) && echo "$persistence" | grep -q '^loading:0' && echo "$persistence" | grep -q '^aof_enabled:1'`,
                ],
              },
              periodSeconds: 5,
              timeoutSeconds: 3,
            },
            // Started once it listens, which it does only once the server before it on its volume
            // has ended (within that pod's grace) and its append-only file is loaded: five minutes
            // for both, so the wait is not ended by the liveness probe and put back by a crash loop.
            startupProbe: { tcpSocket: { port: "valkey" }, periodSeconds: 5, failureThreshold: 60 },
            livenessProbe: { tcpSocket: { port: "valkey" }, periodSeconds: 10, failureThreshold: 6 },
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources: instance.resources,
            volumeMounts: [{ name: "data", mountPath: "/data" }, { name: "config", mountPath: "/etc/valkey", readOnly: true }],
          }, ...sidecars],
          volumes: [{ name: "config", configMap: { name } }],
        },
      },
      volumeClaimTemplates: [{ metadata: { name: "data" }, spec: claimSpec(instance.storage) }],
    },
  };
  return [configMap, service, statefulSet, verticalPodAutoscaler(statefulSet, "Recommended")];
}
