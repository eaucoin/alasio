/**
 * The manifests (cli/src/manifests): what they say of alasio's objects, Neon's,
 * workspace storage's, and those that confine what runs; and how the install
 * configuration is checked and defaulted.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, test } from "node:test";
import { gunzipSync, gzipSync } from "node:zlib";

import type {
  KubernetesObject,
  V1ClusterRole,
  V1ClusterRoleBinding,
  V1ConfigMap,
  V1Container,
  V1CronJob,
  V1DaemonSet,
  V1Deployment,
  V1Job,
  V1LabelSelector,
  V1Namespace,
  V1PersistentVolumeClaim,
  V1PodDisruptionBudget,
  V1PodSpec,
  V1PriorityClass,
  V1Role,
  V1RoleBinding,
  V1Service,
  V1ServiceAccount,
  V1StatefulSet,
} from "@kubernetes/client-node";
import { Result } from "effect";

import { LOG_PRINCIPALS } from "../../neon/control/secrets.ts";
import { DRAIN_WITHIN_MS } from "../../neon/lake/src/drain.ts";
import { memoryBytes } from "../src/capacity.ts";
import { NODE_PINS } from "../src/cluster/node.ts";
import { installConfigOf } from "../src/config.ts";
import { branchEnvironment, branchObjects } from "../src/manifests/branch.ts";
import { MAIN_ENVIRONMENT } from "../src/manifests/common.ts";
import { lakeReconcile } from "../src/manifests/lake.ts";
import type { ScaledObject } from "../src/manifests/scalers.ts";
import type { VerticalPodAutoscaler } from "../src/manifests/sizing.ts";
import { everyIntervalCron, filerStoreBackupHours } from "../src/manifests/config.ts";
import { carriedFrom, decodeInstallConfig, type InstallConfig, manifests, NOTHING_CARRIED, NOTHING_KEPT } from "../src/manifests/index.ts";
import { filerStoreRestore } from "../src/manifests/object-store.ts";
import { goferOf, STORE_PROXY_PORT } from "../../src/node-agent/names.ts";
import { apiServerUnconfined } from "../src/install.ts";
import { environmentAccess } from "../src/manifests/log.ts";
import { carriedPools, workspaceMountReady, workspaceMountScript } from "../src/manifests/sandbox-gateway.ts";
import { folderStateDir, templateHash } from "../../src/gateway/names.ts";
import { hookOf } from "../../src/gateway/snapshots.ts";
import { VERSION } from "../src/release.ts";

/** The configuration `file`, with the Telegram Secret every configuration names. */
/** A NetworkPolicy as rendered, its ingress rules' peers under `from`. */
interface NetworkPolicyObject extends KubernetesObject {
  readonly spec: {
    readonly podSelector: V1LabelSelector;
    readonly policyTypes?: readonly string[];
    readonly ingress?: readonly { readonly from?: readonly Peer[]; readonly ports?: readonly { readonly port?: number | string }[] }[];
    readonly egress?: readonly { readonly to?: readonly Peer[]; readonly ports?: readonly { readonly port?: number | string }[] }[];
  };
}

/** A peer of an ingress rule. */
interface Peer {
  readonly namespaceSelector?: V1LabelSelector;
  readonly podSelector?: V1LabelSelector;
  readonly ipBlock?: { readonly cidr: string; readonly except?: readonly string[] };
}

/** A pod, as NetworkPolicies select it: by its labels, and its namespace's; and its address, a pod's of the cluster's unless it says. */
interface Pod {
  readonly namespace: string;
  readonly namespaceLabels: Readonly<Record<string, string>>;
  readonly labels: Readonly<Record<string, string>>;
  readonly address?: string;
}

/** A pod's address, where it says none: the cluster's, private. */
const POD_ADDRESS = "10.42.0.9";

/** Whether the IPv4 `address` is in `cidr`. */
function within(address: string, cidr: string): boolean {
  const [network = "", bits = "32"] = cidr.split("/");
  const value = (each: string) => each.split(".").reduce((sum, octet) => sum * 256 + Number(octet), 0);
  const size = 2 ** (32 - Number(bits));
  return Math.floor(value(address) / size) === Math.floor(value(network) / size);
}

/** Whether `selector` selects what has `labels`. */
function selects(selector: V1LabelSelector, labels: Readonly<Record<string, string>>): boolean {
  return Object.entries(selector.matchLabels ?? {}).every(([key, value]) => labels[key] === value) &&
    (selector.matchExpressions ?? []).every(({ key, operator, values = [] }) => {
      const value = labels[key];
      if (operator === "In") return value !== undefined && values.includes(value);
      if (operator === "NotIn") return value === undefined || !values.includes(value);
      if (operator === "Exists") return value !== undefined;
      if (operator === "DoesNotExist") return value === undefined;
      throw new Error(`no selector operator ${operator}`);
    });
}

/**
 * Whether `policies` let `from` connect to `to` on `port`, as Kubernetes reads them:
 * anything, where none of them selects `to` for ingress; otherwise what a rule of one
 * that does admits. An address block is taken to admit any pod, so none goes unseen.
 */
function admits(policies: readonly NetworkPolicyObject[], from: Pod, to: Pod, port: number): boolean {
  const selecting = policies.filter(({ metadata, spec }) =>
    metadata?.namespace === to.namespace && (spec.policyTypes ?? ["Ingress"]).includes("Ingress") && selects(spec.podSelector, to.labels)
  );
  const peerAdmits = (peer: Peer, namespace: string) =>
    peer.ipBlock !== undefined ||
    ((peer.namespaceSelector ? selects(peer.namespaceSelector, from.namespaceLabels) : from.namespace === namespace) &&
      (peer.podSelector ? selects(peer.podSelector, from.labels) : true));
  return selecting.length === 0 || selecting.some(({ metadata, spec }) =>
    (spec.ingress ?? []).some((rule) =>
      (rule.ports === undefined || rule.ports.some((allowed) => allowed.port === undefined || allowed.port === port)) &&
      (rule.from === undefined || rule.from.some((peer) => peerAdmits(peer, metadata?.namespace ?? "")))
    )
  );
}

/**
 * Whether `policies` let `from` connect to `to` on `port` on its way out, as Kubernetes
 * reads them: anywhere, where none of them selects `from` for egress; otherwise where a rule
 * of one that does reaches, an address block reaching what its address is in, and not in
 * what it excepts.
 */
function reaches(policies: readonly NetworkPolicyObject[], from: Pod, to: Pod, port: number): boolean {
  const selecting = policies.filter(({ metadata, spec }) =>
    metadata?.namespace === from.namespace && (spec.policyTypes ?? []).includes("Egress") && selects(spec.podSelector, from.labels)
  );
  const peerReaches = (peer: Peer, namespace: string) =>
    peer.ipBlock !== undefined
      ? within(to.address ?? POD_ADDRESS, peer.ipBlock.cidr) && !(peer.ipBlock.except ?? []).some((cidr) => within(to.address ?? POD_ADDRESS, cidr))
      : (peer.namespaceSelector ? selects(peer.namespaceSelector, to.namespaceLabels) : to.namespace === namespace) &&
        (peer.podSelector ? selects(peer.podSelector, to.labels) : true);
  return selecting.length === 0 || selecting.some(({ metadata, spec }) =>
    (spec.egress ?? []).some((rule) =>
      (rule.ports === undefined || rule.ports.some((allowed) => allowed.port === undefined || allowed.port === port)) &&
      (rule.to === undefined || rule.to.some((peer) => peerReaches(peer, metadata?.namespace ?? "")))
    )
  );
}

/** Whether `policies` let `from` connect to `to` on `port`: out of the one, and into the other. */
const connects = (policies: readonly NetworkPolicyObject[], from: Pod, to: Pod, port: number): boolean =>
  reaches(policies, from, to, port) && admits(policies, from, to, port);

function configOf(file: Record<string, unknown> = {}): InstallConfig {
  const { alasio, ...rest } = file;
  const decoded = decodeInstallConfig({ ...rest, alasio: { telegram: { existingSecret: "telegram" }, ...(alasio as object | undefined) } });
  if (Result.isFailure(decoded)) throw new Error(decoded.failure.message);
  return decoded.success;
}

/** The CPU baseline the node agents' pods say, as alasio up records it. */
const BASELINE = ["avx", "avx2", "sse"];

/** The objects of the configuration `file` and the Telegram Secret every configuration names, holding sessions to `cpuBaseline`. */
function install(file: Record<string, unknown> = {}, cpuBaseline: readonly string[] | null = BASELINE): readonly KubernetesObject[] {
  return manifests(configOf(file), { ...NOTHING_CARRIED, cpuBaseline });
}

/** The objects of `kind`, as that kind. */
function all<T extends KubernetesObject>(objects: readonly KubernetesObject[], kind: string): T[] {
  return objects.filter((object) => object.kind === kind) as T[];
}

/** The object of `kind` and `name`, as that kind. */
function one<T extends KubernetesObject>(objects: readonly KubernetesObject[], kind: string, name: string): T {
  const found = all<T>(objects, kind).filter((object) => object.metadata?.name === name);
  assert.equal(found.length, 1, `one ${kind} ${name}`);
  return found[0] as T;
}

/** The first container of a Deployment's or StatefulSet's pod. */
function container(workload: V1Deployment | V1StatefulSet): V1Container {
  const first = workload.spec?.template.spec?.containers[0];
  assert.ok(first);
  return first;
}

/**
 * A container's shell script, run here with `env` and with `commands` (name to a shell
 * script) in place of the programs of the image it runs in; in a directory of its own,
 * `root`, which they may keep their state in. What it printed, or why it failed.
 */
function runScript(workload: { readonly command?: string[] | undefined }, env: Readonly<Record<string, string>>, commands: Readonly<Record<string, string>>) {
  const [shell, flag, text] = workload.command ?? [];
  assert.deepEqual([shell, flag], ["/bin/sh", "-c"]);
  const root = mkdtempSync(join(tmpdir(), "alasio-script-"));
  const bin = join(root, "bin");
  mkdirSync(bin);
  for (const [name, body] of Object.entries(commands)) writeFileSync(join(bin, name), `#!/bin/sh\n${body}`, { mode: 0o755 });
  const run = (more: Readonly<Record<string, string>> = {}): string =>
    execFileSync("/bin/sh", ["-c", text ?? ""], { env: { ...env, ...more, ROOT: root, PATH: `${bin}:${process.env["PATH"]}` }, encoding: "utf8" });
  return { root, run, remove: () => rmSync(root, { recursive: true, force: true }) };
}

describe("configuration", () => {
  test("defaults what is left out", () => {
    const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } } });
    assert.ok(Result.isSuccess(decoded));
    const config = decoded.success;
    assert.equal(config.sessions.namespace, "alasio-sessions");
    assert.equal(config.sessions.runtimeClassName, "gvisor");
    assert.equal(config.host.enabled, false);
    assert.deepEqual(config.neon.safekeepers.scaling, { min: 3, max: 3 });
    assert.equal(config.neon.image.digest, "sha256:166022a72bf9983eba96d061d794f4740edbd4c3301e66202c1180acce9a323c");
    assert.deepEqual(config.alasio.resources, { requests: { cpu: "250m", memory: "768Mi" }, limits: { memory: "8Gi" } });
    assert.deepEqual(config.sessions.snapshots, {
      releaseAfter: "5m",
      liveEvery: "10m",
      stopWait: "60s",
      nodeCache: "5Gi",
      nodeAgent: { resources: { requests: { cpu: "20m", memory: "160Mi" }, limits: { memory: "512Mi" } } },
    });
  });

  test("adds the quantities given to the defaults", () => {
    const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" }, resources: { limits: { cpu: 2, memory: "4Gi" } } } });
    assert.ok(Result.isSuccess(decoded));
    assert.deepEqual(decoded.success.alasio.resources, { requests: { cpu: "250m", memory: "768Mi" }, limits: { memory: "4Gi", cpu: "2" } });
  });

  test("refuses what alasio does not install from, saying which key", () => {
    const refused = (file: Record<string, unknown>) => {
      const decoded = decodeInstallConfig({ ...file, alasio: { telegram: { existingSecret: "telegram" }, ...(file["alasio"] as object | undefined) } });
      assert.ok(Result.isFailure(decoded));
      return decoded.failure.message;
    };
    assert.match(refused({ alasio: { telegram: { existingSecret: "" } } }), /^alasio\.telegram\.existingSecret /u);
    assert.match(refused({ alasio: { unknown: true } }), /^alasio\.unknown /u);
    assert.match(refused({ sessions: { namespace: "Not A Namespace" } }), /^sessions\.namespace must be a namespace name$/u);
    assert.match(refused({ sessions: { fullModeNameservers: ["1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4"] } }), /^sessions\.fullModeNameservers /u);
    assert.match(refused({ sessions: { fullModeNameservers: ["1.1.1"] } }), /^sessions\.fullModeNameservers\.0 must be an IPv4 address$/u);
    assert.match(refused({ telemetry: { receiverPort: 80 } }), /^telemetry\.receiverPort /u);
    assert.match(refused({ telemetry: { otlpEndpoint: "collector:4318" } }), /^telemetry\.otlpEndpoint /u);
    assert.match(refused({ telemetry: { retentionDays: 0 } }), /^telemetry\.retentionDays must be at least 1$/u);
    // The collector is telemetry's, not Neon's.
    assert.match(refused({ neon: { collector: {} } }), /^neon\.collector /u);
    assert.match(refused({ images: { alasio: { digest: "sha256:abc" } } }), /^images\.alasio\.digest /u);
    assert.match(refused({ objectStore: { bundled: { volumes: 29 } } }), /^objectStore\.bundled\.volumes /u);
    assert.match(refused({ host: { mounts: [{ name: "home", hostPath: "home", mountPath: "/home" }] } }), /^host\.mounts\.0\.hostPath /u);
    assert.match(refused({ sessions: { snapshots: { releaseAfter: "five minutes" } } }), /^sessions\.snapshots\.releaseAfter must be a duration such as 5m$/u);
    assert.match(refused({ sessions: { snapshots: { releaseAfter: "0s" } } }), /^sessions\.snapshots\.releaseAfter must be a duration such as 5m$/u);
    assert.match(refused({ sessions: { snapshots: { liveEvery: "never" } } }), /^sessions\.snapshots\.liveEvery must be a duration such as 10m$/u);
    for (const stopWait of ["never", "15s", "601s"]) {
      assert.match(refused({ sessions: { snapshots: { stopWait } } }), /^sessions\.snapshots\.stopWait must be a duration of more than 15s and at most 10m, such as 60s$/u);
    }
    // Past one look for ending pods, and as long as a drain sanely waits on one.
    for (const stopWait of ["16s", "10m"]) assert.equal(configOf({ sessions: { snapshots: { stopWait } } }).sessions.snapshots.stopWait, stopWait);
    assert.match(refused({ sessions: { snapshots: { nodeCache: "lots" } } }), /^sessions\.snapshots\.nodeCache must be a storage size, such as 20Gi$/u);
  });

  test("takes every component's scale as { min, max }, refusing what the component cannot run, and the key it replaced", () => {
    const refused = (file: Record<string, unknown>) => {
      const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, ...file });
      assert.ok(Result.isFailure(decoded));
      return decoded.failure.message;
    };
    const safekeepers = (scaling: unknown) => ({ neon: { safekeepers: { scaling } } });
    assert.deepEqual(configOf(safekeepers({ min: 5, max: 5 })).neon.safekeepers.scaling, { min: 5, max: 5 });
    // One left out is its default, moved as far as the other needs.
    assert.deepEqual(configOf(safekeepers({ min: 5 })).neon.safekeepers.scaling, { min: 5, max: 5 });
    assert.deepEqual(configOf(safekeepers({ max: 1 })).neon.safekeepers.scaling, { min: 1, max: 1 });
    assert.equal(refused(safekeepers({ min: 3, max: 2 })), "neon.safekeepers.scaling.max must be at least min");
    assert.equal(refused(safekeepers({ min: 0, max: 0 })), "neon.safekeepers.scaling.min must be at least 1: it never sleeps");
    assert.equal(refused(safekeepers({ min: 3, max: 5 })), "neon.safekeepers.scaling.max must equal min: nothing scales it between them");
    assert.equal(refused(safekeepers({ min: 8, max: 8 })), "neon.safekeepers.scaling.max must be at most 7");
    assert.equal(refused(safekeepers({ min: -1, max: 3 })), "neon.safekeepers.scaling.min must be a number of replicas, 0 or more");
    // The key it replaced is a key alasio does not know, as any other.
    assert.match(refused({ neon: { safekeepers: { replicas: 3 } } }), /^neon\.safekeepers\.replicas /u);
  });

  test("requires what stands in for what is turned off", () => {
    const message = (file: Record<string, unknown>) => {
      const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, ...file });
      return Result.isFailure(decoded) ? decoded.failure.message : "";
    };
    assert.equal(message({ neon: { enabled: false } }), "neon.external.existingSecret is required when neon.enabled is false");
    assert.equal(message({ objectStore: { bundled: { enabled: false } } }), "objectStore.external.endpoint is required when objectStore.bundled.enabled is false");
    assert.equal(
      message({ objectStore: { bundled: { enabled: false }, external: { endpoint: "https://s3.example.com" } } }),
      "objectStore.external.existingSecret is required with an external object store",
    );
    assert.equal(
      message({ objectStore: { bundled: { enabled: false }, external: { endpoint: "https://s3.example.com", existingSecret: "s3" } } }),
      "objectStore.external.snapshotsSecret is required with an external object store where sessions are snapshotted: a key that reaches the snapshots bucket alone",
    );
    assert.equal(message({ sessions: { enabled: false } }), "workspaceStorage.enabled must be false when sessions.enabled is false, as its volumes are sessions' workspaces");
    assert.equal(
      message({ neon: { enabled: false, external: { existingSecret: "db" } } }),
      "workspaceStorage.enabled must be false when neon.enabled is false, as its data is in the object store that runs with Neon",
    );
    assert.equal(
      message({ neon: { enabled: false, external: { existingSecret: "db" } }, objectStore: { bundled: { enabled: false } }, workspaceStorage: { enabled: false } }),
      "",
    );
  });

  test("defaults workspace storage to JuiceFS, and refuses what JuiceFS does not take", () => {
    const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } } });
    assert.ok(Result.isSuccess(decoded));
    const { workspaceStorage, objectStore } = decoded.success;
    assert.equal(workspaceStorage.enabled, true);
    assert.equal(workspaceStorage.backupInterval, "1h");
    assert.equal(workspaceStorage.valkey.maxmemory, "384mb");
    assert.equal(workspaceStorage.image.repository, "juicedata/mount");
    assert.equal(objectStore.buckets.workspaces, "workspaces");
    assert.equal(objectStore.buckets.snapshots, "snapshots");
    const refused = (given: Record<string, unknown>) => {
      const result = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, workspaceStorage: given });
      return Result.isFailure(result) ? result.failure.message : "";
    };
    for (const interval of ["4m59s", "0", "7m", "1h30m"]) assert.equal(refused({ backupInterval: interval }), "workspaceStorage.backupInterval must be 5m or more, of whole minutes that divide an hour or whole hours that divide a day, such as 1h", interval);
    assert.equal(refused({ backupInterval: "30m" }), "");
    assert.equal(refused({ trashDays: -1 }), "workspaceStorage.trashDays must be 0 or more");
    assert.equal(refused({ name: "Workspaces" }), "workspaceStorage.name must be a JuiceFS file system name");
    assert.equal(refused({ valkey: { maxmemory: "384MB" } }), "workspaceStorage.valkey.maxmemory must be an amount of memory such as 384mb");
    assert.equal(refused({ image: { digest: "latest" } }), "workspaceStorage.image.digest must be a sha256 digest, or empty");
  });
});

describe("alasio", () => {
  test("refuses to install without the Telegram Secret", () => {
    const decoded = decodeInstallConfig({});
    assert.ok(Result.isFailure(decoded));
    assert.match(decoded.failure.message, /telegram\.existingSecret/u);
  });

  // One alasio at a time, never two, on main and on a branch, a rollout's new one starting
  // only once the old is gone: alasio still runs its turns in its own process, and its start
  // settles what the last left (it lets go of every hold and ends every active turn), so a
  // second, or a rollout's surge, would end the turns the first runs. Until conversations'
  // workers are claimed from the store and recovery comes from the harnesses' lost bindings
  // alone, nothing may run two.
  test("runs one alasio at a time, never two, on main and on a branch", () => {
    for (const objects of [install(), branchObjects(configOf(), "try", BASELINE)]) {
      const deployment = one<V1Deployment>(objects, "Deployment", "alasio");
      assert.equal(deployment.spec?.replicas, 1);
      assert.equal(deployment.spec?.strategy?.type, "Recreate");
    }
  });

  test("keeps no volume: its state is in Neon, its home and what it writes on emptyDirs", () => {
    const objects = install();
    const deployment = one<V1Deployment>(objects, "Deployment", "alasio");
    const env = container(deployment).env;
    assert.deepEqual(env?.find(({ name }) => name === "ALASIO_STATE_DIR"), { name: "ALASIO_STATE_DIR", value: "/var/lib/alasio/state" });
    assert.deepEqual(env?.find(({ name }) => name === "HOME"), { name: "HOME", value: "/var/lib/alasio/home" });
    assert.deepEqual(env?.find(({ name }) => name === "ALASIO_KEEP_CODEX_LOGIN"), { name: "ALASIO_KEEP_CODEX_LOGIN", value: "1" });
    const spec = deployment.spec?.template.spec;
    assert.deepEqual(spec?.volumes?.filter((volume) => volume.persistentVolumeClaim), []);
    assert.equal(spec?.initContainers, undefined);
    assert.deepEqual(container(deployment).volumeMounts?.filter(({ name }) => name === "state" || name === "alasio-home"), [
      { name: "state", mountPath: "/var/lib/alasio/state" },
      { name: "alasio-home", mountPath: "/var/lib/alasio/home" },
    ]);
    assert.deepEqual(spec?.volumes?.filter(({ name }) => name === "state" || name === "alasio-home"), [{ name: "state", emptyDir: {} }, { name: "alasio-home", emptyDir: {} }]);
    // Of the stack's alone: the volume the filer store's backups are kept on.
    assert.deepEqual(all(objects, "PersistentVolumeClaim").map((claim) => claim.metadata?.name), ["alasio-seaweedfs-filer-store-backups"]);
    assert.equal(spec?.securityContext?.runAsNonRoot, true);
  });

  test("refuses the volume alasio no longer has", () => {
    const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" }, persistence: { size: "20Gi" } } });
    assert.ok(Result.isFailure(decoded));
    assert.match(decoded.failure.message, /persistence/u);
  });

  test("says every object is managed by alasio", () => {
    const objects = install({ host: { enabled: true }, telemetry: { otlpEndpoint: "http://collector:4318" } });
    for (const object of objects.filter(({ kind }) => kind !== "CustomResourceDefinition")) {
      assert.equal(object.metadata?.labels?.["app.kubernetes.io/managed-by"], "alasio", `${object.kind} ${object.metadata?.name}`);
    }
  });

  test("gives Claude Code its token from a Secret only when one is named", () => {
    const tokenOf = (objects: readonly KubernetesObject[]) =>
      container(one<V1Deployment>(objects, "Deployment", "alasio")).env?.find(({ name }) => name === "CLAUDE_CODE_OAUTH_TOKEN");
    assert.equal(tokenOf(install()), undefined);
    assert.deepEqual(tokenOf(install({ alasio: { claude: { existingSecret: "claude" } } })), {
      name: "CLAUDE_CODE_OAUTH_TOKEN",
      valueFrom: { secretKeyRef: { name: "claude", key: "token" } },
    });
  });

  test("runs as the operator, in their home, with their mounts, under the host profile", () => {
    const objects = install({
      host: { enabled: true, uid: 1234, gid: 1235, supplementalGroups: [999], home: "/home/op", mounts: [{ name: "home", hostPath: "/home", mountPath: "/home" }] },
    });
    const spec = one<V1Deployment>(objects, "Deployment", "alasio").spec?.template.spec;
    assert.equal(spec?.securityContext?.runAsUser, 1234);
    assert.deepEqual(spec?.securityContext?.supplementalGroups, [999]);
    assert.deepEqual(spec?.containers[0]?.env?.find(({ name }) => name === "HOME"), { name: "HOME", value: "/home/op" });
    assert.deepEqual(spec?.containers[0]?.volumeMounts?.find(({ name }) => name === "home"), { name: "home", mountPath: "/home" });
    assert.deepEqual(spec?.volumes?.find(({ name }) => name === "home"), { name: "home", hostPath: { path: "/home" } });
    // Their home keeps their own Codex login.
    assert.equal(spec?.containers[0]?.env?.find(({ name }) => name === "ALASIO_KEEP_CODEX_LOGIN"), undefined);
    assert.equal(spec?.volumes?.find(({ name }) => name === "alasio-home"), undefined);
  });

  test("gives alasio no template of workspaces, whose pods are the sandbox gateway's, and tells main's alone that the gateway serves folder instances, where it does", () => {
    for (const objects of [install(), install({ host: { enabled: true } })]) {
      assert.equal(all<V1ConfigMap>(objects, "ConfigMap").some(({ metadata }) => metadata?.name === "alasio-sandbox-templates"), false);
      const deployment = one<V1Deployment>(objects, "Deployment", "alasio");
      assert.equal(deployment.spec?.template.metadata?.annotations?.["checksum/sandbox-templates"], undefined);
      assert.equal(container(deployment).env?.some(({ name }) => name === "ALASIO_KUBE_TEMPLATES"), false);
    }
    const folders = (objects: readonly KubernetesObject[]) => container(one<V1Deployment>(objects, "Deployment", "alasio")).env?.find(({ name }) => name === "ALASIO_GATEWAY_FOLDERS");
    assert.equal(folders(install()), undefined);
    assert.deepEqual(folders(install({ host: { enabled: true } })), { name: "ALASIO_GATEWAY_FOLDERS", value: "1" });
  });
});

describe("neon", () => {
  test("makes the stack's secrets in a Job of its own", () => {
    const job = one<V1Job>(install(), "Job", "alasio-neon-setup");
    assert.equal(job.spec?.template.spec?.serviceAccountName, "alasio-neon-setup");
    const setup = job.spec?.template.spec?.containers[0];
    assert.deepEqual(setup?.command, ["node", "/opt/alasio/neon/control/kube-setup.ts"]);
    assert.equal(setup?.env?.find(({ name }) => name === "S3_EXTERNAL"), undefined);
  });

  test("runs each workload at the count its scale fixes: one, but for the safekeepers, the compute, whose count neon-control owns, and the lake's intake and loader and the sandbox pools, which KEDA scales", () => {
    const objects = install({ host: { enabled: true } });
    const workloads = [...all<V1Deployment>(objects, "Deployment"), ...all<V1StatefulSet>(objects, "StatefulSet")];
    assert.ok(workloads.length > 10);
    const counts: Record<string, number | undefined> = { "alasio-neon-safekeeper": 3, "alasio-neon-compute": undefined, "alasio-neon-compute-ro": undefined, "alasio-lake-intake": undefined, "alasio-lake-loader": undefined, "sandboxes-none": undefined, "sandboxes-full": undefined };
    for (const { metadata, spec } of workloads) assert.equal(spec?.replicas, Object.hasOwn(counts, metadata?.name ?? "") ? counts[metadata?.name ?? ""] : 1, metadata?.name);
  });

  test("places each timeline on as many safekeepers as run", () => {
    const objects = install({ neon: { safekeepers: { scaling: { min: 5, max: 5 } } } });
    assert.equal(one<V1StatefulSet>(objects, "StatefulSet", "alasio-neon-safekeeper").spec?.replicas, 5);
    assert.match(container(one<V1Deployment>(objects, "Deployment", "alasio-neon-storage-controller")).command?.[2] ?? "", /--timeline-safekeeper-count 5/u);
  });

  test("has each safekeeper upload the segment it writes at most hourly", () => {
    const command = container(one<V1StatefulSet>(install(), "StatefulSet", "alasio-neon-safekeeper")).command?.[2] ?? "";
    assert.match(command, / --partial-backup-timeout 1h /u);
  });

  test("registers every safekeeper with neon-control by its stable name", () => {
    const env = container(one<V1Deployment>(install(), "Deployment", "alasio-neon-control")).env;
    assert.deepEqual(env?.find(({ name }) => name === "NEON_SAFEKEEPER_HOSTS"), {
      name: "NEON_SAFEKEEPER_HOSTS",
      value: [0, 1, 2].map((index) => `alasio-neon-safekeeper-${index}.alasio-neon-safekeeper.alasio.svc.cluster.local`).join(","),
    });
  });

  test("runs as many pageservers as their scale fixes, started at once, each on a node of its own, registering itself as its ordinal plus one at a Service of its pod's name, all in one zone", () => {
    const objects = install({ neon: { pageserver: { scaling: { min: 3, max: 3 } } } });
    const statefulSet = one<V1StatefulSet>(objects, "StatefulSet", "alasio-neon-pageserver");
    assert.equal(statefulSet.spec?.replicas, 3);
    assert.equal(statefulSet.spec?.podManagementPolicy, "Parallel");
    // Governed by a headless Service of their own, which alasio renders.
    const governing = one<V1Service>(objects, "Service", statefulSet.spec?.serviceName ?? "");
    assert.equal(governing.metadata?.name, "alasio-neon-pageservers");
    assert.equal(governing.spec?.clusterIP, "None");
    assert.deepEqual(governing.spec?.selector, statefulSet.spec?.selector.matchLabels);
    assert.deepEqual(statefulSet.spec?.template.spec?.topologySpreadConstraints?.map(({ topologyKey, whenUnsatisfiable }) => [topologyKey, whenUnsatisfiable]), [
      ["kubernetes.io/hostname", "DoNotSchedule"],
      ["topology.kubernetes.io/zone", "ScheduleAnyway"],
    ]);
    for (const index of [0, 1, 2]) {
      const service = one<V1Service>(objects, "Service", `alasio-neon-pageserver-${index}`);
      assert.equal(service.spec?.selector?.["statefulset.kubernetes.io/pod-name"], `alasio-neon-pageserver-${index}`);
      assert.equal(service.spec?.publishNotReadyAddresses, true);
    }
    const init = statefulSet.spec?.template.spec?.initContainers?.[0];
    assert.ok(init?.command);
    const [shell, flag, text = ""] = init.command;
    const identities = [0, 2].map((index) => {
      const { root, run, remove } = runScript({ command: [shell ?? "", flag ?? "", text.replaceAll("/data/.neon/", "$ROOT/")] }, { POD_NAME: `alasio-neon-pageserver-${index}` }, { cp: "true" });
      try {
        run();
        return [readFileSync(join(root, "identity.toml"), "utf8"), JSON.parse(readFileSync(join(root, "metadata.json"), "utf8"))];
      } finally {
        remove();
      }
    });
    assert.deepEqual(identities, [
      ["id=1\n", { host: "alasio-neon-pageserver-0", port: 6400, http_host: "alasio-neon-pageserver-0", http_port: 9898, availability_zone_id: "az-pageserver" }],
      ["id=3\n", { host: "alasio-neon-pageserver-2", port: 6400, http_host: "alasio-neon-pageserver-2", http_port: 9898, availability_zone_id: "az-pageserver" }],
    ]);
    assert.ok(one(objects, "PodDisruptionBudget", "alasio-neon-pageserver"));
    // neon-control waits for each, and splits the tenant into as many shards as configured.
    const env = container(one<V1Deployment>(install({ neon: { pageserver: { scaling: { min: 2, max: 2 } }, shards: 4 } }), "Deployment", "alasio-neon-control")).env ?? [];
    assert.deepEqual(["NEON_PAGESERVERS", "NEON_SHARDS"].map((key) => env.find(({ name }) => name === key)?.value), ["2", "4"]);
  });

  test("takes the tenant's shards as a power of two from 1 to 16, one by default, and the pageservers' count fixed", () => {
    assert.equal(configOf().neon.shards, 1);
    assert.equal(configOf({ neon: { shards: 8 } }).neon.shards, 8);
    for (const shards of [0, 3, 32]) {
      const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, neon: { shards } });
      assert.ok(Result.isFailure(decoded), String(shards));
      assert.equal(decoded.failure.message, "neon.shards must be a power of two from 1 to 16");
    }
    const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, neon: { pageserver: { scaling: { min: 1, max: 2 } } } });
    assert.ok(Result.isFailure(decoded));
    assert.equal(decoded.failure.message, "neon.pageserver.scaling.max must equal min: nothing scales it between them");
  });

  test("runs a read replica of main's compute while the lake runs, which the lake's loader loads from, asleep as the compute may be", () => {
    const objects = install();
    const replica = one<V1Deployment>(objects, "Deployment", "alasio-neon-compute-ro");
    const args = container(replica).args ?? [];
    assert.equal(args[args.indexOf("--compute-id") + 1], "alasio-ro");
    assert.equal(replica.spec?.replicas, undefined);
    assert.equal(replica.metadata?.annotations?.["alasio.dev/suspend-after-seconds"], "300");
    assert.deepEqual(container(replica).resources, { requests: { cpu: "100m", memory: "192Mi" }, limits: { memory: "1Gi" } });
    assert.equal(one<V1Service>(objects, "Service", "alasio-neon-compute-ro").spec?.publishNotReadyAddresses, true);
    assert.equal(container(one<V1Deployment>(objects, "Deployment", "alasio-lake-loader")).env?.find(({ name }) => name === "LAKE_SOURCE_ENDPOINT")?.value, "alasio-ro");
    // Without the lake, nothing reads from one.
    const without = install({ lake: { enabled: false } });
    assert.deepEqual(without.filter(({ kind, metadata }) => kind !== "NetworkPolicy" && metadata?.name === "alasio-neon-compute-ro"), []);
  });

  test("runs one compute at a time, its spec fetched from neon-control", () => {
    const compute = one<V1Deployment>(install(), "Deployment", "alasio-neon-compute");
    assert.equal(compute.spec?.strategy?.type, "Recreate");
    assert.ok(container(compute).args?.includes("--control-plane-uri"));
    // Main's, whose spec neon-control serves under the id it has always had.
    const args = container(compute).args ?? [];
    assert.equal(args[args.indexOf("--compute-id") + 1], "alasio");
  });

  test("leaves the compute's count to neon-control, which wakes it and puts it to sleep once idle as long as configured", () => {
    const compute = one<V1Deployment>(install(), "Deployment", "alasio-neon-compute");
    assert.equal(compute.spec?.replicas, undefined);
    assert.equal(compute.metadata?.annotations?.["alasio.dev/suspend-after-seconds"], "300");
    assert.equal(one<V1Deployment>(install({ neon: { compute: { suspendAfter: "1h30m" } } }), "Deployment", "alasio-neon-compute").metadata?.annotations?.["alasio.dev/suspend-after-seconds"], "5400");
    for (const suspendAfter of ["30s", "5", "soon"]) {
      const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, neon: { compute: { suspendAfter } } });
      assert.ok(Result.isFailure(decoded), suspendAfter);
      assert.match(decoded.failure.message, /^neon\.compute\.suspendAfter must be a duration of 1m or more/u);
    }
    // Its Service takes connections as soon as Postgres does, before kubelet next probes the pod,
    // of a pod no sleep has taken off it, which its Deployment's selector does not ask of a pod.
    const service = one<V1Service>(install(), "Service", "alasio-neon-compute");
    assert.equal(service.spec?.publishNotReadyAddresses, true);
    assert.deepEqual(service.spec?.selector, { ...compute.spec?.selector.matchLabels, "alasio.dev/serving": "true" });
    assert.equal(compute.spec?.template.metadata?.labels?.["alasio.dev/serving"], "true");
    assert.equal(compute.spec?.selector.matchLabels?.["alasio.dev/serving"], undefined);
  });

  test("runs Neon's proxy, from Neon's image, which asks neon-control as its control plane with a token of its own, and keeps no TLS", () => {
    const objects = install();
    const proxy = one<V1Deployment>(objects, "Deployment", "alasio-neon-proxy");
    assert.equal(container(proxy).image, container(one<V1StatefulSet>(objects, "StatefulSet", "alasio-neon-pageserver")).image);
    assert.deepEqual(container(proxy).command, [
      "proxy",
      "--proxy", "0.0.0.0:5432",
      "--http", "0.0.0.0:7001",
      "--auth-backend", "control-plane",
      "--auth-endpoint", "http://alasio-neon-control:8080",
      "--wake-compute-cache", "size=4000,ttl=4m",
      "--project-info-cache", "size=10000,ttl=10s,max_roles=10,gc_interval=60m",
    ]);
    assert.deepEqual(container(proxy).env, [{ name: "NEON_PROXY_TO_CONTROLPLANE_TOKEN", valueFrom: { secretKeyRef: { name: "alasio-neon-proxy", key: "NEON_PROXY_TO_CONTROLPLANE_TOKEN" } } }]);
    assert.deepEqual(one<V1Service>(objects, "Service", "alasio-neon-proxy").spec?.ports, [{ name: "postgres", port: 5432 }, { name: "http", port: 7001 }]);
    assert.equal(proxy.spec?.replicas, 1);
    const two = install({ neon: { proxy: { scaling: { min: 2, max: 2 } }, control: { scaling: { min: 2, max: 2 } } } });
    for (const name of ["alasio-neon-proxy", "alasio-neon-control"]) {
      assert.equal(one<V1Deployment>(two, "Deployment", name).spec?.replicas, 2, name);
      assert.ok(one(two, "PodDisruptionBudget", name), name);
    }
  });

  test("runs neon-control with no volume, its record in the storage controller's Postgres, and its own ServiceAccount, which may scale main's compute alone", () => {
    const objects = install();
    const control = one<V1Deployment>(objects, "Deployment", "alasio-neon-control");
    assert.deepEqual(control.spec?.template.spec?.volumes?.map(({ name }) => name), ["root", "keys"]);
    assert.equal(control.spec?.strategy, undefined, "rolled out a replica at a time, as any of them serves");
    assert.equal(control.spec?.template.spec?.serviceAccountName, "alasio-neon-control");
    const env = Object.fromEntries((container(control).env ?? []).map(({ name, value, valueFrom }) => [name, value ?? valueFrom]));
    assert.deepEqual([env["NEON_CONTROLLER_DB_HOST"], env["NEON_COMPUTE_NAME"], env["NAMESPACE"]], ["alasio-neon-controller-db", "alasio-neon-compute", { fieldRef: { fieldPath: "metadata.namespace" } }]);
    assert.deepEqual(one<V1Role>(objects, "Role", "alasio-neon-control").rules, [
      { apiGroups: ["apps"], resources: ["deployments"], resourceNames: ["alasio-neon-compute", "alasio-neon-compute-ro"], verbs: ["get", "patch"] },
      { apiGroups: ["apps"], resources: ["deployments/scale"], resourceNames: ["alasio-neon-compute", "alasio-neon-compute-ro"], verbs: ["patch"] },
      { apiGroups: [""], resources: ["pods"], verbs: ["list", "patch"] },
      { apiGroups: ["discovery.k8s.io"], resources: ["endpointslices"], verbs: ["list"] },
    ]);
  });

  test("dumps the database through the proxy, which wakes main's compute", () => {
    const dump = one<V1CronJob>(install(), "CronJob", "alasio-neon-backup").spec?.jobTemplate.spec?.template.spec?.initContainers?.[0];
    const env = Object.fromEntries((dump?.env ?? []).flatMap(({ name, value }) => (value === undefined ? [] : [[name, value]])));
    assert.deepEqual([env["PGHOST"], env["PGPORT"], env["PGOPTIONS"], env["PGCONNECT_TIMEOUT"]], ["alasio-neon-proxy.alasio.svc", "5432", "endpoint=alasio", "60"]);
    assert.equal(dump?.command?.[2], "until pg_isready --quiet; do sleep 2; done && pg_dump --username=alasio --dbname=alasio --format=custom --file=/backup/alasio.dump");
  });

  test("pins every third-party image by digest", () => {
    const statefulSets = all<V1StatefulSet>(install({ workspaceStorage: { enabled: false } }), "StatefulSet");
    assert.equal(statefulSets.length, 8);
    // But the log's, which is alasio's own (alasio-log).
    for (const statefulSet of statefulSets.filter(({ metadata }) => metadata?.name !== "alasio-log")) assert.match(container(statefulSet).image ?? "", /@sha256:[0-9a-f]{64}$/u, statefulSet.metadata?.name);
    assert.match(container(one<V1Deployment>(install(), "Deployment", "alasio-seaweedfs")).image ?? "", /^chrislusf\/seaweedfs:4\.48@sha256:[0-9a-f]{64}$/u);
  });

  test("applies the object store's lifecycle rules every hour, reading the filers' log to now, through its parts, each pass done before the next", () => {
    const pass = one<V1CronJob>(install(), "CronJob", "alasio-seaweedfs-lifecycle").spec;
    assert.equal(pass?.schedule, "41 * * * *");
    assert.equal(pass?.concurrencyPolicy, "Forbid");
    const command = pass?.jobTemplate.spec?.template.spec?.containers[0]?.command?.[2] ?? "";
    assert.match(command, /s3\.lifecycle\.run-shard -shards 0-15 -s3 alasio-seaweedfs:18333 -events 0 -runtime 10m\\n/u);
    assert.match(command, / -master=alasio-seaweedfs-master-0\.alasio-seaweedfs-master\.alasio\.svc\.cluster\.local:9333 -filer=alasio-seaweedfs-filer:8888$/u);
    // Keeping one copy of each object, it copies none again.
    assert.doesNotMatch(command, /volume\.fix\.replication/u);
    assert.equal(pass?.jobTemplate.spec?.activeDeadlineSeconds, 2700);
  });

  /** The lifecycle rules each gateway gives Neon's bucket. */
  const lifecycleRules = () => {
    const lifecycle = one<V1Deployment>(install(), "Deployment", "alasio-seaweedfs").spec?.template.spec?.containers.find(({ name }) => name === "lifecycle");
    const rules = /--lifecycle-configuration '([^']+)'/u.exec(lifecycle?.command?.[2] ?? "")?.[1];
    assert.ok(rules);
    return (JSON.parse(rules) as { Rules: { Filter: { Prefix?: string }; NoncurrentVersionExpiration?: { NoncurrentDays: number }; Expiration?: unknown }[] }).Rules;
  };

  test("expires Neon's replaced objects a day past the history it keeps, and the safekeepers' re-uploads after a day", () => {
    const expiries = lifecycleRules().flatMap(({ Filter, NoncurrentVersionExpiration }) => (NoncurrentVersionExpiration ? [[Filter.Prefix, NoncurrentVersionExpiration.NoncurrentDays]] : []));
    assert.deepEqual(expiries, [["pageserver/", 2], ["safekeeper/", 1]]);
  });

  test("expires the safekeepers' full WAL segments a day after the pageserver's objects can no longer be put back, not the segments they write", () => {
    assert.deepEqual(lifecycleRules().filter(({ Expiration }) => Expiration), [{
      ID: "expire-wal",
      Status: "Enabled",
      Filter: { And: { Prefix: "safekeeper/", ObjectSizeGreaterThan: 16 * 1024 * 1024 - 1 } },
      Expiration: { Days: 3 },
    }]);
  });

  test("makes the snapshots bucket, its uploads left incomplete aborted after a day, and has the setup make its Secret, where sessions run under gVisor", () => {
    const snapshots = (objects: readonly KubernetesObject[]) => {
      const containers = one<V1Deployment>(objects, "Deployment", "alasio-seaweedfs").spec?.template.spec?.containers ?? [];
      const script = (name: string) => containers.find((container) => container.name === name)?.command?.[2] ?? "";
      return {
        buckets: /^for bucket in (.*); do$/mu.exec(script("buckets"))?.[1],
        rules: /--bucket snapshots --lifecycle-configuration '([^']+)'/u.exec(script("lifecycle"))?.[1],
        setup: one<V1Job>(objects, "Job", "alasio-neon-setup").spec?.template.spec?.containers[0]?.env?.find(({ name }) => name === "SNAPSHOTS_BUCKET")?.value,
      };
    };
    const made = snapshots(install());
    assert.equal(made.buckets, "neon lake backups workspaces snapshots");
    assert.deepEqual(JSON.parse(made.rules ?? ""), {
      Rules: [{ ID: "abort-incomplete-uploads", Status: "Enabled", Filter: { Prefix: "" }, AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 } }],
    });
    assert.equal(made.setup, "snapshots");
    // Its rules given as its own identity, the one that may write it, Neon's as the admin.
    const lifecycle = one<V1Deployment>(install(), "Deployment", "alasio-seaweedfs").spec?.template.spec?.containers.find(({ name }) => name === "lifecycle");
    assert.match(lifecycle?.command?.[2] ?? "", /AWS_ACCESS_KEY_ID="\$SNAPSHOTS_KEY_ID" AWS_SECRET_ACCESS_KEY="\$SNAPSHOTS_SECRET" aws s3api --endpoint-url http:\/\/127\.0\.0\.1:8333 put-bucket-lifecycle-configuration --bucket snapshots /u);
    assert.match(lifecycle?.command?.[2] ?? "", /AWS_ACCESS_KEY_ID="\$ADMIN_KEY_ID" AWS_SECRET_ACCESS_KEY="\$ADMIN_SECRET" aws s3api --endpoint-url http:\/\/127\.0\.0\.1:8333 put-bucket-lifecycle-configuration --bucket neon /u);
    assert.deepEqual(lifecycle?.env?.filter(({ name }) => name.endsWith("_KEY_ID")).map(({ name, valueFrom }) => [name, valueFrom?.secretKeyRef?.name]), [
      ["ADMIN_KEY_ID", "alasio-neon-s3-admin"],
      ["SNAPSHOTS_KEY_ID", "alasio-snapshots"],
    ]);
    assert.equal(lifecycle?.envFrom, undefined);
    assert.deepEqual(snapshots(install({ sessions: { runtimeClassName: "" } })), { buckets: "neon lake backups workspaces", rules: undefined, setup: undefined });
    assert.deepEqual(snapshots(install({ sessions: { enabled: false }, workspaceStorage: { enabled: false } })), { buckets: "neon lake backups", rules: undefined, setup: undefined });
  });

  test("uses the operator's object store and makes no SeaweedFS when told", () => {
    const objects = install({ objectStore: { bundled: { enabled: false }, external: { endpoint: "https://s3.example.com", existingSecret: "s3", snapshotsSecret: "s3-snapshots" } } });
    assert.deepEqual(objects.filter((object) => object.metadata?.name?.startsWith("alasio-seaweedfs")), []);
    const env = one<V1Job>(objects, "Job", "alasio-neon-setup").spec?.template.spec?.containers[0]?.env;
    assert.deepEqual(env?.find(({ name }) => name === "S3_EXTERNAL"), { name: "S3_EXTERNAL", value: "1" });
    // Its operator makes the snapshots bucket, which the setup still gives the node agents, with its key of their own.
    assert.deepEqual(env?.find(({ name }) => name === "SNAPSHOTS_BUCKET"), { name: "SNAPSHOTS_BUCKET", value: "snapshots" });
    assert.deepEqual(env?.find(({ name }) => name === "S3_SNAPSHOTS_ACCESS_KEY")?.valueFrom, { secretKeyRef: { name: "s3-snapshots", key: "accessKey" } });
  });

  test("runs no Neon when the database is the operator's, nor the object store beside it, but the stack's setup and the log", () => {
    const objects = install({ neon: { enabled: false, external: { existingSecret: "db" } }, workspaceStorage: { enabled: false } });
    const components = new Set(objects.filter((object) => object.metadata?.labels?.["alasio.dev/stack"] === "neon").map(({ metadata }) => metadata?.labels?.["app.kubernetes.io/component"]));
    assert.deepEqual([...components].filter((component) => component?.startsWith("neon-")), ["neon-setup"]);
    assert.ok(components.has("log"));
    assert.ok(![...components].some((component) => component?.startsWith("seaweedfs")), [...components].join(", "));
    assert.deepEqual(objects.filter((object) => /^alasio-lake/u.test(object.metadata?.name ?? "")), []);
    // Its setup makes none of Neon's Secrets.
    const env = one<V1Job>(objects, "Job", "alasio-neon-setup").spec?.template.spec?.containers[0]?.env ?? [];
    assert.deepEqual(env.filter(({ name }) => name.startsWith("NEON_")), []);
  });

  test("serves the compute's metrics where the collector scrapes them", () => {
    assert.ok(one<V1Service>(install(), "Service", "alasio-neon-compute").spec?.ports?.some(({ name, port }) => name === "http" && port === 3080));
  });

  test("backs the database up daily unless told not to", () => {
    assert.equal(one<V1CronJob>(install(), "CronJob", "alasio-neon-backup").spec?.schedule, "17 3 * * *");
    assert.deepEqual(install({ neon: { backup: { enabled: false } } }).filter((object) => object.metadata?.name === "alasio-neon-backup"), []);
  });

  test("backs JuiceFS's newest metadata dump up beside the database's, keeping as many of each", () => {
    const upload = one<V1CronJob>(install({ neon: { backup: { keep: 2 } } }), "CronJob", "alasio-neon-backup").spec?.jobTemplate.spec?.template.spec?.containers[0];
    assert.ok(upload);
    // As an identity of its own, which writes the backups and reads what it backs up alone.
    assert.deepEqual(upload.envFrom, [{ secretRef: { name: "alasio-neon-s3-backups" } }]);
    const env = Object.fromEntries((upload.env ?? []).flatMap(({ name, value }) => (value === undefined ? [] : [[name, value]])));
    assert.equal(env["WORKSPACES_BUCKET"], "workspaces");
    assert.equal(env["WORKSPACES_NAME"], "workspaces");
    // The object store, a directory of buckets, as aws s3 lists, reads, writes and deletes from it; the dump's volume under ROOT.
    // Its own copy, from a bucket to a bucket, the backup never asks for (CopyObject: SeaweedFS failed one, run after run);
    // and a write it corrupts, where CORRUPT says, as one the store kept wrong.
    const aws = [
      "shift 3; verb=$1; shift",
      'at() { case $1 in s3://*) printf "%s/%s" "$ROOT/store" "${1#s3://}" ;; *) printf "%s" "$1" ;; esac; }',
      "case $verb in",
      '  ls) [ -d "$(at "$1")" ] || exit 1; for f in "$(at "$1")"*; do [ -e "$f" ] && echo "2026-10-05 03:17:00 1 ${f##*/}"; done; true ;;',
      '  cp) case "$1$2" in s3://*s3://*) echo "the object store\'s own copy" >&2; exit 1 ;; esac',
      '      mkdir -p "$(dirname "$(at "$2")")"; cp "$(at "$1")" "$(at "$2")"',
      '      case $2 in s3://*) [ -n "${CORRUPT:-}" ] && printf x >> "$(at "$2")" ;; esac; true ;;',
      '  rm) rm "$(at "$1")" ;;',
      "esac",
    ].join("\n");
    const { root, run: runIn, remove } = runScript(upload, env, { aws });
    // Its dumps' volume, the test's own.
    const run = (more: Readonly<Record<string, string>> = {}) => runIn({ BACKUP_DIR: join(root, "backup"), ...more });
    try {
      const store = join(root, "store");
      const backups = () => readdirSync(join(store, "backups")).sort();
      mkdirSync(join(root, "backup"));
      writeFileSync(join(root, "backup", "alasio.dump"), "database");
      mkdirSync(join(store, "backups"), { recursive: true });
      const older = ["alasio-20261003T031700Z.dump", "alasio-20261004T031700Z.dump", "workspaces-dump-2026-10-03-021000.json.gz", "workspaces-dump-2026-10-04-021000.json.gz"];
      for (const name of older) writeFileSync(join(store, "backups", name), "older");
      // Before JuiceFS has made any, there is none to copy.
      assert.match(run(), /^JuiceFS has dumped no metadata of workspaces yet$/mu);
      assert.equal(backups().filter((name) => name.startsWith("alasio-")).length, 2);
      assert.ok(!backups().includes("alasio-20261003T031700Z.dump"));

      const meta = join(store, "workspaces", "workspaces", "meta");
      mkdirSync(meta, { recursive: true });
      writeFileSync(join(meta, "dump-2026-10-04-021000.json.gz"), gzipSync("older"));
      writeFileSync(join(meta, "dump-2026-10-05-021000.json.gz"), gzipSync("newest"));
      // Copied by its bytes, read and written as any object is, not by the store's own copy.
      run();
      assert.deepEqual(backups().filter((name) => name.startsWith("workspaces-")), ["workspaces-dump-2026-10-04-021000.json.gz", "workspaces-dump-2026-10-05-021000.json.gz"]);
      assert.equal(gunzipSync(readFileSync(join(store, "backups", "workspaces-dump-2026-10-05-021000.json.gz"))).toString(), "newest");
      assert.equal(backups().filter((name) => name.startsWith("alasio-")).length, 2);
      // A copy the store keeps wrong fails the backup, saying so; as does a dump that is no whole gzip.
      assert.throws(() => run({ CORRUPT: "1" }), /the copy of dump-2026-10-05-021000\.json\.gz in backups is not the dump read/u);
      writeFileSync(join(meta, "dump-2026-10-06-021000.json.gz"), gzipSync("cut short").subarray(0, 12));
      assert.throws(() => run(), /dump-2026-10-06-021000\.json\.gz is no whole gzip/u);
    } finally {
      remove();
    }

    const without = one<V1CronJob>(install({ workspaceStorage: { enabled: false } }), "CronJob", "alasio-neon-backup").spec?.jobTemplate.spec?.template.spec?.containers[0];
    assert.doesNotMatch(without?.command?.[2] ?? "", /WORKSPACES/u);
    assert.equal(without?.env?.find(({ name }) => name.startsWith("WORKSPACES_")), undefined);
  });
});

/** The variables that name the files of the verifiers alasio is given, and the files. */
const verifierFiles = (workload: V1Deployment) => container(workload).env?.filter(({ name }) => name.endsWith("_VERIFIER_FILE")).map(({ name, value }) => [name, value]);

describe("the lake's readers", () => {
  test("read the lake with the reader's credentials alone, behind the lake's Service, each ready once it has the lake open", () => {
    const objects = install({ lake: { query: { resources: { limits: { memory: "2Gi" } } } } });
    const readers = one<V1Deployment>(objects, "Deployment", "alasio-lake-query");
    const query = container(readers);
    assert.deepEqual([query.name, query.command, query.envFrom, readers.spec?.replicas, readers.spec?.template.spec?.priorityClassName], ["query", ["node", "src/endpoint.ts"], undefined, 1, "alasio-floor"]);
    const secrets = query.env?.flatMap(({ name, valueFrom }) => (valueFrom?.secretKeyRef ? [`${name}=${valueFrom.secretKeyRef.name}/${valueFrom.secretKeyRef.key}`] : []));
    assert.deepEqual(secrets, ["LAKE_READER_PASSWORD", "LAKE_READER_S3_KEY", "LAKE_READER_S3_SECRET", "LAKE_QUERY_TOKEN"].map((key) => `${key}=alasio-lake/${key}`));
    assert.deepEqual(query.resources?.limits, { memory: "2Gi" });
    assert.deepEqual(query.ports, [{ name: "query", containerPort: 8090 }]);
    assert.deepEqual(query.readinessProbe?.httpGet, { path: "/readyz", port: "query" });
    // The lake's Service, which Grafana and alasio lake reach, is theirs; each finds the others by a headless one.
    const selector = { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "lake-query" };
    assert.deepEqual(one<V1Service>(objects, "Service", "alasio-lake").spec, { selector, ports: [{ name: "query", port: 8090 }] });
    assert.deepEqual(one<V1Service>(objects, "Service", "alasio-lake-query-peers").spec, { clusterIP: "None", selector, ports: [{ name: "query", port: 8090 }] });
    assert.deepEqual(query.env?.find(({ name }) => name === "LAKE_QUERY_PEERS"), { name: "LAKE_QUERY_PEERS", value: "alasio-lake-query-peers.alasio.svc.cluster.local" });
  });

  test("wait, as they stop, for the queries under way in them, alasio lake's among them, within their grace to stop", () => {
    const readers = one<V1Deployment>(install(), "Deployment", "alasio-lake-query");
    assert.deepEqual(container(readers).lifecycle, { preStop: { exec: { command: ["node", "src/drain.ts"] } } });
    // The drain at its longest, and then the endpoint's own stop.
    const grace = readers.spec?.template.spec?.terminationGracePeriodSeconds ?? 30;
    assert.ok(grace * 1000 >= DRAIN_WITHIN_MS + 10_000, `a reader has ${grace} s to stop, its drain ${DRAIN_WITHIN_MS / 1000} s`);
  });

  test("scale on the queries the pool has not answered, as any reader sums them, two to a reader, never below one", () => {
    const objects = install({ lake: { query: { scaling: { min: 1, max: 3 } } } });
    assert.equal(one<V1Deployment>(objects, "Deployment", "alasio-lake-query").spec?.replicas, undefined);
    const scaled = one<ScaledObject>(objects, "ScaledObject", "alasio-lake-query");
    assert.deepEqual([scaled.spec.minReplicaCount, scaled.spec.maxReplicaCount, scaled.spec.triggers], [1, 3, [{
      type: "metrics-api",
      metadata: { url: "http://alasio-lake.alasio.svc.cluster.local:8090/pool", valueLocation: "queries", targetValue: "2" },
    }]]);
    // KEDA's operator asks any of them, as do the others, which are pods of the stack.
    const pod = (name: string) => ({ namespace: "alasio", namespaceLabels: { "kubernetes.io/metadata.name": "alasio" }, labels: one<V1Deployment>(objects, "Deployment", name).spec?.template.metadata?.labels ?? {} });
    const policies = all<NetworkPolicyObject>(objects, "NetworkPolicy");
    assert.deepEqual([admits(policies, pod("alasio-keda-operator"), pod("alasio-lake-query"), 8090), admits(policies, pod("alasio-lake-query"), pod("alasio-lake-query"), 8090)], [true, true]);
    // Several, a budget of one at a time, spread across nodes.
    assert.ok(one(objects, "PodDisruptionBudget", "alasio-lake-query"));
    assert.ok(one<V1Deployment>(objects, "Deployment", "alasio-lake-query").spec?.template.spec?.topologySpreadConstraints);
    // At one, neither, nor a scaler; and never none, as Grafana's alerts read them.
    assert.deepEqual(install().filter(({ kind, metadata }) => ["ScaledObject", "PodDisruptionBudget"].includes(kind ?? "") && metadata?.name === "alasio-lake-query"), []);
    const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, lake: { query: { scaling: { min: 0, max: 2 } } } });
    assert.ok(Result.isFailure(decoded));
    assert.equal(decoded.failure.message, "lake.query.scaling.min must be at least 1: it never sleeps");
  });

  test("has alasio make the lake reader's role while the lake runs", () => {
    const alasio = (file: Record<string, unknown>) => one<V1Deployment>(install(file), "Deployment", "alasio");
    assert.deepEqual(verifierFiles(alasio({}))?.slice(0, 3), [
      ["ALASIO_LAKE_VERIFIER_FILE", "/run/alasio/database/lake-verifier"],
      ["ALASIO_HARNESS_VERIFIER_FILE", "/run/alasio/database/harness-verifier"],
      ["ALASIO_LAKE_READER_VERIFIER_FILE", "/run/alasio/database/lake-reader-verifier"],
    ]);
    const database = alasio({}).spec?.template.spec?.volumes?.find(({ name }) => name === "database");
    assert.deepEqual(database?.secret?.items?.map(({ key }) => key).slice(0, 4), ["url", "lake-verifier", "harness-verifier", "lake-reader-verifier"]);
    // The lake's role and the harness host's alasio makes whatever runs.
    assert.deepEqual(verifierFiles(alasio({ lake: { enabled: false } }))?.map(([name]) => name), ["ALASIO_LAKE_VERIFIER_FILE", "ALASIO_HARNESS_VERIFIER_FILE"]);
    // The log's password beside them, as alasio reaches its Telegram topic.
    assert.equal(container(alasio({})).env?.find(({ name }) => name === "ALASIO_LOG_PASSWORD_FILE")?.value, "/run/alasio/log/password");
  });
});

describe("the lake's writers", () => {
  test("have VPA size the memory alone of the loader and the intake while they sleep, woken from none, and their CPU too where they never do", () => {
    const controlled = (objects: readonly KubernetesObject[], name: string) =>
      one<VerticalPodAutoscaler>(objects, "VerticalPodAutoscaler", name).spec.resourcePolicy.containerPolicies.map((policy) => (policy as { controlledResources?: string[] }).controlledResources);
    const asleep = install();
    assert.deepEqual([controlled(asleep, "alasio-lake-loader"), controlled(asleep, "alasio-lake-intake")], [[["memory"]], [["memory"]]]);
    const awake = install({ lake: { loader: { scaling: { min: 1, max: 1 } }, intake: { scaling: { min: 1, max: 2 } } } });
    assert.deepEqual([controlled(awake, "alasio-lake-loader"), controlled(awake, "alasio-lake-intake")], [[undefined], [undefined]]);
  });

  /** `workload`'s variables, by name: each's value, or the Secret's key it is from. */
  const variables = (from: V1Container) =>
    Object.fromEntries((from.env ?? []).map(({ name, value, valueFrom }) => [name, value ?? `${valueFrom?.secretKeyRef?.name}/${valueFrom?.secretKeyRef?.key}`]));

  test("load it with a loader of each environment, which KEDA wakes from none as soon as its group is behind on the environment's wake topic", () => {
    const objects = install();
    const loader = one<V1Deployment>(objects, "Deployment", "alasio-lake-loader");
    assert.deepEqual([loader.spec?.replicas, loader.spec?.strategy?.type, loader.spec?.template.spec?.priorityClassName], [undefined, "Recreate", "alasio-component"]);
    const env = variables(container(loader));
    assert.deepEqual(
      [env["LOG_BROKERS"], env["LOG_USERNAME"], env["LOG_PASSWORD"], env["LAKE_WAKE_TOPIC"], env["LAKE_WAKE_GROUP"], env["LAKE_DATABASE_PASSWORD"], env["OTEL_SERVICE_NAME"]],
      ["alasio-log.alasio.svc:9092", "alasio-log-loader-main/username", "alasio-log-loader-main/password", "lake.main", "lake-loader.main", "alasio-lake/LAKE_DATABASE_PASSWORD", "alasio-lake-loader"],
    );
    // None of the reader's credentials, nor maintenance's.
    assert.deepEqual([container(loader).envFrom, env["LAKE_READER_PASSWORD"], env["LAKE_BRANCHES_TOKEN"]], [undefined, undefined, undefined]);
    const scaled = one<ScaledObject>(objects, "ScaledObject", "alasio-lake-loader");
    assert.deepEqual([scaled.spec.minReplicaCount, scaled.spec.maxReplicaCount, scaled.spec.triggers], [0, 1, [{
      type: "kafka",
      metadata: { bootstrapServers: "alasio-log.alasio.svc:9092", consumerGroup: "lake-loader.main", topic: "lake.main", lagThreshold: "1", activationLagThreshold: "0", offsetResetPolicy: "earliest" },
      authenticationRef: { name: "alasio-lake-loader" },
    }]]);
    assert.deepEqual((one<KubernetesObject & { spec: object }>(objects, "TriggerAuthentication", "alasio-lake-loader")).spec, {
      secretTargetRef: ["sasl", "username", "password"].map((parameter) => ({ parameter, name: "alasio-log-scaler", key: parameter })),
    });
    // alasio tells it on that topic.
    assert.deepEqual(container(one<V1Deployment>(objects, "Deployment", "alasio")).env?.find(({ name }) => name === "ALASIO_LAKE_WAKE_TOPIC"), { name: "ALASIO_LAKE_WAKE_TOPIC", value: "lake.main" });
  });

  test("maintain it in passes of their own, main's: daily on lake.maintenance.schedule, and the telemetry's files merged hourly, each waiting for the one before", () => {
    const objects = install({ lake: { maintenance: { schedule: "5 4 * * *" } }, telemetry: { retentionDays: 7 } });
    const pass = (name: string) => {
      const cronJob = one<V1CronJob>(objects, "CronJob", name);
      const job = cronJob.spec?.jobTemplate.spec?.template.spec;
      assert.ok(job?.containers[0], name);
      return { cronJob, container: job.containers[0] };
    };
    const daily = pass("alasio-lake-maintenance");
    const hourly = pass("alasio-lake-merge");
    assert.deepEqual([daily.cronJob.spec?.schedule, daily.container.command], ["5 4 * * *", ["node", "src/maintain.ts", "maintain"]]);
    assert.deepEqual([hourly.cronJob.spec?.schedule, hourly.container.command], ["47 * * * *", ["node", "src/maintain.ts", "merge"]]);
    for (const { cronJob } of [daily, hourly]) assert.equal(cronJob.spec?.concurrencyPolicy, "Forbid");
    // It keeps telemetry.retentionDays days of it, and keeps its files while Neon has branches, whose lakes read them, asking neon-control with its token.
    const env = variables(daily.container);
    assert.deepEqual([env["LAKE_RETENTION_DAYS"], env["LAKE_BRANCHES_URL"], env["LAKE_BRANCHES_TOKEN"]], ["7", "http://alasio-neon-control:8080/branches", "alasio-lake/LAKE_BRANCHES_TOKEN"]);
    // Without the lake, none.
    assert.deepEqual(install({ lake: { enabled: false } }).filter(({ metadata }) => /^alasio-lake/u.test(metadata?.name ?? "")).map(({ metadata }) => metadata?.name), []);
  });

  test("reconcile each environment's lake on demand alone, as a Job of the pass in its namespace, against its own catalog", () => {
    const config = configOf();
    // No object of the installation's runs it on a schedule.
    assert.equal(install().some(({ kind, metadata }) => kind === "CronJob" && metadata?.name === "alasio-lake-reconcile"), false);
    for (const [environment, endpoint, replica] of [[MAIN_ENVIRONMENT, "alasio", "alasio-ro"], [branchEnvironment("try"), "branch-try", "branch-try-ro"]] as const) {
      const job = lakeReconcile(config, environment);
      const pass = job.spec?.template.spec?.containers[0];
      assert.ok(pass);
      assert.deepEqual([job.metadata?.name, job.metadata?.namespace, job.spec?.backoffLimit, pass.command], ["alasio-lake-reconcile", environment.namespace, 0, ["node", "src/maintain.ts", "reconcile"]]);
      const env = variables(pass);
      // Its catalog on its primary, alasio's database read on its read replica, as its loader reads it.
      assert.deepEqual([env["LAKE_DATABASE_ENDPOINT"], env["LAKE_SOURCE_ENDPOINT"], env["LAKE_DATABASE_PASSWORD"]], [endpoint, replica, "alasio-lake/LAKE_DATABASE_PASSWORD"]);
      // Its DuckDB sized as the other passes' are.
      assert.equal(env["LAKE_MEMORY_LIMIT"], "614MiB");
      // The bundled store's filer and masters, which it asks whether a file the store fails a read of is gone, as the reconciliation's pod, which they admit.
      assert.deepEqual(
        [env["LAKE_STORE_FILER_URL"], env["LAKE_STORE_MASTER_URL"], env["LAKE_STORE_VOLUME_SERVERS"]],
        ["http://alasio-seaweedfs-filer.alasio.svc:8888", "http://alasio-seaweedfs-master.alasio.svc:9333", "1"],
      );
      assert.equal(job.spec?.template.metadata?.labels?.["app.kubernetes.io/component"], "lake-reconcile");
      // A branch environment's telemetry, main's as of its branch point, which main's reads again from the log.
      assert.equal(env["LAKE_BRANCH"], environment.branch ?? undefined);
    }
    // Another store: no parts to ask.
    const external = variables(lakeReconcile(configOf({ objectStore: { bundled: { enabled: false }, external: { endpoint: "https://s3.example.com", existingSecret: "s3", snapshotsSecret: "s3-snapshots" } } })).spec?.template.spec?.containers[0] ?? { name: "" });
    assert.deepEqual([external["LAKE_STORE_FILER_URL"], external["LAKE_STORE_MASTER_URL"], external["LAKE_STORE_VOLUME_SERVERS"]], [undefined, undefined, undefined]);
    // As many as the store runs.
    assert.equal(variables(lakeReconcile(configOf({ objectStore: { bundled: { replication: "001", volumeServers: { scaling: { min: 3, max: 3 } } } } })).spec?.template.spec?.containers[0] ?? { name: "" })["LAKE_STORE_VOLUME_SERVERS"], "3");
  });

  test("give each pass's DuckDB three fifths of its container's memory limit, the rest Node's and the Parquet reader's", () => {
    const limitOf = (objects: readonly KubernetesObject[]) =>
      all<V1CronJob>(objects, "CronJob").filter(({ metadata }) => metadata?.name?.startsWith("alasio-lake-")).map(({ metadata, spec }) => [metadata?.name, variables(spec?.jobTemplate.spec?.template.spec?.containers[0] ?? { name: "" })["LAKE_MEMORY_LIMIT"]]);
    assert.deepEqual(limitOf(install()), [["alasio-lake-maintenance", "614MiB"], ["alasio-lake-merge", "614MiB"]]);
    assert.deepEqual(limitOf(install({ lake: { maintenance: { resources: { limits: { memory: "2Gi" } } } } })), [["alasio-lake-maintenance", "1228MiB"], ["alasio-lake-merge", "1228MiB"]]);
  });

  test("take their settings apart from the readers', refusing the key they replaced", () => {
    const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, lake: { resources: { limits: { memory: "2Gi" } } } });
    assert.ok(Result.isFailure(decoded));
    assert.match(decoded.failure.message, /^lake\.resources /u);
    const loader = one<V1Deployment>(install({ lake: { loader: { resources: { limits: { memory: "2Gi" } } } } }), "Deployment", "alasio-lake-loader");
    assert.deepEqual(container(loader).resources?.limits, { memory: "2Gi" });
  });
});

describe("object store", () => {
  const MASTERS = "alasio-seaweedfs-master-0.alasio-seaweedfs-master.alasio.svc.cluster.local:9333";
  const args = (objects: readonly KubernetesObject[], kind: "StatefulSet" | "Deployment", name: string) => container(one<V1StatefulSet>(objects, kind, name)).args ?? [];

  test("runs SeaweedFS as its parts, each at one replica, under the names the stack reaches it by, on the floor, above it those its claims hold to a node", () => {
    const objects = install();
    const parts = [
      ["StatefulSet", "alasio-seaweedfs-master", "alasio-bound"],
      ["StatefulSet", "alasio-seaweedfs-volume", "alasio-bound"],
      ["StatefulSet", "alasio-seaweedfs-filer", "alasio-floor"],
      ["StatefulSet", "alasio-seaweedfs-filer-store", "alasio-bound"],
      ["Deployment", "alasio-seaweedfs", "alasio-floor"],
    ] as const;
    for (const [kind, name, priorityClassName] of parts) {
      const workload = one<V1StatefulSet>(objects, kind, name);
      assert.deepEqual([workload.spec?.replicas, workload.spec?.template.spec?.priorityClassName], [1, priorityClassName], name);
    }
    // No all-in-one server.
    assert.deepEqual(args(objects, "StatefulSet", "alasio-seaweedfs-master").slice(0, 1), ["master"]);
    assert.deepEqual(one<V1Service>(objects, "Service", "alasio-seaweedfs").spec?.ports?.map(({ port }) => port), [8333, 18333, 9327]);
    for (const name of ["alasio-seaweedfs-master", "alasio-seaweedfs-volume", "alasio-seaweedfs-filer-peers"]) {
      assert.deepEqual([one<V1Service>(objects, "Service", name).spec?.clusterIP, one<V1Service>(objects, "Service", name).spec?.publishNotReadyAddresses], ["None", true], name);
    }
    // Any filer, for the shells, by a Service of its own; each by the headless one, its StatefulSet's.
    assert.deepEqual(one<V1Service>(objects, "Service", "alasio-seaweedfs-filer").spec?.ports?.map(({ port }) => port), [8888, 18888]);
    assert.equal(one<V1Service>(objects, "Service", "alasio-seaweedfs-filer").spec?.clusterIP, undefined);
    assert.equal(one<V1StatefulSet>(objects, "StatefulSet", "alasio-seaweedfs-filer").spec?.serviceName, "alasio-seaweedfs-filer-peers");
    // One master needs no quorum; the parts reach it by its pod's name.
    assert.ok(args(objects, "StatefulSet", "alasio-seaweedfs-master").includes("-peers=none"));
    assert.ok(args(objects, "StatefulSet", "alasio-seaweedfs-master").includes("-defaultReplication=000"));
    assert.ok(args(objects, "StatefulSet", "alasio-seaweedfs-master").includes("-garbageThreshold=0.1"));
    assert.ok(args(objects, "StatefulSet", "alasio-seaweedfs-master").includes("-telemetry=false"));
    assert.ok(args(objects, "StatefulSet", "alasio-seaweedfs-volume").includes(`-master=${MASTERS}`));
    assert.ok(args(objects, "StatefulSet", "alasio-seaweedfs-filer").includes(`-master=${MASTERS}`));
    assert.ok(args(objects, "Deployment", "alasio-seaweedfs").includes("-filer=alasio-seaweedfs-filer-0.alasio-seaweedfs-filer-peers.alasio.svc.cluster.local:8888"));
    assert.ok(args(objects, "StatefulSet", "alasio-seaweedfs-filer").includes("-ip=$(POD_NAME).alasio-seaweedfs-filer-peers.$(POD_NAMESPACE).svc.cluster.local"));
    // Each serves its metrics where the collector scrapes them.
    for (const [kind, name] of parts.filter(([, name]) => name !== "alasio-seaweedfs-filer-store")) assert.ok(args(objects, kind, name).includes("-metricsPort=9327"), name);
    // What keeps state is recommended its requests; what keeps none resized in place.
    const modes = Object.fromEntries(all<VerticalPodAutoscaler>(objects, "VerticalPodAutoscaler").filter(({ metadata }) => metadata?.name?.startsWith("alasio-seaweedfs")).map(({ metadata, spec }) => [metadata?.name, spec.updatePolicy.updateMode]));
    assert.deepEqual(modes, { "alasio-seaweedfs-master": "Off", "alasio-seaweedfs-volume": "Off", "alasio-seaweedfs-filer-store": "Off", "alasio-seaweedfs-filer": "InPlace", "alasio-seaweedfs": "InPlace" });
  });

  test("bounds what an S3 gateway holds of uploads under way, not of GETs, holds its connections' buffers small, and has Go collect before its memory limit", () => {
    const gateway = (objects: readonly KubernetesObject[]) => container(one<V1Deployment>(objects, "Deployment", "alasio-seaweedfs"));
    const s3 = gateway(install());
    // Its limit 2Gi by default, an eighth of it for uploads; no budget of its GETs, which livelocks one (./object-store.ts).
    assert.equal(s3.resources?.limits?.["memory"], "2Gi");
    assert.deepEqual(s3.args?.filter((arg) => /^-(readerCacheSizeMB|concurrentUploadLimitMB)=/u.test(arg)), ["-concurrentUploadLimitMB=256"]);
    // Three fifths of its limit for Go.
    assert.deepEqual(s3.env, [{ name: "GOMEMLIMIT", value: "1228MiB" }]);
    // Its connections' kernel buffers at most 1 MiB each.
    assert.deepEqual(one<V1Deployment>(install(), "Deployment", "alasio-seaweedfs").spec?.template.spec?.securityContext?.sysctls, [
      { name: "net.ipv4.tcp_rmem", value: "4096 131072 1048576" },
      { name: "net.ipv4.tcp_wmem", value: "4096 16384 1048576" },
    ]);
    const smaller = gateway(install({ objectStore: { bundled: { gateways: { resources: { limits: { memory: "1Gi" } } } } } }));
    assert.deepEqual(smaller.args?.filter((arg) => /^-concurrentUploadLimitMB=/u.test(arg)), ["-concurrentUploadLimitMB=128"]);
    assert.deepEqual(smaller.env, [{ name: "GOMEMLIMIT", value: "614MiB" }]);
  });

  test("keeps objects on the volume servers' volumes, a fixed count of volumes each, in one rack, so a copy goes to another server", () => {
    const objects = install({ objectStore: { bundled: { volumeServers: { volumes: 40, storage: { size: "50Gi" } } } } });
    const volume = one<V1StatefulSet>(objects, "StatefulSet", "alasio-seaweedfs-volume");
    for (const arg of ["-dir=/data", "-max=40", "-minFreeSpace=5GiB", "-dataCenter=dc1", "-rack=rack1"]) assert.ok(container(volume).args?.includes(arg), arg);
    assert.equal(volume.spec?.volumeClaimTemplates?.[0]?.spec?.resources?.requests?.["storage"], "50Gi");
    assert.equal(volume.spec?.podManagementPolicy, "Parallel");
  });

  test("starts the filers one after another, each once its store has loaded and a volume server has registered, on the filer store", () => {
    const objects = install();
    const filer = one<V1StatefulSet>(objects, "StatefulSet", "alasio-seaweedfs-filer");
    assert.equal(filer.spec?.podManagementPolicy, "OrderedReady");
    const waits = filer.spec?.template.spec?.initContainers ?? [];
    assert.deepEqual(waits.map(({ name }) => name), ["wait-for-filer-store", "wait-for-volume-server"]);
    assert.match(waits[0]?.command?.[2] ?? "", /valkey-cli -h alasio-seaweedfs-filer-store --user filer ping/u);
    assert.deepEqual(waits[0]?.env, [{ name: "VALKEYCLI_AUTH", valueFrom: { secretKeyRef: { name: "alasio-seaweedfs-filer-store-filer", key: "password" } } }]);
    assert.match(waits[1]?.command?.[2] ?? "", /http:\/\/alasio-seaweedfs-master:9333\/dir\/status \| grep -q '"DataNodes":\\\[\{'/u);
    assert.deepEqual(container(filer).env?.find(({ name }) => name === "WEED_REDIS2_PASSWORD"), { name: "WEED_REDIS2_PASSWORD", valueFrom: { secretKeyRef: { name: "alasio-seaweedfs-filer-store-filer", key: "password" } } });
    assert.equal(
      one<V1ConfigMap>(objects, "ConfigMap", "alasio-seaweedfs-filer").data?.["filer.toml"],
      '[redis2]\nenabled = true\naddress = "alasio-seaweedfs-filer-store:6379"\nusername = "filer"\ndatabase = 0\nkeyPrefix = "seaweedfs:"\nsuperLargeDirectories = []\n',
    );
  });

  test("sizes the filer store's memory from the entries it holds, and refuses a limit that leaves no room for its snapshots", () => {
    const conf = (objects: readonly KubernetesObject[]) => one<V1ConfigMap>(objects, "ConfigMap", "alasio-seaweedfs-filer-store").data?.["valkey.conf"] ?? "";
    assert.match(conf(install()), /^maxmemory 256mb$/mu);
    assert.match(conf(install()), /^maxmemory-policy noeviction$/mu);
    assert.match(conf(install({ objectStore: { bundled: { filerStore: { entries: 400_000, resources: { limits: { memory: "2Gi" } } } } } })), /^maxmemory 704mb$/mu);
    const refused = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, objectStore: { bundled: { filerStore: { entries: 400_000 } } } });
    assert.ok(Result.isFailure(refused));
    assert.equal(refused.failure.message, "objectStore.bundled.filerStore.resources.limits.memory must be at least 1.4 GiB, twice what its entries take, for the fork that writes its snapshots");
  });

  test("runs three masters in Raft when asked, each naming the others, and never two", () => {
    const objects = install({ objectStore: { bundled: { masters: { scaling: { min: 3, max: 3 } } } } });
    const peers = [0, 1, 2].map((ordinal) => `alasio-seaweedfs-master-${ordinal}.alasio-seaweedfs-master.alasio.svc.cluster.local:9333`).join(",");
    assert.ok(args(objects, "StatefulSet", "alasio-seaweedfs-master").includes(`-peers=${peers}`));
    assert.ok(args(objects, "StatefulSet", "alasio-seaweedfs-volume").includes(`-master=${peers}`));
    assert.ok(one<KubernetesObject>(objects, "PodDisruptionBudget", "alasio-seaweedfs-master"));
    const refused = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, objectStore: { bundled: { masters: { scaling: { min: 2, max: 2 } } } } });
    assert.ok(Result.isFailure(refused) && refused.failure.message === "objectStore.bundled.masters.scaling must be 1 or 3: two masters elect no leader once one is gone");
  });

  test("keeps as many copies as the replication says, each on another volume server, which there must be enough of", () => {
    const objects = install({ objectStore: { bundled: { replication: "001", volumeServers: { scaling: { min: 3, max: 3 } } } } });
    assert.ok(args(objects, "StatefulSet", "alasio-seaweedfs-master").includes("-defaultReplication=001"));
    const volume = one<V1StatefulSet>(objects, "StatefulSet", "alasio-seaweedfs-volume");
    assert.equal(volume.spec?.replicas, 3);
    // Each on a node of its own while there are as many; zones as they come.
    assert.deepEqual(volume.spec?.template.spec?.topologySpreadConstraints?.map(({ topologyKey, whenUnsatisfiable }) => [topologyKey, whenUnsatisfiable]), [
      ["kubernetes.io/hostname", "DoNotSchedule"],
      ["topology.kubernetes.io/zone", "ScheduleAnyway"],
    ]);
    // Each hour, under the shell's lock, every volume's replication is set to the store's, as one made before it changed differs, and what has fewer copies than that is copied again.
    const pass = (objects: readonly KubernetesObject[]) => one<V1CronJob>(objects, "CronJob", "alasio-seaweedfs-lifecycle").spec?.jobTemplate.spec?.template.spec?.containers[0]?.command?.[2] ?? "";
    assert.match(pass(objects), /-runtime 10m\\nlock\\nvolume\.configure\.replication -replication 001 -collectionPattern \*\\nvolume\.fix\.replication -apply\\nunlock\\n' \| weed shell/u);
    // Without copies, a spread as any other, and nothing to copy.
    const singly = install({ objectStore: { bundled: { volumeServers: { scaling: { min: 3, max: 3 } } } } });
    assert.match(pass(singly), /-runtime 10m\\nlock\\nvolume\.configure\.replication -replication 000 -collectionPattern \*\\nunlock\\n' \| weed shell/u);
    const single = one<V1StatefulSet>(singly, "StatefulSet", "alasio-seaweedfs-volume");
    assert.deepEqual(single.spec?.template.spec?.topologySpreadConstraints?.map(({ whenUnsatisfiable }) => whenUnsatisfiable), ["ScheduleAnyway", "ScheduleAnyway"]);
    assert.ok(one<KubernetesObject>(objects, "PodDisruptionBudget", "alasio-seaweedfs-volume"));
    const refused = (install: Record<string, unknown>) => {
      const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, objectStore: { bundled: install } });
      return Result.isFailure(decoded) ? decoded.failure.message : null;
    };
    assert.equal(refused({ replication: "001" }), "objectStore.bundled.volumeServers.scaling must be at least 2, one for each copy objectStore.bundled.replication keeps");
    assert.equal(refused({ replication: "010", volumeServers: { scaling: { min: 2 } } }), "objectStore.bundled.replication must be 000, or 00N for N more copies, each on another volume server");
  });

  test("scales its gateways on their CPU where their scale is a range, KEDA owning their replicas", () => {
    // Held off scaling up past the start of each, which a rollout's gateways each have.
    assert.equal(one<ScaledObject>(install({ objectStore: { bundled: { gateways: { scaling: { min: 2, max: 4 } } } } }), "ScaledObject", "alasio-seaweedfs").spec.advanced?.horizontalPodAutoscalerConfig.behavior.scaleUp.stabilizationWindowSeconds, 120);
    const objects = install({ objectStore: { bundled: { gateways: { scaling: { min: 2, max: 4 } } } } });
    const gateway = one<V1Deployment>(objects, "Deployment", "alasio-seaweedfs");
    assert.equal(gateway.spec?.replicas, undefined);
    const scaled = one<ScaledObject>(objects, "ScaledObject", "alasio-seaweedfs");
    assert.deepEqual([scaled.spec.minReplicaCount, scaled.spec.maxReplicaCount, scaled.spec.triggers], [2, 4, [{ type: "cpu", metricType: "Utilization", metadata: { value: "80" } }]]);
    const sizing = one<VerticalPodAutoscaler>(objects, "VerticalPodAutoscaler", "alasio-seaweedfs");
    assert.ok(sizing.spec.resourcePolicy.containerPolicies.every((policy) => JSON.stringify(policy).includes('"controlledResources":["memory"]')));
    assert.deepEqual(all(install(), "ScaledObject").filter(({ metadata }) => metadata?.name === "alasio-seaweedfs"), []);
  });

  test("makes the buckets and their lifecycle rules in each gateway as it starts, ready only once they are", () => {
    const gateway = one<V1Deployment>(install(), "Deployment", "alasio-seaweedfs");
    assert.deepEqual(gateway.spec?.template.spec?.containers.map(({ name }) => name), ["s3", "buckets", "lifecycle"]);
    const buckets = gateway.spec?.template.spec?.containers[1]?.command?.[2] ?? "";
    assert.match(buckets, /weed shell -master=alasio-seaweedfs-master-0\.alasio-seaweedfs-master\.alasio\.svc\.cluster\.local:9333 -filer=alasio-seaweedfs-filer:8888/u);
    assert.match(buckets, /for bucket in neon lake backups workspaces snapshots; do/u);
  });

  test("makes no bucket anew, empty, whose objects the volumes hold while the filers know no such bucket, and its gateway is not ready", () => {
    const gateway = one<V1Deployment>(install(), "Deployment", "alasio-seaweedfs");
    const buckets = gateway.spec?.template.spec?.containers.find(({ name }) => name === "buckets") ?? {};
    // The shell as the filers and the volumes answer it: the buckets the filers know, and the collection a volume holds.
    const run = (known: string, held: string) => {
      const weed = [
        "read -r command",
        'case "$command" in',
        `  "s3.bucket.list") cat "$ROOT/made" 2>/dev/null; echo "${known}" ;;`,
        `  "volume.list") echo "  volume Id:7, Size:1, ReplicaPlacement:000, Collection:${held}, Version:3" ;;`,
        '  "s3.bucket.create -name "*) echo "${command#s3.bucket.create -name }" >> "$ROOT/made" ;;',
        "  *versioning*) echo Enabled ;;",
        "esac",
      ].join("\n");
      const { root, run: ran, remove } = runScript(buckets, {}, { weed, wget: "true", touch: "true", sleep: "true" });
      try {
        let said = "";
        let status = 0;
        try {
          ran();
        } catch (error) {
          ({ status, stderr: said } = error as { status: number; stderr: string });
        }
        const made = existsSync(join(root, "made")) ? readFileSync(join(root, "made"), "utf8").trim().split("\n") : [];
        return { status, made, said };
      } finally {
        remove();
      }
    };
    // A fresh store: every bucket made.
    assert.deepEqual(run("none", "elsewhere"), { status: 0, made: ["neon", "lake", "backups", "workspaces", "snapshots"], said: "" });
    // A store that lost its filers' metadata, its volumes holding the lake's objects: no lake made anew, and the gateway never ready.
    const lost = run("neon", "lake");
    assert.equal(lost.status, 1);
    assert.deepEqual(lost.made, []);
    assert.match(lost.said, /the store's volumes hold objects of the bucket lake, which its filers do not know: their store has lost what it kept; alasio restore-filer-store puts it back from its backups, rather than make lake anew, empty/u);
    // A bucket is known by its name exactly: another named after it is not it.
    assert.equal(run("lake-archive\tsize:0\tlogical:0\tchunk:0", "lake").status, 1);
  });

  test("admits the filers alone, the collector's scraper and the backups to the filer store, which is no part of the data stack", () => {
    const objects = install();
    const store = one<V1StatefulSet>(objects, "StatefulSet", "alasio-seaweedfs-filer-store");
    assert.equal(store.spec?.template.metadata?.labels?.["alasio.dev/stack"], undefined);
    const policy = one<KubernetesObject & { spec: { ingress: unknown } }>(objects, "NetworkPolicy", "alasio-seaweedfs-filer-store");
    assert.deepEqual(policy.spec.ingress, [{
      from: [
        { podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "seaweedfs-filer" } } },
        { podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "collector-scraper" } } },
        { podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "seaweedfs-filer-store-backup" } } },
      ],
      ports: [{ protocol: "TCP", port: 6379 }],
    }]);
  });

  test("admits to each part behind the gateways what talks to it alone, and the collector's scraper, and leaves them out of the data stack's policy", () => {
    const objects = install();
    type Rule = { from: { namespaceSelector?: unknown; podSelector: { matchLabels: Record<string, string> } }[]; ports: { port: number }[] };
    const admitted = (name: string) =>
      one<KubernetesObject & { spec: { ingress: Rule[] } }>(objects, "NetworkPolicy", name).spec.ingress.map(({ from, ports }) => [
        from.map(({ namespaceSelector, podSelector }) => `${podSelector.matchLabels["app.kubernetes.io/component"]}${namespaceSelector ? ` in ${JSON.stringify(namespaceSelector)}` : ""}`),
        ports.map(({ port }) => port),
      ]);
    // Each environment's lake's reconciliation, on their HTTP port alone.
    const main = `lake-reconcile in ${JSON.stringify({ matchLabels: { "kubernetes.io/metadata.name": "alasio" } })}`;
    const branches = `lake-reconcile in ${JSON.stringify({ matchExpressions: [{ key: "alasio.dev/branch", operator: "Exists" }] })}`;
    assert.deepEqual(admitted("alasio-seaweedfs-master"), [
      [["seaweedfs-master", "seaweedfs-volume", "seaweedfs-filer", "seaweedfs", "seaweedfs-lifecycle"], [9333, 19333]],
      [[main, branches], [9333]],
      [["collector-scraper"], [9327]],
    ]);
    assert.deepEqual(admitted("alasio-seaweedfs-volume"), [
      [["seaweedfs-volume", "seaweedfs-master", "seaweedfs-filer", "seaweedfs", "seaweedfs-lifecycle"], [8080, 18080]],
      [[main, branches], [8080]],
      [["collector-scraper"], [9327]],
    ]);
    assert.deepEqual(admitted("alasio-seaweedfs-filer"), [[["seaweedfs-filer", "seaweedfs", "seaweedfs-lifecycle"], [8888, 18888]], [[main, branches], [8888]], [["collector-scraper"], [9327]]]);
    // The gateways take the data stack's pods on their S3 port, the lifecycle pass alone on their gRPC port, and the collector's scraper alone on their metrics port.
    const gateway = one<KubernetesObject & { spec: { ingress: { from: { podSelector: { matchLabels: Record<string, string> } }[]; ports: { port: number }[] }[] } }>(objects, "NetworkPolicy", "alasio-seaweedfs").spec.ingress;
    assert.deepEqual(gateway.map(({ from, ports }) => [from.map(({ podSelector }) => podSelector.matchLabels), ports.map(({ port }) => port)]), [
      [[{ "app.kubernetes.io/instance": "alasio", "alasio.dev/stack": "neon" }], [8333]],
      [[{ "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "seaweedfs-lifecycle" }], [18333]],
      [[{ "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "collector-scraper" }], [9327]],
    ]);
    // The filer store takes its filers, the collector's scraper and its backups; the Valkey that serves a backup as it is restored, the Job that restores it alone.
    assert.deepEqual(admitted("alasio-seaweedfs-filer-store"), [[["seaweedfs-filer", "collector-scraper", "seaweedfs-filer-store-backup"], [6379]]]);
    assert.deepEqual(admitted("alasio-seaweedfs-filer-store-restore-source"), [[["seaweedfs-filer-store-restore"], [6379]]]);
    // The data stack's pods reach none of the parts, nor the log, nor the computes, but as their own policies say.
    const stack = one<KubernetesObject & { spec: { podSelector: object } }>(objects, "NetworkPolicy", "alasio-neon").spec.podSelector;
    assert.deepEqual(stack, {
      matchLabels: { "app.kubernetes.io/instance": "alasio", "alasio.dev/stack": "neon" },
      matchExpressions: [{ key: "app.kubernetes.io/component", operator: "NotIn", values: ["seaweedfs", "seaweedfs-master", "seaweedfs-volume", "seaweedfs-filer", "log", "neon-compute", "neon-compute-ro"] }],
    });
    // The lifecycle pass and the backups are admitted by the components their pods carry.
    const pass = one<V1CronJob>(objects, "CronJob", "alasio-seaweedfs-lifecycle").spec?.jobTemplate.spec?.template.metadata?.labels;
    assert.equal(pass?.["app.kubernetes.io/component"], "seaweedfs-lifecycle");
    const backup = one<V1CronJob>(objects, "CronJob", "alasio-seaweedfs-filer-store-backup").spec?.jobTemplate.spec?.template.metadata?.labels;
    assert.equal(backup?.["app.kubernetes.io/component"], "seaweedfs-filer-store-backup");
    // Of the data stack neither they nor a restore's pods are, which reach nothing of it.
    assert.equal(backup?.["alasio.dev/stack"], undefined);
    for (const job of filerStoreRestore(configOf(), "filer-store-20261013T092300Z.rdb").filter(({ kind }) => kind === "Job") as V1Job[]) {
      assert.equal(job.spec?.template.metadata?.labels?.["alasio.dev/stack"], undefined, job.metadata?.name);
    }
  });

  test("logs in to the filer store as users of its own, each with its own Secret, its default user off: the filers to their keys alone, the collector to INFO, the backups to a snapshot", () => {
    const objects = install();
    const store = container(one<V1StatefulSet>(objects, "StatefulSet", "alasio-seaweedfs-filer-store"));
    // Started by the script that waits for any server before it on its volume to end.
    assert.deepEqual(store.command?.slice(0, 2), ["/bin/sh", "-c"]);
    assert.deepEqual(store.command?.slice(3), [
      "valkey-server",
      "/etc/valkey/valkey.conf",
      "--user",
      "default",
      "off",
      "--user",
      "filer",
      "on",
      ">$(VALKEY_FILER_PASSWORD)",
      "~seaweedfs:*",
      "+@all",
      "-@dangerous",
      "--user",
      "collector",
      "on",
      ">$(VALKEY_COLLECTOR_PASSWORD)",
      "+info",
      "+ping",
      "--user",
      "backup",
      "on",
      ">$(VALKEY_BACKUP_PASSWORD)",
      "+replconf",
      "+sync",
      "+psync",
      "+ping",
    ]);
    const secret = (name: string) => ({ secretKeyRef: { name, key: "password" } });
    assert.deepEqual(store.env, [
      { name: "VALKEY_FILER_PASSWORD", valueFrom: secret("alasio-seaweedfs-filer-store-filer") },
      { name: "VALKEY_COLLECTOR_PASSWORD", valueFrom: secret("alasio-seaweedfs-filer-store-collector") },
      { name: "VALKEY_BACKUP_PASSWORD", valueFrom: secret("alasio-seaweedfs-filer-store-backup") },
      { name: "VALKEYCLI_AUTH", valueFrom: secret("alasio-seaweedfs-filer-store-collector") },
    ]);
    // Its readiness probe logs in as the collector; workspace storage's Valkey, whose clients log in as its default user, the node agents' beside it, as its default user.
    assert.match(store.readinessProbe?.exec?.command?.[2] ?? "", /^persistence=\$\(valkey-cli --user collector info persistence\)/u);
    const valkey = container(one<V1StatefulSet>(objects, "StatefulSet", "alasio-valkey"));
    assert.deepEqual(valkey.command?.slice(2, 7), [store.command?.[2], "valkey-server", "/etc/valkey/valkey.conf", "--requirepass", "$(VALKEY_PASSWORD)"]);
    assert.match(valkey.readinessProbe?.exec?.command?.[2] ?? "", /^persistence=\$\(valkey-cli info persistence\)/u);
    // A server waiting for the one before it on its volume, which ends within its pod's grace, is not ended by its liveness probe as it waits.
    for (const server of [store, valkey]) {
      const grace = one<V1StatefulSet>(objects, "StatefulSet", server === store ? "alasio-seaweedfs-filer-store" : "alasio-valkey").spec?.template.spec?.terminationGracePeriodSeconds ?? 30;
      const { periodSeconds = 10, failureThreshold = 3 } = server.startupProbe ?? {};
      assert.ok(server.startupProbe?.tcpSocket && periodSeconds * failureThreshold >= 2 * grace, `${server.name}'s start-up allows ${periodSeconds * failureThreshold} s`);
    }
  });

  test("backs the filer store up hourly, a snapshot of it as a replica takes one, onto a volume of its own on another node where one takes it, renamed once whole", () => {
    const objects = install({ objectStore: { bundled: { filerStore: { backups: { keep: 2 } } } } });
    const claim = one<V1PersistentVolumeClaim>(objects, "PersistentVolumeClaim", "alasio-seaweedfs-filer-store-backups");
    assert.equal(claim.spec?.resources?.requests?.["storage"], "10Gi");
    const backup = one<V1CronJob>(objects, "CronJob", "alasio-seaweedfs-filer-store-backup").spec;
    assert.deepEqual([backup?.schedule, backup?.concurrencyPolicy], ["23 * * * *", "Forbid"]);
    const pod = backup?.jobTemplate.spec?.template.spec;
    assert.deepEqual(pod?.volumes, [{ name: "backups", persistentVolumeClaim: { claimName: "alasio-seaweedfs-filer-store-backups" } }]);
    // Its volume is made where its first pod runs: away from the filer store, where a node can take it.
    assert.deepEqual(pod?.affinity?.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution, [{
      weight: 100,
      podAffinityTerm: { labelSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "seaweedfs-filer-store" } }, topologyKey: "kubernetes.io/hostname" },
    }]);
    // The one container: no copy in the store it backs up.
    assert.deepEqual(pod?.containers.map(({ name }) => name), ["save"]);
    const save: V1Container | undefined = pod?.containers[0];
    assert.deepEqual(save?.env, [{ name: "KEEP", value: "2" }, { name: "VALKEYCLI_AUTH", valueFrom: { secretKeyRef: { name: "alasio-seaweedfs-filer-store-backup", key: "password" } } }]);
    // Run with a valkey-cli that writes its snapshot where it is told, then succeeds or fails, in a directory of its own for /backups.
    const take = (fails: boolean) => {
      const valkey = ['case "$*" in', "  *ping*) echo PONG ;;", `  *--rdb*) for last; do :; done; echo snapshot > "$last"; exit ${fails ? 1 : 0} ;;`, "esac"].join("\n");
      const text = (save?.command?.[2] ?? "").replaceAll("/backups", () => "$ROOT/backups");
      return runScript({ command: ["/bin/sh", "-c", text] }, { KEEP: "2" }, { "valkey-cli": valkey });
    };
    const { root, run, remove } = take(false);
    try {
      mkdirSync(join(root, "backups"));
      for (const stamp of ["20261013T062300Z", "20261013T072300Z"]) writeFileSync(join(root, "backups", `filer-store-${stamp}.rdb`), "older");
      writeFileSync(join(root, "backups", ".filer-store-20261013T072300Z.rdb"), "half");
      run();
      const kept = readdirSync(join(root, "backups")).sort();
      // The newest two, the one just taken among them, and nothing a backup that failed left behind.
      assert.equal(kept.length, 2);
      assert.equal(kept[0], "filer-store-20261013T072300Z.rdb");
      assert.match(kept[1] ?? "", /^filer-store-\d{8}T\d{6}Z\.rdb$/u);
      assert.equal(readFileSync(join(root, "backups", kept[1] ?? ""), "utf8").trim(), "snapshot");
    } finally {
      remove();
    }
    const failing = take(true);
    try {
      mkdirSync(join(failing.root, "backups"));
      assert.throws(() => failing.run());
      // A snapshot that failed is never a backup.
      assert.deepEqual(readdirSync(join(failing.root, "backups")).filter((file) => file.startsWith("filer-store-")), []);
    } finally {
      failing.remove();
    }
    // None where they are off.
    const off = install({ objectStore: { bundled: { filerStore: { backups: { enabled: false } } } } });
    assert.deepEqual([...all(off, "CronJob"), ...all(off, "PersistentVolumeClaim")].filter(({ metadata }) => metadata?.name?.includes("filer-store-backup")), []);
  });

  test("restores the filer store from a backup a Valkey on the backups' volume serves, onto the filer store's volume, as the filer store then loads it", () => {
    const [service, source, restore] = filerStoreRestore(configOf(), "filer-store-20261013T092300Z.rdb") as [V1Service, V1Job, V1Job];
    assert.deepEqual(service.spec?.selector, { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "seaweedfs-filer-store-restore-source" });
    const serving = source.spec?.template.spec;
    assert.deepEqual(serving?.volumes, [{ name: "backups", persistentVolumeClaim: { claimName: "alasio-seaweedfs-filer-store-backups", readOnly: true } }]);
    assert.deepEqual(serving?.containers[0]?.command, [
      "valkey-server", "--port", "6379", "--dir", "/backups", "--dbfilename", "filer-store-20261013T092300Z.rdb", "--appendonly", "no", "--save", "", "--requirepass", "$(VALKEYCLI_AUTH)",
    ]);
    const loading = restore.spec?.template.spec;
    assert.deepEqual(loading?.volumes, [{ name: "data", persistentVolumeClaim: { claimName: "data-alasio-seaweedfs-filer-store-0" } }]);
    // As the filer store's Valkey runs, so the files it writes are its own.
    assert.deepEqual(loading?.securityContext, one<V1StatefulSet>(install(), "StatefulSet", "alasio-seaweedfs-filer-store").spec?.template.spec?.securityContext);
    const script = loading?.containers[0]?.command?.[2] ?? "";
    for (
      const step of [
        "backup --rdb /data/restore.rdb",
        '[ "$loaded" = "$expected" ] || { echo "loaded $loaded keys, and the backup has $expected" >&2; exit 1; }',
        "valkey-cli -p 6380 config set appendonly yes >/dev/null",
        "grep -q '^aof_last_bgrewrite_status:ok'",
        'echo "restored $loaded keys"',
      ]
    ) assert.ok(script.includes(step), step);
    // What the volume held goes only once the backup is fetched and holds as many keys.
    assert.ok(script.indexOf("rm -rf /data/appendonlydir /data/dump.rdb") > script.indexOf('[ "$loaded" = "$expected" ]'));
    assert.ok(!script.slice(0, script.indexOf('[ "$loaded" = "$expected" ]')).includes("appendonlydir"));
  });
});

describe("Grafana", () => {
  const GRAFANA = { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "grafana" };
  type Policy = KubernetesObject & { spec: { podSelector: unknown; policyTypes: string[]; ingress?: unknown; egress?: unknown } };

  test("runs beside the lake, unless it is off", () => {
    assert.ok(one(install(), "Deployment", "alasio-grafana"));
    for (const off of [{ grafana: { enabled: false } }, { lake: { enabled: false } }]) {
      assert.deepEqual(install(off).filter(({ metadata }) => metadata?.name?.startsWith("alasio-grafana")), [], JSON.stringify(off));
    }
  });

  test("runs one Grafana, from its image, on its database on main's compute, alerting through the bot, keeping nothing", () => {
    const grafana = one<V1Deployment>(install({ images: { grafana: { tag: "1.2.3" } } }), "Deployment", "alasio-grafana");
    // Of one replica, stopped before its successor starts, so no two send an alert.
    assert.deepEqual([grafana.spec?.replicas, grafana.spec?.strategy], [1, { type: "Recreate" }]);
    const pod = grafana.spec?.template.spec;
    assert.deepEqual([pod?.automountServiceAccountToken, pod?.securityContext?.runAsUser], [false, 472]);
    assert.equal(container(grafana).image, "ghcr.io/eaucoin/alasio-grafana:1.2.3");
    assert.deepEqual(container(grafana).env, [
      // Through Neon's proxy, which passes a session through whole, as its migrations' advisory lock needs, naming main's compute;
      // its password the Secret's, which envFrom gives it before.
      {
        name: "GF_DATABASE_URL",
        value: "postgres://grafana:$(GF_DATABASE_PASSWORD)@alasio-neon-proxy.alasio.svc:5432/grafana?options=endpoint%3Dalasio&sslmode=disable&connect_timeout=60",
      },
      { name: "LAKE_QUERY_URL", value: "http://alasio-lake:8090" },
      { name: "TELEGRAM_BOT_TOKEN", valueFrom: { secretKeyRef: { name: "telegram", key: "token" } } },
      { name: "TELEGRAM_ALLOWED_USER_IDS", valueFrom: { secretKeyRef: { name: "telegram", key: "allowedUserIds" } } },
    ]);
    assert.deepEqual(container(grafana).envFrom, [{ secretRef: { name: "alasio-grafana" } }]);
    assert.equal(container(grafana).securityContext?.readOnlyRootFilesystem, true);
    assert.ok(pod?.volumes?.every(({ emptyDir }) => emptyDir), "no volume but emptyDirs");
  });

  test("runs Grafana highly available when asked: its replicas gossip, admitted to each other alone, one evaluating, spread, one evicted at a time", () => {
    const objects = install({ grafana: { scaling: { min: 2, max: 2 } } });
    const grafana = one<V1Deployment>(objects, "Deployment", "alasio-grafana");
    assert.equal(grafana.spec?.replicas, 2);
    // Rolled out a pod at a time, the new one ready before the old stops.
    assert.deepEqual(grafana.spec?.strategy, { type: "RollingUpdate", rollingUpdate: { maxSurge: 1, maxUnavailable: 0 } });
    const env = Object.fromEntries((container(grafana).env ?? []).map(({ name, value }) => [name, value]));
    assert.deepEqual(
      [env["GF_UNIFIED_ALERTING_HA_PEERS"], env["GF_UNIFIED_ALERTING_HA_ADVERTISE_ADDRESS"], env["GF_UNIFIED_ALERTING_HA_SINGLE_NODE_EVALUATION"]],
      ["alasio-grafana-peers.alasio.svc.cluster.local:9094", "$(POD_IP):9094", "true"],
    );
    const peers = one<V1Service>(objects, "Service", "alasio-grafana-peers");
    assert.deepEqual([peers.spec?.clusterIP, peers.spec?.publishNotReadyAddresses, peers.spec?.ports?.map(({ protocol }) => protocol)], ["None", true, ["TCP", "UDP"]]);
    assert.deepEqual(grafana.spec?.template.spec?.topologySpreadConstraints?.map(({ topologyKey }) => topologyKey), ["kubernetes.io/hostname", "topology.kubernetes.io/zone"]);
    assert.deepEqual(one<KubernetesObject & { spec: object }>(objects, "PodDisruptionBudget", "alasio-grafana").spec, {
      maxUnavailable: 1,
      selector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "grafana" } },
    });
    const policies = all<NetworkPolicyObject>(objects, "NetworkPolicy");
    const pod = { namespace: "alasio", namespaceLabels: { "kubernetes.io/metadata.name": "alasio" }, labels: grafana.spec?.template.metadata?.labels ?? {} };
    const alasio = { ...pod, labels: one<V1Deployment>(objects, "Deployment", "alasio").spec?.template.metadata?.labels ?? {} };
    assert.deepEqual([admits(policies, pod, pod, 9094), admits(policies, alasio, pod, 9094), admits(policies, pod, pod, 3000)], [true, false, false]);
    // Of one replica, none of that.
    const single = install();
    assert.deepEqual(all(single, "PodDisruptionBudget").filter(({ metadata }) => metadata?.namespace === "alasio" && metadata.name !== "alasio-busy").map(({ metadata }) => metadata?.name), ["alasio-neon-safekeeper", "alasio-lake-intake"]);
    assert.equal(all(single, "Service").find(({ metadata }) => metadata?.name === "alasio-grafana-peers"), undefined);
    assert.equal(container(one<V1Deployment>(single, "Deployment", "alasio-grafana")).env?.find(({ name }) => name.startsWith("GF_UNIFIED_ALERTING_HA")), undefined);
  });

  test("scales Grafana on its CPU between min and max, by KEDA, which owns its replicas", () => {
    const objects = install({ grafana: { scaling: { min: 1, max: 3 } } });
    const grafana = one<V1Deployment>(objects, "Deployment", "alasio-grafana");
    assert.equal(grafana.spec?.replicas, undefined);
    assert.deepEqual(one<ScaledObject>(objects, "ScaledObject", "alasio-grafana").spec, {
      scaleTargetRef: { apiVersion: "apps/v1", kind: "Deployment", name: "alasio-grafana" },
      minReplicaCount: 1,
      maxReplicaCount: 3,
      triggers: [{ type: "cpu", metricType: "Utilization", metadata: { value: "80" } }],
      advanced: { horizontalPodAutoscalerConfig: { behavior: { scaleUp: { stabilizationWindowSeconds: 180 } } } },
    });
    // VPA sizes its memory alone, as its CPU is what it scales on.
    assert.deepEqual(one<VerticalPodAutoscaler>(objects, "VerticalPodAutoscaler", "alasio-grafana").spec.resourcePolicy.containerPolicies.map((policy) => (policy as { controlledResources?: string[] }).controlledResources), [["memory"]]);
    assert.deepEqual(all(install(), "ScaledObject").filter(({ metadata }) => metadata?.name === "alasio-grafana"), []);
  });

  test("starts Grafana once it can log in to its database, which alasio lets it once the database is made", () => {
    const wait = one<V1Deployment>(install(), "Deployment", "alasio-grafana").spec?.template.spec?.initContainers?.[0];
    assert.ok(wait);
    const env = Object.fromEntries((wait.env ?? []).flatMap(({ name, value }) => (value === undefined ? [] : [[name, value]])));
    assert.deepEqual(
      [env["PGHOST"], env["PGPORT"], env["PGOPTIONS"], env["PGDATABASE"], env["PGUSER"], env["PGCONNECT_TIMEOUT"]],
      ["alasio-neon-proxy.alasio.svc", "5432", "endpoint=alasio", "grafana", "grafana", "60"],
    );
    assert.deepEqual(wait.env?.find(({ name }) => name === "PGPASSWORD")?.valueFrom, { secretKeyRef: { name: "alasio-grafana", key: "GF_DATABASE_PASSWORD" } });
    // A login refused twice, then let in.
    const psql = 'n=$(cat "$ROOT/tries" 2>/dev/null || echo 0); echo $((n + 1)) >"$ROOT/tries"; [ "$n" -ge 2 ] && echo "$PGUSER@$PGDATABASE" >"$ROOT/in"';
    const { root, run, remove } = runScript(wait, env, { psql, sleep: "true" });
    try {
      run();
      assert.deepEqual([readFileSync(join(root, "tries"), "utf8"), readFileSync(join(root, "in"), "utf8")], ["3\n", "grafana@grafana\n"]);
    } finally {
      remove();
    }
  });

  test("admits no pod to Grafana, and lets it reach DNS, its database, the lake's readers and the internet's HTTPS alone", () => {
    const objects = install();
    const own = one<Policy>(objects, "NetworkPolicy", "alasio-grafana").spec;
    assert.deepEqual([own.podSelector, own.policyTypes, own.ingress], [{ matchLabels: GRAFANA }, ["Ingress", "Egress"], undefined]);
    const egress = own.egress as { to: Record<string, unknown>[]; ports: { protocol: string; port: number }[] }[];
    assert.deepEqual(egress.map(({ ports }) => ports.map(({ protocol, port }) => `${protocol}/${port}`).join(",")), ["UDP/53,TCP/53", "TCP/5432", "TCP/8090", "TCP/443"]);
    assert.deepEqual(egress[3]?.to, [{ ipBlock: { cidr: "0.0.0.0/0", except: ["0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16", "224.0.0.0/4", "240.0.0.0/4"] } }]);
    // Neither Neon's proxy nor the lake's readers admit it as a pod of the stack: each does so on its port alone.
    assert.equal(one<V1Deployment>(objects, "Deployment", "alasio-grafana").spec?.template.metadata?.labels?.["alasio.dev/stack"], undefined);
    for (const [component, port] of [["neon-proxy", 5432], ["lake-query", 8090]] as const) {
      assert.deepEqual(one<Policy>(objects, "NetworkPolicy", `alasio-${component}-from-grafana`).spec.ingress, [{ from: [{ podSelector: { matchLabels: GRAFANA } }], ports: [{ protocol: "TCP", port }] }]);
    }
  });

  test("has alasio make Grafana's role while Grafana runs", () => {
    const alasio = (file: Record<string, unknown>) => one<V1Deployment>(install(file), "Deployment", "alasio");
    assert.deepEqual(verifierFiles(alasio({}))?.at(-1), ["ALASIO_GRAFANA_VERIFIER_FILE", "/run/alasio/database/grafana-verifier"]);
    const database = alasio({}).spec?.template.spec?.volumes?.find(({ name }) => name === "database");
    assert.deepEqual(database?.secret?.items?.map(({ key }) => key), ["url", "lake-verifier", "harness-verifier", "lake-reader-verifier", "grafana-verifier"]);
    assert.deepEqual(verifierFiles(alasio({ grafana: { enabled: false } }))?.map(([name]) => name), ["ALASIO_LAKE_VERIFIER_FILE", "ALASIO_HARNESS_VERIFIER_FILE", "ALASIO_LAKE_READER_VERIFIER_FILE"]);
  });
});

describe("telemetry", () => {
  /** The configuration of the collector's gateway, or of its scraper, as its ConfigMap holds it. */
  const collectorConfig = (objects: readonly KubernetesObject[], name = "alasio-collector") => JSON.parse(one<V1ConfigMap>(objects, "ConfigMap", name).data?.["config.yaml"] ?? "");
  const scraperConfig = (objects: readonly KubernetesObject[]) => collectorConfig(objects, "alasio-collector-scraper");
  const COLLECTOR = "http://alasio-collector.alasio.svc:4318";
  const variable = (workload: V1Deployment, name: string) => container(workload).env?.find((env) => env.name === name);

  test("builds into its collector image every component and provider its gateway's and its scraper's configurations name", () => {
    const builder = readFileSync(new URL("../../neon/collector/builder.yaml", import.meta.url), "utf8");
    // Each module's component, its last segment, of the kind its path names, as receiver/receivercreator's receivercreatorreceiver.
    const built = new Set([...builder.matchAll(/^ {2}- gomod: (\S+) v\S+$/gmu)].map(([, path = ""]) => {
      const segments = path.split("/");
      const name = segments.at(-1) ?? "";
      const kind = segments.find((segment) => ["receiver", "processor", "exporter", "extension", "provider"].includes(segment)) ?? "";
      return name.endsWith(kind) ? name : `${name}${kind}`;
    }));
    const named = new Set<string>();
    for (const external of [{ otlpEndpoint: "https://otlp.example.com", otlpProtocol: "grpc", headersSecret: "headers" }, { otlpEndpoint: "https://otlp.example.com" }]) {
      const objects = install({ telemetry: external });
      for (const config of [collectorConfig(objects), scraperConfig(objects)]) {
        for (const kind of ["receivers", "processors", "exporters", "extensions"] as const) {
          for (const id of Object.keys(config[kind] ?? {})) named.add(`${id.split("/")[0]?.replaceAll("_", "")}${kind.slice(0, -1)}`);
          // The receivers a receiver creator makes, as it finds what they read.
          for (const created of Object.values<{ receivers?: object }>(config[kind] ?? {})) {
            if (kind === "receivers") for (const id of Object.keys(created.receivers ?? {})) named.add(`${id.split("/")[0]}receiver`);
          }
        }
        // The providers of the variables it reads, as ${env:...} and ${file:...}.
        for (const [, scheme] of JSON.stringify(config).matchAll(/\$\{(\w+):/gu)) named.add(`${scheme}provider`);
      }
    }
    assert.ok(named.has("kafkaexporter") && named.has("redisreceiver") && named.has("kubeletstatsreceiver") && named.has("fileprovider"), [...named].join(", "));
    assert.deepEqual([...named].filter((component) => !built.has(component)), []);
  });

  test("runs the collector with the lake, or with an endpoint to send to, and not with neither", () => {
    assert.ok(one(install(), "Deployment", "alasio-collector"));
    assert.ok(one(install({ lake: { enabled: false }, telemetry: { otlpEndpoint: "https://otlp.example.com" } }), "Deployment", "alasio-collector"));
    assert.deepEqual(install({ lake: { enabled: false } }).filter(({ metadata }) => metadata?.name === "alasio-collector"), []);
  });

  test("takes every signal over OTLP/HTTP at its gateway, and writes each export to the log's topic of its signal, answering only once the log has it", () => {
    const objects = install();
    const config = collectorConfig(objects);
    assert.deepEqual(config.receivers.otlp, { protocols: { http: { endpoint: "0.0.0.0:4318" } } });
    assert.deepEqual(config.exporters, {
      kafka: {
        brokers: ["alasio-log.alasio.svc:9092"],
        protocol_version: "4.2.0",
        auth: { sasl: { username: "gateway", password: "${env:LOG_PASSWORD}", mechanism: "SCRAM-SHA-512" } },
        traces: { topic: "otel.spans", encoding: "otlp_proto" },
        logs: { topic: "otel.logs", encoding: "otlp_proto" },
        metrics: { topic: "otel.metrics", encoding: "otlp_proto" },
        // Acked by the log, as large a record as it takes, compressed as the log keeps it, and neither queued nor retried here.
        producer: { required_acks: -1, max_message_bytes: 8_388_608, compression: "zstd" },
        timeout: "10s",
        sending_queue: { enabled: false },
        retry_on_failure: { enabled: false },
      },
    });
    // Nothing between the receiver and the log: a batch processor would answer before the log had it.
    for (const pipeline of ["traces", "logs", "metrics"]) assert.deepEqual(config.service.pipelines[pipeline], { receivers: ["otlp"], exporters: ["kafka"] }, pipeline);
    assert.equal(config.processors, undefined);
    const gateway = one<V1Deployment>(objects, "Deployment", "alasio-collector");
    assert.deepEqual(variable(gateway, "LOG_PASSWORD"), { name: "LOG_PASSWORD", valueFrom: { secretKeyRef: { name: "alasio-log-gateway", key: "password" } } });
    assert.deepEqual(one<V1Service>(objects, "Service", "alasio-collector").spec?.ports, [{ name: "otlp-http", port: 4318, targetPort: "otlp-http" }]);
    assert.deepEqual(one<V1Service>(objects, "Service", "alasio-lake").spec?.ports?.map(({ name }) => name), ["query"]);
    // Its scraper sends what it scrapes to it, a minute's at a time, as the stack's.
    const scraper = scraperConfig(objects);
    assert.deepEqual(scraper.exporters, { "otlphttp/gateway": { endpoint: "http://alasio-collector:4318" } });
    assert.deepEqual(scraper.service.pipelines, {
      "metrics/nodes": { receivers: ["receiver_creator/nodes"], processors: ["batch"], exporters: ["otlphttp/gateway"] },
      metrics: { receivers: ["prometheus", "redis", "redis/filer-store"], processors: ["resource/stack", "batch"], exporters: ["otlphttp/gateway"] },
    });
    assert.deepEqual(scraper.processors.batch, { send_batch_size: 10_000, send_batch_max_size: 10_000, timeout: "60s" });
    assert.equal(scraper.receivers.otlp, undefined);
    assert.equal(all(objects, "Service").find(({ metadata }) => metadata?.name === "alasio-collector-scraper"), undefined);
  });

  test("runs the gateway as a ServiceAccount of its own, with no token, which its pods name, so none names one the installation no longer has", () => {
    const objects = install();
    const spec = one<V1Deployment>(objects, "Deployment", "alasio-collector").spec?.template.spec;
    assert.deepEqual([spec?.serviceAccountName, spec?.automountServiceAccountToken], ["alasio-collector", false]);
    assert.equal(one<V1ServiceAccount>(objects, "ServiceAccount", "alasio-collector").automountServiceAccountToken, false);
    // Its rights none: no binding names it.
    assert.deepEqual(all<V1RoleBinding>(objects, "RoleBinding").filter(({ subjects }) => subjects?.some(({ name }) => name === "alasio-collector")), []);
  });

  test("runs one scraper, and scales the gateway on its CPU between min and max, by KEDA, one evicted at a time", () => {
    const objects = install({ telemetry: { collector: { gateway: { scaling: { min: 2, max: 4 } } } } });
    const gateway = one<V1Deployment>(objects, "Deployment", "alasio-collector");
    assert.equal(gateway.spec?.replicas, undefined);
    const scaled = one<ScaledObject>(objects, "ScaledObject", "alasio-collector");
    assert.deepEqual([scaled.spec.minReplicaCount, scaled.spec.maxReplicaCount, scaled.spec.triggers], [2, 4, [{ type: "cpu", metricType: "Utilization", metadata: { value: "80" } }]]);
    assert.equal(scaled.spec.advanced?.horizontalPodAutoscalerConfig.behavior.scaleUp.stabilizationWindowSeconds, 60);
    assert.ok(one(objects, "PodDisruptionBudget", "alasio-collector"));
    assert.equal(one<V1Deployment>(objects, "Deployment", "alasio-collector-scraper").spec?.replicas, 1);
    const single = install();
    assert.equal(one<V1Deployment>(single, "Deployment", "alasio-collector").spec?.replicas, 1);
    assert.deepEqual(all(single, "ScaledObject").filter(({ metadata }) => metadata?.name === "alasio-collector"), []);
  });

  test("reads each node's filesystem from its kubelet, as a node the cluster lists, with its scraper's ServiceAccount, which may read nodes and their stats alone", () => {
    const objects = install();
    const config = scraperConfig(objects);
    assert.deepEqual(config.extensions.k8s_observer, { auth_type: "serviceAccount", observe_nodes: true, observe_pods: false });
    assert.deepEqual(config.service.extensions, ["health_check", "k8s_observer"]);
    assert.deepEqual(config.receivers["receiver_creator/nodes"], {
      watch_observers: ["k8s_observer"],
      receivers: {
        kubeletstats: {
          rule: 'type == "k8s.node"',
          config: { auth_type: "serviceAccount", endpoint: "`endpoint`:`kubelet_endpoint_port`", insecure_skip_verify: true, collection_interval: "60s", metric_groups: ["node"] },
        },
      },
    });
    assert.equal(one<V1Deployment>(objects, "Deployment", "alasio-collector-scraper").spec?.template.spec?.serviceAccountName, "alasio-collector-scraper");
    assert.deepEqual(one<V1ClusterRole>(objects, "ClusterRole", "alasio-collector-scraper").rules, [
      { apiGroups: [""], resources: ["nodes"], verbs: ["get", "list", "watch"] },
      { apiGroups: [""], resources: ["nodes/stats"], verbs: ["get"] },
    ]);
    assert.deepEqual(one<V1ClusterRoleBinding>(objects, "ClusterRoleBinding", "alasio-collector-scraper").subjects, [{ kind: "ServiceAccount", name: "alasio-collector-scraper", namespace: "alasio" }]);
    // The gateway reads none.
    assert.equal(collectorConfig(objects).receivers["receiver_creator/nodes"], undefined);
  });

  test("sends it all to telemetry.otlpEndpoint too when one is set, in its protocol, the headers going there alone", () => {
    const telemetry = { otlpEndpoint: "https://otlp.example.com", headersSecret: "otlp-headers" };
    const objects = install({ telemetry });
    const config = collectorConfig(objects);
    assert.deepEqual(config.exporters["otlphttp/external"], { endpoint: "https://otlp.example.com", encoding: "proto", headers: "${file:/etc/otelcol/headers/headers.yaml}" });
    assert.deepEqual(config.service.pipelines.traces.exporters, ["kafka", "otlphttp/external"]);
    const collector = one<V1Deployment>(objects, "Deployment", "alasio-collector");
    assert.deepEqual(collector.spec?.template.spec?.volumes?.find(({ name }) => name === "headers"), {
      name: "headers",
      secret: { secretName: "otlp-headers", items: [{ key: "headers.yaml", path: "headers.yaml" }] },
    });
    for (const workload of all<V1Deployment>(objects, "Deployment")) {
      assert.equal(variable(workload, "OTEL_EXPORTER_OTLP_HEADERS"), undefined, workload.metadata?.name);
    }
    assert.deepEqual(collectorConfig(install({ telemetry: { ...telemetry, otlpProtocol: "http/json" } })).exporters["otlphttp/external"].encoding, "json");
    assert.deepEqual(collectorConfig(install({ telemetry: { otlpEndpoint: "http://tempo:4317", otlpProtocol: "grpc" } })).exporters["otlp/external"], {
      endpoint: "http://tempo:4317",
      tls: { insecure: true },
    });
    // Without the lake, there alone.
    assert.deepEqual(Object.keys(collectorConfig(install({ lake: { enabled: false }, telemetry })).exporters), ["otlphttp/external"]);
  });

  test("has alasio, the lake's loader and readers and the compute export to the collector, and nothing export without it", () => {
    const objects = install({ telemetry: { otlpEndpoint: "https://otlp.example.com", resourceAttributes: "deployment.environment.name=prod" } });
    for (const name of ["alasio", "alasio-lake-loader", "alasio-lake-query", "alasio-neon-compute"]) {
      const workload = one<V1Deployment>(objects, "Deployment", name);
      assert.deepEqual(variable(workload, "OTEL_EXPORTER_OTLP_ENDPOINT"), { name: "OTEL_EXPORTER_OTLP_ENDPOINT", value: COLLECTOR }, name);
      assert.deepEqual(variable(workload, "OTEL_EXPORTER_OTLP_PROTOCOL"), { name: "OTEL_EXPORTER_OTLP_PROTOCOL", value: "http/protobuf" }, name);
      assert.deepEqual(variable(workload, "OTEL_RESOURCE_ATTRIBUTES"), { name: "OTEL_RESOURCE_ATTRIBUTES", value: "deployment.environment.name=prod" }, name);
    }
    assert.deepEqual(variable(one<V1Deployment>(objects, "Deployment", "alasio-neon-compute"), "OTEL_SERVICE_NAME"), { name: "OTEL_SERVICE_NAME", value: "compute_ctl" });
    const without = install({ lake: { enabled: false } });
    assert.equal(variable(one<V1Deployment>(without, "Deployment", "alasio"), "OTEL_EXPORTER_OTLP_ENDPOINT"), undefined);
    assert.deepEqual(variable(one<V1Deployment>(without, "Deployment", "alasio-neon-compute"), "OTEL_SDK_DISABLED"), { name: "OTEL_SDK_DISABLED", value: "true" });
  });

  test("admits alasio, the node agents, the sandbox service's gateway and receiver, and branch environments' alasio, compute and lake to the collector, beside the stack, and folder instances not at all, whose telemetry the receiver takes", () => {
    const ingress = (objects: readonly KubernetesObject[]) =>
      one<KubernetesObject & { spec: { podSelector: unknown; ingress: unknown } }>(objects, "NetworkPolicy", "alasio-collector").spec;
    const alasio = { podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "alasio" } } };
    const nodeAgents = { podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "node-agent" } } };
    const sandboxService = ["sandbox-gateway", "receiver"].map((component) => ({ podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": component } } }));
    const branches = { matchExpressions: [{ key: "alasio.dev/branch", operator: "Exists" }] };
    const ofBranches = [
      { namespaceSelector: branches, ...alasio },
      { namespaceSelector: branches, podSelector: { matchLabels: { "app.kubernetes.io/instance": "alasio", "alasio.dev/stack": "neon" } } },
    ];
    assert.deepEqual(ingress(install()), {
      podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "collector" } },
      policyTypes: ["Ingress"],
      ingress: [{ from: [alasio, nodeAgents, ...sandboxService, ...ofBranches], ports: [{ protocol: "TCP", port: 4318 }] }],
    });
    assert.deepEqual(ingress(install({ host: { enabled: true } })), ingress(install()));
    assert.deepEqual(ingress(install({ host: { enabled: true }, sessions: { runtimeClassName: "" } })).ingress, [{ from: [alasio, ...ofBranches], ports: [{ protocol: "TCP", port: 4318 }] }]);
    // A pod of the stack's, as the compute and the lake are, which the stack's own policy admits.
    assert.equal(one<V1Deployment>(install(), "Deployment", "alasio-collector").spec?.template.metadata?.labels?.["alasio.dev/stack"], "neon");
  });

  test("scrapes each pod of every service of the stack, every minute, found in its namespace, so one that sleeps is not scraped", () => {
    const config = scraperConfig(install());
    const jobs: { job_name: string; scrape_interval: string }[] = config.receivers.prometheus.config.scrape_configs;
    assert.deepEqual(jobs.map(({ job_name }) => job_name), ["compute", "lake-intake", "lake-loader", "lake-query", "log", "neon-control", "pageserver", "proxy", "safekeeper", "storage-broker", "storage-controller", "webhook", "seaweedfs", "juicefs"]);
    assert.deepEqual(jobs.find(({ job_name }) => job_name === "safekeeper"), {
      job_name: "safekeeper",
      scrape_interval: "60s",
      kubernetes_sd_configs: [{ role: "pod", namespaces: { names: ["alasio"] } }],
      relabel_configs: [
        { source_labels: ["__meta_kubernetes_pod_label_app_kubernetes_io_component"], regex: "neon-safekeeper", action: "keep" },
        { source_labels: ["__meta_kubernetes_pod_phase"], regex: "Running", action: "keep" },
        { source_labels: ["__meta_kubernetes_pod_ip"], target_label: "__address__", replacement: "$$1:7676" },
        { source_labels: ["__meta_kubernetes_pod_name"], target_label: "pod" },
      ],
    });
    const port = (job: string) => (jobs.find(({ job_name }) => job_name === job) as unknown as { relabel_configs: { replacement?: string }[] }).relabel_configs[2]?.replacement;
    // The primary compute and, where the lake runs, its read replica, at compute_ctl's port.
    const compute = jobs.find(({ job_name }) => job_name === "compute") as unknown as { relabel_configs: { regex?: string; replacement?: string }[] };
    assert.deepEqual([compute.relabel_configs[0]?.regex, compute.relabel_configs[2]?.replacement], ["neon-compute|neon-compute-ro", "$$1:3080"]);
    // Each of neon-control's replicas among them, as any of them wakes a compute, and the proxy's.
    assert.deepEqual(["lake-loader", "lake-query", "lake-intake", "log", "neon-control", "pageserver", "proxy"].map(port), ["$$1:9464", "$$1:8090", "$$1:9464", "$$1:9464", "$$1:8080", "$$1:9898", "$$1:7001"]);
    for (const job of jobs) assert.equal(job.scrape_interval, "60s", job.job_name);
    assert.equal(config.receivers.redis.collection_interval, "60s");
    // Without the lake, none of its parts; the log, which carries Telegram's updates too, still.
    const without: { job_name: string }[] = scraperConfig(install({ lake: { enabled: false }, telemetry: { otlpEndpoint: "https://otlp.example.com" } })).receivers.prometheus.config.scrape_configs;
    assert.deepEqual(without.filter(({ job_name }) => job_name.startsWith("lake") || job_name === "log").map(({ job_name }) => job_name), ["log"]);
  });

  test("scrapes each node's mount of workspace storage, JuiceFS's client, on the port it serves its metrics on, and Valkey, with its password, when workspace storage is on", () => {
    const objects = install();
    const config = scraperConfig(objects);
    assert.deepEqual(config.receivers.prometheus.config.scrape_configs.find(({ job_name }: { job_name: string }) => job_name === "juicefs"), {
      job_name: "juicefs",
      scrape_interval: "60s",
      kubernetes_sd_configs: [{ role: "pod", namespaces: { names: ["alasio"] } }],
      relabel_configs: [
        { source_labels: ["__meta_kubernetes_pod_label_app_kubernetes_io_component"], regex: "workspace-mount", action: "keep" },
        { source_labels: ["__meta_kubernetes_pod_phase"], regex: "Running", action: "keep" },
        { source_labels: ["__meta_kubernetes_pod_ip"], target_label: "__address__", replacement: "$$1:9567" },
        { source_labels: ["__meta_kubernetes_pod_name"], target_label: "pod" },
      ],
    });
    // Which the mount serves on every address of its pod, not the loopback alone, by the port it names.
    const mount = one<V1DaemonSet>(objects, "DaemonSet", "alasio-workspace-mount");
    const mountContainer = mount.spec?.template.spec?.containers.find(({ name }) => name === "workspace-mount");
    assert.match(mountContainer?.command?.join("\n") ?? "", / mount --no-bgjob --metrics 0\.0\.0\.0:9567 /u);
    assert.deepEqual(mountContainer?.ports, [{ name: "metrics", containerPort: 9567 }]);
    assert.deepEqual(config.receivers.redis, {
      endpoint: "alasio-valkey:6379",
      password: "${env:VALKEY_PASSWORD}",
      collection_interval: "60s",
      metrics: { "redis.maxmemory": { enabled: true } },
      resource_attributes: { "server.address": { enabled: true } },
    });
    assert.deepEqual(config.service.pipelines.metrics.receivers, ["prometheus", "redis", "redis/filer-store"]);
    const scraper = one<V1Deployment>(objects, "Deployment", "alasio-collector-scraper");
    assert.equal(scraper.spec?.template.spec?.serviceAccountName, "alasio-collector-scraper");
    assert.deepEqual(container(scraper).env, [
      { name: "VALKEY_PASSWORD", valueFrom: { secretKeyRef: { name: "alasio-valkey", key: "password" } } },
      { name: "FILER_STORE_PASSWORD", valueFrom: { secretKeyRef: { name: "alasio-seaweedfs-filer-store-collector", key: "password" } } },
    ]);
    // It finds the pods it scrapes in its own namespace, where they all are.
    const roles = all<V1Role>(objects, "Role").filter(({ metadata }) => metadata?.name === "alasio-collector-scraper");
    assert.deepEqual(roles.map(({ metadata }) => metadata?.namespace), ["alasio"]);
    for (const role of roles) assert.deepEqual(role.rules, [{ apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch"] }]);
    const bindings = all<V1RoleBinding>(objects, "RoleBinding").filter(({ metadata }) => metadata?.name === "alasio-collector-scraper");
    assert.deepEqual(bindings.map(({ metadata, subjects }) => [metadata?.namespace, subjects]), [["alasio", [{ kind: "ServiceAccount", name: "alasio-collector-scraper", namespace: "alasio" }]]]);
    assert.ok(one(objects, "ServiceAccount", "alasio-collector-scraper"));

    const off = install({ workspaceStorage: { enabled: false } });
    const offConfig = scraperConfig(off);
    assert.equal(offConfig.receivers.redis, undefined);
    assert.deepEqual(offConfig.service.pipelines.metrics.receivers, ["prometheus", "redis/filer-store"]);
    assert.equal(offConfig.receivers.prometheus.config.scrape_configs.find(({ job_name }: { job_name: string }) => job_name === "juicefs"), undefined);
  });

  test("scrapes each pod of each of the object store's parts, found in its own namespace, and reads the filer store, with its password", () => {
    const config = scraperConfig(install());
    const job = config.receivers.prometheus.config.scrape_configs.find(({ job_name }: { job_name: string }) => job_name === "seaweedfs");
    assert.deepEqual(job, {
      job_name: "seaweedfs",
      scrape_interval: "60s",
      kubernetes_sd_configs: [{ role: "pod", namespaces: { names: ["alasio"] } }],
      relabel_configs: [
        { source_labels: ["__meta_kubernetes_pod_label_app_kubernetes_io_component"], regex: "seaweedfs|seaweedfs-master|seaweedfs-volume|seaweedfs-filer", action: "keep" },
        { source_labels: ["__meta_kubernetes_pod_phase"], regex: "Running", action: "keep" },
        { source_labels: ["__meta_kubernetes_pod_ip"], target_label: "__address__", replacement: "$$1:9327" },
        { source_labels: ["__meta_kubernetes_pod_name"], target_label: "pod" },
        { source_labels: ["__meta_kubernetes_pod_label_app_kubernetes_io_component"], target_label: "component" },
      ],
    });
    // As the filer store's user that may read its INFO alone.
    assert.deepEqual(config.receivers["redis/filer-store"], {
      endpoint: "alasio-seaweedfs-filer-store:6379",
      username: "collector",
      password: "${env:FILER_STORE_PASSWORD}",
      collection_interval: "60s",
      metrics: { "redis.maxmemory": { enabled: true } },
      resource_attributes: { "server.address": { enabled: true } },
    });
  });

  test("scrapes no service of Neon's without it, nor the object store's beside it, but the log's and the webhook intake's, its gateway sending what it takes to the endpoint alone", () => {
    const objects = install({ neon: { enabled: false, external: { existingSecret: "db" } }, workspaceStorage: { enabled: false }, telemetry: { otlpEndpoint: "https://otlp.example.com" } });
    const jobs: { job_name: string }[] = scraperConfig(objects).receivers.prometheus.config.scrape_configs;
    assert.deepEqual(jobs.map(({ job_name }) => job_name), ["log", "webhook"]);
    assert.deepEqual(Object.keys(collectorConfig(objects).exporters), ["otlphttp/external"]);
    assert.equal(variable(one<V1Deployment>(objects, "Deployment", "alasio-collector"), "LOG_PASSWORD"), undefined);
  });
});

describe("the log", () => {
  const log = (objects: readonly KubernetesObject[]) => one<V1StatefulSet>(objects, "StatefulSet", "alasio-log");
  const logConfig = (objects: readonly KubernetesObject[]) => one<V1ConfigMap>(objects, "ConfigMap", "alasio-log").data ?? {};
  const properties = (objects: readonly KubernetesObject[]) => logConfig(objects)["server.properties"] ?? "";
  const refusal = (file: Record<string, unknown>) => {
    const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, ...file });
    return Result.isFailure(decoded) ? decoded.failure.message : "taken";
  };

  test("runs Kafka on the floor, one node by default, restricted, its admin made as it is formatted, its metrics by the JMX exporter", () => {
    const objects = install();
    const statefulSet = log(objects);
    const pod = statefulSet.spec?.template.spec;
    assert.deepEqual([statefulSet.spec?.replicas, statefulSet.spec?.podManagementPolicy, statefulSet.spec?.serviceName, pod?.priorityClassName], [1, "Parallel", "alasio-log", "alasio-bound"]);
    assert.deepEqual(pod?.containers.map(({ name }) => name), ["log", "access"]);
    const [node, access] = pod?.containers ?? [];
    assert.equal(node?.image, "ghcr.io/eaucoin/alasio-log:0.0.0-development");
    const given = log(install({ images: { log: { repository: "registry.example/log", tag: "1", digest: "" } } })).spec?.template.spec;
    assert.deepEqual([given?.containers[0]?.image, given?.initContainers?.[0]?.image], ["registry.example/log:1", "registry.example/log:1"]);
    assert.deepEqual(node?.command, ["/bin/sh", "/etc/log/start.sh", "serve"]);
    assert.deepEqual(pod?.initContainers?.map(({ name, command }) => [name, command]), [["format", ["/bin/sh", "/etc/log/start.sh", "format"]]]);
    const env = Object.fromEntries((node?.env ?? []).map(({ name, value, valueFrom }) => [name, value ?? valueFrom?.secretKeyRef?.name]));
    assert.deepEqual(env, {
      KAFKA_CLUSTER_ID: "alasio-log",
      LOG_ADMIN_PASSWORD: "alasio-log",
      LOG_DIR: "/tmp/logs",
      KAFKA_LOG4J_OPTS: "-Dlog4j2.configurationFile=/etc/log/log4j2.properties",
      KAFKA_HEAP_OPTS: "-Xms512m -Xmx512m",
      KAFKA_OPTS: "-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9464:/etc/log/metrics.yaml",
      EXTRA_ARGS: "-name kafkaServer",
    });
    assert.deepEqual(node?.resources, { requests: { cpu: "100m", memory: "512Mi" }, limits: { memory: "1Gi" } });
    for (const each of [...(pod?.containers ?? []), ...(pod?.initContainers ?? [])]) {
      assert.deepEqual(each.securityContext, { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] }, readOnlyRootFilesystem: true }, each.name);
    }
    assert.deepEqual(pod?.securityContext?.runAsNonRoot, true);
    assert.deepEqual(statefulSet.spec?.volumeClaimTemplates?.map(({ metadata, spec }) => [metadata?.name, spec?.resources?.requests?.["storage"]]), [["data", "4Gi"]]);
    // Its access applier, on alasio's image, logs in as its admin beside it, and its node is ready once it has applied what it applies.
    assert.deepEqual(access?.command, ["node", "/opt/alasio/src/log/access.ts"]);
    assert.deepEqual(access?.readinessProbe?.httpGet, { path: "/readyz", port: "access" });
    // It writes every credential again through the controllers' listener first, unauthenticated there, of each voter in turn (src/log/access.ts).
    assert.deepEqual(access?.env?.filter(({ name }) => name.startsWith("LOG_") && name !== "LOG_ADMIN_PASSWORD").map(({ name, value }) => [name, value]), [
      ["LOG_BROKERS", "127.0.0.1:9092"],
      ["LOG_CONTROLLERS", "alasio-log-0.alasio-log.alasio.svc:9093"],
      ["LOG_ACCESS", "/etc/log/access.json"],
      ["LOG_PASSWORDS", "/etc/log-passwords"],
      ["LOG_ACCESS_PORT", "8080"],
    ]);
    assert.deepEqual(pod?.volumes?.find(({ name }) => name === "passwords")?.secret, { secretName: "alasio-log-access" });
    // Headless, each node reached by its pod's name before it is ready, as KRaft's quorum forms.
    const service = one<V1Service>(objects, "Service", "alasio-log").spec;
    assert.deepEqual([service?.clusterIP, service?.publishNotReadyAddresses, service?.ports?.map(({ port }) => port)], ["None", true, [9092, 9093, 9464]]);
    // Formatted once, its admin made with KRaft's bootstrap; its node's id and address its pod's.
    const start = logConfig(objects)["start.sh"] ?? "";
    assert.match(start, /\/opt\/kafka\/bin\/kafka-storage\.sh format --ignore-formatted -t "\$KAFKA_CLUSTER_ID" -c \/tmp\/server\.properties --add-scram "SCRAM-SHA-512=\[name=admin,password=\$LOG_ADMIN_PASSWORD\]"/u);
    assert.match(start, /echo "advertised\.listeners=CLIENT:\/\/\$HOSTNAME\.alasio-log\.alasio\.svc:9092"/u);
    assert.match(logConfig(objects)["log4j2.properties"] ?? "", /^appender\.stdout\.type=Console$/mu);
    // What it serves of its MBeans: its Produce requests' times, its partitions' sizes, its broker's state.
    const metrics = (logConfig(objects)["metrics.yaml"] ?? "").split("\n");
    for (const name of ["kafka_request_total_time_ms", "kafka_log_size_bytes", "kafka_server_broker_state"]) assert.ok(metrics.includes(`    name: ${name}`), name);
    // Its pod is replaced when its configuration changes.
    assert.match(statefulSet.spec?.template.metadata?.annotations?.["checksum/config"] ?? "", /^[0-9a-f]{64}$/u);
  });

  test("takes clients on SCRAM-SHA-512 alone, allows nothing without an ACL, and keeps its segments on its volume for their retention", () => {
    const lines = properties(install()).trim().split("\n");
    for (const line of [
      "process.roles=broker,controller",
      "controller.quorum.voters=0@alasio-log-0.alasio-log.alasio.svc:9093",
      "listener.security.protocol.map=CLIENT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
      "sasl.enabled.mechanisms=SCRAM-SHA-512",
      "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
      "allow.everyone.if.no.acl.found=false",
      "auto.create.topics.enable=false",
      "default.replication.factor=1",
      "min.insync.replicas=1",
      "log.dirs=/data/log",
      "log.retention.ms=259200000",
      "log.segment.bytes=67108864",
      "log.roll.ms=3600000",
      "message.max.bytes=8388608",
      // Its flusher looking each second for what its topics' flush.ms says to flush, where it would look never.
      "log.flush.scheduler.interval.ms=1000",
      // A snapshot of its metadata every ten minutes where it changed, so that a start replays little.
      "metadata.log.max.snapshot.interval.ms=600000",
    ]) assert.ok(lines.includes(line), line);
    assert.match(properties(install({ log: { retention: "6h" }, objectStore: { bundled: { filerStore: { backups: { enabled: false } } } } })), /^log\.retention\.ms=21600000$/mu);
  });

  test("keeps its records as long as the filer store's backups go back, and a day more, for the lake's telemetry a restore loses to be read again", () => {
    // 48 hourly backups by default: 72 hours.
    assert.equal(filerStoreBackupHours({ schedule: "23 * * * *", keep: 48 }), 48);
    assert.equal(filerStoreBackupHours({ schedule: "0 */6 * * *", keep: 8 }), 48);
    assert.equal(filerStoreBackupHours({ schedule: "15 3 * * *", keep: 7 }), 168);
    assert.equal(filerStoreBackupHours({ schedule: "0 3 * * 1", keep: 4 }), null);
    assert.equal(refusal({}), "taken");
    assert.equal(
      refusal({ log: { retention: "48h" } }),
      "log.retention must be at least 72h: the filer store's backups go back 48h (objectStore.bundled.filerStore.backups), a restore of the oldest loses the lake's telemetry written since, which the log refills only while it holds it, and a day more",
    );
    assert.equal(refusal({ log: { retention: "48h" }, objectStore: { bundled: { filerStore: { backups: { keep: 24 } } } } }), "taken");
    assert.equal(refusal({ objectStore: { bundled: { filerStore: { backups: { schedule: "15 3 * * *", keep: 7 } } } } }).startsWith("log.retention must be at least 192h"), true);
  });

  test("runs 1 node or 3 in a quorum, spread strictly, one evicted at a time, each partition on all three and acknowledged by two", () => {
    const objects = install({ log: { scaling: { min: 3, max: 3 } } });
    assert.equal(log(objects).spec?.replicas, 3);
    assert.match(properties(objects), /^controller\.quorum\.voters=0@alasio-log-0\.alasio-log\.alasio\.svc:9093,1@alasio-log-1\.alasio-log\.alasio\.svc:9093,2@alasio-log-2\.alasio-log\.alasio\.svc:9093$/mu);
    // Its access applier writes credentials through whichever voter is KRaft's active controller.
    const access = log(objects).spec?.template.spec?.containers.find(({ name }) => name === "access");
    assert.equal(access?.env?.find(({ name }) => name === "LOG_CONTROLLERS")?.value, "alasio-log-0.alasio-log.alasio.svc:9093,alasio-log-1.alasio-log.alasio.svc:9093,alasio-log-2.alasio-log.alasio.svc:9093");
    const lines = properties(objects).split("\n");
    for (const line of ["default.replication.factor=3", "offsets.topic.replication.factor=3", "transaction.state.log.replication.factor=3", "min.insync.replicas=2", "transaction.state.log.min.isr=2"]) {
      assert.ok(lines.includes(line), line);
    }
    assert.deepEqual(log(objects).spec?.template.spec?.topologySpreadConstraints?.map(({ whenUnsatisfiable }) => whenUnsatisfiable), ["DoNotSchedule", "ScheduleAnyway"]);
    assert.ok(one(objects, "PodDisruptionBudget", "alasio-log"));
    assert.equal(refusal({ log: { scaling: { min: 2, max: 2 } } }), "log.scaling must be 1 or 3: two of KRaft's voters elect no leader once one is gone");
    assert.equal(refusal({ log: { scaling: { min: 1, max: 3 } } }), "log.scaling.max must equal min: nothing scales it between them");
    assert.equal(refusal({ log: { scaling: { min: 5, max: 5 } } }), "log.scaling.max must be at most 3");
    assert.equal(refusal({ log: { resources: { limits: { memory: "512Mi" } } } }), "log.resources.limits.memory must be at least 768Mi: a heap of 512 MiB, and the JVM's own");
    assert.equal(refusal({ log: { retention: "1d" } }), "log.retention must be a number of hours, such as 24h");
  });

  test("keeps each telemetry partition within its share of the volume, and refuses a volume too small to keep a segment of each", () => {
    const retentionBytes = (file: Record<string, unknown>) => JSON.parse(logConfig(install(file))["access.json"] ?? "").topics[0].retentionBytes;
    // Three quarters of 4 GiB among 6 partitions, less the segment each writes.
    assert.equal(retentionBytes({}), 448 * 1024 * 1024);
    assert.equal(retentionBytes({ log: { storage: { size: "8Gi" } } }), 960 * 1024 * 1024);
    assert.equal(refusal({ log: { storage: { size: "1Gi" } } }), "taken");
    assert.equal(
      refusal({ log: { storage: { size: "512Mi" } } }),
      "log.storage.size must be at least 1.0 GiB: each of the telemetry topics' 6 partitions keeps a segment of 64 MiB at least beside the one it writes, in three quarters of it",
    );
    assert.match(refusal({ log: { storage: { size: "1Gi" } }, lake: { intake: { scaling: { max: 4 } } } }), /^log\.storage\.size must be at least 2\.0 GiB: each of the telemetry topics' 12 partitions /u);
  });

  test("gives each principal what it does alone: the gateway writes the telemetry topics, the intake reads them in its group, KEDA describes both and the wake topics, the webhook intake writes the Telegram topics, the sandbox gateway the events topics, main's alasio reads its own and its events, each in its group, writes its wake topic, reads the branches' requests to fork its sessions and answers on their events, main's loader reads that, and main's harness pools' hosts write its events and its wake topic alone", () => {
    const access = JSON.parse(logConfig(install())["access.json"] ?? "");
    // Each telemetry topic flushed to the volume within a second, so a host's crash loses a second of what it acknowledged at most, and kept compressed.
    assert.deepEqual(access.topics, [
      ...["otel.spans", "otel.logs", "otel.metrics"].map((name) => ({ name, partitions: 2, retentionMs: 259_200_000, retentionBytes: 448 * 1024 * 1024, configs: { "flush.ms": "1000", "compression.type": "zstd" } })),
      // A week of main's Telegram updates, on one partition, as its one alasio reads them, within a segment beside the one it writes, each flushed as it is written.
      { name: "telegram.main", partitions: 1, retentionMs: 604_800_000, retentionBytes: 64 * 1024 * 1024, configs: { "flush.messages": "1" } },
      // A day of main's wake records, on one partition, as its one loader reads them, within a segment beside the one it writes.
      { name: "lake.main", partitions: 1, retentionMs: 86_400_000, retentionBytes: 64 * 1024 * 1024, configs: {} },
      // A week of main's events, on one partition, as its one alasio reads them, each flushed as it is written, as what told of it tells no more.
      { name: "events.main", partitions: 1, retentionMs: 604_800_000, retentionBytes: 64 * 1024 * 1024, configs: { "flush.messages": "1" } },
      // A day of the branches' requests to fork main's sessions, past the wait for an answer, each flushed as it is written, as a branch waits for its answer.
      { name: "forks.main", partitions: 1, retentionMs: 86_400_000, retentionBytes: 64 * 1024 * 1024, configs: { "flush.messages": "1" } },
    ]);
    const telemetry = { resource: "topic", name: "otel.", pattern: "prefixed" };
    const group = { resource: "group", name: "lake-intake", pattern: "literal" };
    assert.deepEqual(access.principals, [
      { name: "gateway", acls: [{ ...telemetry, operations: ["write", "describe"] }] },
      { name: "intake", acls: [{ ...telemetry, operations: ["read", "describe"] }, { ...group, operations: ["read"] }] },
      {
        name: "scaler",
        acls: [
          { ...telemetry, operations: ["describe"] },
          { ...group, operations: ["describe"] },
          { resource: "topic", name: "lake.", pattern: "prefixed", operations: ["describe"] },
          { resource: "group", name: "lake-loader.", pattern: "prefixed", operations: ["describe"] },
        ],
      },
      { name: "webhook", acls: [{ resource: "topic", name: "telegram.", pattern: "prefixed", operations: ["write", "describe"] }] },
      { name: "sandbox-gateway", acls: [{ resource: "topic", name: "events.", pattern: "prefixed", operations: ["write", "describe"] }] },
      {
        name: "alasio-main",
        acls: [
          { resource: "topic", name: "telegram.main", pattern: "literal", operations: ["read", "describe"] },
          { resource: "group", name: "alasio.main", pattern: "literal", operations: ["read"] },
          { resource: "topic", name: "lake.main", pattern: "literal", operations: ["write", "describe"] },
          { resource: "topic", name: "events.main", pattern: "literal", operations: ["read", "describe"] },
          { resource: "group", name: "alasio-events.main", pattern: "literal", operations: ["read"] },
          { resource: "topic", name: "forks.main", pattern: "literal", operations: ["read", "describe"] },
          { resource: "group", name: "alasio-forks.main", pattern: "literal", operations: ["read"] },
          { resource: "topic", name: "events.", pattern: "prefixed", operations: ["write", "describe"] },
        ],
      },
      {
        name: "loader-main",
        acls: [{ resource: "topic", name: "lake.main", pattern: "literal", operations: ["read", "describe"] }, { resource: "group", name: "lake-loader.main", pattern: "literal", operations: ["read"] }],
      },
    ]);
    // Each one the stack's setup makes a password for, main's environment's among them.
    assert.deepEqual(access.principals.map(({ name }: { name: string }) => name), [...LOG_PRINCIPALS]);
    // A partition for each replica the intake may run.
    assert.equal(JSON.parse(logConfig(install({ lake: { intake: { scaling: { max: 5 } } } }))["access.json"] ?? "").topics[0].partitions, 5);
  });

  test("admits the gateway, the intake, KEDA's operator, the webhook intake, the sandbox gateway, alasio and the lake's loader alone as its clients, its own nodes to its controllers, and the scraper to its metrics", () => {
    const objects = install();
    const policies = all<NetworkPolicyObject>(objects, "NetworkPolicy");
    const labelsOf = (kind: string, name: string) => one<V1Deployment | V1StatefulSet>(objects, kind, name).spec?.template.metadata?.labels ?? {};
    const pod = (labels: Readonly<Record<string, string>>) => ({ namespace: "alasio", namespaceLabels: { "kubernetes.io/metadata.name": "alasio" }, labels });
    const node = pod(labelsOf("StatefulSet", "alasio-log"));
    const reach = (name: string, port: number, kind = "Deployment") => admits(policies, pod(labelsOf(kind, name)), node, port);
    assert.deepEqual(
      [
        reach("alasio-collector", 9092),
        reach("alasio-lake-intake", 9092),
        reach("alasio-keda-operator", 9092),
        reach("alasio-webhook", 9092),
        reach("alasio", 9092),
        reach("alasio-lake-loader", 9092),
        reach("alasio-sandbox-gateway", 9092),
        reach("alasio-log", 9093, "StatefulSet"),
        reach("alasio-collector-scraper", 9464),
      ],
      [true, true, true, true, true, true, true, true, true],
    );
    // The sandbox gateway reaches it, as it writes each environment's events; the receiver it takes not.
    assert.equal(reaches(policies, pod(labelsOf("Deployment", "alasio-sandbox-gateway")), node, 9092), true);
    assert.equal(reach("alasio-receiver", 9092), false);
    // Not the lake's readers, Neon, nor the scraper as a client, nor a client to the controllers.
    assert.deepEqual(
      [reach("alasio-lake-query", 9092), reach("alasio-neon-pageserver", 9092, "StatefulSet"), reach("alasio-collector-scraper", 9092), reach("alasio-collector", 9093), reach("alasio-webhook", 9093)],
      [false, false, false, false, false],
    );
    // The cluster's own KEDA's operator, where it scales alasio, in its namespace.
    const own = all<NetworkPolicyObject>(install({ autoscaling: { keda: { enabled: false } } }), "NetworkPolicy");
    const theirs = { namespace: "keda", namespaceLabels: { "kubernetes.io/metadata.name": "keda" }, labels: { "app.kubernetes.io/name": "keda-operator" } };
    assert.equal(admits(own, theirs, node, 9092), true);
    assert.equal(admits(policies, theirs, node, 9092), false);
  });

  test("runs the lake's intake as a group of its own, which KEDA scales on its lag from none, as a principal that reads nothing", () => {
    const objects = install();
    const intake = one<V1Deployment>(objects, "Deployment", "alasio-lake-intake");
    const pod = intake.spec?.template.spec;
    assert.deepEqual([intake.spec?.replicas, pod?.priorityClassName, container(intake).command], [undefined, "alasio-component", ["node", "src/intake-service.ts"]]);
    const env = Object.fromEntries((container(intake).env ?? []).map(({ name, value, valueFrom }) => [name, value ?? `${valueFrom?.secretKeyRef?.name}/${valueFrom?.secretKeyRef?.key}`]));
    assert.deepEqual(
      [env["LOG_BROKERS"], env["LOG_USERNAME"], env["LOG_PASSWORD"], env["LAKE_DATABASE_PASSWORD"], env["LAKE_S3_KEY"], env["OTEL_SERVICE_NAME"]],
      ["alasio-log.alasio.svc:9092", "alasio-log-intake/username", "alasio-log-intake/password", "alasio-lake/LAKE_DATABASE_PASSWORD", "alasio-lake/LAKE_S3_KEY", "alasio-lake-intake"],
    );
    // How long the log is to keep what it reads, log.retention, which it says beside how long it does.
    assert.equal(env["LOG_RETENTION_MS"], String(72 * 3_600_000));
    // None of the reader's credentials, nor the lake's other Secrets.
    assert.equal(container(intake).envFrom, undefined);
    const scaled = one<ScaledObject>(objects, "ScaledObject", "alasio-lake-intake");
    assert.deepEqual([scaled.spec.minReplicaCount, scaled.spec.maxReplicaCount], [0, 2]);
    assert.deepEqual(scaled.spec.triggers, ["otel.spans", "otel.logs", "otel.metrics"].map((topic) => ({
      type: "kafka",
      metadata: { bootstrapServers: "alasio-log.alasio.svc:9092", consumerGroup: "lake-intake", topic, lagThreshold: "1000", activationLagThreshold: "0", offsetResetPolicy: "earliest" },
      authenticationRef: { name: "alasio-lake-intake" },
    })));
    assert.deepEqual((one<KubernetesObject & { spec: object }>(objects, "TriggerAuthentication", "alasio-lake-intake")).spec, {
      secretTargetRef: ["sasl", "username", "password"].map((parameter) => ({ parameter, name: "alasio-log-scaler", key: parameter })),
    });
    // At a fixed count, neither.
    const fixed = install({ lake: { intake: { scaling: { min: 1, max: 1 } } } });
    assert.equal(one<V1Deployment>(fixed, "Deployment", "alasio-lake-intake").spec?.replicas, 1);
    assert.deepEqual([...all(fixed, "ScaledObject"), ...all(fixed, "TriggerAuthentication")].filter(({ metadata }) => metadata?.name === "alasio-lake-intake"), []);
    // Nor an intake without the lake; the log runs all the same.
    const without = install({ lake: { enabled: false } });
    assert.deepEqual(without.filter(({ metadata }) => /^alasio-lake-intake/u.test(metadata?.name ?? "")), []);
    assert.ok(one(without, "StatefulSet", "alasio-log"));
  });
});

describe("the webhook intake", () => {
  const intake = (file: Record<string, unknown> = {}) => one<V1Deployment>(install(file), "Deployment", "alasio-webhook");

  test("runs one replica, on its node's port by default, the public port inside its pod too, bound as its own user, on the floor, holding no bot token", () => {
    const deployment = intake({ webhook: { publicAddress: "203.0.113.10" } });
    const pod = deployment.spec?.template.spec;
    assert.deepEqual([deployment.spec?.replicas, deployment.spec?.strategy, pod?.priorityClassName], [1, { type: "Recreate" }, "alasio-floor"]);
    assert.match(container(deployment).image ?? "", /^ghcr\.io\/eaucoin\/alasio-webhook:/u);
    assert.deepEqual(container(deployment).ports, [{ name: "https", containerPort: 443, hostPort: 443 }, { name: "health", containerPort: 8080 }]);
    assert.deepEqual(pod?.securityContext?.sysctls, [{ name: "net.ipv4.ip_unprivileged_port_start", value: "0" }]);
    assert.deepEqual([pod?.securityContext?.runAsUser, pod?.securityContext?.runAsNonRoot, pod?.automountServiceAccountToken], [65532, true, false]);
    assert.equal(container(deployment).securityContext?.readOnlyRootFilesystem, true);
    // Its certificate, and its routes, which are none until alasio's command line registers a bot.
    assert.deepEqual(pod?.volumes, [{ name: "tls", secret: { secretName: "alasio-webhook-tls" } }, { name: "routes", secret: { secretName: "alasio-webhook-routes", optional: true } }]);
    const env = Object.fromEntries((container(deployment).env ?? []).map(({ name, value, valueFrom }) => [name, value ?? `${valueFrom?.secretKeyRef?.name}/${valueFrom?.secretKeyRef?.key}`]));
    assert.deepEqual([env["LOG_USERNAME"], env["LOG_PASSWORD"], env["WEBHOOK_PORT"]], ["alasio-log-webhook/username", "alasio-log-webhook/password", "443"]);
    assert.ok(!(container(deployment).env ?? []).some(({ valueFrom }) => valueFrom?.secretKeyRef?.name === "alasio-telegram"));
    // Its health, its routes and its metrics, inside the cluster alone.
    assert.deepEqual(one<V1Service>(install(), "Service", "alasio-webhook").spec?.ports, [{ name: "health", port: 8080, targetPort: "health" }]);
  });

  test("binds this machine's address alone from k3s on it, and takes Telegram's ranges alone through a LoadBalancer where the cluster has one", () => {
    const bound = intake({ webhook: { publicAddress: "203.0.113.10", exposure: "hostAddress", port: 8443 } });
    assert.deepEqual(container(bound).ports?.[0], { name: "https", containerPort: 8443, hostPort: 8443, hostIP: "203.0.113.10" });
    const objects = install({ webhook: { publicAddress: "bot.example.com", exposure: "loadBalancer", scaling: { min: 2, max: 2 } } });
    const balanced = one<V1Deployment>(objects, "Deployment", "alasio-webhook");
    assert.deepEqual([balanced.spec?.replicas, balanced.spec?.strategy, container(balanced).ports?.[0]], [2, undefined, { name: "https", containerPort: 443 }]);
    assert.deepEqual(one<V1Service>(objects, "Service", "alasio-webhook-public").spec, {
      type: "LoadBalancer",
      selector: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "webhook" },
      ports: [{ name: "https", port: 443, targetPort: "https" }],
      loadBalancerSourceRanges: ["149.154.160.0/20", "91.108.4.0/22"],
      externalTrafficPolicy: "Local",
    });
    assert.equal(all<V1Service>(install(), "Service").some(({ metadata }) => metadata?.name === "alasio-webhook-public"), false);
  });

  test("refuses a name for the machine's address it binds, a port Telegram does not deliver to, and more than one replica on a node's port", () => {
    const refused = (webhook: Record<string, unknown>) => {
      const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, webhook });
      assert.ok(Result.isFailure(decoded));
      return decoded.failure.message;
    };
    assert.match(refused({ exposure: "hostAddress", publicAddress: "bot.example.com" }), /^webhook\.publicAddress must be this machine's public IPv4 address/u);
    assert.match(refused({ port: 8080 }), /^webhook\.port /u);
    assert.match(refused({ publicAddress: "Not An Address" }), /^webhook\.publicAddress must be an IPv4 address or a host name, or empty$/u);
    assert.equal(refused({ scaling: { min: 2, max: 2 } }), "webhook.scaling must be 1 where the webhook intake binds its node's port, which one pod holds");
  });

  test("is reached on its port from anywhere, its health from the scraper alone, and reaches the cluster's DNS and the log alone", () => {
    const objects = install();
    const policies = all<NetworkPolicyObject>(objects, "NetworkPolicy");
    const pod = (labels: Readonly<Record<string, string>>) => ({ namespace: "alasio", namespaceLabels: { "kubernetes.io/metadata.name": "alasio" }, labels });
    const labelsOf = (name: string) => one<V1Deployment>(objects, "Deployment", name).spec?.template.metadata?.labels ?? {};
    const webhook = pod(labelsOf("alasio-webhook"));
    assert.deepEqual(
      [admits(policies, pod(labelsOf("alasio-collector-scraper")), webhook, 8080), admits(policies, pod(labelsOf("alasio")), webhook, 8080), admits(policies, pod({}), webhook, 443)],
      [true, false, true],
    );
    const egress = (one<NetworkPolicyObject>(objects, "NetworkPolicy", "alasio-webhook").spec as { egress?: readonly { readonly to: readonly Peer[]; readonly ports: readonly { readonly port: number }[] }[] }).egress;
    assert.deepEqual(egress?.map(({ to, ports }) => [to.map(({ podSelector }) => podSelector?.matchLabels), ports.map(({ port }) => port)]), [
      [[{ "k8s-app": "kube-dns" }], [53, 53]],
      [[{ "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "log" }], [9092]],
    ]);
  });

  test("on this machine's public address, takes its port from Telegram's ranges alone, or the sources it is given, as the machine's firewall never sees what is forwarded to it", () => {
    const sources = (webhook: Record<string, unknown>) =>
      (one<NetworkPolicyObject>(install({ webhook }), "NetworkPolicy", "alasio-webhook").spec.ingress ?? [])[0]?.from?.map(({ ipBlock }) => ipBlock);
    assert.deepEqual(sources({ exposure: "hostAddress", publicAddress: "203.0.113.7" }), [{ cidr: "149.154.160.0/20" }, { cidr: "91.108.4.0/22" }]);
    assert.deepEqual(sources({ exposure: "hostAddress", publicAddress: "203.0.113.7", sources: ["198.51.100.0/24"] }), [{ cidr: "198.51.100.0/24" }]);
    assert.deepEqual(sources({ exposure: "node" }), [{ cidr: "0.0.0.0/0" }]);
    assert.deepEqual(sources({ exposure: "loadBalancer" }), [{ cidr: "0.0.0.0/0" }]);
    // A LoadBalancer's ranges are the sources too.
    const balanced = install({ webhook: { publicAddress: "bot.example.com", exposure: "loadBalancer", sources: ["198.51.100.0/24"] } });
    assert.deepEqual(one<V1Service>(balanced, "Service", "alasio-webhook-public").spec?.loadBalancerSourceRanges, ["198.51.100.0/24"]);
    const refused = (webhook: Record<string, unknown>) => {
      const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, webhook });
      return Result.isFailure(decoded) ? decoded.failure.message : "taken";
    };
    assert.equal(refused({ sources: [] }), "webhook.sources must list a network at least");
    assert.match(refused({ sources: ["149.154.160.0"] }), /^webhook\.sources\.0 must be an IPv4 CIDR$/u);
  });
});

describe("workspace storage", () => {
  /** The setup Job's variable of `name`, if it has one. */
  const setupVariable = (objects: readonly KubernetesObject[], name: string) =>
    one<V1Job>(objects, "Job", "alasio-neon-setup").spec?.template.spec?.containers[0]?.env?.find((variable) => variable.name === name)?.value;

  test("keeps its metadata in a Valkey configured as JuiceFS's Redis best practices say", () => {
    const objects = install({ workspaceStorage: { valkey: { maxmemory: "256mb" } } });
    const conf = one<V1ConfigMap>(objects, "ConfigMap", "alasio-valkey").data?.["valkey.conf"]?.split("\n") ?? [];
    for (const line of ["appendonly yes", "appendfsync always", "aof-use-rdb-preamble yes", "save 3600 1 300 100 60 10000", "stop-writes-on-bgsave-error yes"]) {
      assert.ok(conf.includes(line), line);
    }
    assert.ok(conf.includes("maxmemory 256mb"));
    assert.ok(conf.includes("maxmemory-policy noeviction"));
    const valkey = one<V1StatefulSet>(objects, "StatefulSet", "alasio-valkey");
    const server = container(valkey);
    assert.equal(server.image, "valkey/valkey:9.1.2-alpine@sha256:48332870af354a799964c0012ae1194a0bf2bf894eb508f945810596dc2d8d11");
    assert.deepEqual(server.env?.find(({ name }) => name === "VALKEY_PASSWORD")?.valueFrom, { secretKeyRef: { name: "alasio-valkey", key: "password" } });
    assert.match(server.readinessProbe?.exec?.command?.[2] ?? "", /\^loading:0.*\^aof_enabled:1/u);
    assert.equal(server.securityContext?.readOnlyRootFilesystem, true);
    assert.equal(valkey.spec?.template.spec?.securityContext?.runAsNonRoot, true);
    assert.equal(valkey.spec?.volumeClaimTemplates?.[0]?.spec?.resources?.requests?.["storage"], "2Gi");
    assert.equal(valkey.spec?.template.metadata?.labels?.["alasio.dev/stack"], undefined);
    assert.match(valkey.spec?.template.metadata?.annotations?.["checksum/config"] ?? "", /^[0-9a-f]{64}$/u);
  });

  test("formats the file system beside Valkey, unless it is formatted already, before Valkey's pod is ready", () => {
    const valkey = one<V1StatefulSet>(install({ workspaceStorage: { trashDays: 3 } }), "StatefulSet", "alasio-valkey");
    const format = valkey.spec?.template.spec?.containers.find(({ name }) => name === "juicefs-format");
    assert.ok(format);
    assert.equal(format.image, "juicedata/mount:ce-v1.4.1@sha256:ab99388a397fe52575fdeb84a9e017c3c594a6b07d63966cd4805bbf6f172673");
    assert.deepEqual(format.readinessProbe?.exec?.command, ["test", "-f", "/tmp/ready"]);
    assert.deepEqual(format.env?.map(({ name, valueFrom }) => [name, valueFrom?.secretKeyRef?.name, valueFrom?.secretKeyRef?.key]), [
      ["VALKEY_PASSWORD", "alasio-valkey", "password"],
      ["NAME", "alasio-workspaces-juicefs", "name"],
      ["STORAGE", "alasio-workspaces-juicefs", "storage"],
      ["BUCKET", "alasio-workspaces-juicefs", "bucket"],
      ["ACCESS_KEY", "alasio-workspaces-juicefs", "access-key"],
      ["SECRET_KEY", "alasio-workspaces-juicefs", "secret-key"],
    ]);
    // Not formatted, and its first format refused, as the object store is not up yet.
    const juicefs = [
      'echo "$@" >> "$ROOT/calls"',
      "case $1 in",
      '  status) [ -e "$ROOT/formatted" ] ;;',
      '  format) if [ -e "$ROOT/refused" ]; then : > "$ROOT/formatted"; else : > "$ROOT/refused"; exit 1; fi ;;',
      "esac",
    ].join("\n");
    const recorded = (name: string) => `echo "${name} $*" >> "$ROOT/calls"`;
    const env = { VALKEY_PASSWORD: "secret", NAME: "workspaces", STORAGE: "s3", BUCKET: "http://store:8333/workspaces", ACCESS_KEY: "key", SECRET_KEY: "shh" };
    const { root, run, remove } = runScript(format, env, { juicefs, sleep: recorded("sleep"), touch: recorded("touch") });
    const calls = () => {
      const made = readFileSync(join(root, "calls"), "utf8").trim().split("\n");
      rmSync(join(root, "calls"));
      return made;
    };
    try {
      const metaUrl = "redis://:secret@127.0.0.1:6379/1";
      const formats = `format --storage s3 --bucket http://store:8333/workspaces --access-key key --secret-key shh --trash-days 3 ${metaUrl} workspaces`;
      run();
      assert.deepEqual(calls(), [`status ${metaUrl}`, formats, "sleep 5", `status ${metaUrl}`, formats, "touch /tmp/ready", "sleep infinity"]);
      run();
      assert.deepEqual(calls(), [`status ${metaUrl}`, "touch /tmp/ready", "sleep infinity"]);
    } finally {
      remove();
    }
  });

  test("installs no CSI driver nor StorageClass, nothing that a driver would need kept: each node's mount serves the workspaces", () => {
    const objects = install();
    for (const kind of ["CSIDriver", "StorageClass"]) assert.deepEqual(all(objects, kind), [], kind);
    assert.deepEqual(objects.filter(({ metadata }) => metadata?.labels?.["alasio.dev/volume-driver"] !== undefined).map(({ kind, metadata }) => `${kind} ${metadata?.name}`), []);
    // In the driver's namespace, kube-system, only what lets KEDA read the API server's authentication.
    assert.deepEqual(objects.filter(({ metadata }) => metadata?.namespace === "kube-system").map(({ kind, metadata }) => `${kind} ${metadata?.name}`), ["RoleBinding alasio-keda-auth-reader"]);
    assert.deepEqual(all(objects, "PriorityClass").filter(({ metadata }) => metadata?.name === "alasio-juicefs-mount"), []);
    assert.ok(one(objects, "DaemonSet", "alasio-workspace-mount"));
  });

  test("checks each workspace's quota daily and repairs it, as a JuiceFS admin on the pinned client", () => {
    const quotaCheck = one<V1CronJob>(install(), "CronJob", "alasio-juicefs-quota-check");
    assert.equal(quotaCheck.metadata?.namespace, "alasio");
    assert.equal(quotaCheck.spec?.schedule, "47 3 * * *");
    assert.equal(quotaCheck.spec?.concurrencyPolicy, "Forbid");
    const pod = quotaCheck.spec?.jobTemplate.spec?.template;
    assert.equal(pod?.metadata?.labels?.["alasio.dev/workload"], "juicefs-admin");
    assert.equal(pod?.spec?.securityContext?.runAsNonRoot, true);
    const check = pod?.spec?.containers[0];
    assert.ok(check);
    assert.equal(check.image, "juicedata/mount:ce-v1.4.1@sha256:ab99388a397fe52575fdeb84a9e017c3c594a6b07d63966cd4805bbf6f172673");
    assert.deepEqual(check.env, [{ name: "META_URL", valueFrom: { secretKeyRef: { name: "alasio-workspaces-juicefs", key: "metaurl" } } }]);
    // `juicefs quota list` as JuiceFS 1.4.1 tables quotas: directories' by path (a deleted
    // one's in the trash, or by inode once it has none), users' and groups' by id. A
    // workspace's is its directory's in its environment's, /<environment>/<workspace>.
    const juicefs = [
      'echo "$@" >> "$ROOT/calls"',
      '[ "$2" = list ] || exit 0',
      "cat <<'TABLE'",
      "+-------------------------------------------------+---------+---------+------+-----------+-------+-------+",
      "|                     Path/ID                     |   Size  |   Used  | Use% |   Inodes  | IUsed | IUse% |",
      "+-------------------------------------------------+---------+---------+------+-----------+-------+-------+",
      "| /.trash/2026-10-05-03/1-77-fs-old               | 1.0 GiB | 8.0 KiB |   0% | unchanged |     2 |       |",
      "| /main/fs-abc123                                 | 1.0 GiB | 1.6 MiB |   0% | unchanged |   314 |       |",
      "| /main/fs-abc123/files                           | 1.0 GiB |     0 B |   0% | unchanged |     0 |       |",
      "| /try/fs-def456                                  | 2.0 GiB |     0 B |   0% | unchanged |     0 |       |",
      "| /stray                                          | 1.0 GiB | 4.0 KiB |   0% | unchanged |     3 |       |",
      "| inode:42                                        | 1.0 GiB |     0 B |   0% | unchanged |     0 |       |",
      "| uid:1000                                        | 4.0 GiB | 1.6 MiB |   0% | unchanged |   314 |       |",
      "+-------------------------------------------------+---------+---------+------+-----------+-------+-------+",
      "TABLE",
    ].join("\n");
    const metaUrl = "redis://:secret@alasio-valkey.alasio.svc.cluster.local:6379/1";
    const { root, run, remove } = runScript(check, { META_URL: metaUrl }, { juicefs });
    try {
      run();
      assert.deepEqual(readFileSync(join(root, "calls"), "utf8").trim().split("\n"), [
        `quota list ${metaUrl}`,
        `quota check ${metaUrl} --path /main/fs-abc123 --repair`,
        `quota check ${metaUrl} --path /try/fs-def456 --repair`,
      ]);
    } finally {
      remove();
    }
  });

  test("dumps the file system's metadata every backupInterval, one at a time, into its bucket's meta/ as JuiceFS names its dumps, read back the same, the newest kept", () => {
    assert.deepEqual(["5m", "15m", "30m", "1h", "6h", "24h", "4m", "7m", "90m", "5h", "1h30m", "10s"].map(everyIntervalCron), ["*/5 * * * *", "*/15 * * * *", "*/30 * * * *", "0 */1 * * *", "0 */6 * * *", "0 0 * * *", null, null, null, null, null, null]);
    const dumping = one<V1CronJob>(install({ workspaceStorage: { backupInterval: "15m" } }), "CronJob", "alasio-juicefs-dump");
    assert.equal(dumping.spec?.schedule, "*/15 * * * *");
    assert.equal(dumping.spec?.concurrencyPolicy, "Forbid");
    const pod = dumping.spec?.jobTemplate.spec?.template;
    assert.equal(pod?.metadata?.labels?.["alasio.dev/workload"], "juicefs-admin");
    assert.equal(pod?.spec?.securityContext?.runAsNonRoot, true);
    const [dump] = pod?.spec?.initContainers ?? [];
    assert.ok(dump);
    // Dumped by JuiceFS, its keys left out, of bounded memory.
    assert.match(dump.command?.[2] ?? "", /^juicefs dump "\$META_URL" \/dump\/dump\.json\.gz$/mu);
    assert.deepEqual(dump.resources?.limits, { memory: "1Gi" });
    const [upload] = pod?.spec?.containers ?? [];
    assert.ok(upload);
    const env = Object.fromEntries((upload.env ?? []).map(({ name, value, valueFrom }) => [name, value ?? `${valueFrom?.secretKeyRef?.name}/${valueFrom?.secretKeyRef?.key}`]));
    assert.deepEqual([env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"], env["WORKSPACES_BUCKET"], env["WORKSPACES_NAME"]], ["alasio-workspaces-juicefs/access-key", "alasio-workspaces-juicefs/secret-key", "workspaces", "workspaces"]);
    // The object store, a directory of buckets, as aws s3 lists, reads, writes and deletes from it; a write it corrupts, where CORRUPT says.
    const aws = [
      "shift 3; verb=$1; shift",
      'at() { case $1 in s3://*) printf "%s/%s" "$ROOT/store" "${1#s3://}" ;; *) printf "%s" "$1" ;; esac; }',
      "case $verb in",
      '  ls) [ -d "$(at "$1")" ] || exit 1; for f in "$(at "$1")"*; do [ -e "$f" ] && echo "2026-10-05 03:17:00 1 ${f##*/}"; done; true ;;',
      '  cp) mkdir -p "$(dirname "$(at "$2")")"; cp "$(at "$1")" "$(at "$2")"',
      '      case $2 in s3://*) [ -n "${CORRUPT:-}" ] && printf x >> "$(at "$2")" ;; esac; true ;;',
      '  rm) rm "$(at "$1")" ;;',
      "esac",
    ].join("\n");
    const { root, run: runIn, remove } = runScript(upload, Object.fromEntries((upload.env ?? []).flatMap(({ name, value }) => (value === undefined ? [] : [[name, value]]))), { aws });
    const run = (more: Readonly<Record<string, string>> = {}) => runIn({ DUMP: join(root, "dump"), ...more });
    try {
      const meta = join(root, "store", "workspaces", "workspaces", "meta");
      mkdirSync(meta, { recursive: true });
      mkdirSync(join(root, "dump"));
      // More than it keeps, of before.
      const older = Array.from({ length: 25 }, (_, hour) => `dump-2026-10-04-${String(hour % 24).padStart(2, "0")}${hour >= 24 ? "3" : "0"}0000.json.gz`);
      for (const name of older) writeFileSync(join(meta, name), gzipSync("older"));
      writeFileSync(join(root, "dump", "dump.json.gz"), gzipSync("the metadata"));
      const said = run();
      const put = /dumped the metadata of workspaces to s3:\/\/workspaces\/workspaces\/meta\/(dump-\d{4}-\d{2}-\d{2}-\d{6}\.json\.gz)$/mu.exec(said)?.[1];
      assert.ok(put, said);
      assert.equal(gunzipSync(readFileSync(join(meta, put))).toString(), "the metadata");
      // The newest 24 kept, the one put among them.
      const kept = readdirSync(meta).sort();
      assert.equal(kept.length, 24);
      assert.ok(kept.includes(put) && !kept.includes(older.toSorted()[0] ?? ""), kept.join(", "));
      // One the store keeps wrong fails it, saying so; as does a dump that is no whole gzip.
      assert.throws(() => run({ CORRUPT: "1" }), /in the bucket is not of the dump's size/u);
      writeFileSync(join(root, "dump", "dump.json.gz"), gzipSync("cut short").subarray(0, 12));
      assert.throws(() => run(), /the dump is no whole gzip/u);
    } finally {
      remove();
    }
  });

  test("cleans the sessions of clients that died every five minutes, one run at a time, by a client of background jobs run past two of its refreshes, on no mount, of no backup of its own", () => {
    const cleaning = one<V1CronJob>(install(), "CronJob", "alasio-juicefs-sessions");
    assert.equal(cleaning.spec?.schedule, "*/5 * * * *");
    assert.equal(cleaning.spec?.concurrencyPolicy, "Forbid");
    const pod = cleaning.spec?.jobTemplate.spec?.template;
    assert.equal(pod?.metadata?.labels?.["alasio.dev/workload"], "juicefs-admin");
    assert.equal(pod?.spec?.securityContext?.runAsNonRoot, true);
    const [client] = pod?.spec?.containers ?? [];
    assert.ok(client);
    // Unprivileged, as it mounts nothing.
    assert.deepEqual(client.securityContext, { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } });
    const juicefs = [
      'echo "$@" >> "$ROOT/calls"',
      'if [ "$1" = status ]; then echo \'{"Sessions": [\'; echo \'{"Sid": 1,\'; echo \'{"Sid": 2,\'; echo "]}"; exit 0; fi',
      '[ -n "$MINIO_ROOT_PASSWORD" ] && [ "${#MINIO_ROOT_PASSWORD}" -ge 8 ] || exit 3',
      "trap 'echo stopped >> \"$ROOT/calls\"; exit 0' TERM",
      "while :; do sleep 0.1; done",
    ].join("\n");
    const metaUrl = "redis://:secret@alasio-valkey.alasio.svc.cluster.local:6379/1";
    const { root, run, remove } = runScript(client, { META_URL: metaUrl }, { juicefs, sleep: 'if [ "$1" = 40 ]; then echo "slept $1" >> "$ROOT/calls"; /bin/sleep 1; else /bin/sleep "$1"; fi' });
    try {
      const said = run();
      assert.match(said, /^sessions as it begins: 2$/mu);
      assert.match(said, /^sessions as it ends: 2$/mu);
      const calls = readFileSync(join(root, "calls"), "utf8").trim().split("\n").filter((call) => !call.startsWith("0.1"));
      // The sessions counted, the client started beside the wait, stopped once it is through, and the sessions counted again.
      assert.deepEqual([calls[0], calls.at(-1), calls.at(-2)], [`status ${metaUrl}`, `status ${metaUrl}`, "stopped"]);
      assert.deepEqual(calls.slice(1, -2).sort(), [`gateway --no-usage-report --backup-meta 0 --cache-dir /tmp/cache --cache-size 0 ${metaUrl} 127.0.0.1:9000`, "slept 40"]);
    } finally {
      remove();
    }
  });

  test("collects the file system daily, as a JuiceFS admin on the pinned client", () => {
    const collection = one<V1CronJob>(install(), "CronJob", "alasio-juicefs-gc");
    assert.equal(collection.metadata?.namespace, "alasio");
    assert.equal(collection.spec?.schedule, "17 4 * * *");
    assert.equal(collection.spec?.concurrencyPolicy, "Forbid");
    const pod = collection.spec?.jobTemplate.spec?.template;
    assert.equal(pod?.metadata?.labels?.["alasio.dev/workload"], "juicefs-admin");
    assert.equal(pod?.spec?.securityContext?.runAsNonRoot, true);
    const gc = pod?.spec?.containers[0];
    assert.ok(gc);
    assert.equal(gc.image, "juicedata/mount:ce-v1.4.1@sha256:ab99388a397fe52575fdeb84a9e017c3c594a6b07d63966cd4805bbf6f172673");
    assert.deepEqual(gc.securityContext, { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } });
    const metaUrl = "redis://:secret@alasio-valkey.alasio.svc.cluster.local:6379/1";
    const { root, run, remove } = runScript(gc, { META_URL: metaUrl }, { juicefs: 'echo "$@" >> "$ROOT/calls"' });
    try {
      run();
      assert.deepEqual(readFileSync(join(root, "calls"), "utf8").trim().split("\n"), [`gc ${metaUrl} --compact --delete`]);
    } finally {
      remove();
    }
  });

  test("admits JuiceFS's clients alone, alasio's JuiceFS admins and the nodes' mount, to Valkey, but for the node agents setting quotas and the collector's scraper reading its metrics, and to the object store's S3 port, with Valkey's, which formats the file system", () => {
    const objects = install();
    const policy = (name: string) =>
      one<KubernetesObject & { spec: { podSelector: unknown; ingress: Array<{ from: unknown[]; ports: unknown }> } }>(objects, "NetworkPolicy", name).spec;
    const peers = [{ podSelector: { matchLabels: { "alasio.dev/workload": "juicefs-admin" } } }];
    // The nodes' mount is one of them.
    assert.equal(one<V1DaemonSet>(objects, "DaemonSet", "alasio-workspace-mount").spec?.template.metadata?.labels?.["alasio.dev/workload"], "juicefs-admin");
    assert.deepEqual(policy("alasio-valkey").podSelector, { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "valkey" } });
    const agents = { podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "node-agent" } } };
    assert.deepEqual(policy("alasio-valkey").ingress, [
      { from: [...peers, agents], ports: [{ protocol: "TCP", port: 6379 }] },
      {
        from: [{ podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "collector-scraper" } } }],
        ports: [{ protocol: "TCP", port: 6379 }],
      },
    ]);
    const valkey = { podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "valkey" } } };
    assert.deepEqual(policy("alasio-seaweedfs-workspaces").ingress, [{ from: [...peers, valkey], ports: [{ protocol: "TCP", port: 8333 }] }]);
  });

  test("makes its bucket and its identity, and gives the setup what its Secrets are made from", () => {
    const objects = install();
    const buckets = one<V1Deployment>(objects, "Deployment", "alasio-seaweedfs").spec?.template.spec?.containers.find(({ name }) => name === "buckets");
    assert.match(buckets?.command?.[2] ?? "", /^for bucket in neon lake backups workspaces snapshots; do$/mu);
    assert.equal(setupVariable(objects, "WORKSPACES_NAME"), "workspaces");
    assert.equal(setupVariable(objects, "WORKSPACES_BUCKET"), "workspaces");
    assert.equal(setupVariable(objects, "WORKSPACES_BUCKET_URL"), "http://alasio-seaweedfs.alasio.svc.cluster.local:8333/workspaces");
    assert.equal(setupVariable(objects, "WORKSPACES_TRASH_DAYS"), "1");
    assert.equal(setupVariable(objects, "VALKEY_ADDRESS"), "alasio-valkey.alasio.svc.cluster.local:6379");
  });

  test("keeps its data in the operator's object store, with its keys, when it is theirs", () => {
    const objects = install({ objectStore: { bundled: { enabled: false }, external: { endpoint: "https://s3.example.com/", existingSecret: "s3", snapshotsSecret: "s3-snapshots" } } });
    assert.equal(setupVariable(objects, "WORKSPACES_BUCKET_URL"), "https://s3.example.com/workspaces");
    assert.equal(setupVariable(objects, "S3_EXTERNAL"), "1");
    assert.deepEqual(all(objects, "NetworkPolicy").filter((policy) => policy.metadata?.name === "alasio-seaweedfs-workspaces"), []);
    assert.ok(one(objects, "NetworkPolicy", "alasio-valkey"));
  });

  test("installs nothing of it when it is off", () => {
    const objects = install({ workspaceStorage: { enabled: false } });
    assert.deepEqual(all(objects, "DaemonSet").filter(({ metadata }) => metadata?.name !== "alasio-node-agent"), []);
    assert.deepEqual(objects.filter(({ metadata }) => /valkey|juicefs|workspaces/u.test(metadata?.name ?? "")), []);
    assert.equal(setupVariable(objects, "WORKSPACES_NAME"), undefined);
    const buckets = one<V1Deployment>(objects, "Deployment", "alasio-seaweedfs").spec?.template.spec?.containers.find(({ name }) => name === "buckets");
    assert.match(buckets?.command?.[2] ?? "", /^for bucket in neon lake backups snapshots; do$/mu);
  });
});

describe("node agents", () => {
  const agentOf = (objects: readonly KubernetesObject[]) => one<V1DaemonSet>(objects, "DaemonSet", "alasio-node-agent");

  test("refuse a memory limit that leaves no room for the agent's own memory and a restore's parts not yet on the disk, where sessions are snapshotted", () => {
    const limited = (memory: string, sessions: Record<string, unknown> = {}) =>
      decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, sessions: { ...sessions, snapshots: { nodeAgent: { resources: { limits: { memory } } } } } });
    const refused = limited("160Mi");
    assert.ok(Result.isFailure(refused));
    assert.equal(refused.failure.message, "sessions.snapshots.nodeAgent.resources.limits.memory must be at least 192 MiB: the node agent's own memory, and what a restore holds written and not yet on the disk");
    assert.ok(Result.isSuccess(limited("192Mi")));
    // Sessions not snapshotted run no node agent.
    assert.ok(Result.isSuccess(limited("160Mi", { runtimeClassName: "" })));
  });

  test("run privileged in the node's PID namespace, on every node sessions run on, with the node's paths at the same paths", () => {
    const objects = install({ sessions: { nodeSelector: { "alasio.dev/sessions": "true" }, tolerations: [{ key: "sessions", operator: "Exists" }] } });
    const agent = agentOf(objects);
    assert.equal(agent.metadata?.namespace, "alasio");
    assert.deepEqual(agent.spec?.selector.matchLabels, { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "node-agent" });
    const pod = agent.spec?.template.spec;
    assert.equal(pod?.hostPID, true);
    assert.equal(pod?.automountServiceAccountToken, false);
    // Stopped, or replaced one node at a time, it lets what it runs end first: the restores that
    // read the bucket through it, the last prepared just before taking as long as one takes to
    // its resume (5 minutes), then to read a session's 2 GiB at 4 MiB/s (512 s); then its
    // operations, within the longest one may take (15 minutes).
    assert.equal(pod?.terminationGracePeriodSeconds, 300 + 512 + 900);
    assert.equal(agentOf(install({ sessions: { resources: { limits: { memory: "8Gi" } } } })).spec?.template.spec?.terminationGracePeriodSeconds, 300 + 2048 + 900);
    assert.deepEqual(agent.spec?.updateStrategy, { type: "RollingUpdate", rollingUpdate: { maxUnavailable: 1, maxSurge: 0 } });
    assert.deepEqual(pod?.nodeSelector, { "alasio.dev/sessions": "true" });
    assert.deepEqual(pod?.tolerations, [{ key: "sessions", operator: "Exists" }]);
    assert.deepEqual(pod?.volumes, [
      { name: "run", hostPath: { path: "/run", type: "Directory" } },
      { name: "gvisor", hostPath: { path: "/usr/local/lib/alasio/gvisor", type: "DirectoryOrCreate" } },
      { name: "containerd", hostPath: { path: "/var/lib/rancher/k3s/agent/etc/containerd", type: "DirectoryOrCreate" } },
      { name: "alasio", hostPath: { path: "/var/lib/alasio", type: "DirectoryOrCreate" } },
    ]);
    const [container] = pod?.containers ?? [];
    assert.equal(container?.image, one<V1Deployment>(objects, "Deployment", "alasio").spec?.template.spec?.containers[0]?.image);
    assert.deepEqual(container?.command, ["node", "/opt/alasio/src/node-agent/main.ts"]);
    assert.deepEqual(container?.securityContext, { privileged: true, runAsUser: 0 });
    assert.deepEqual(container?.volumeMounts, [
      { name: "run", mountPath: "/run", mountPropagation: "HostToContainer" },
      { name: "gvisor", mountPath: "/usr/local/lib/alasio/gvisor", readOnly: true },
      { name: "containerd", mountPath: "/var/lib/rancher/k3s/agent/etc/containerd", readOnly: true },
      // alasio's directory, not its mount point: the node's mount of workspace storage, made there anew after the agent starts, is the agent's too.
      { name: "alasio", mountPath: "/var/lib/alasio", mountPropagation: "HostToContainer" },
    ]);
    assert.deepEqual(container?.ports, [{ name: "api", containerPort: 8484 }]);
    assert.deepEqual(container?.readinessProbe, { httpGet: { path: "/healthz", port: "api" }, periodSeconds: 10 });
    assert.deepEqual(container?.resources, { requests: { cpu: "20m", memory: "160Mi" }, limits: { memory: "512Mi" } });
    // First, as the pod starts, it says the node's CPU features under the current release, as runsc does, with the paths runsc reads.
    const [features, ...others] = pod?.initContainers ?? [];
    assert.deepEqual(others, []);
    assert.equal(features?.name, "cpu-features");
    assert.equal(features?.image, container?.image);
    assert.deepEqual(features?.command, ["node", "/opt/alasio/src/node-agent/cpu-features.ts", NODE_PINS.gvisor.releases[0]?.release]);
    assert.deepEqual(features?.securityContext, { privileged: true, runAsUser: 0 });
    assert.deepEqual(features?.volumeMounts, container?.volumeMounts?.filter(({ name }) => name !== "snapshots"));
  });

  test("take their bucket from its Secret whole, alasio's token from theirs, their node's name and their pod's address, their cache's budget in bytes, the first gVisor release that restores from the bucket, how long a resumed restore may read it through them, and workspace storage's metadata engine", () => {
    const container = (file: Record<string, unknown> = {}) => agentOf(install(file)).spec?.template.spec?.containers[0];
    const env = (file: Record<string, unknown> = {}) => Object.fromEntries((container(file)?.env ?? []).map(({ name, value, valueFrom }) => [name, value ?? valueFrom]));
    assert.deepEqual(container()?.envFrom, [{ secretRef: { name: "alasio-snapshots" } }]);
    assert.deepEqual(env(), {
      NODE_NAME: { fieldRef: { fieldPath: "spec.nodeName" } },
      POD_IP: { fieldRef: { fieldPath: "status.podIP" } },
      ALASIO_NODE_AGENT_TOKEN: { secretKeyRef: { name: "alasio-node-agent", key: "token" } },
      ALASIO_SNAPSHOT_CACHE_BYTES: "5368709120",
      ALASIO_GVISOR_RESTORES_FROM_BUCKET_SINCE: "20260921.0-eaucoin.7",
      // A session's 2 GiB, at 4 MiB/s.
      ALASIO_BACKGROUND_READ_SECONDS: "512",
      META_URL: { secretKeyRef: { name: "alasio-workspaces-quotas", key: "metaurl" } },
      OTEL_SERVICE_NAME: "alasio-node-agent",
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://alasio-collector.alasio.svc:4318",
      OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf",
    });
    assert.equal(env({ sessions: { snapshots: { nodeCache: "1.5G" } } })["ALASIO_SNAPSHOT_CACHE_BYTES"], "1500000000");
    assert.equal(env({ sessions: { snapshots: { nodeCache: "750Mi" } } })["ALASIO_SNAPSHOT_CACHE_BYTES"], "786432000");
    assert.equal(env({ sessions: { resources: { limits: { memory: "8Gi" } } } })["ALASIO_BACKGROUND_READ_SECONDS"], "2048");
    // A minute at least, however small a session.
    assert.equal(env({ sessions: { resources: { limits: { memory: "64Mi" } } } })["ALASIO_BACKGROUND_READ_SECONDS"], "60");
    // Without a collector, it exports nothing; without workspace storage, it sets no quota.
    assert.deepEqual(Object.keys(env({ lake: { enabled: false } })).filter((name) => name.startsWith("OTEL_")), []);
    assert.equal(env({ workspaceStorage: { enabled: false } })["META_URL"], undefined);
  });

  test("run only where sessions run under gVisor, beside Neon, whose setup makes their Secrets", () => {
    const agents = (file: Record<string, unknown>) => all(install(file), "DaemonSet").filter(({ metadata }) => metadata?.name === "alasio-node-agent");
    assert.equal(agents({}).length, 1);
    assert.deepEqual(agents({ sessions: { runtimeClassName: "" } }), []);
    assert.deepEqual(agents({ sessions: { enabled: false }, workspaceStorage: { enabled: false } }), []);
    assert.deepEqual(agents({ neon: { enabled: false, external: { existingSecret: "db" } }, workspaceStorage: { enabled: false } }), []);
    assert.deepEqual(all(install({ sessions: { runtimeClassName: "" } }), "NetworkPolicy").filter(({ metadata }) => /node-agent|snapshots/u.test(metadata?.name ?? "")), []);
  });

  test("admit no pod, of any of the installation's workloads, nor any address, as another node's is, to the port they serve the bucket to their node on", () => {
    for (const file of [{}, { objectStore: { bundled: { enabled: false }, external: { endpoint: "https://s3.example.com", existingSecret: "s3", snapshotsSecret: "s3-snapshots" } }, lake: { enabled: false } }]) {
      const objects = install(file);
      const policies = all<NetworkPolicyObject>(objects, "NetworkPolicy");
      const agent = { namespace: "alasio", namespaceLabels: { "kubernetes.io/metadata.name": "alasio" }, labels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "node-agent" } };
      // Its ingress is held to rules of ports, none of them that one: an address block, as another node's host is, is admitted to none of it.
      const selecting = policies.filter(({ metadata, spec }) => metadata?.namespace === "alasio" && (spec.policyTypes ?? ["Ingress"]).includes("Ingress") && selects(spec.podSelector, agent.labels));
      assert.ok(selecting.length > 0);
      for (const { metadata, spec } of selecting) {
        for (const rule of spec.ingress ?? []) {
          assert.ok(rule.ports !== undefined && rule.ports.every(({ port }) => port !== undefined && port !== STORE_PROXY_PORT), `${metadata?.name} admits ${JSON.stringify(rule)}`);
        }
      }
      // Nor a pod of any workload the installation runs.
      const templates = objects.flatMap((object) =>
        ["Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob"].includes(object.kind ?? "")
          ? [{ namespace: object.metadata?.namespace ?? "", labels: ((object as V1Deployment).spec?.template?.metadata?.labels ?? (object as V1CronJob).spec?.jobTemplate?.spec?.template?.metadata?.labels ?? {}) as Record<string, string> }]
          : []
      );
      assert.ok(templates.length > 10);
      for (const { namespace, labels } of templates) {
        assert.equal(admits(policies, { namespace, namespaceLabels: { "kubernetes.io/metadata.name": namespace }, labels }, agent, STORE_PROXY_PORT), false, `${namespace} ${JSON.stringify(labels)}`);
      }
    }
  });

  test("take the sandbox gateway's calls alone, and reach the cluster's DNS, the object store's S3 port, workspace storage's metadata engine and the collector alone", () => {
    const objects = install();
    const policies = all<NetworkPolicyObject>(objects, "NetworkPolicy");
    const podOf = (component: string, namespace = "alasio") => ({
      namespace,
      namespaceLabels: { "kubernetes.io/metadata.name": namespace },
      labels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": component },
    });
    const agent = podOf("node-agent");
    assert.equal(admits(policies, podOf("sandbox-gateway"), agent, 8484), true);
    assert.equal(admits(policies, podOf("alasio"), agent, 8484), false);
    assert.equal(admits(policies, podOf("collector"), agent, 8484), false);
    assert.equal(admits(policies, podOf("alasio", "alasio-branch-try"), agent, 8484), false);
    assert.equal(admits(policies, { ...podOf("session", "alasio-sessions"), labels: { "alasio.dev/workload": "session" } }, agent, 8484), false);
    assert.equal(admits(policies, podOf("sandbox-gateway"), agent, 8485), false);
    // Where no gateway runs, as without workspace storage, none.
    const ungated = all<NetworkPolicyObject>(install({ workspaceStorage: { enabled: false } }), "NetworkPolicy");
    assert.equal(admits(ungated, podOf("sandbox-gateway"), agent, 8484), false);
    assert.equal(admits(policies, agent, podOf("seaweedfs"), 8333), true);
    assert.equal(admits(policies, agent, podOf("collector"), 4318), true);
    const egress = (file: Record<string, unknown>) =>
      one<KubernetesObject & { spec: { policyTypes: string[]; egress: unknown[] } }>(install(file), "NetworkPolicy", "alasio-node-agent").spec;
    const dns = {
      to: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "kube-system" } }, podSelector: { matchLabels: { "k8s-app": "kube-dns" } } }],
      ports: [{ protocol: "UDP", port: 53 }, { protocol: "TCP", port: 53 }],
    };
    const selected = (component: string) => ({ podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": component } } });
    assert.deepEqual(egress({}).policyTypes, ["Ingress", "Egress"]);
    const metadata = { to: [selected("valkey")], ports: [{ protocol: "TCP", port: 6379 }] };
    assert.deepEqual(egress({}).egress, [
      dns,
      { to: [selected("seaweedfs")], ports: [{ protocol: "TCP", port: 8333 }] },
      metadata,
      { to: [selected("collector")], ports: [{ protocol: "TCP", port: 4318 }] },
    ]);
    // An operator's object store, on its endpoint's port at any address, and no collector to reach without one.
    const external = (endpoint: string) => ({ objectStore: { bundled: { enabled: false }, external: { endpoint, existingSecret: "s3", snapshotsSecret: "s3-snapshots" } }, lake: { enabled: false } });
    assert.deepEqual(egress(external("https://s3.example.com")).egress, [dns, { to: [{ ipBlock: { cidr: "0.0.0.0/0" } }], ports: [{ protocol: "TCP", port: 443 }] }, metadata]);
    assert.deepEqual(egress(external("http://minio.lan:9000")).egress, [dns, { to: [{ ipBlock: { cidr: "0.0.0.0/0" } }], ports: [{ protocol: "TCP", port: 9000 }] }, metadata]);
    assert.deepEqual(all(install(external("http://minio.lan:9000")), "NetworkPolicy").filter(({ metadata }) => metadata?.name === "alasio-seaweedfs-snapshots"), []);
  });

  test("are reached by the sandbox gateway, not alasio, which watches no pod", () => {
    for (const file of [{}, { workspaceStorage: { enabled: false } }, { sessions: { runtimeClassName: "" } }]) {
      const own = all<V1Role>(install(file), "Role").find(({ metadata }) => metadata?.name === "alasio" && metadata.namespace === "alasio")?.rules;
      assert.deepEqual(own, [{ apiGroups: [""], resources: ["pods"], verbs: ["list", "patch"] }], JSON.stringify(file));
    }
    const branchRules = all<V1Role>(branchObjects(configOf(), "try", BASELINE), "Role").filter(({ metadata }) => metadata?.name === "alasio").flatMap(({ rules }) => rules ?? []);
    assert.ok(!branchRules.some(({ resources, verbs }) => resources?.includes("pods") && verbs.includes("watch")));
  });
});

describe("security", () => {
  test("enforces Pod Security restricted in the sessions namespace, and, where the sandbox service's pools mount their directories, warns and audits of it, its pods held to the pools' templates instead", () => {
    const levels = (file: Record<string, unknown>) => {
      const namespaces = all<V1Namespace>(install(file), "Namespace");
      assert.equal(namespaces.length, 1);
      return Object.fromEntries(Object.entries(namespaces[0]?.metadata?.labels ?? {}).filter(([label]) => label.startsWith("pod-security.kubernetes.io/")));
    };
    assert.deepEqual(levels({}), {
      "pod-security.kubernetes.io/enforce": "privileged",
      "pod-security.kubernetes.io/audit": "restricted",
      "pod-security.kubernetes.io/audit-version": "latest",
      "pod-security.kubernetes.io/warn": "restricted",
      "pod-security.kubernetes.io/warn-version": "latest",
    });
    assert.equal(one(install(), "ValidatingAdmissionPolicy", "alasio-sandbox-pods").kind, "ValidatingAdmissionPolicy");
    assert.deepEqual(levels({ workspaceStorage: { enabled: false } }), {
      "pod-security.kubernetes.io/enforce": "restricted",
      "pod-security.kubernetes.io/enforce-version": "latest",
      "pod-security.kubernetes.io/warn": "restricted",
      "pod-security.kubernetes.io/warn-version": "latest",
    });
  });

  test("makes the privileged host namespace only with the host profile", () => {
    const namespaces = all<V1Namespace>(install({ host: { enabled: true } }), "Namespace");
    assert.equal(namespaces.length, 2);
    assert.equal(namespaces[1]?.metadata?.labels?.["pod-security.kubernetes.io/enforce"], "privileged");
  });

  test("denies everything in the sessions namespace but what the pools' policies admit, alasio reaching none of it and taking nothing of it", () => {
    const objects = install();
    const policy = (name: string) => one<KubernetesObject & { spec: Record<string, unknown> }>(objects, "NetworkPolicy", name).spec;
    assert.deepEqual(policy("default-deny")["policyTypes"], ["Ingress", "Egress"]);
    assert.deepEqual(policy("default-deny")["podSelector"], {});
    const inSessions = all<KubernetesObject>(objects, "NetworkPolicy").filter(({ metadata }) => metadata?.namespace === "alasio-sessions").map(({ metadata }) => metadata?.name).sort();
    assert.deepEqual(inSessions, ["alasio-sandboxes", "alasio-sandboxes-full-internet", "default-deny"]);
    // alasio takes none of its sessions' telemetry: no rule of its admits its port.
    const ports = ((policy("alasio-alasio")["ingress"] ?? []) as Array<{ ports?: Array<{ port: number }> }>).flatMap((rule) => rule.ports ?? []).map(({ port }) => port);
    assert.ok(!ports.includes(4318), String(ports));
  });

  test("keeps a session with internet off private, link-local and metadata addresses", () => {
    const policy = one<KubernetesObject & { spec: { podSelector: { matchLabels: Record<string, string> }; egress: Array<{ to: Array<{ ipBlock: { except: string[] } }> }> } }>(
      install(),
      "NetworkPolicy",
      "alasio-sandboxes-full-internet",
    );
    assert.equal(policy.spec.podSelector.matchLabels["alasio.dev/net-mode"], "full");
    const except = policy.spec.egress[0]?.to[0]?.ipBlock.except;
    for (const range of ["10.0.0.0/8", "169.254.0.0/16", "100.64.0.0/10"]) assert.ok(except?.includes(range), range);
  });

  test("renders no policies when they are turned off", () => {
    assert.deepEqual(all(install({ networkPolicies: { enabled: false } }), "NetworkPolicy"), []);
  });

  test("lets alasio label its own pod busy in its own namespace and nothing else, nothing of its sessions or folder instances, which are the sandbox gateway's", () => {
    const rolesOf = (objects: readonly KubernetesObject[]) =>
      Object.fromEntries(all<V1Role>(objects, "Role").filter((role) => role.metadata?.name === "alasio").map(({ metadata, rules }) => [metadata?.namespace, rules]));
    const own = [{ apiGroups: [""], resources: ["pods"], verbs: ["list", "patch"] }];
    const objects = install({ host: { enabled: true } });
    assert.deepEqual(rolesOf(objects), { alasio: own });
    const binding = one<V1RoleBinding>(objects.filter(({ metadata }) => metadata?.namespace === "alasio"), "RoleBinding", "alasio");
    assert.deepEqual(binding.subjects, [{ kind: "ServiceAccount", name: "alasio", namespace: "alasio" }]);
    assert.deepEqual(binding.roleRef, { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: "alasio" });
    // Of no cluster's, but what the gateway reads of the nodes is the gateway's.
    const main = install();
    assert.deepEqual(rolesOf(main), { alasio: own });
    const ofAlasio = ({ subjects }: V1RoleBinding) => subjects?.some(({ kind, name, namespace }) => kind === "ServiceAccount" && name === "alasio" && namespace === "alasio");
    assert.deepEqual(all<V1RoleBinding>(main, "ClusterRoleBinding").filter(ofAlasio), []);
    assert.deepEqual(one<V1ClusterRoleBinding>(main, "ClusterRoleBinding", "alasio-sandbox-gateway-nodes").subjects, [{ kind: "ServiceAccount", name: "alasio-sandbox-gateway", namespace: "alasio" }]);
    assert.deepEqual(one<V1ClusterRole>(main, "ClusterRole", "alasio-sandbox-gateway-nodes").rules, [{ apiGroups: [""], resources: ["nodes"], verbs: ["get"] }]);
    // No node agent's token, nor JuiceFS's mount pods, which are the gateway's and the nodes' mount's.
    assert.equal(container(one<V1Deployment>(main, "Deployment", "alasio")).env?.find(({ name }) => name === "ALASIO_NODE_AGENT_TOKEN"), undefined);
    assert.deepEqual(all<V1RoleBinding>(main, "RoleBinding").filter((each) => ofAlasio(each) && each.metadata?.namespace !== "alasio").map(({ metadata }) => metadata?.namespace), []);
    // Its templates are alasio up's to render, not alasio's to write: no role of its own lets it write a ConfigMap.
    const bound = all<V1RoleBinding>(objects, "RoleBinding").filter(ofAlasio).map(({ metadata, roleRef }) => `${metadata?.namespace}/${roleRef.name}`);
    const alasioRoles = all<V1Role>(objects, "Role").filter(({ metadata }) => bound.includes(`${metadata?.namespace}/${metadata?.name}`));
    assert.ok(alasioRoles.length > 0);
    assert.ok(!alasioRoles.some(({ rules }) => rules?.some(({ resources, verbs }) => resources?.includes("configmaps") && verbs.some((verb) => verb !== "get" && verb !== "list" && verb !== "watch"))));
  });

  test("lets the host profile's agents restart alasio's own Deployment and nothing else", () => {
    const role = one<V1Role>(install({ host: { enabled: true } }), "Role", "alasio-restart");
    assert.deepEqual(role.rules?.[0], { apiGroups: ["apps"], resources: ["deployments"], resourceNames: ["alasio"], verbs: ["get", "patch"] });
  });
});

describe("branch environments", () => {
  const branch = branchObjects(configOf(), "try", BASELINE);
  const variable = (workload: V1Deployment, name: string) => container(workload).env?.find((env) => env.name === name)?.value;
  /** An object of the branch's, of `kind` and `name`, in `namespace`. */
  const of = <T extends KubernetesObject>(kind: string, name: string, namespace = "alasio-branch-try") =>
    one<T>(branch.filter(({ metadata }) => metadata?.namespace === namespace || (kind === "Namespace" && metadata?.name === name)), kind, name);

  test("are an alasio, a compute and a lake's loader in a namespace of the branch's own, its sessions in another, everything labelled with the branch", () => {
    assert.deepEqual(all<V1Namespace>(branch, "Namespace").map(({ metadata }) => [metadata?.name, metadata?.labels?.["alasio.dev/branch"]]), [
      ["alasio-branch-try", "try"],
      ["alasio-branch-try-sessions", "try"],
    ]);
    // Its pods held to the sandbox pools' templates, as main's are (./admission.ts sandboxPodPolicy).
    assert.equal(of<V1Namespace>("Namespace", "alasio-branch-try-sessions").metadata?.labels?.["pod-security.kubernetes.io/enforce"], "privileged");
    assert.equal(of<V1Namespace>("Namespace", "alasio-branch-try-sessions").metadata?.labels?.["pod-security.kubernetes.io/warn"], "restricted");
    // Its sessions' namespace records main its parent, as the sandbox gateway forks main's sessions into it for that alone; its alasio's, and main's, record none.
    assert.deepEqual(all<V1Namespace>(branch, "Namespace").map(({ metadata }) => metadata?.labels?.["alasio.dev/branch-parent"]), [undefined, "main"]);
    assert.deepEqual(all<V1Namespace>(install(), "Namespace").map(({ metadata }) => metadata?.labels?.["alasio.dev/branch-parent"]).filter((parent) => parent !== undefined), []);
    assert.deepEqual(branch.filter(({ metadata }) => metadata?.labels?.["alasio.dev/branch"] !== "try"), []);
    // Nothing of the cluster's.
    assert.deepEqual(branch.filter(({ kind, metadata }) => kind !== "Namespace" && !metadata?.namespace?.startsWith("alasio-branch-try")).map(({ kind, metadata }) => [kind, metadata?.name]), []);
    // Its sessions' pools beside, the sandbox service's (main's binds in them).
    assert.deepEqual(all<V1Deployment>(branch, "Deployment").map(({ metadata }) => metadata?.name).sort(), ["alasio", "alasio-lake-loader", "alasio-neon-compute", "alasio-neon-compute-ro", "sandboxes-full", "sandboxes-none"]);
    // Named as main's are, as each namespace is the branch's own.
    assert.deepEqual(all<V1Service>(branch, "Service").map(({ metadata }) => metadata?.name).sort(), ["alasio-neon-compute", "alasio-neon-compute-ro"]);
  });

  test("run an alasio that knows it is the branch, asks main to fork what it inherited with its token, on the log, and sends its telemetry tagged with the branch", () => {
    const alasio = of<V1Deployment>("Deployment", "alasio");
    assert.equal(variable(alasio, "ALASIO_BRANCH"), "try");
    assert.equal(variable(alasio, "ALASIO_BRANCH_FORK_TOKEN_FILE"), "/run/alasio/branch/token");
    assert.deepEqual(alasio.spec?.template.spec?.volumes?.find(({ name }) => name === "branch"), { name: "branch", secret: { secretName: "alasio-branch-fork", items: [{ key: "token", path: "token" }] } });
    // Its sessions' telemetry is the sandbox service's receiver's, never its own.
    assert.equal(variable(alasio, "ALASIO_TELEMETRY_RECEIVER_SERVICE"), undefined);
    assert.equal(container(alasio).ports, undefined);
    // Its sessions through main's gateway, as the branch, by the branch's own key of the gateway's, in its own namespace.
    assert.equal(variable(alasio, "ALASIO_GATEWAY_URL"), "http://alasio-sandbox-gateway.alasio.svc.cluster.local:8080");
    assert.equal(variable(alasio, "ALASIO_GATEWAY_ENVIRONMENT"), "try");
    assert.deepEqual(container(alasio).env?.find(({ name }) => name === "ALASIO_GATEWAY_CALLER_KEY")?.valueFrom, { secretKeyRef: { name: "alasio-sessions-caller", key: "caller-key" } });
    assert.equal(variable(alasio, "ALASIO_BRANCH_FORK_KEY_FILE"), undefined);
    // It makes no role for what reads main's Neon beside it, which a branch has none of.
    assert.equal(variable(alasio, "ALASIO_LAKE_READER_VERIFIER_FILE"), undefined);
    assert.equal(variable(alasio, "ALASIO_GRAFANA_VERIFIER_FILE"), undefined);
    assert.deepEqual(alasio.spec?.template.spec?.volumes?.find(({ name }) => name === "database")?.secret?.items?.map(({ key }) => key), ["url", "lake-verifier", "harness-verifier"]);
    // Its sandboxes' telemetry is tagged by main's receiver, as each binding's token names its namespace.
    for (const workload of all<V1Deployment>(branch, "Deployment").filter(({ metadata }) => !metadata?.name?.startsWith("sandboxes-"))) {
      assert.equal(variable(workload, "OTEL_RESOURCE_ATTRIBUTES"), "alasio.branch=try", workload.metadata?.name);
    }
    const tagged = branchObjects(configOf({ telemetry: { resourceAttributes: "deployment.environment.name=prod" } }), "try", BASELINE);
    assert.equal(variable(one<V1Deployment>(tagged, "Deployment", "alasio"), "OTEL_RESOURCE_ATTRIBUTES"), "deployment.environment.name=prod,alasio.branch=try");
  });

  test("have an alasio principal on the log that reads its own topics in its groups, writes its wake topic, and asks main to fork its sessions on main's forks topic, which it reads not, beside topics of their own alone", () => {
    const access = environmentAccess("try");
    assert.deepEqual(access.principals.find(({ name }) => name === "alasio-try")?.acls, [
      { resource: "topic", name: "telegram.try", pattern: "literal", operations: ["read", "describe"] },
      { resource: "group", name: "alasio.try", pattern: "literal", operations: ["read"] },
      { resource: "topic", name: "lake.try", pattern: "literal", operations: ["write", "describe"] },
      { resource: "topic", name: "events.try", pattern: "literal", operations: ["read", "describe"] },
      { resource: "group", name: "alasio-events.try", pattern: "literal", operations: ["read"] },
      { resource: "topic", name: "forks.main", pattern: "literal", operations: ["write", "describe"] },
    ]);
    // Main's forks topic is main's, made and forgotten with it alone.
    assert.deepEqual(access.topics.map(({ name }) => name), ["telegram.try", "lake.try", "events.try"]);
  });

  test("make no session of its alasio's, its sessions main's sandbox service's in the branch's own namespace", () => {
    const roles = all<V1Role>(branch, "Role");
    assert.deepEqual(roles.map(({ metadata }) => [metadata?.namespace, metadata?.name]), [
      // Its own pod, which it labels busy as main's does.
      ["alasio-branch-try", "alasio"],
      // main's neon-control, which wakes its compute and puts it to sleep.
      ["alasio-branch-try", "alasio-neon-control"],
      // main's sandbox gateway and receiver, which bind in its pools and watch their Leases.
      ["alasio-branch-try-sessions", "alasio-sandbox-gateway"],
      ["alasio-branch-try-sessions", "alasio-receiver"],
    ]);
    assert.deepEqual(of<V1Role>("Role", "alasio", "alasio-branch-try").rules, [{ apiGroups: [""], resources: ["pods"], verbs: ["list", "patch"] }]);
  });

  test("run a compute on the branch's timeline, from main's neon-control, which alone scales it, and a lake's loader woken on the branch's wake topic, and never maintained", () => {
    const compute = of<V1Deployment>("Deployment", "alasio-neon-compute");
    const args = container(compute).args ?? [];
    assert.equal(args[args.indexOf("--compute-id") + 1], "branch-try");
    assert.equal(args[args.indexOf("--control-plane-uri") + 1], "http://alasio-neon-control.alasio.svc:8080");
    const lake = of<V1Deployment>("Deployment", "alasio-lake-loader");
    assert.deepEqual([variable(lake, "LAKE_WAKE_TOPIC"), variable(lake, "LAKE_WAKE_GROUP")], ["lake.try", "lake-loader.try"]);
    assert.deepEqual(container(lake).env?.find(({ name }) => name === "LOG_USERNAME")?.valueFrom?.secretKeyRef, { name: "alasio-log-loader", key: "username" });
    assert.equal(variable(of<V1Deployment>("Deployment", "alasio"), "ALASIO_LAKE_WAKE_TOPIC"), "lake.try");
    assert.deepEqual(of<ScaledObject>("ScaledObject", "alasio-lake-loader").spec.triggers.map(({ metadata }) => [metadata["consumerGroup"], metadata["topic"]]), [["lake-loader.try", "lake.try"]]);
    // KEDA reads it as the scaler, from the Secret's copy in the branch's namespace.
    assert.ok(of("TriggerAuthentication", "alasio-lake-loader"));
    // Nor has it maintenance, readers, as it has no Grafana, or an intake: what it sends is written to main's lake.
    assert.deepEqual(branch.filter(({ kind }) => kind === "CronJob"), []);
    assert.deepEqual(branch.filter(({ metadata }) => /lake-(query|intake|maintenance|merge)/u.test(metadata?.name ?? "")), []);
    assert.deepEqual(branch.filter(({ metadata }) => metadata?.name?.includes("grafana")), []);
    // Its compute, through main's Neon proxy.
    assert.deepEqual([variable(lake, "LAKE_DATABASE_HOST"), variable(lake, "LAKE_DATABASE_PORT"), variable(lake, "LAKE_DATABASE_ENDPOINT")], ["alasio-neon-proxy.alasio.svc", "5432", "branch-try"]);
    assert.equal(compute.spec?.replicas, undefined);
    assert.deepEqual(of<V1Role>("Role", "alasio-neon-control").rules, [
      { apiGroups: ["apps"], resources: ["deployments"], resourceNames: ["alasio-neon-compute", "alasio-neon-compute-ro"], verbs: ["get", "patch"] },
      { apiGroups: ["apps"], resources: ["deployments/scale"], resourceNames: ["alasio-neon-compute", "alasio-neon-compute-ro"], verbs: ["patch"] },
      { apiGroups: [""], resources: ["pods"], verbs: ["list", "patch"] },
      { apiGroups: ["discovery.k8s.io"], resources: ["endpointslices"], verbs: ["list"] },
    ]);
    assert.deepEqual(of<V1RoleBinding>("RoleBinding", "alasio-neon-control").subjects, [{ kind: "ServiceAccount", name: "alasio-neon-control", namespace: "alasio" }]);
    assert.equal(variable(lake, "LAKE_S3_ENDPOINT"), "http://alasio-seaweedfs.alasio.svc:8333");
    const main = install();
    assert.equal(container(one<V1Deployment>(main, "Deployment", "alasio-neon-compute")).args?.at(-3), "alasio");
    const control = one<V1Deployment>(main, "Deployment", "alasio-neon-control");
    assert.deepEqual([variable(control, "NEON_PAGESERVERS"), variable(control, "NEON_SHARDS")], ["1", "1"]);
  });

  test("run a read replica of the branch's compute, which its lake's loader loads from", () => {
    const replica = of<V1Deployment>("Deployment", "alasio-neon-compute-ro");
    const args = container(replica).args ?? [];
    assert.equal(args[args.indexOf("--compute-id") + 1], "branch-try-ro");
    assert.equal(replica.spec?.replicas, undefined);
    assert.equal(variable(of<V1Deployment>("Deployment", "alasio-lake-loader"), "LAKE_SOURCE_ENDPOINT"), "branch-try-ro");
    assert.ok(of<KubernetesObject>("NetworkPolicy", "alasio-neon-compute-ro"));
  });

  test("confine the branch's sessions to what the pools' policies admit, its alasio reached by nothing, and have a policy of its compute's", () => {
    const policy = (name: string, namespace: string) => of<KubernetesObject & { spec: Record<string, unknown> }>("NetworkPolicy", name, namespace).spec;
    assert.deepEqual(policy("default-deny", "alasio-branch-try-sessions")["podSelector"], {});
    assert.deepEqual(all<KubernetesObject>(branch, "NetworkPolicy").filter(({ metadata }) => metadata?.namespace === "alasio-branch-try-sessions").map(({ metadata }) => metadata?.name).sort(), ["alasio-sandboxes", "alasio-sandboxes-full-internet", "default-deny"]);
    // Its alasio takes no connection, its sessions' telemetry the sandbox service's receiver's.
    assert.equal(policy("alasio-alasio", "alasio-branch-try")["ingress"], undefined);
    assert.ok(policy("alasio-neon-compute", "alasio-branch-try"));
  });

  test("reach none of main's compute, alasio or lake, as the policies of both admit them, asking main to fork their sessions on the log", () => {
    const main = install();
    const policies = [...all<NetworkPolicyObject>(main, "NetworkPolicy"), ...all<NetworkPolicyObject>(branch, "NetworkPolicy")];
    const namespaces = new Map(all<V1Namespace>(branch, "Namespace").map(({ metadata }) => [metadata?.name ?? "", metadata?.labels ?? {}]));
    /** The pods of `objects`' Deployment `name`, as the cluster labels them and their namespace. */
    const podOf = (objects: readonly KubernetesObject[], name: string, namespace: string): Pod => {
      const deployment = all<V1Deployment>(objects, "Deployment").find(({ metadata }) => metadata?.name === name && metadata.namespace === namespace);
      assert.ok(deployment, `${namespace}/${name}`);
      return { namespace, namespaceLabels: { ...namespaces.get(namespace), "kubernetes.io/metadata.name": namespace }, labels: deployment.spec?.template.metadata?.labels ?? {} };
    };
    const branchPods = {
      alasio: podOf(branch, "alasio", "alasio-branch-try"),
      compute: podOf(branch, "alasio-neon-compute", "alasio-branch-try"),
      lake: podOf(branch, "alasio-lake-loader", "alasio-branch-try"),
      session: { namespace: "alasio-branch-try-sessions", namespaceLabels: { ...namespaces.get("alasio-branch-try-sessions"), "kubernetes.io/metadata.name": "alasio-branch-try-sessions" }, labels: { "alasio.dev/workload": "session" } },
    };
    const targets: [string, Pod, number][] = [
      ["main's compute", podOf(main, "alasio-neon-compute", "alasio"), 55433],
      ["main's alasio on 4318", podOf(main, "alasio", "alasio"), 4318],
      [`main's lake's readers`, podOf(main, "alasio-lake-query", "alasio"), 8090],
      [`main's lake's loader`, podOf(main, "alasio-lake-loader", "alasio"), 9464],
    ];
    const reached = Object.entries(branchPods).flatMap(([from, pod]) =>
      targets.filter(([, target, port]) => admits(policies, pod, target, port)).map(([to]) => `${from} → ${to}`)
    );
    assert.deepEqual(reached, []);
  });

  test("reach every compute through main's Neon proxy alone, which with main's neon-control alone reaches one, and main's collector's scraper main's metrics alone", () => {
    const main = install({ grafana: { enabled: true } });
    const policies = [...all<NetworkPolicyObject>(main, "NetworkPolicy"), ...all<NetworkPolicyObject>(branch, "NetworkPolicy")];
    const namespaces = new Map(all<V1Namespace>(branch, "Namespace").map(({ metadata }) => [metadata?.name ?? "", metadata?.labels ?? {}]));
    const podIn = (namespace: string, labels: Readonly<Record<string, string>> = {}): Pod => ({ namespace, namespaceLabels: { ...namespaces.get(namespace), "kubernetes.io/metadata.name": namespace }, labels });
    const podOf = (objects: readonly KubernetesObject[], name: string, namespace: string): Pod => {
      const deployment = all<V1Deployment>(objects, "Deployment").find(({ metadata }) => metadata?.name === name && metadata.namespace === namespace);
      assert.ok(deployment, `${namespace}/${name}`);
      return podIn(namespace, deployment.spec?.template.metadata?.labels);
    };
    const clients: [string, Pod][] = [
      ["main's alasio", podOf(main, "alasio", "alasio")],
      ["main's lake's loader", podOf(main, "alasio-lake-loader", "alasio")],
      ["main's lake's readers", podOf(main, "alasio-lake-query", "alasio")],
      ["main's lake's maintenance", podIn("alasio", one<V1CronJob>(main, "CronJob", "alasio-lake-maintenance").spec?.jobTemplate.spec?.template.metadata?.labels)],
      ["main's lake intake", podOf(main, "alasio-lake-intake", "alasio")],
      ["main's Grafana", podOf(main, "alasio-grafana", "alasio")],
      ["main's backup", podIn("alasio", one<V1CronJob>(main, "CronJob", "alasio-neon-backup").spec?.jobTemplate.spec?.template.metadata?.labels)],
      ["the branch's alasio", podOf(branch, "alasio", "alasio-branch-try")],
      ["the branch's lake's loader", podOf(branch, "alasio-lake-loader", "alasio-branch-try")],
    ];
    const proxy = podOf(main, "alasio-neon-proxy", "alasio");
    const control = podOf(main, "alasio-neon-control", "alasio");
    const computes: [string, Pod][] = [
      ["main's compute", podOf(main, "alasio-neon-compute", "alasio")],
      ["main's read replica", podOf(main, "alasio-neon-compute-ro", "alasio")],
      ["the branch's compute", podOf(branch, "alasio-neon-compute", "alasio-branch-try")],
      ["the branch's read replica", podOf(branch, "alasio-neon-compute-ro", "alasio-branch-try")],
    ];
    for (const [client, pod] of clients) {
      assert.ok(admits(policies, pod, proxy, 5432), `${client} → the proxy`);
      for (const [name, compute] of computes) assert.ok(!admits(policies, pod, compute, 55433), `${client} → ${name}`);
    }
    for (const [name, compute] of computes) {
      assert.ok(admits(policies, proxy, compute, 55433), `the proxy → ${name}`);
      assert.ok(admits(policies, control, compute, 55433), `neon-control → ${name}`);
      // compute_ctl's port, where neon-control gives a running compute its spec anew.
      assert.ok(admits(policies, control, compute, 3080), `neon-control → ${name}'s compute_ctl`);
      assert.ok(!admits(policies, proxy, compute, 3080), `the proxy → ${name}'s compute_ctl`);
    }
    const scraper = podOf(main, "alasio-collector-scraper", "alasio");
    assert.deepEqual(computes.filter(([, compute]) => admits(policies, scraper, compute, 3080)).map(([name]) => name), ["main's compute", "main's read replica"]);
    // Sessions reach no database at all.
    assert.ok(!admits(policies, podIn("alasio-branch-try-sessions", { "alasio.dev/workload": "session" }), proxy, 5432));
  });

  test("are admitted by main to what they share of its storage alone, and have their sessions forked by main alone, which is given the key their tokens are signed with", () => {
    const main = install();
    const policy = (name: string) => one<KubernetesObject & { spec: Record<string, unknown> }>(main, "NetworkPolicy", name).spec;
    const branches = { matchExpressions: [{ key: "alasio.dev/branch", operator: "Exists" }] };
    assert.deepEqual(policy("alasio-neon-branches"), {
      podSelector: {
        matchLabels: { "app.kubernetes.io/instance": "alasio" },
        matchExpressions: [{ key: "app.kubernetes.io/component", operator: "In", values: ["neon-pageserver", "neon-safekeeper", "neon-control", "neon-proxy", "seaweedfs"] }],
      },
      policyTypes: ["Ingress"],
      ingress: [{
        from: [{ namespaceSelector: branches, podSelector: { matchLabels: { "app.kubernetes.io/instance": "alasio", "alasio.dev/stack": "neon" } } }],
        ports: [6400, 5454, 8080, 5432, 8333].map((port) => ({ protocol: "TCP", port })),
      }],
    });
    // alasio takes no connections, theirs asking main's to fork their sessions on the log.
    assert.equal(policy("alasio-alasio")["ingress"], undefined);
    const alasio = one<V1Deployment>(main, "Deployment", "alasio");
    assert.equal(container(alasio).env?.find(({ name }) => name === "ALASIO_BRANCH_FORK_KEY_FILE")?.value, "/run/alasio/branch/fork-key");
    assert.deepEqual(alasio.spec?.template.spec?.volumes?.find(({ name }) => name === "branch"), {
      name: "branch",
      secret: { secretName: "alasio-branches", items: [{ key: "fork-key", path: "fork-key" }], optional: true },
    });
    assert.equal(container(alasio).ports, undefined);
    // Where main forks no session, it is given no key.
    const unforked = install({ workspaceStorage: { enabled: false } });
    assert.equal(container(one<V1Deployment>(unforked, "Deployment", "alasio")).env?.find(({ name }) => name === "ALASIO_BRANCH_FORK_KEY_FILE"), undefined);
  });
});

/** Every pod spec of `objects`, by what makes it: its workload's, Job's or CronJob's name, or the folder template's, of the folder instances the gateway makes. */
function podSpecs(objects: readonly KubernetesObject[]): [string, V1PodSpec][] {
  const workloads = [...all<V1Deployment>(objects, "Deployment"), ...all<V1StatefulSet>(objects, "StatefulSet"), ...all<V1DaemonSet>(objects, "DaemonSet"), ...all<V1Job>(objects, "Job")];
  const gateway = all<V1ConfigMap>(objects, "ConfigMap").find(({ metadata }) => metadata?.name === "alasio-sandbox-gateway");
  const parsed = JSON.parse(gateway?.data?.["config.json"] ?? "{}") as { folders?: { template: { spec: V1PodSpec } } | null };
  return [
    ...workloads.map(({ metadata, spec }): [string, V1PodSpec] => [metadata?.name ?? "", spec?.template.spec ?? { containers: [] }]),
    ...all<V1CronJob>(objects, "CronJob").map(({ metadata, spec }): [string, V1PodSpec] => [metadata?.name ?? "", spec?.jobTemplate.spec?.template.spec ?? { containers: [] }]),
    ...(parsed.folders ? [["folder-bayma", parsed.folders.template.spec] as [string, V1PodSpec]] : []),
  ];
}

describe("scaling", () => {
  test("runs every pod under a PriorityClass: the floor's and what runs turns given up last, of one value, so neither takes the other's place, and the floor its claims hold to a node above both; the headroom's first and preempting nothing", () => {
    const objects = install({ host: { enabled: true } });
    const classes = new Map(all<V1PriorityClass>(objects, "PriorityClass").map((each) => [each.metadata?.name ?? "", each]));
    const value = (name: string) => classes.get(name)?.value ?? Number.NaN;
    assert.equal(value("alasio-floor"), value("alasio-turn"));
    assert.ok(value("alasio-bound") > value("alasio-floor"));
    assert.ok(value("alasio-turn") > value("alasio-component") && value("alasio-component") > value("alasio-headroom"));
    assert.deepEqual([...classes.values()].filter(({ preemptionPolicy }) => preemptionPolicy === "Never").map(({ metadata }) => metadata?.name), ["alasio-headroom"]);
    const tiers = new Map(podSpecs(objects).map(([name, spec]) => [name, spec.priorityClassName]));
    for (const [name, priorityClassName] of [...tiers, ...podSpecs(branchObjects(configOf(), "try", BASELINE)).map(([name, spec]) => [name, spec.priorityClassName])]) {
      assert.ok(priorityClassName && (classes.has(priorityClassName) || priorityClassName.startsWith("system-")), name);
    }
    assert.deepEqual(["alasio-seaweedfs", "alasio-grafana", "alasio-collector", "alasio-lake-query"].map((name) => tiers.get(name)), Array(4).fill("alasio-floor"));
    // The lake's loader sleeps, and its maintenance waits a pass.
    assert.deepEqual(["alasio-lake-loader", "alasio-lake-maintenance", "alasio-lake-merge"].map((name) => tiers.get(name)), Array(3).fill("alasio-component"));
    // Above the floor, exactly its StatefulSets whose claims hold them to a node; a branch has none.
    const held = (of: readonly KubernetesObject[]) =>
      all<V1StatefulSet>(of, "StatefulSet").filter(({ spec }) => (spec?.volumeClaimTemplates?.length ?? 0) > 0).map(({ metadata }) => metadata?.name).sort();
    assert.deepEqual(held(objects), [
      "alasio-log",
      "alasio-neon-controller-db",
      "alasio-neon-pageserver",
      "alasio-neon-safekeeper",
      "alasio-seaweedfs-filer-store",
      "alasio-seaweedfs-master",
      "alasio-seaweedfs-volume",
      "alasio-valkey",
    ]);
    assert.deepEqual([...tiers].filter(([, priorityClassName]) => priorityClassName === "alasio-bound").map(([name]) => name).sort(), held(objects));
    assert.deepEqual(held(branchObjects(configOf(), "try", BASELINE)), []);
    // Main's compute is alasio's database, which every turn needs.
    assert.equal(tiers.get("alasio-neon-compute"), "alasio-floor");
    // Its read replica serves the lake's loads, which may wait.
    assert.equal(tiers.get("alasio-neon-compute-ro"), "alasio-component");
    // The Jobs alasio up waits for are of the floor, taking what sleeps' place where the node is full; a backup waits.
    assert.deepEqual(["alasio-neon-setup", "alasio-vpa-certgen", "alasio-neon-backup"].map((name) => tiers.get(name)), ["alasio-floor", "alasio-floor", "alasio-component"]);
    assert.deepEqual(["alasio", "sandboxes-none", "sandboxes-full", "folder-bayma"].map((name) => tiers.get(name)), Array(4).fill("alasio-turn"));
    // A branch's alasio runs turns; its lake's loader and its compute sleep.
    const branch = new Map(podSpecs(branchObjects(configOf(), "try", BASELINE)).map(([name, spec]) => [name, spec.priorityClassName]));
    assert.deepEqual(["alasio", "alasio-lake-loader", "alasio-neon-compute"].map((name) => branch.get(name)), ["alasio-turn", "alasio-component", "alasio-component"]);
  });

  test("lets alasio's pods take others' places only on a cluster it makes, unless told, and keeps no headroom where they may not", () => {
    const policies = (objects: readonly KubernetesObject[]) =>
      all<V1PriorityClass>(objects, "PriorityClass").map(({ metadata, preemptionPolicy }) => [metadata?.name, preemptionPolicy]);
    assert.deepEqual(policies(install()), [
      ["alasio-bound", "PreemptLowerPriority"],
      ["alasio-floor", "PreemptLowerPriority"],
      ["alasio-turn", "PreemptLowerPriority"],
      ["alasio-component", "PreemptLowerPriority"],
      ["alasio-headroom", "Never"],
    ]);
    assert.deepEqual(new Set(policies(install({ autoscaling: { preemption: false } })).map(([, policy]) => policy)), new Set(["Never"]));
    const ofTarget = (target: "docker" | "kubeconfig", given: Record<string, unknown> = {}) => {
      const config = installConfigOf({ autoscaling: given }, { claude: false, target });
      return Result.isSuccess(config) ? config.success.autoscaling.preemption : config.failure;
    };
    assert.deepEqual([ofTarget("docker"), ofTarget("kubeconfig"), ofTarget("kubeconfig", { preemption: true })], [true, false, true]);
    const refused = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, autoscaling: { preemption: false, headroom: { scaling: { min: 1, max: 1 } } } });
    assert.ok(Result.isFailure(refused) && refused.failure.message === "autoscaling.headroom.scaling must be 0 when autoscaling.preemption is false, as no pod takes a placeholder's place then");
  });

  test("sizes every workload by a VerticalPodAutoscaler beside it: in place what keeps no state and runs no turn, as it is made alasio's pod and the log, the rest recommended alone, from a small floor to half a core and its memory limit", () => {
    const objects = [...install({ host: { enabled: true } }), ...branchObjects(configOf(), "try", BASELINE)];
    const workloads = [...all<V1Deployment>(objects, "Deployment"), ...all<V1StatefulSet>(objects, "StatefulSet"), ...all<V1DaemonSet>(objects, "DaemonSet")];
    const sized = all<VerticalPodAutoscaler>(objects, "VerticalPodAutoscaler");
    const key = ({ metadata }: KubernetesObject) => `${metadata?.namespace}/${metadata?.name}`;
    assert.deepEqual(sized.map(key).sort(), workloads.map(key).sort());
    const modes = new Map(sized.map((vpa) => [key(vpa), vpa.spec.updatePolicy.updateMode]));
    // Sized as each pod is made: alasio's pod and the log, which come to use more than they are given.
    const atStart = ["alasio", "alasio-log"];
    // Recommended alone: what keeps state, and the sandbox pools, whose snapshots restore onto their templates, among them.
    const recommended = [
      "alasio-neon-compute",
      "alasio-neon-compute-ro",
      "alasio-neon-storage-controller",
      "alasio-neon-safekeeper",
      "alasio-neon-pageserver",
      "alasio-neon-controller-db",
      "alasio-neon-control",
      "alasio-node-agent",
      "alasio-seaweedfs-master",
      "alasio-seaweedfs-volume",
      "alasio-seaweedfs-filer-store",
      "alasio-valkey",
      "sandboxes-none",
      "sandboxes-full",
      "alasio-workspace-mount",
    ];
    const modeOf = (name: string) => (atStart.includes(name) ? "Initial" : recommended.includes(name) ? "Off" : "InPlace");
    for (const [name, mode] of modes) assert.equal(mode, modeOf(name.split("/")[1] ?? ""), name);
    // A branch's alasio runs turns as main's does.
    assert.equal(modes.get("alasio-branch-try/alasio"), "Initial");
    // Of the memory alone of what sleeps, woken from none: the lakes' loaders and the intake.
    const sleeping = ["alasio-lake-loader", "alasio-lake-intake"];
    for (const vpa of sized) {
      const workload = workloads.find((each) => key(each) === key(vpa));
      assert.deepEqual(vpa.spec.targetRef, { apiVersion: workload?.apiVersion, kind: workload?.kind, name: workload?.metadata?.name });
      assert.equal(vpa.spec.updatePolicy.minReplicas, vpa.spec.updatePolicy.updateMode === "InPlace" ? 1 : undefined);
      assert.deepEqual(vpa.spec.resourcePolicy.containerPolicies, (workload?.spec?.template.spec?.containers ?? []).map(({ name, resources }) => ({
        containerName: name,
        ...(sleeping.includes(workload?.metadata?.name ?? "") ? { controlledResources: ["memory"] } : {}),
        minAllowed: { cpu: "5m", memory: "16Mi" },
        maxAllowed: { cpu: "500m", ...(resources?.limits?.["memory"] ? { memory: resources.limits["memory"] } : {}) },
        controlledValues: "RequestsOnly",
      })));
    }
  });

  test("lets no busy pod of alasio's be evicted, in its namespace, and tells alasio its pod's name to label", () => {
    const objects = [...install({ host: { enabled: true } }), ...branchObjects(configOf(), "try", BASELINE)];
    const budgets = all<KubernetesObject & { spec: object }>(objects, "PodDisruptionBudget").filter(({ metadata }) => metadata?.name === "alasio-busy");
    assert.deepEqual(budgets.map(({ metadata }) => metadata?.namespace).sort(), ["alasio", "alasio-branch-try"]);
    // But one not ready, whose label a turn that may never end holds.
    for (const { spec } of budgets) assert.deepEqual(spec, { maxUnavailable: 0, selector: { matchLabels: { "alasio.dev/busy": "true" } }, unhealthyPodEvictionPolicy: "AlwaysAllow" });
    assert.deepEqual(container(one<V1Deployment>(install(), "Deployment", "alasio")).env?.find(({ name }) => name === "ALASIO_POD"), {
      name: "ALASIO_POD",
      valueFrom: { fieldRef: { fieldPath: "metadata.name" } },
    });
  });

  test("lets alasio change no more of a pod than its busy label and annotations, main's and each branch's alasio", () => {
    const objects = install();
    const policy = one<KubernetesObject & { spec: { matchConditions: { expression: string }[]; validations: { expression: string }[] } }>(objects, "ValidatingAdmissionPolicy", "alasio-busy-label");
    const expression = policy.spec.matchConditions[0]?.expression ?? "";
    const accounts = new RegExp(expression.slice(expression.indexOf("'") + 1, expression.lastIndexOf("'")), "u");
    assert.deepEqual(
      ["system:serviceaccount:alasio:alasio", "system:serviceaccount:alasio-branch-try:alasio", "system:serviceaccount:alasio:alasio-collector", "system:serviceaccount:kube-system:alasio"].map((user) => accounts.test(user)),
      [true, true, false, false],
    );
    assert.deepEqual(policy.spec.validations.map(({ expression }) => expression.split(" ")[0]), [
      "object.spec",
      "object.metadata.?finalizers.orValue([])",
      "request.userInfo.username",
      "variables.newLabels.all(key,",
      "variables.newAnnotations.all(key,",
    ]);
    // In its own namespace, its own pods alone.
    assert.equal(
      policy.spec.validations[2]?.expression,
      'request.userInfo.username != "system:serviceaccount:" + request.namespace + ":alasio" || ("app.kubernetes.io/component" in variables.oldLabels && variables.oldLabels["app.kubernetes.io/component"] == "alasio")',
    );
    assert.ok(policy.spec.validations[1]?.expression.includes("object.metadata.?ownerReferences.orValue([]) == oldObject.metadata.?ownerReferences.orValue([])"));
    assert.ok(policy.spec.validations[3]?.expression.includes('key in ["alasio.dev/busy"]'));
    assert.deepEqual(one<KubernetesObject & { spec: object }>(objects, "ValidatingAdmissionPolicyBinding", "alasio-busy-label").spec, { policyName: "alasio-busy-label", validationActions: ["Deny"] });
    // The cluster's, made with main's alone.
    assert.deepEqual(all(branchObjects(configOf(), "try", BASELINE), "ValidatingAdmissionPolicy"), []);
  });

  test("lets neon-control change no more of a compute's Deployment than its pod template's restartedAt, which restarts it", () => {
    type Policy = KubernetesObject & {
      spec: { matchConstraints: { resourceRules: object[] }; matchConditions: { expression: string }[]; validations: { expression: string }[]; variables: { name: string; expression: string }[] };
    };
    const policy = one<Policy>(install(), "ValidatingAdmissionPolicy", "alasio-compute-restarts");
    assert.deepEqual(policy.spec.matchConstraints.resourceRules, [{ apiGroups: ["apps"], resources: ["deployments"], operations: ["UPDATE"], apiVersions: ["*"] }]);
    assert.deepEqual(policy.spec.matchConditions.map(({ expression }) => expression), ['request.userInfo.username == "system:serviceaccount:alasio:alasio-neon-control"']);
    const [pod, spec, metadata, labelled, annotated] = policy.spec.validations.map(({ expression }) => expression);
    assert.equal(pod, "object.spec.template.spec == oldObject.spec.template.spec");
    for (const field of ["replicas", "selector", "strategy", "minReadySeconds", "revisionHistoryLimit", "paused", "progressDeadlineSeconds"]) {
      assert.ok(spec?.includes(`(!has(object.spec.${field}) || object.spec.${field} == oldObject.spec.${field})`), field);
    }
    assert.ok(metadata?.includes("object.metadata.?annotations.orValue({}) == oldObject.metadata.?annotations.orValue({})"));
    for (const [field, none] of [["labels", "{}"], ["finalizers", "[]"], ["ownerReferences", "[]"], ["name", '""'], ["generateName", '""']]) {
      assert.ok(labelled?.includes(`object.spec.template.metadata.?${field}.orValue(${none}) == oldObject.spec.template.metadata.?${field}.orValue(${none})`), field);
    }
    assert.ok(annotated?.includes('key in ["kubectl.kubernetes.io/restartedAt"]'));
    assert.deepEqual(policy.spec.variables.map(({ expression }) => expression), [
      "oldObject.spec.template.metadata.?annotations.orValue({})",
      "object.spec.template.metadata.?annotations.orValue({})",
    ]);
    assert.deepEqual(all(install({ neon: { enabled: false, external: { existingSecret: "db" } }, workspaceStorage: { enabled: false } }), "ValidatingAdmissionPolicy").filter(({ metadata }) => metadata?.name === "alasio-compute-restarts"), []);
  });

  test("lets neon-control change no more of a pod than a compute's serving label, which takes it off its Service as a sleep stops it", () => {
    type Policy = KubernetesObject & {
      spec: { matchConstraints: { resourceRules: object[] }; matchConditions: { expression: string }[]; validations: { expression: string }[]; variables: { name: string; expression: string }[] };
    };
    const policy = one<Policy>(install(), "ValidatingAdmissionPolicy", "alasio-compute-serving");
    assert.deepEqual(policy.spec.matchConstraints.resourceRules, [{ apiGroups: [""], resources: ["pods"], operations: ["UPDATE"], apiVersions: ["*"] }]);
    assert.deepEqual(policy.spec.matchConditions.map(({ expression }) => expression), ['request.userInfo.username == "system:serviceaccount:alasio:alasio-neon-control"']);
    const [spec, compute, metadata, labelled, value] = policy.spec.validations.map(({ expression }) => expression);
    assert.equal(spec, "object.spec == oldObject.spec");
    assert.equal(compute, '"app.kubernetes.io/component" in variables.oldLabels && variables.oldLabels["app.kubernetes.io/component"] in ["neon-compute","neon-compute-ro"]');
    for (const field of ["annotations.orValue({})", "finalizers.orValue([])", "ownerReferences.orValue([])"]) assert.ok(metadata?.includes(`object.metadata.?${field} == oldObject.metadata.?${field}`), field);
    assert.ok(labelled?.includes('key in ["alasio.dev/serving"]'));
    assert.equal(value, '!("alasio.dev/serving" in variables.newLabels) || variables.newLabels["alasio.dev/serving"] == "true"');
    assert.deepEqual(one<KubernetesObject & { spec: object }>(install(), "ValidatingAdmissionPolicyBinding", "alasio-compute-serving").spec, { policyName: "alasio-compute-serving", validationActions: ["Deny"] });
    assert.deepEqual(all(install({ neon: { enabled: false, external: { existingSecret: "db" } }, workspaceStorage: { enabled: false } }), "ValidatingAdmissionPolicy").filter(({ metadata }) => metadata?.name === "alasio-compute-serving"), []);
  });

  test("keeps room for a node autoscaler only when asked: placeholders at the lowest priority, which preempt nothing", () => {
    assert.deepEqual(all(install(), "Deployment").filter(({ metadata }) => metadata?.name === "alasio-headroom"), []);
    const headroom = one<V1Deployment>(install({ autoscaling: { headroom: { scaling: { min: 2, max: 2 }, resources: { requests: { memory: "1Gi" } } } } }), "Deployment", "alasio-headroom");
    assert.equal(headroom.spec?.replicas, 2);
    assert.equal(headroom.spec?.template.spec?.priorityClassName, "alasio-headroom");
    assert.match(container(headroom).image ?? "", /^registry\.k8s\.io\/pause:3\.10\.1@sha256:[0-9a-f]{64}$/u);
    assert.deepEqual(container(headroom).resources, { requests: { cpu: "500m", memory: "1Gi" }, limits: {} });
    assert.deepEqual(all(install({ autoscaling: { headroom: { scaling: { min: 2, max: 2 } } } }), "VerticalPodAutoscaler").filter(({ metadata }) => metadata?.name === "alasio-headroom"), []);
    const refused = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, autoscaling: { headroom: { scaling: { min: 1, max: 3 } } } });
    assert.ok(Result.isFailure(refused) && refused.failure.message === "autoscaling.headroom.scaling.max must equal min: nothing scales it between them");
  });

  test("installs KEDA as its release does, in alasio's namespace under alasio's names, restricted, pinned and small, on the floor; none where the cluster has its own", () => {
    const objects = install();
    assert.deepEqual(
      all(objects, "CustomResourceDefinition").map(({ metadata }) => metadata?.name).filter((name) => name?.endsWith("keda.sh")).sort(),
      ["cloudeventsources.eventing.keda.sh", "clustercloudeventsources.eventing.keda.sh", "clustertriggerauthentications.keda.sh", "scaledjobs.keda.sh", "scaledobjects.keda.sh", "triggerauthentications.keda.sh"],
    );
    for (const name of ["alasio-keda-operator", "alasio-keda-metrics-apiserver", "alasio-keda-admission-webhooks"]) {
      const deployment = one<V1Deployment>(objects, "Deployment", name);
      const pod = deployment.spec?.template.spec;
      assert.equal(deployment.metadata?.namespace, "alasio");
      assert.equal(pod?.priorityClassName, "alasio-floor");
      assert.deepEqual(pod?.securityContext, { runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532, fsGroup: 65532, seccompProfile: { type: "RuntimeDefault" } });
      assert.deepEqual(container(deployment).securityContext, { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] }, readOnlyRootFilesystem: true });
      assert.match(container(deployment).image ?? "", /^ghcr\.io\/kedacore\/keda[-a-z]*:2\.21\.0@sha256:[0-9a-f]{64}$/u, name);
      assert.ok(memoryBytes(container(deployment).resources?.requests?.["memory"] ?? "") <= 64 * 2 ** 20, name);
    }
    const operator = container(one<V1Deployment>(objects, "Deployment", "alasio-keda-operator"));
    for (const arg of ["--cert-secret-name=alasio-keda-certs", "--operator-service-name=alasio-keda-operator", "--metrics-server-service-name=alasio-keda-metrics-apiserver", "--webhooks-service-name=alasio-keda-admission-webhooks", "--validating-webhook-name=alasio-keda-admission"]) {
      assert.ok(operator.args?.includes(arg), arg);
    }
    assert.ok(container(one<V1Deployment>(objects, "Deployment", "alasio-keda-metrics-apiserver")).args?.includes("--metrics-service-address=alasio-keda-operator.alasio.svc.cluster.local:9666"));
    const apiService = one<KubernetesObject & { spec: { service: object } }>(objects, "APIService", "v1beta1.external.metrics.k8s.io");
    assert.deepEqual(apiService.spec.service, { name: "alasio-keda-metrics-apiserver", namespace: "alasio" });
    // It reads Secrets in its own namespace alone, and of those writes its certificate's alone.
    const clusterRole = one<V1ClusterRole>(objects, "ClusterRole", "alasio-keda-operator");
    assert.ok(!(clusterRole.rules ?? []).some(({ resources }) => resources?.includes("secrets") || resources?.includes("*")));
    // It writes no Job, and of the cluster's webhooks and APIServices its own alone.
    const writes = (resource: string) =>
      (clusterRole.rules ?? []).filter(({ resources, verbs }) => resources?.includes(resource) && verbs.some((verb) => ["create", "update", "patch", "delete"].includes(verb))).map(({ resourceNames }) => resourceNames);
    assert.deepEqual([writes("jobs"), writes("validatingwebhookconfigurations"), writes("apiservices")], [[], [["alasio-keda-admission"]], [["v1beta1.external.metrics.k8s.io"]]]);
    assert.ok(operator.env?.some(({ name, value }) => name === "KEDA_RESTRICT_SECRET_ACCESS" && value === "true"));
    const role = one<V1Role>(objects, "Role", "alasio-keda-operator");
    for (const { resources, verbs, resourceNames } of role.rules ?? []) {
      if (resources?.includes("secrets") && verbs.some((verb) => ["update", "patch", "delete"].includes(verb))) assert.deepEqual(resourceNames, ["alasio-keda-certs"]);
    }

    const without = install({ autoscaling: { keda: { enabled: false } } });
    assert.deepEqual(without.filter(({ metadata }) => /keda/u.test(metadata?.name ?? "") || metadata?.name === "v1beta1.external.metrics.k8s.io").map(({ metadata }) => metadata?.name), []);
  });

  test("installs VPA as its release does, its certificate made as its chart makes it, in alasio's namespace, restricted, pinned and small, on the floor; none where the cluster has its own", () => {
    const objects = install();
    assert.deepEqual(all(objects, "CustomResourceDefinition").map(({ metadata }) => metadata?.name).filter((name) => name?.endsWith("autoscaling.k8s.io")).sort(), [
      "verticalpodautoscalercheckpoints.autoscaling.k8s.io",
      "verticalpodautoscalers.autoscaling.k8s.io",
    ]);
    const args = (name: string) => container(one<V1Deployment>(objects, "Deployment", name)).args ?? [];
    for (const name of ["alasio-vpa-recommender", "alasio-vpa-updater", "alasio-vpa-admission-controller"]) {
      const deployment = one<V1Deployment>(objects, "Deployment", name);
      assert.equal(deployment.spec?.template.spec?.priorityClassName, "alasio-floor");
      assert.deepEqual(container(deployment).securityContext, { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] }, readOnlyRootFilesystem: true });
      assert.match(container(deployment).image ?? "", /^registry\.k8s\.io\/autoscaling\/vpa-[-a-z]+:1\.8\.0@sha256:[0-9a-f]{64}$/u, name);
      assert.ok(memoryBytes(container(deployment).resources?.requests?.["memory"] ?? "") <= 64 * 2 ** 20, name);
    }
    // It tracks what has a VerticalPodAutoscaler alone, recommends as little as a small component uses, of memory each hour's peak over eight days, and resizes a workload of one replica.
    assert.deepEqual(args("alasio-vpa-recommender"), [
      "--memory-saver",
      "--pod-recommendation-min-cpu-millicores=10",
      "--pod-recommendation-min-memory-mb=16",
      "--memory-aggregation-interval=1h",
      "--memory-aggregation-interval-count=192",
    ]);
    // Both take the InPlace update mode, which resizes in place and never recreates.
    assert.deepEqual(args("alasio-vpa-updater"), ["--min-replicas=1", "--feature-gates=InPlace=true"]);
    assert.deepEqual(args("alasio-vpa-admission-controller"), ["--register-webhook=false", "--webhook-service=alasio-vpa-webhook", "--feature-gates=InPlace=true"]);
    const webhook = one<KubernetesObject & { webhooks: { failurePolicy: string; objectSelector: object; clientConfig: object }[] }>(objects, "MutatingWebhookConfiguration", "alasio-vpa-admission").webhooks[0];
    assert.deepEqual([webhook?.failurePolicy, webhook?.objectSelector, webhook?.clientConfig], [
      "Ignore",
      { matchLabels: { "app.kubernetes.io/part-of": "alasio" } },
      { service: { name: "alasio-vpa-webhook", namespace: "alasio", port: 443 } },
    ]);
    const certgen = one<V1Job>(objects, "Job", "alasio-vpa-certgen").spec?.template.spec;
    assert.deepEqual(certgen?.initContainers?.map(({ args }) => args), [["create", "--host=alasio-vpa-webhook,alasio-vpa-webhook.alasio.svc", "--namespace=alasio", "--secret-name=alasio-vpa-tls"]]);
    assert.deepEqual(certgen?.containers.map(({ args }) => args), [["patch", "--webhook-name=alasio-vpa-admission", "--namespace=alasio", "--secret-name=alasio-vpa-tls", "--patch-validating=false"]]);
    assert.match(certgen?.containers[0]?.image ?? "", /^registry\.k8s\.io\/ingress-nginx\/kube-webhook-certgen:v1\.6\.9@sha256:[0-9a-f]{64}$/u);
    // It may patch its own webhook alone.
    assert.deepEqual(one<V1ClusterRole>(objects, "ClusterRole", "alasio-vpa-certgen").rules, [
      { apiGroups: ["admissionregistration.k8s.io"], resources: ["mutatingwebhookconfigurations"], verbs: ["get", "update", "patch"], resourceNames: ["alasio-vpa-admission"] },
    ]);

    const without = install({ autoscaling: { vpa: { enabled: false } } });
    assert.deepEqual(without.filter(({ metadata }) => /vpa|verticalpodautoscaler/u.test(metadata?.name ?? "")).map(({ metadata }) => metadata?.name), []);
  });
});

describe("admission", () => {
  type Policy = KubernetesObject & {
    spec: { matchConstraints: { resourceRules: object[] }; matchConditions: { expression: string }[]; validations: { expression: string }[]; variables?: { name: string; expression: string }[] };
  };
  /** The expression of the variable `name` of the policy `policy` of `objects`. */
  const variableOf = (objects: readonly KubernetesObject[], policy: string, name: string): string =>
    one<Policy>(objects, "ValidatingAdmissionPolicy", policy).spec.variables?.find((variable) => variable.name === name)?.expression ?? "";
  const binding = (objects: readonly KubernetesObject[], name: string) => one<KubernetesObject & { spec: object }>(objects, "ValidatingAdmissionPolicyBinding", name).spec;

  test("lets the sandbox gateway set of a pod's status a restore's resumed condition alone, true, where main's sessions are snapshotted", () => {
    const objects = install();
    const resumed = one<Policy>(objects, "ValidatingAdmissionPolicy", "alasio-resumed-condition");
    assert.deepEqual(resumed.spec.matchConstraints, { resourceRules: [{ apiGroups: [""], resources: ["pods/status"], operations: ["UPDATE"], apiVersions: ["*"] }] });
    assert.deepEqual(resumed.spec.matchConditions.map(({ expression }) => expression), ['request.userInfo.username == "system:serviceaccount:alasio:alasio-sandbox-gateway"']);
    const [which, conditions, rest] = resumed.spec.validations.map(({ expression }) => expression);
    assert.ok(which?.startsWith('request.namespace == "alasio-sessions" && "alasio.dev/pool" in object.metadata.?labels.orValue({}) && '));
    assert.ok(which?.endsWith('object.spec.?readinessGates.orValue([]).exists(gate, gate.conditionType == "alasio.dev/resumed")'));
    assert.ok(conditions?.includes('object.status.?conditions.orValue([]).all(condition, condition.type != "alasio.dev/resumed" || condition.status == "True")'));
    for (const field of ["podIP", "podIPs", "hostIP", "phase", "containerStatuses"]) assert.ok(rest?.includes(`object.status.${field} == oldObject.status.${field}`), field);
    assert.deepEqual(binding(objects, "alasio-resumed-condition"), { policyName: "alasio-resumed-condition", validationActions: ["Deny"] });
    // None where no session is snapshotted, as the gateway restores none there.
    assert.ok(!all(install({ sessions: { runtimeClassName: "" } }), "ValidatingAdmissionPolicy").some(({ metadata }) => metadata?.name === "alasio-resumed-condition"));
  });

  test("lets no one make a pod in the folder namespace but the sandbox gateway, of the folder template alone, nor a Secret there but an instance's token, and alasio make neither", () => {
    const objects = install({ host: { enabled: true } });
    const pods = one<Policy>(objects, "ValidatingAdmissionPolicy", "alasio-folder-pods");
    // Of anyone, in the folder namespace: none is made there but by the gateway.
    assert.deepEqual(pods.spec.matchConditions.map(({ expression }) => expression), ['request.namespace == "alasio-host"']);
    assert.ok(pods.spec.validations.some(({ expression }) => expression === `request.operation != 'CREATE' || request.userInfo.username == "system:serviceaccount:alasio:alasio-sandbox-gateway"`));
    assert.deepEqual(pods.spec.variables?.map(({ name }) => name), ["oldAnnotations", "newAnnotations", "folderSpec", "folderLabels", "folderAnnotations"]);
    // As the folder template's account alone, of nothing a restore adds.
    const spec = variableOf(objects, "alasio-folder-pods", "folderSpec");
    assert.ok(spec.includes('object.spec["serviceAccountName"] == "alasio-host-agent"'), spec.slice(0, 400));
    for (const key of ["dev.gvisor.internal.restore", "alasio.dev/resumed", "alasio.dev/session-template"]) assert.ok(!spec.includes(key), key);
    assert.deepEqual(binding(objects, "alasio-folder-pods"), { policyName: "alasio-folder-pods", validationActions: ["Deny"] });
    const secrets = one<Policy>(objects, "ValidatingAdmissionPolicy", "alasio-folder-secrets");
    assert.deepEqual(secrets.spec.matchConditions.map(({ expression }) => expression), ['request.userInfo.username == "system:serviceaccount:alasio:alasio-sandbox-gateway"']);
    assert.ok(secrets.spec.validations[0]?.expression.startsWith('request.namespace == "alasio-host" && object.?type.orValue("Opaque") == "Opaque" && '));
    assert.deepEqual(binding(objects, "alasio-folder-secrets"), { policyName: "alasio-folder-secrets", validationActions: ["Deny"] });
    // Nothing of alasio's own, which makes neither.
    for (const name of ["alasio-sandboxes", "alasio-secrets", "alasio-clone-job"]) assert.ok(!all(objects, "ValidatingAdmissionPolicy").some(({ metadata }) => metadata?.name === name), name);
  });

  test("holds a folder instance's state directory under the state root as the gateway joins it, however the root is written", () => {
    for (const stateRoot of ["/home/op/state", "/home/op//state/", "/"]) {
      const spec = variableOf(install({ host: { enabled: true, stateRoot } }), "alasio-folder-pods", "folderSpec");
      // The patterns of path segments: the state directory's.
      const patterns = [...spec.matchAll(/\.matches\(("(?:[^"\\]|\\.)*")\)/gu)]
        .map(([, quoted = '""']) => JSON.parse(quoted) as string)
        .filter((pattern) => pattern.includes("[A-Za-z0-9_][A-Za-z0-9_.-]*/"))
        .map((pattern) => new RegExp(pattern, "u"));
      assert.equal(patterns.length, 1, stateRoot);
      assert.match(folderStateDir(stateRoot, "claude", "telegram:1"), patterns[0] ?? /^$/u, stateRoot);
      assert.match(folderStateDir(stateRoot, "claude", "../x"), patterns[0] ?? /^$/u, stateRoot);
      assert.doesNotMatch("/elsewhere/claude/telegram-1", patterns[0] ?? /^$/u, stateRoot);
    }
  });
});

describe("the sandbox service", () => {
  test("makes folder conversations' instances of the host profile, which the gateway alone makes and reaches, each on its conversation's state directory, released after an hour idle", () => {
    const objects = install({ host: { enabled: true, stateRoot: "/home/operator/.alasio/bayma" } });
    const config = JSON.parse(one<V1ConfigMap>(objects, "ConfigMap", "alasio-sandbox-gateway").data?.["config.json"] ?? "{}");
    assert.deepEqual([config.folders.namespace, config.folders.stateRoot, config.folders.releaseAfterSeconds], ["alasio-host", "/home/operator/.alasio/bayma", 3600]);
    assert.equal(JSON.parse(one<V1ConfigMap>(install({ host: { enabled: true, releaseAfter: "4h" } }), "ConfigMap", "alasio-sandbox-gateway").data?.["config.json"] ?? "{}").folders.releaseAfterSeconds, 4 * 3600);
    // The host profile's bayma, as the operator, under the cluster's own runtime, exporting its telemetry to the receiver with its binding's token.
    assert.equal(config.folders.template.spec.serviceAccountName, "alasio-host-agent");
    // Its account's token the template's own, where the API server would mount one, so the pod the policy pins is the pod as made.
    assert.equal(config.folders.template.spec.automountServiceAccountToken, false);
    assert.deepEqual((config.folders.template.spec.containers[0] as V1Container).volumeMounts?.at(-1), { name: "alasio-host-agent-token", mountPath: "/var/run/secrets/kubernetes.io/serviceaccount", readOnly: true });
    assert.deepEqual(config.folders.template.spec.volumes.at(-1).projected.sources.map((source: object) => Object.keys(source)[0]), ["serviceAccountToken", "configMap", "downwardAPI"]);
    const [bayma] = config.folders.template.spec.containers as V1Container[];
    assert.equal(config.folders.template.spec.runtimeClassName, undefined);
    assert.deepEqual(bayma?.securityContext?.capabilities?.add, ["CHECKPOINT_RESTORE", "SYS_PTRACE"]);
    assert.deepEqual(bayma?.args?.slice(-2), ["--telemetry-token-file", "/run/alasio/bayma/token"]);
    assert.equal(bayma?.env?.find(({ name }) => name === "OTEL_EXPORTER_OTLP_ENDPOINT")?.value, "http://alasio-receiver.alasio.svc.cluster.local:4318");
    // The gateway's identity there: pods, their tokens' Secrets, the bindings' Leases and what the pods use; the receiver's, which watches the Leases.
    const role = (name: string) => all<V1Role>(objects, "Role").find(({ metadata }) => metadata?.name === name && metadata.namespace === "alasio-host")?.rules;
    assert.deepEqual(role("alasio-sandbox-gateway"), [
      { apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch", "create", "update", "delete"] },
      { apiGroups: [""], resources: ["secrets"], verbs: ["create", "patch", "delete"] },
      { apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["get", "list", "watch", "create", "update", "patch", "delete"] },
      { apiGroups: ["metrics.k8s.io"], resources: ["pods"], verbs: ["list"] },
    ]);
    assert.deepEqual(role("alasio-receiver"), [{ apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["list", "watch"] }]);
    assert.deepEqual(container(one<V1Deployment>(objects, "Deployment", "alasio-receiver")).env?.find(({ name }) => name === "FOLDERS_NAMESPACE"), { name: "FOLDERS_NAMESPACE", value: "alasio-host" });
    for (const name of ["alasio-folder-pods", "alasio-folder-secrets"]) assert.ok(one(objects, "ValidatingAdmissionPolicy", name), name);
    // No drain evicts a bound one, whose REPLs live in it until its release.
    const bound = all<KubernetesObject & { spec: object }>(objects, "PodDisruptionBudget").find(({ metadata }) => metadata?.name === "alasio-bound" && metadata.namespace === "alasio-host");
    assert.deepEqual(bound?.spec, { maxUnavailable: 0, selector: { matchExpressions: [{ key: "alasio.dev/binding", operator: "Exists" }] }, unhealthyPodEvictionPolicy: "AlwaysAllow" });
    // Reached by the gateway alone on bayma's port, as the gateway reaches it, and its telemetry taken by the receiver.
    const policies = all<NetworkPolicyObject>(objects, "NetworkPolicy");
    const pod = (component: string, namespace = "alasio", extra: Record<string, string> = {}): Pod => ({
      namespace,
      namespaceLabels: { "kubernetes.io/metadata.name": namespace },
      labels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": component, ...extra },
    });
    const folder = pod("folder-bayma", "alasio-host", { "alasio.dev/pool": "folder" });
    assert.equal(admits(policies, pod("sandbox-gateway"), folder, 7290), true);
    assert.equal(reaches(policies, pod("sandbox-gateway"), folder, 7290), true);
    assert.equal(admits(policies, folder, pod("receiver"), 4318), true);
    assert.equal(admits(policies, pod("collector"), folder, 7290), false);
    // None without the host profile.
    const off = install();
    assert.deepEqual(all(off, "ValidatingAdmissionPolicy").filter(({ metadata }) => /folder/u.test(metadata?.name ?? "")), []);
    assert.equal(all<V1Role>(off, "Role").some(({ metadata }) => metadata?.namespace === "alasio-host"), false);
  });

  test("pools each internet's sessions as pods of the agent image under gVisor, resolving names as its internet says, each mounting its own directory on its node, its bayma started on its binding's token, kept free by KEDA", () => {
    const objects = install();
    for (const mode of ["none", "full"] as const) {
      const pool = one<V1Deployment>(objects, "Deployment", `sandboxes-${mode}`);
      assert.equal(pool.metadata?.namespace, "alasio-sessions");
      assert.deepEqual(pool.spec?.selector.matchLabels, { "alasio.dev/pool": `sandboxes-${mode}`, "alasio.dev/free": "true" });
      // None is ready before it is bound: the old free pods go as the new come.
      assert.deepEqual(pool.spec?.strategy, { type: "Recreate" });
      assert.equal(pool.spec?.replicas, undefined);
      const template = pool.spec?.template;
      assert.equal(template?.metadata?.labels?.["alasio.dev/net-mode"], mode);
      const spec = template?.spec;
      assert.deepEqual([spec?.runtimeClassName, spec?.automountServiceAccountToken, spec?.enableServiceLinks], ["gvisor", false, false]);
      // Names resolved as its internet says: by the public resolvers, or by none but itself.
      assert.deepEqual([spec?.dnsPolicy, spec?.dnsConfig], ["None", { nameservers: mode === "full" ? ["1.1.1.1", "8.8.8.8"] : ["127.0.0.1"] }]);
      assert.deepEqual(spec?.volumes?.[0], { name: "attach", hostPath: { path: "/var/lib/alasio/attach", type: "DirectoryOrCreate" } });
      const [bayma] = spec?.containers ?? [];
      // Its directory by its namespace and its name, as pods of two namespaces may share a name.
      assert.deepEqual(bayma?.volumeMounts?.[0], { name: "attach", mountPath: "/attach", subPathExpr: "$(POD_NAMESPACE)/$(POD_NAME)", mountPropagation: "HostToContainer" });
      assert.deepEqual(bayma?.env?.slice(0, 2), [
        { name: "POD_NAME", valueFrom: { fieldRef: { fieldPath: "metadata.name" } } },
        { name: "POD_NAMESPACE", valueFrom: { fieldRef: { fieldPath: "metadata.namespace" } } },
      ]);
      assert.deepEqual(bayma?.command, ["/usr/bin/tini", "-s", "--", "/usr/local/bin/alasio-start", "node", "/opt/bayma/bayma.js"]);
      assert.deepEqual(bayma?.args?.slice(-6, -2), ["--token-file", "/attach/token", "--telemetry-token-file", "/attach/token"]);
      // Its sessions snapshotted, as main's are by the gateway: held to the nodes' CPU baseline, its bayma holding a stop it is
      // not given for the gateway's save, and its template named by its own hash, which a snapshot taken on it records.
      assert.deepEqual(bayma?.args?.slice(-2), ["--stop-wait-ms", "60000"]);
      assert.equal(spec?.terminationGracePeriodSeconds, 60 + 30);
      const { ["alasio.dev/session-template"]: hash, ...annotations } = template?.metadata?.annotations ?? {};
      assert.equal(annotations["dev.gvisor.internal.cpufeatures"], BASELINE.join(","));
      assert.equal(hash, templateHash({ ...template, metadata: { ...template?.metadata, annotations } }));
      // Its hook, as a release's snapshot runs it, through the same start.
      assert.deepEqual(hookOf(bayma ?? { name: "" }), ["/usr/local/bin/alasio-start", "node", "/opt/bayma/bayma.js", "sandbox-hook", "--state-dir", "/home/agent/.bayma"]);
      const scaled = one<ScaledObject>(objects, "ScaledObject", `sandboxes-${mode}`);
      assert.deepEqual([scaled.spec.minReplicaCount, scaled.spec.maxReplicaCount], [1, 10]);
      assert.deepEqual(scaled.spec.triggers, [{
        type: "metrics-api",
        metadata: { url: `http://alasio-sandbox-gateway.alasio.svc.cluster.local:8080/counts/alasio-sessions/sandboxes-${mode}`, valueLocation: "waiting", targetValue: "1" },
      }]);
    }
  });

  test("pins each pool's template, and each a restore is made onto, the one before the current carried, in a variable of its own, each within CEL's size limit of an expression", () => {
    const kept = { ...NOTHING_KEPT, cpuBaseline: BASELINE };
    const cpu = (request: string) => ({ sessions: { resources: { requests: { cpu: request } } } });
    const first = manifests(configOf(cpu("101m")), carriedFrom(configOf(cpu("101m")), kept));
    const gateway = one<V1ConfigMap>(first, "ConfigMap", "alasio-sandbox-gateway").data?.["config.json"];
    const objects = manifests(configOf(cpu("102m")), carriedFrom(configOf(cpu("102m")), kept, gateway));
    const policy = one<KubernetesObject & { spec: { variables: { name: string; expression: string }[]; validations: { expression: string }[] } }>(objects, "ValidatingAdmissionPolicy", "alasio-sandbox-pods");
    const named = (prefix: string) => policy.spec.variables.filter(({ name }) => name.startsWith(prefix));
    assert.equal(named("made").length, 2);
    // Each pool's current template and the one before it.
    assert.equal(named("restored").length, 4);
    assert.equal(new Set(named("restored").map(({ expression }) => expression)).size, 4);
    for (const { expression } of [...policy.spec.variables, ...policy.spec.validations]) assert.ok([...expression].length <= 100_000, `an expression of ${[...expression].length} code points`);
  });

  test("carries each pool's template before the current one, as the gateway's configuration it replaces held it, for one change of it and no more", () => {
    const kept = { ...NOTHING_KEPT, cpuBaseline: BASELINE };
    const gatewayOf = (objects: readonly KubernetesObject[]) => one<V1ConfigMap>(objects, "ConfigMap", "alasio-sandbox-gateway").data?.["config.json"];
    const templateOf = (objects: readonly KubernetesObject[], mode: string) => JSON.parse(JSON.stringify(one<V1Deployment>(objects, "Deployment", `sandboxes-${mode}`).spec?.template)) as unknown;
    const cpu = (request: string) => ({ sessions: { resources: { requests: { cpu: request } } } });
    const first = manifests(configOf(cpu("101m")), carriedFrom(configOf(cpu("101m")), kept));
    assert.deepEqual(carriedPools(configOf(cpu("101m")), undefined, kept), { none: null, full: null });
    // Unchanged: none carried; changed: the one before.
    assert.deepEqual(carriedPools(configOf(cpu("101m")), gatewayOf(first), kept), { none: null, full: null });
    const carried = carriedPools(configOf(cpu("102m")), gatewayOf(first), kept);
    assert.deepEqual(carried, { none: templateOf(first, "none"), full: templateOf(first, "full") });
    const second = manifests(configOf(cpu("102m")), { ...NOTHING_CARRIED, ...kept, previousPools: carried });
    assert.deepEqual(JSON.parse(gatewayOf(second) ?? "{}").snapshots.templates.none, { current: templateOf(second, "none"), previous: templateOf(first, "none") });
    // Unchanged since: carried on; changed again: the second, the first no more.
    assert.deepEqual(carriedPools(configOf(cpu("102m")), gatewayOf(second), kept), carried);
    assert.deepEqual(carriedPools(configOf(cpu("103m")), gatewayOf(second), kept).none, templateOf(second, "none"));
    // Of a configuration that is none, or of sessions not snapshotted, none.
    assert.deepEqual(carriedPools(configOf(cpu("102m")), "{ not JSON", kept), { none: null, full: null });
    assert.deepEqual(carriedPools(configOf(cpu("102m")), gatewayOf(first), { ...kept, cpuBaseline: null }), { none: null, full: null });
  });

  test("a restore of a snapshot taken under a release at or past the first that restores through a socket asks for its checkpoint gofer through a socket, reading again pages a read of failed; one before it, or where none is named, the agent's own way", () => {
    assert.deepEqual(
      ["20260921.0-eaucoin.8", "20260921.0-eaucoin.9", "20260921.1-eaucoin.1"].map((release) => goferOf(release, "20260921.0-eaucoin.9")),
      [null, { transport: "socket", pagesRetry: true }, { transport: "socket", pagesRetry: true }],
    );
    assert.equal(goferOf("20260921.0-eaucoin.9", ""), null);
  });

  test("binds through a gateway of alasio's image, configured with its pools and the node agents, its keys the stack's, which may claim and delete its pools' pods and hold their Leases alone", () => {
    const objects = install();
    const gateway = one<V1Deployment>(objects, "Deployment", "alasio-sandbox-gateway");
    const [container] = gateway.spec?.template.spec?.containers ?? [];
    assert.deepEqual(container?.command, ["node", "/opt/alasio/src/gateway/main.ts"]);
    const env = Object.fromEntries((container?.env ?? []).map(({ name, value, valueFrom }) => [name, value ?? valueFrom?.secretKeyRef ?? valueFrom?.fieldRef]));
    assert.deepEqual([env["CALLER_KEY"], env["BINDING_KEY"], env["ALASIO_NODE_AGENT_TOKEN"]], [
      { name: "alasio-sandbox-gateway", key: "caller-key" },
      { name: "alasio-sandbox-gateway", key: "binding-key" },
      { name: "alasio-node-agent", key: "token" },
    ]);
    // Every environment's events, written as its principal on the log.
    assert.deepEqual([env["EVENTS_BROKERS"], env["EVENTS_USERNAME"], env["EVENTS_PASSWORD_FILE"]], ["alasio-log.alasio.svc:9092", { name: "alasio-log-sandbox-gateway", key: "username" }, "/run/alasio/log/password"]);
    assert.deepEqual(gateway.spec?.template.spec?.volumes?.find(({ name }) => name === "log"), { name: "log", secret: { secretName: "alasio-log-sandbox-gateway", items: [{ key: "password", path: "password" }] } });
    const config = JSON.parse(one<V1ConfigMap>(objects, "ConfigMap", "alasio-sandbox-gateway").data?.["config.json"] ?? "{}");
    assert.deepEqual(config, {
      port: 8080,
      peers: "alasio-sandbox-gateway-peers.alasio.svc.cluster.local",
      podPort: 7290,
      waitSeconds: 120,
      releaseAfterSeconds: 300,
      environment: { name: "main", namespace: "alasio-sessions" },
      // A new session's volume's size, 10Gi by default.
      quotaBytes: 10 * 1024 ** 3,
      pools: { none: "sandboxes-none", full: "sandboxes-full" },
      nodeAgents: { namespace: "alasio", selector: "app.kubernetes.io/name=alasio,app.kubernetes.io/instance=alasio,app.kubernetes.io/component=node-agent", port: 8484 },
      // Where bound pods' bayma exports its telemetry: the receiver's Service.
      receiver: { host: "alasio-receiver.alasio.svc.cluster.local", port: 4318 },
      // No folder workspaces, of no host profile.
      folders: null,
      // Conversations' harnesses in alasio's process, of no pools.
      harnesses: null,
      // Main's sessions snapshotted as alasio's own were, under the releases kept, each pool's template as the pool runs it, none carried before it.
      snapshots: {
        liveEverySeconds: 600,
        // Each restored through the checkpoint gofer as it restores best: through the agent's socket, its reads of pages
        // retried, from eaucoin.9 on, as .11 does; over TCP, the agent's own way, before, as .8 does.
        releases: NOTHING_KEPT.gvisorReleases.map((release) => ({
          release,
          runtimeClassName: `gvisor-${release.split(".").join("-")}`,
          gofer: release === "20260921.0-eaucoin.8" ? null : { transport: "socket", pagesRetry: true },
        })),
        templates: Object.fromEntries((["none", "full"] as const).map((mode) => [mode, { current: JSON.parse(JSON.stringify(one<V1Deployment>(objects, "Deployment", `sandboxes-${mode}`).spec?.template)), previous: null }])),
      },
    });
    // Not snapshotted without the nodes' CPU baseline.
    assert.equal(JSON.parse(one<V1ConfigMap>(install({}, null), "ConfigMap", "alasio-sandbox-gateway").data?.["config.json"] ?? "{}").snapshots, null);
    const sessions = all<V1Role>(objects, "Role").find(({ metadata }) => metadata?.name === "alasio-sandbox-gateway" && metadata.namespace === "alasio-sessions");
    assert.deepEqual(sessions?.rules, [
      // A restore's pod made too, its readiness set, and the policies that select it read.
      { apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch", "create", "update", "delete"] },
      { apiGroups: [""], resources: ["pods/status"], verbs: ["patch"] },
      { apiGroups: ["networking.k8s.io"], resources: ["networkpolicies"], verbs: ["list"] },
      { apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["get", "list", "watch", "create", "update", "patch", "delete"] },
      { apiGroups: ["apps"], resources: ["deployments"], verbs: ["get"], resourceNames: ["sandboxes-none", "sandboxes-full"] },
      // The Events of the pods the sandbox pods' policy refused, which it counts.
      { apiGroups: [""], resources: ["events"], verbs: ["list"] },
    ]);
    assert.equal(one<V1Service>(objects, "Service", "alasio-sandbox-gateway-peers").spec?.clusterIP, "None");
    // Of the cluster, the nodes it reads whether a restore's pod may be scheduled onto, alone.
    assert.deepEqual(one<V1ClusterRole>(objects, "ClusterRole", "alasio-sandbox-gateway-nodes").rules, [{ apiGroups: [""], resources: ["nodes"], verbs: ["get"] }]);
  });

  test("has alasio reach main's sessions through the gateway, as main, by main's own key of the gateway's, and not where the sandbox service runs not", () => {
    const env = (objects: readonly KubernetesObject[]) => container(one<V1Deployment>(objects, "Deployment", "alasio")).env ?? [];
    const of = (objects: readonly KubernetesObject[], name: string) => env(objects).find((each) => each.name === name);
    const objects = install();
    assert.equal(of(objects, "ALASIO_GATEWAY_URL")?.value, "http://alasio-sandbox-gateway.alasio.svc.cluster.local:8080");
    assert.equal(of(objects, "ALASIO_GATEWAY_ENVIRONMENT")?.value, "main");
    assert.deepEqual(of(objects, "ALASIO_GATEWAY_CALLER_KEY")?.valueFrom, { secretKeyRef: { name: "alasio-sessions-caller", key: "caller-key" } });
    const without = install({ sessions: { enabled: false }, workspaceStorage: { enabled: false } });
    assert.ok(!env(without).some(({ name }) => name.startsWith("ALASIO_GATEWAY_")));
  });

  test("receives sessions' telemetry in a receiver of its own, which watches its pools' Leases, and mounts workspace storage on each node, in the node's own mount namespace, from a pod of its own, whose replacement takes its mount over first", () => {
    const objects = install();
    const receiver = one<V1Deployment>(objects, "Deployment", "alasio-receiver");
    const env = Object.fromEntries((receiver.spec?.template.spec?.containers[0]?.env ?? []).map(({ name, value, valueFrom }) => [name, value ?? valueFrom?.secretKeyRef]));
    assert.deepEqual([env["SESSIONS_NAMESPACE"], env["BINDING_KEY"]], ["alasio-sessions", { name: "alasio-sandbox-gateway", key: "binding-key" }]);
    const watching = all<V1Role>(objects, "Role").find(({ metadata }) => metadata?.name === "alasio-receiver");
    assert.deepEqual([watching?.metadata?.namespace, watching?.rules], ["alasio-sessions", [{ apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["list", "watch"] }]]);
    const mount = one<V1DaemonSet>(objects, "DaemonSet", "alasio-workspace-mount");
    // The new pod beside the old, which it takes the mount over from, by the node's PIDs and mount namespace.
    assert.deepEqual(mount.spec?.updateStrategy, { type: "RollingUpdate", rollingUpdate: { maxSurge: 1, maxUnavailable: 0 } });
    assert.equal(mount.spec?.template.spec?.hostPID, true);
    const [juicefs] = mount.spec?.template.spec?.containers ?? [];
    // Of alasio's image, for JuiceFS's static release, which runs in the node's namespace as it is; of the node's mount, no mount of its own.
    assert.equal(juicefs?.image, one<V1Deployment>(objects, "Deployment", "alasio").spec?.template.spec?.containers[0]?.image);
    assert.deepEqual(juicefs?.securityContext, { privileged: true });
    assert.deepEqual(juicefs?.volumeMounts, [
      { name: "bin", mountPath: "/var/lib/alasio/bin" },
      { name: "state", mountPath: "/var/lib/alasio/juicefs-state" },
      { name: "cache", mountPath: "/var/lib/alasio/juicefs-cache" },
      { name: "run", mountPath: "/run/alasio" },
    ]);
    assert.deepEqual(mount.spec?.template.spec?.volumes?.[1], { name: "state", hostPath: { path: "/var/lib/alasio/juicefs-state", type: "DirectoryOrCreate" } });
    assert.deepEqual(juicefs?.command, ["sh", "-c", workspaceMountScript("/var/lib/alasio/workspaces", 1024)]);
    assert.match(juicefs?.command?.[2] ?? "", /nsenter -t 1 -m -- "\$node_juicefs" mount --no-bgjob --metrics 0\.0\.0\.0:9567 --bucket "\$bucket" --cache-dir \/var\/lib\/alasio\/juicefs-cache --cache-size 1024 "\$meta" \/var\/lib\/alasio\/workspaces /u);
    assert.deepEqual(juicefs?.readinessProbe?.exec?.command, ["sh", "-c", workspaceMountReady("/var/lib/alasio/workspaces")]);
    // Given the metadata engine's URL and the bucket's, which it resolves by the pod's DNS, and the bucket's keys.
    assert.deepEqual(juicefs?.env?.map(({ name, valueFrom }) => `${name}=${valueFrom?.secretKeyRef?.key}`), ["META_URL=metaurl", "BUCKET=bucket", "ACCESS_KEY=access-key", "SECRET_KEY=secret-key"]);
    assert.equal(mount.spec?.template.metadata?.labels?.["alasio.dev/workload"], "juicefs-admin");
  });

  test("rolls the nodes' mount where a Service it reaches by address is made anew, or its bucket is another, and not otherwise", () => {
    const annotation = (objects: readonly KubernetesObject[]) => one<V1DaemonSet>(objects, "DaemonSet", "alasio-workspace-mount").spec?.template.metadata?.annotations?.["checksum/storage-services"];
    const reaching = (mountServices: Record<string, string | null>, file: Record<string, unknown> = {}) => annotation(manifests(configOf(file), { ...NOTHING_CARRIED, cpuBaseline: BASELINE, mountServices }));
    const before = reaching({ "alasio-valkey": "uid-1", "alasio-seaweedfs": "uid-2" });
    assert.match(before ?? "", /^[0-9a-f]{16}$/u);
    assert.equal(reaching({ "alasio-valkey": "uid-1", "alasio-seaweedfs": "uid-2" }), before);
    // Valkey made anew, the object store's Service, or one not there yet, as up makes it.
    for (const remade of [{ "alasio-valkey": "uid-3", "alasio-seaweedfs": "uid-2" }, { "alasio-valkey": "uid-1", "alasio-seaweedfs": "uid-4" }, { "alasio-valkey": null, "alasio-seaweedfs": "uid-2" }]) {
      assert.notEqual(reaching(remade), before, JSON.stringify(remade));
    }
    // Another bucket.
    assert.notEqual(reaching({ "alasio-valkey": "uid-1", "alasio-seaweedfs": "uid-2" }, { objectStore: { buckets: { workspaces: "elsewhere" } } }), before);
  });

  test("confines a pool's pods to the gateway in and the receiver out, with internet to the public internet beside, and admits every environment's alasio, its replicas and KEDA to the gateway, and the gateway to the node agents", () => {
    const policies = all<NetworkPolicyObject>(install(), "NetworkPolicy");
    const podOf = (component: string, namespace = "alasio", labels: Record<string, string> = {}): Pod => ({
      namespace,
      namespaceLabels: { "kubernetes.io/metadata.name": namespace },
      labels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": component, ...labels },
    });
    const pooled = (mode: "none" | "full", bound: boolean) =>
      podOf("session", "alasio-sessions", { "alasio.dev/pool": `sandboxes-${mode}`, "alasio.dev/net-mode": mode, ...(bound ? { "alasio.dev/binding": "0".repeat(40) } : { "alasio.dev/free": "true" }) });
    const [gateway, receiver, alasio, agent, collector, keda] = ["sandbox-gateway", "receiver", "alasio", "node-agent", "collector", "keda-operator"].map((component) => podOf(component));
    assert.ok(gateway && receiver && alasio && agent && collector && keda);
    for (const pod of [pooled("none", false), pooled("none", true), pooled("full", false), pooled("full", true)]) {
      assert.equal(connects(policies, gateway, pod, 7290), true);
      assert.equal(connects(policies, alasio, pod, 7290), false);
      assert.equal(connects(policies, pooled("none", true), pod, 7290), false);
      assert.equal(connects(policies, gateway, pod, 7291), false);
      assert.equal(connects(policies, pod, receiver, 4318), true);
      assert.equal(connects(policies, pod, alasio, 4318), false);
      assert.equal(connects(policies, pod, collector, 4318), false);
      assert.equal(connects(policies, pod, gateway, 8080), false);
      assert.equal(connects(policies, pod, agent, 8484), false);
      assert.equal(connects(policies, pod, pooled("full", true), 7290), false);
    }
    // The public internet, for a pod with internet alone, none of the private networks.
    const internet = policies.filter(({ spec }) => (spec.egress ?? []).some(({ to }) => (to ?? []).some(({ ipBlock }) => ipBlock?.cidr === "0.0.0.0/0")));
    const sandboxes = internet.filter(({ metadata, spec }) => metadata?.namespace === "alasio-sessions" && selects(spec.podSelector, pooled("full", true).labels));
    assert.deepEqual(sandboxes.map(({ metadata, spec }) => [metadata?.name, spec.egress]), [["alasio-sandboxes-full-internet", [{
      to: [{ ipBlock: { cidr: "0.0.0.0/0", except: ["0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16", "224.0.0.0/4", "240.0.0.0/4"] } }],
    }]]]);
    assert.deepEqual(internet.filter(({ metadata, spec }) => metadata?.namespace === "alasio-sessions" && selects(spec.podSelector, pooled("none", true).labels)), []);
    // The gateway, from main's alasio and a branch environment's, its replicas and KEDA's operator, on its port alone; not an alasio of a namespace of no branch's.
    const branchAlasio: Pod = { ...podOf("alasio", "alasio-branch-try"), namespaceLabels: { "kubernetes.io/metadata.name": "alasio-branch-try", "alasio.dev/branch": "try" } };
    for (const [from, admitted] of [[alasio, true], [branchAlasio, true], [gateway, true], [keda, true], [receiver, false], [collector, false], [podOf("alasio", "elsewhere"), false]] as const) {
      assert.equal(connects(policies, from, gateway, 8080), admitted, from.labels["app.kubernetes.io/component"]);
    }
    assert.equal(connects(policies, alasio, gateway, 8081), false);
    assert.equal(connects(policies, alasio, receiver, 4318), false);
    assert.equal(connects(policies, gateway, agent, 8484), true);
    assert.equal(connects(policies, gateway, collector, 4318), true);
    assert.equal(connects(policies, receiver, collector, 4318), true);
  });

  test("lets no drain evict a pod bound to a workspace, but one not ready", () => {
    const budget = one<V1PodDisruptionBudget>(install(), "PodDisruptionBudget", "alasio-bound");
    assert.deepEqual([budget.metadata?.namespace, budget.spec], ["alasio-sessions", {
      maxUnavailable: 0,
      selector: { matchExpressions: [{ key: "alasio.dev/binding", operator: "Exists" }] },
      unhealthyPodEvictionPolicy: "AlwaysAllow",
    }]);
  });

  test("lets the gateway and the receiver reach the cluster's DNS, what each talks to, and the API server alone, where up finds it, and anywhere where it cannot, which up warns of", () => {
    const apiServer = { addresses: ["172.18.0.2", "172.18.0.3"], ports: [6443] };
    const policies = all<NetworkPolicyObject>(manifests(configOf({}), { ...NOTHING_CARRIED, cpuBaseline: BASELINE, apiServer }), "NetworkPolicy");
    const podOf = (component: string, namespace = "alasio", labels: Record<string, string> = {}): Pod => ({
      namespace,
      namespaceLabels: { "kubernetes.io/metadata.name": namespace },
      labels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": component, ...labels },
    });
    const elsewhere = (address: string): Pod => ({ namespace: "", namespaceLabels: {}, labels: {}, address });
    const dns: Pod = { namespace: "kube-system", namespaceLabels: { "kubernetes.io/metadata.name": "kube-system" }, labels: { "k8s-app": "kube-dns" } };
    const pooled = podOf("session", "alasio-sessions", { "alasio.dev/pool": "sandboxes-none", "alasio.dev/net-mode": "none", "alasio.dev/binding": "0".repeat(40) });
    const branchPooled = {
      ...pooled,
      namespace: "alasio-branch-try-sessions",
      namespaceLabels: { "kubernetes.io/metadata.name": "alasio-branch-try-sessions", "alasio.dev/branch": "try" },
    };
    const [gateway, receiver, alasio, agent, collector, proxy] = ["sandbox-gateway", "receiver", "alasio", "node-agent", "collector", "neon-proxy"].map((component) => podOf(component));
    assert.ok(gateway && receiver && alasio && agent && collector && proxy);
    const [api, apiToo, notApi, internet] = [elsewhere("172.18.0.2"), elsewhere("172.18.0.3"), elsewhere("172.18.0.9"), elsewhere("1.1.1.1")];
    assert.ok(api && apiToo && notApi && internet);
    const reached = (from: Pod) =>
      ([[dns, 53], [api, 6443], [apiToo, 6443], [api, 443], [notApi, 6443], [internet, 443], [collector, 4318], [alasio, 4318], [proxy, 6432], [pooled, 7290], [branchPooled, 7290], [agent, 8484], [gateway, 8080]] as const)
        .filter(([to, port]) => reaches(policies, from, to, port))
        .map(([to, port]) => `${to.labels["app.kubernetes.io/component"] ?? to.labels["k8s-app"] ?? to.address}:${port}`);
    assert.deepEqual(reached(receiver), ["kube-dns:53", "172.18.0.2:6443", "172.18.0.3:6443", "collector:4318"]);
    assert.deepEqual(reached(gateway), ["kube-dns:53", "172.18.0.2:6443", "172.18.0.3:6443", "collector:4318", "session:7290", "session:7290", "node-agent:8484", "sandbox-gateway:8080"]);
    // A branch environment's pool pods reach the receiver as main's do; a pod of a namespace of no environment's, none.
    assert.equal(connects(policies, branchPooled, receiver, 4318), true);
    assert.equal(admits(policies, { ...pooled, namespace: "elsewhere", namespaceLabels: { "kubernetes.io/metadata.name": "elsewhere" } }, receiver, 4318), false);
    // Where up could not read where the API server is: anywhere, as up says.
    const unread = all<NetworkPolicyObject>(install(), "NetworkPolicy");
    assert.deepEqual([reaches(unread, receiver, notApi, 6443), reaches(unread, gateway, internet, 443)], [true, true]);
    assert.deepEqual([
      apiServerUnconfined(configOf({}), null),
      apiServerUnconfined(configOf({}), apiServer),
      apiServerUnconfined(configOf({ networkPolicies: { enabled: false } }), null),
      apiServerUnconfined(configOf({ workspaceStorage: { enabled: false } }), null),
    ], [true, false, false, false]);
  });

  test("finds the branch environments by their namespaces, which the gateway and the receiver may read, and nothing else of the cluster", () => {
    const objects = install();
    for (const account of ["alasio-sandbox-gateway", "alasio-receiver"]) {
      assert.deepEqual(one<V1ClusterRole>(objects, "ClusterRole", account).rules, [{ apiGroups: [""], resources: ["namespaces"], verbs: ["get", "list", "watch"] }]);
      const binding = one<V1ClusterRoleBinding>(objects, "ClusterRoleBinding", account);
      assert.deepEqual([binding.roleRef.name, binding.subjects], [account, [{ kind: "ServiceAccount", name: account, namespace: "alasio" }]]);
    }
  });

  test("binds a branch environment's sessions in pools of its own, in its sessions' namespace, through main's gateway and receiver, which nothing of a branch changes", () => {
    const main = install();
    const branch = branchObjects(configOf(), "try", BASELINE);
    const namespace = "alasio-branch-try-sessions";
    const ofBranch = <T extends KubernetesObject>(kind: string, name: string) => one<T>(branch.filter(({ metadata }) => metadata?.namespace === namespace), kind, name);
    for (const mode of ["none", "full"] as const) {
      const pool = ofBranch<V1Deployment>("Deployment", `sandboxes-${mode}`);
      assert.deepEqual(pool.spec?.selector.matchLabels, { "alasio.dev/pool": `sandboxes-${mode}`, "alasio.dev/free": "true" });
      assert.deepEqual(ofBranch<ScaledObject>("ScaledObject", `sandboxes-${mode}`).spec.triggers[0]?.metadata["url"], `http://alasio-sandbox-gateway.alasio.svc.cluster.local:8080/counts/${namespace}/sandboxes-${mode}`);
    }
    assert.deepEqual(ofBranch<V1Role>("Role", "alasio-sandbox-gateway").rules, one<V1Role>(main.filter(({ metadata }) => metadata?.namespace === "alasio-sessions"), "Role", "alasio-sandbox-gateway").rules);
    assert.deepEqual(ofBranch<V1Role>("Role", "alasio-receiver").rules, [{ apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["list", "watch"] }]);
    assert.deepEqual(ofBranch<V1RoleBinding>("RoleBinding", "alasio-receiver").subjects, [{ kind: "ServiceAccount", name: "alasio-receiver", namespace: "alasio" }]);
    assert.ok(ofBranch("PodDisruptionBudget", "alasio-bound"));
    // Main's gateway reaches its pools, and they its receiver, as main's do.
    const policies = [...all<NetworkPolicyObject>(main, "NetworkPolicy"), ...all<NetworkPolicyObject>(branch, "NetworkPolicy")];
    const pod = (component: string, at: string, labels: Record<string, string> = {}, namespaceLabels: Record<string, string> = {}): Pod => ({
      namespace: at,
      namespaceLabels: { "kubernetes.io/metadata.name": at, ...namespaceLabels },
      labels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": component, ...labels },
    });
    const pooled = pod("session", namespace, { "alasio.dev/pool": "sandboxes-none", "alasio.dev/net-mode": "none", "alasio.dev/free": "true" }, { "alasio.dev/branch": "try" });
    assert.equal(connects(policies, pod("sandbox-gateway", "alasio"), pooled, 7290), true);
    assert.equal(connects(policies, pod("alasio", "alasio-branch-try"), pooled, 7290), false);
    assert.equal(connects(policies, pooled, pod("receiver", "alasio"), 4318), true);
    assert.equal(connects(policies, pooled, pod("alasio", "alasio-branch-try"), 4318), false);
    // Nothing of main's is a branch's, so a branch made or deleted changes, and restarts, nothing of main's.
    const named = ({ kind, metadata }: KubernetesObject) => `${kind}/${metadata?.namespace ?? ""}/${metadata?.name ?? ""}`;
    const mains = new Set(main.map(named));
    assert.deepEqual(branch.map(named).filter((each) => mains.has(each)), []);
    assert.deepEqual(branch.filter(({ metadata }) => metadata?.namespace === "alasio").map(named), []);
  });

  test("gives the node agent the metadata engine alone, which it sets quotas on with JuiceFS's own command, of the release the nodes mount with", () => {
    const objects = install();
    const [agent] = one<V1DaemonSet>(objects, "DaemonSet", "alasio-node-agent").spec?.template.spec?.containers ?? [];
    // As a Valkey user of its own, of no command Valkey calls dangerous but INFO, and no key of workspace storage's object store.
    assert.deepEqual(agent?.env?.find(({ name }) => name === "META_URL")?.valueFrom?.secretKeyRef, { name: "alasio-workspaces-quotas", key: "metaurl" });
    assert.deepEqual((agent?.env ?? []).filter(({ valueFrom }) => valueFrom?.secretKeyRef?.name === "alasio-workspaces-juicefs"), []);
    const [server] = one<V1StatefulSet>(objects, "StatefulSet", "alasio-valkey").spec?.template.spec?.containers ?? [];
    const args = server?.command ?? [];
    assert.deepEqual(args.slice(args.indexOf("--user")), ["--user", "quotas", "on", ">$(VALKEY_QUOTAS_PASSWORD)", "~*", "&*", "+@all", "-@dangerous", "+info"]);
    assert.ok(args.includes("--requirepass"));
    assert.deepEqual(server?.env?.find(({ name }) => name === "VALKEY_QUOTAS_PASSWORD")?.valueFrom?.secretKeyRef, { name: "alasio-workspaces-quotas", key: "password" });
    // JuiceFS's own release of the mount image's version, pinned by its checksum, checked to be that version as the image is built.
    const { tag } = configOf().workspaceStorage.image;
    const dockerfile = readFileSync(new URL("../../Dockerfile", import.meta.url), "utf8");
    assert.equal(/^ARG JUICEFS_VERSION=(.+)$/mu.exec(dockerfile)?.[1], tag.replace(/^ce-v/u, ""));
    assert.match(dockerfile, /^ARG JUICEFS_SHA256=[0-9a-f]{64}$/mu);
    assert.match(dockerfile, /sha256sum -c -/u);
    assert.match(dockerfile, /juicefs version \| grep -F "juicefs version \$\{JUICEFS_VERSION\}\+"/u);
    // And the metadata engine takes them, as they reach it.
    const policies = all<NetworkPolicyObject>(objects, "NetworkPolicy");
    const pod = (component: string): Pod => ({ namespace: "alasio", namespaceLabels: { "kubernetes.io/metadata.name": "alasio" }, labels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": component } });
    assert.equal(connects(policies, pod("node-agent"), pod("valkey"), 6379), true);
    assert.equal(connects(policies, pod("sandbox-gateway"), pod("valkey"), 6379), false);
  });

  test("runs none of it without workspace storage, or where sandboxes are not snapshotted, as the node agents attach its workspaces", () => {
    for (const file of [{ workspaceStorage: { enabled: false } }, { sessions: { runtimeClassName: "" } }]) {
      const objects = install(file);
      assert.deepEqual(objects.filter(({ metadata }) => /sandbox-gateway|sandboxes-|alasio-receiver|workspace-mount/u.test(metadata?.name ?? "")).map(({ kind, metadata }) => `${kind}/${metadata?.name}`), [], JSON.stringify(file));
    }
  });
});

test("every workload KEDA scales on its CPU is scaled up only on a use that outlasts its start-up, so no rollout scales it up", () => {
  const range = { scaling: { min: 2, max: 4 } };
  const objects = install({ grafana: range, telemetry: { collector: { gateway: range } }, objectStore: { bundled: { gateways: range } }, lake: { query: range } });
  const onCpu = all<ScaledObject>(objects, "ScaledObject").filter(({ spec }) => spec.triggers.some(({ type }) => type === "cpu"));
  assert.deepEqual(
    onCpu.map(({ metadata, spec }) => [metadata?.name, spec.advanced?.horizontalPodAutoscalerConfig.behavior.scaleUp.stabilizationWindowSeconds]).sort(),
    [["alasio-collector", 60], ["alasio-grafana", 180], ["alasio-seaweedfs", 120]],
  );
  // What scales on what waits for it rather than on CPU (the lake's readers, on their queue) has no start-up to outlast.
  assert.equal(one<ScaledObject>(objects, "ScaledObject", "alasio-lake-query").spec.advanced, undefined);
});

test("no field Kubernetes changes of no object once made carries the release's version, which would refuse every upgrade: workloads' selectors, StatefulSets' service names and claim templates", () => {
  /** The fields of `object` Kubernetes changes of none once made, by their names. */
  const immutable = ({ kind, spec }: KubernetesObject & { spec?: Record<string, unknown> }): [string, unknown][] => {
    switch (kind) {
      case "Deployment":
      case "DaemonSet":
      case "ReplicaSet":
        return [["selector", spec?.["selector"]]];
      case "StatefulSet":
        return [["selector", spec?.["selector"]], ["serviceName", spec?.["serviceName"]], ["volumeClaimTemplates", spec?.["volumeClaimTemplates"]]];
      default:
        // A Job's pod template, which cannot change either, is run afresh, one of its name deleted first (src/kube/apply.ts).
        return [];
    }
  };
  for (const objects of [install(), branchObjects(configOf(), "try", BASELINE)]) {
    for (const object of objects) {
      for (const [field, value] of immutable(object)) {
        const said = JSON.stringify(value ?? null);
        assert.ok(!said.includes("app.kubernetes.io/version") && !said.includes(VERSION), `${object.kind} ${object.metadata?.name}'s ${field}: ${said}`);
      }
    }
  }
  // The log's claims carry its pods' other labels.
  const log = one<V1StatefulSet>(install(), "StatefulSet", "alasio-log");
  const { "app.kubernetes.io/version": _version, ...podLabels } = log.spec?.template.metadata?.labels ?? {};
  assert.deepEqual(log.spec?.volumeClaimTemplates?.[0]?.metadata?.labels, podLabels);
});

describe("conversations' harnesses' pools", () => {
  const pooling = { harness: { pools: { enabled: true } } };
  const podOf = (component: string, namespace = "alasio", labels: Record<string, string> = {}, namespaceLabels: Record<string, string> = {}): Pod => ({
    namespace,
    namespaceLabels: { "kubernetes.io/metadata.name": namespace, ...namespaceLabels },
    labels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": component, ...labels },
  });

  test("run in alasio's process unless turned on, and only where the sandbox service binds them", () => {
    const objects = install();
    assert.deepEqual(all<V1Deployment>(objects, "Deployment").filter(({ metadata }) => metadata?.name?.startsWith("harness-")), []);
    assert.equal(container(one<V1Deployment>(objects, "Deployment", "alasio")).env?.some(({ name }) => name === "ALASIO_HARNESS_POOLS"), false);
    const refused = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, ...pooling, workspaceStorage: { enabled: false } });
    assert.ok(Result.isFailure(refused));
    assert.match(refused.failure.message, /^harness\.pools\.enabled must be false unless the sandbox service runs/u);
  });

  test("their pods' variables are alasio's first, then their own, which win; their homes the configuration may not set", () => {
    const objects = install({ ...pooling, alasio: { telegram: { existingSecret: "telegram" }, env: { TELEGRAM_API_ROOT: "http://telegram", ALASIO_STATE_DIR: "/elsewhere" } } });
    const env = container(one<V1Deployment>(objects, "Deployment", "harness-codex")).env ?? [];
    assert.deepEqual(env.slice(0, 2), [{ name: "ALASIO_STATE_DIR", value: "/elsewhere" }, { name: "TELEGRAM_API_ROOT", value: "http://telegram" }]);
    // The last of a name is what the container is given.
    assert.equal(env.findLast(({ name }) => name === "ALASIO_STATE_DIR")?.value, "/var/lib/alasio/state");
    for (const name of ["HOME", "CODEX_HOME"]) {
      const refused = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" }, env: { [name]: "/home/elsewhere" } }, ...pooling });
      assert.ok(Result.isFailure(refused), name);
      assert.match(refused.failure.message, new RegExp(`^alasio\\.env\\.${name} must not be set while harness\\.pools\\.enabled: a harness pool's pod has a home and a Codex home of its own`, "u"));
      assert.ok(Result.isSuccess(decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" }, env: { [name]: "/home/elsewhere" } } })), name);
    }
  });

  test("their pods run on the nodes the node agents run on, which write their bindings, not alasio's own", () => {
    const sessionNodes = { nodeSelector: { "alasio.dev/sessions": "true" }, tolerations: [{ key: "alasio.dev/sessions", operator: "Exists", effect: "NoSchedule" }] };
    const objects = install({ ...pooling, alasio: { telegram: { existingSecret: "telegram" }, nodeSelector: { "alasio.dev/control": "true" } }, sessions: sessionNodes });
    const agents = one<V1DaemonSet>(objects, "DaemonSet", "alasio-node-agent").spec?.template.spec;
    for (const name of ["harness-codex", "harness-claude"]) {
      const spec = one<V1Deployment>(objects, "Deployment", name).spec?.template.spec;
      assert.deepEqual([spec?.nodeSelector, spec?.tolerations], [agents?.nodeSelector, agents?.tolerations], name);
      assert.deepEqual(spec?.nodeSelector, sessionNodes.nodeSelector, name);
    }
  });

  test("their pods' policy pins each kind of pool's template once, so it stays no larger than the sandbox pods', which the API server compiles in time", () => {
    const objects = install({ ...pooling, host: { enabled: true, uid: 1001, gid: 1002, home: "/home/operator", mounts: [{ name: "home", hostPath: "/home/operator", mountPath: "/home/operator" }] } });
    const policy = (name: string) => JSON.stringify(one(objects, "ValidatingAdmissionPolicy", name)).length;
    // main's session pools and its folder pools: one pin each.
    const pins = (one(objects, "ValidatingAdmissionPolicy", "alasio-harness-pool-pods") as unknown as { spec: { variables: { name: string }[] } }).spec.variables.filter(({ name }) => /^pin\d+$/u.test(name));
    assert.deepEqual(pins.map(({ name }) => name), ["pin0", "pin1"]);
    assert.ok(policy("alasio-harness-pool-pods") < policy("alasio-sandbox-pods"), `${policy("alasio-harness-pool-pods")} of ${policy("alasio-sandbox-pods")}`);
  });

  test("of each harness in each environment's own namespace, alasio's image free until bound, its binding written beside it, its database the harness role's, no principal of the log its own, scaled on the binds that wait", () => {
    const objects = install(pooling);
    for (const harness of ["codex", "claude"]) {
      const pool = one<V1Deployment>(objects, "Deployment", `harness-${harness}`);
      assert.equal(pool.metadata?.namespace, "alasio");
      assert.deepEqual(pool.spec?.selector.matchLabels, { "alasio.dev/pool": `harness-${harness}`, "alasio.dev/free": "true" });
      assert.deepEqual(pool.spec?.strategy, { type: "Recreate" });
      const spec = pool.spec?.template.spec;
      assert.equal(spec?.automountServiceAccountToken, false);
      assert.deepEqual([spec?.securityContext?.runAsUser, spec?.securityContext?.runAsNonRoot], [1000, true]);
      const host = container(pool);
      assert.deepEqual(host.command, ["node", "/opt/alasio/src/harness/main.ts"]);
      const env = Object.fromEntries((host.env ?? []).map(({ name, value, valueFrom }) => [name, value ?? valueFrom?.secretKeyRef ?? valueFrom?.fieldRef?.fieldPath]));
      assert.deepEqual(
        [env["ALASIO_HARNESS_BINDING_FILE"], env["ALASIO_HARNESS_PORT"], env["ALASIO_STATE_DIR"], env["ALASIO_DATABASE_URL_FILE"], env["ALASIO_GATEWAY_URL"], env["ALASIO_BRANCH"]],
        ["/attach/binding.json", "8080", "/var/lib/alasio/state", "/run/alasio/database/url", "http://alasio-sandbox-gateway.alasio.svc.cluster.local:8080", undefined],
      );
      // Nothing of the log: what it tells goes through the gateway, and alasio wakes its lake's loader for it.
      assert.deepEqual(Object.keys(env).filter((name) => /^(EVENTS_|ALASIO_LOG_|ALASIO_LAKE_WAKE)/u.test(name)), []);
      assert.equal(spec?.volumes?.some(({ name }) => name === "log"), false);
      assert.deepEqual(host.readinessProbe?.httpGet, { path: "/healthz", port: "host" });
      assert.deepEqual(host.volumeMounts?.find(({ name }) => name === "attach"), { name: "attach", mountPath: "/attach", subPathExpr: "$(POD_NAMESPACE)/$(POD_NAME)", readOnly: true });
      assert.deepEqual(spec?.volumes?.find(({ name }) => name === "database")?.secret, { secretName: "alasio-harness-database", items: [{ key: "url", path: "url" }] });
      const scaler = one<ScaledObject>(objects, "ScaledObject", `harness-${harness}`);
      assert.equal(JSON.stringify(scaler.spec).includes(`/counts/alasio/harness-${harness}`), true);
    }
    // No folder conversation's, of no host profile.
    assert.deepEqual(all<V1Deployment>(objects, "Deployment").filter(({ metadata }) => metadata?.name?.startsWith("harness-folder-")), []);
    // The gateway configured with them, and alasio told they run so.
    const config = JSON.parse(one<V1ConfigMap>(objects, "ConfigMap", "alasio-sandbox-gateway").data?.["config.json"] ?? "{}");
    assert.deepEqual(config.harnesses, { namespace: "alasio", pools: { codex: "harness-codex", claude: "harness-claude" }, podPort: 8080, owner: { uid: 1000, gid: 1000 }, releaseAfterSeconds: 300, drainSeconds: 120, folders: null });
    const alasio = Object.fromEntries((container(one<V1Deployment>(objects, "Deployment", "alasio")).env ?? []).map(({ name, value }) => [name, value]));
    assert.equal(alasio["ALASIO_HARNESS_POOLS"], "1");
    // Its Codex login relayed to its pools' pods, on its pod's port, through a Service of its own.
    assert.equal(alasio["ALASIO_CODEX_LOGIN_URL"], "http://alasio-codex-login.alasio.svc.cluster.local:4320/v1");
    assert.deepEqual(container(one<V1Deployment>(objects, "Deployment", "alasio")).ports?.find(({ name }) => name === "codex-login")?.containerPort, 4320);
    assert.deepEqual(one<V1Service>(objects, "Service", "alasio-codex-login").spec?.ports, [{ name: "codex-login", port: 4320, targetPort: "codex-login" }]);
  });

  test("of main's folder conversations, beside its alasio, as the operator with the machine's mounts, where the host profile renders folder workspaces", () => {
    const objects = install({ ...pooling, host: { enabled: true, uid: 1001, gid: 1002, home: "/home/op", mounts: [{ name: "home", hostPath: "/home/op", mountPath: "/home/op" }] } });
    const pool = one<V1Deployment>(objects, "Deployment", "harness-folder-claude");
    assert.equal(pool.metadata?.namespace, "alasio");
    assert.deepEqual([pool.spec?.template.spec?.securityContext?.runAsUser, pool.spec?.template.spec?.securityContext?.runAsGroup], [1001, 1002]);
    assert.ok(container(pool).volumeMounts?.some(({ name, mountPath }) => name === "home" && mountPath === "/home/op"));
    // Its Codex's home its own, the operator's, with their login, hidden under the mount that would show it.
    assert.equal(container(pool).env?.find(({ name }) => name === "CODEX_HOME")?.value, "/var/lib/alasio/codex");
    const hidden = container(pool).volumeMounts?.find(({ mountPath }) => mountPath === "/home/op/.codex");
    assert.deepEqual(pool.spec?.template.spec?.volumes?.find(({ name }) => name === hidden?.name), { name: "codex-hidden", emptyDir: {} });
    assert.equal(container(pool).env?.find(({ name }) => name === "HOME")?.value, "/home/op");
    const config = JSON.parse(one<V1ConfigMap>(objects, "ConfigMap", "alasio-sandbox-gateway").data?.["config.json"] ?? "{}");
    assert.deepEqual(config.harnesses.folders, { pools: { codex: "harness-folder-codex", claude: "harness-folder-claude" }, owner: { uid: 1001, gid: 1002 } });
    // None of a branch's, as a branch has no folder.
    const branch = branchObjects(configOf({ ...pooling, host: { enabled: true } }), "try", BASELINE);
    assert.deepEqual(all<V1Deployment>(branch, "Deployment").filter(({ metadata }) => metadata?.name?.startsWith("harness-")).map(({ metadata }) => `${metadata?.namespace}/${metadata?.name}`), ["alasio-branch-try/harness-codex", "alasio-branch-try/harness-claude"]);
  });

  test("Claude Code's login, where the installation gives one, in each pool's pods as in alasio's own, the operator's Claude home with it in a folder conversation's; none where it gives none", () => {
    const host = { enabled: true, uid: 1001, gid: 1002, home: "/home/op", mounts: [{ name: "home", hostPath: "/home/op", mountPath: "/home/op" }] };
    const tokenOf = (pool: V1Deployment) => container(pool).env?.find(({ name }) => name === "CLAUDE_CODE_OAUTH_TOKEN")?.valueFrom?.secretKeyRef;
    const given = install({ ...pooling, host, alasio: { claude: { existingSecret: "claude" } } });
    for (const name of ["harness-claude", "harness-codex", "harness-folder-claude", "harness-folder-codex", "alasio"]) {
      assert.deepEqual(tokenOf(one<V1Deployment>(given, "Deployment", name)), { name: "claude", key: "token" }, name);
    }
    // A folder conversation's, with the operator's home, their ~/.claude among it, as alasio's pod is under the host profile.
    assert.ok(container(one<V1Deployment>(given, "Deployment", "harness-folder-claude")).volumeMounts?.some(({ mountPath }) => mountPath === "/home/op"));
    const none = install({ ...pooling, host });
    for (const name of ["harness-claude", "harness-folder-claude"]) assert.equal(tokenOf(one<V1Deployment>(none, "Deployment", name)), undefined, name);
  });

  test("a branch environment's in its own namespace, and the gateway may claim, read and delete their pods alone, and make none", () => {
    const branch = branchObjects(configOf(pooling), "try", BASELINE);
    const pool = one<V1Deployment>(branch, "Deployment", "harness-codex");
    const env = Object.fromEntries((container(pool).env ?? []).map(({ name, value, valueFrom }) => [name, value ?? valueFrom?.secretKeyRef]));
    assert.equal(env["ALASIO_BRANCH"], "try");
    for (const objects of [install(pooling), branch]) {
      const role = all<V1Role>(objects, "Role").find(({ metadata }) => metadata?.name === "alasio-sandbox-gateway-harnesses");
      assert.deepEqual(role?.rules?.find(({ resources }) => resources?.includes("pods"))?.verbs, ["get", "list", "watch", "update", "delete"]);
    }
    // What the gateway does held by the cluster's policy, made with main's alone; each environment's pools' pods held by a policy of its own, of its namespace alone.
    for (const policy of ["alasio-harness-pods", "alasio-harness-pool-pods"]) assert.equal(all(install(pooling), "ValidatingAdmissionPolicy").filter(({ metadata }) => metadata?.name === policy).length, 1, policy);
    const branchPolicies = all<{ metadata?: { name?: string }; spec: { matchConditions: { name: string; expression: string }[] } }>(branch, "ValidatingAdmissionPolicy");
    assert.deepEqual(branchPolicies.map(({ metadata }) => metadata?.name), ["alasio-harness-pool-pods-try"]);
    assert.deepEqual(branchPolicies[0]?.spec.matchConditions[0], { name: "of-the-environment", expression: 'request.namespace == "alasio-branch-try"' });
    assert.deepEqual(all(branch, "ValidatingAdmissionPolicyBinding").map(({ metadata }) => metadata?.name), ["alasio-harness-pool-pods-try"]);
  });

  test("confines a session conversation's pool's pods to the gateway in, and the gateway, Neon's proxy, the log, and the internet's HTTPS out, none of the cluster's private addresses", () => {
    const policies = all<NetworkPolicyObject>(install(pooling), "NetworkPolicy");
    const harness = (pool: string) => podOf("harness", "alasio", { "alasio.dev/pool": pool, "alasio.dev/binding": "0".repeat(40) });
    const [gateway, alasio, proxy, log, agent] = ["sandbox-gateway", "alasio", "neon-proxy", "log", "node-agent"].map((component) => podOf(component));
    assert.ok(gateway && alasio && proxy && log && agent);
    const internet: Pod = { ...podOf("anthropic", "elsewhere"), address: "160.79.104.10" };
    const privateAddress: Pod = { ...podOf("anything", "elsewhere"), address: "10.0.0.1" };
    for (const pool of ["harness-codex", "harness-claude"]) {
      const pod = harness(pool);
      assert.equal(connects(policies, gateway, pod, 8080), true);
      assert.equal(connects(policies, alasio, pod, 8080), false);
      assert.equal(connects(policies, pod, gateway, 8080), true);
      assert.equal(connects(policies, pod, proxy, 5432), true);
      assert.equal(connects(policies, pod, log, 9092), true);
      assert.equal(connects(policies, pod, internet, 443), true);
      assert.equal(connects(policies, pod, privateAddress, 443), false);
      assert.equal(connects(policies, pod, agent, 8484), false);
      // Its alasio's relay of its Codex login.
      assert.equal(connects(policies, pod, alasio, 4320), true);
    }
    // None of another environment's.
    const elsewhere = podOf("harness", "alasio-branch-try", { "alasio.dev/pool": "harness-codex" }, { "alasio.dev/branch": "try" });
    assert.equal(connects(policies, elsewhere, alasio, 4320), false);
    assert.equal(connects(policies, gateway, alasio, 4320), false);
    // A branch environment's, by the gateway, which reaches its pods.
    const branchPod = podOf("harness", "alasio-branch-try", { "alasio.dev/pool": "harness-codex" }, { "alasio.dev/branch": "try" });
    const both = [...policies, ...all<NetworkPolicyObject>(branchObjects(configOf(pooling), "try", BASELINE), "NetworkPolicy")];
    assert.equal(connects(both, gateway, branchPod, 8080), true);
    assert.equal(connects(both, branchPod, proxy, 5432), true);
  });
});
