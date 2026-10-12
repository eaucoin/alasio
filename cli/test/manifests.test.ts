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

import type {
  KubernetesObject,
  V1ClusterRole,
  V1ConfigMap,
  V1Container,
  V1CronJob,
  V1CSIDriver,
  V1DaemonSet,
  V1Deployment,
  V1Job,
  V1LabelSelector,
  V1Namespace,
  V1PersistentVolumeClaim,
  V1PodSpec,
  V1PriorityClass,
  V1Role,
  V1RoleBinding,
  V1Service,
  V1StatefulSet,
  V1StorageClass,
} from "@kubernetes/client-node";
import { Result } from "effect";

import type { HostProfile, SessionsProfile } from "../../src/kube/config.ts";
import { hostBaymaStateDir } from "../../src/mcp/bayma.ts";
import { memoryBytes } from "../src/capacity.ts";
import { installConfigOf } from "../src/config.ts";
import { branchObjects } from "../src/manifests/branch.ts";
import type { ScaledObject } from "../src/manifests/scalers.ts";
import type { VerticalPodAutoscaler } from "../src/manifests/sizing.ts";
import { decodeInstallConfig, type InstallConfig, manifests } from "../src/manifests/index.ts";
import { filerStoreRestore } from "../src/manifests/object-store.ts";

/** The configuration `file`, with the Telegram Secret every configuration names. */
/** A NetworkPolicy as rendered, its ingress rules' peers under `from`. */
interface NetworkPolicyObject extends KubernetesObject {
  readonly spec: {
    readonly podSelector: V1LabelSelector;
    readonly policyTypes?: readonly string[];
    readonly ingress?: readonly { readonly from?: readonly Peer[]; readonly ports?: readonly { readonly port?: number | string }[] }[];
  };
}

/** A peer of an ingress rule. */
interface Peer {
  readonly namespaceSelector?: V1LabelSelector;
  readonly podSelector?: V1LabelSelector;
  readonly ipBlock?: object;
}

/** A pod, as NetworkPolicies select it: by its labels, and its namespace's. */
interface Pod {
  readonly namespace: string;
  readonly namespaceLabels: Readonly<Record<string, string>>;
  readonly labels: Readonly<Record<string, string>>;
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

function configOf(file: Record<string, unknown> = {}): InstallConfig {
  const { alasio, ...rest } = file;
  const decoded = decodeInstallConfig({ ...rest, alasio: { telegram: { existingSecret: "telegram" }, ...(alasio as object | undefined) } });
  if (Result.isFailure(decoded)) throw new Error(decoded.failure.message);
  return decoded.success;
}

/** The objects of the configuration `file` and the Telegram Secret every configuration names. */
function install(file: Record<string, unknown> = {}): readonly KubernetesObject[] {
  return manifests(configOf(file));
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
  const run = (): string =>
    execFileSync("/bin/sh", ["-c", text ?? ""], { env: { ...env, ROOT: root, PATH: `${bin}:${process.env["PATH"]}` }, encoding: "utf8" });
  return { root, run, remove: () => rmSync(root, { recursive: true, force: true }) };
}

/** The templates alasio makes its workspaces' Sandboxes from, as its ConfigMap holds them. */
function templates(objects: readonly KubernetesObject[]): string {
  return one<V1ConfigMap>(objects, "ConfigMap", "alasio-sandbox-templates").data?.["templates.json"] ?? "";
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
      nodeCache: "5Gi",
      nodeAgent: { resources: { requests: { cpu: "20m", memory: "96Mi" }, limits: { memory: "512Mi" } } },
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
    assert.equal(workspaceStorage.storageClassName, "alasio-workspaces");
    assert.equal(workspaceStorage.backupInterval, "1h");
    assert.equal(workspaceStorage.valkey.maxmemory, "384mb");
    assert.equal(workspaceStorage.csi.shareMountPod, false);
    assert.equal(objectStore.buckets.workspaces, "workspaces");
    assert.equal(objectStore.buckets.snapshots, "snapshots");
    const refused = (given: Record<string, unknown>) => {
      const result = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, workspaceStorage: given });
      return Result.isFailure(result) ? result.failure.message : "";
    };
    assert.equal(refused({ backupInterval: "4m59s" }), "workspaceStorage.backupInterval must be a duration of 5m or more, such as 1h");
    assert.equal(refused({ backupInterval: "0" }), "workspaceStorage.backupInterval must be a duration of 5m or more, such as 1h");
    assert.equal(refused({ backupInterval: "1h30m" }), "");
    assert.equal(refused({ trashDays: -1 }), "workspaceStorage.trashDays must be 0 or more");
    assert.equal(refused({ name: "Workspaces" }), "workspaceStorage.name must be a JuiceFS file system name");
    assert.equal(refused({ valkey: { maxmemory: "384MB" } }), "workspaceStorage.valkey.maxmemory must be an amount of memory such as 384mb");
    assert.equal(refused({ csi: { mountImage: { digest: "latest" } } }), "workspaceStorage.csi.mountImage.digest must be a sha256 digest, or empty");
  });
});

describe("alasio", () => {
  test("refuses to install without the Telegram Secret", () => {
    const decoded = decodeInstallConfig({});
    assert.ok(Result.isFailure(decoded));
    assert.match(decoded.failure.message, /telegram\.existingSecret/u);
  });

  test("runs one alasio at a time, never two", () => {
    const deployment = one<V1Deployment>(install(), "Deployment", "alasio");
    assert.equal(deployment.spec?.replicas, 1);
    assert.equal(deployment.spec?.strategy?.type, "Recreate");
  });

  test("drives Kubernetes from the rendered templates, and keeps no volume: its state is in Neon, its home and what it writes on emptyDirs", () => {
    const objects = install();
    const deployment = one<V1Deployment>(objects, "Deployment", "alasio");
    const env = container(deployment).env;
    assert.deepEqual(env?.find(({ name }) => name === "ALASIO_KUBE_TEMPLATES"), { name: "ALASIO_KUBE_TEMPLATES", value: "/etc/alasio/templates.json" });
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

  test("renders sessions under gVisor with the egress gate, and no host profile by default", () => {
    const json = templates(install());
    assert.match(json, /"runtimeClassName": "gvisor"/u);
    assert.match(json, /"egressGate": true/u);
    assert.doesNotMatch(json, /"host":/u);
  });

  test("renders sessions' pods never restarted, their /tmp a tmpfs of gVisor's own", () => {
    const { podTemplate } = JSON.parse(templates(install())).sessions as SessionsProfile;
    assert.equal(podTemplate.spec?.restartPolicy, "Never");
    assert.deepEqual(podTemplate.metadata?.annotations, {
      "dev.gvisor.spec.mount.tmp.share": "container",
      "dev.gvisor.spec.mount.tmp.type": "tmpfs",
      "dev.gvisor.spec.mount.tmp.options": "rw",
    });
    assert.deepEqual(podTemplate.spec?.volumes, [{ name: "tmp", emptyDir: { sizeLimit: "2Gi" } }]);
  });

  test("renders sessions under the cluster's default runtime when asked", () => {
    assert.doesNotMatch(templates(install({ sessions: { runtimeClassName: "" } })), /runtimeClassName/u);
  });

  test("renders the host profile's bayma with what it needs to snapshot its sessions", () => {
    const json = templates(install({ host: { enabled: true, stateRoot: "/home/op/.alasio/bayma" } }));
    assert.match(json, /"CHECKPOINT_RESTORE"/u);
    assert.match(json, /"stateRoot": "\/home\/op\/\.alasio\/bayma"/u);
    assert.match(json, /"serviceAccountName": "alasio-host-agent"/u);
  });

  test("restarts alasio when the templates change", () => {
    const checksum = (objects: readonly KubernetesObject[]) =>
      one<V1Deployment>(objects, "Deployment", "alasio").spec?.template.metadata?.annotations?.["checksum/sandbox-templates"];
    assert.match(checksum(install()) ?? "", /^[0-9a-f]{64}$/u);
    assert.equal(checksum(install()), checksum(install()));
    assert.notEqual(checksum(install()), checksum(install({ sessions: { egressGate: false } })));
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

  test("runs each workload at the count its scale fixes: one, but for the safekeepers, and the compute, whose count neon-control owns", () => {
    const objects = install({ host: { enabled: true } });
    const workloads = [...all<V1Deployment>(objects, "Deployment"), ...all<V1StatefulSet>(objects, "StatefulSet")];
    assert.ok(workloads.length > 10);
    const count = (name: string | undefined) => (name === "alasio-neon-safekeeper" ? 3 : name === "alasio-neon-compute" ? undefined : 1);
    for (const { metadata, spec } of workloads) assert.equal(spec?.replicas, count(metadata?.name), metadata?.name);
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
    // Its Service takes connections as soon as Postgres does, before kubelet next probes the pod.
    assert.equal(one<V1Service>(install(), "Service", "alasio-neon-compute").spec?.publishNotReadyAddresses, true);
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
      { apiGroups: ["apps"], resources: ["deployments"], resourceNames: ["alasio-neon-compute"], verbs: ["get"] },
      { apiGroups: ["apps"], resources: ["deployments/scale"], resourceNames: ["alasio-neon-compute"], verbs: ["patch"] },
      { apiGroups: [""], resources: ["pods"], verbs: ["list"] },
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
    assert.equal(statefulSets.length, 7);
    for (const statefulSet of statefulSets) assert.match(container(statefulSet).image ?? "", /@sha256:[0-9a-f]{64}$/u, statefulSet.metadata?.name);
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
    assert.deepEqual(snapshots(install({ sessions: { runtimeClassName: "" } })), { buckets: "neon lake backups workspaces", rules: undefined, setup: undefined });
    assert.deepEqual(snapshots(install({ sessions: { enabled: false }, workspaceStorage: { enabled: false } })), { buckets: "neon lake backups", rules: undefined, setup: undefined });
  });

  test("uses the operator's object store and makes no SeaweedFS when told", () => {
    const objects = install({ objectStore: { bundled: { enabled: false }, external: { endpoint: "https://s3.example.com", existingSecret: "s3" } } });
    assert.deepEqual(objects.filter((object) => object.metadata?.name?.startsWith("alasio-seaweedfs")), []);
    const env = one<V1Job>(objects, "Job", "alasio-neon-setup").spec?.template.spec?.containers[0]?.env;
    assert.deepEqual(env?.find(({ name }) => name === "S3_EXTERNAL"), { name: "S3_EXTERNAL", value: "1" });
    // Its operator makes the snapshots bucket, which the setup still gives the node agents.
    assert.deepEqual(env?.find(({ name }) => name === "SNAPSHOTS_BUCKET"), { name: "SNAPSHOTS_BUCKET", value: "snapshots" });
  });

  test("runs no Neon when the database is the operator's", () => {
    const objects = install({ neon: { enabled: false, external: { existingSecret: "db" } }, workspaceStorage: { enabled: false } });
    assert.deepEqual(objects.filter((object) => object.metadata?.labels?.["alasio.dev/stack"] === "neon"), []);
    assert.deepEqual(objects.filter((object) => object.metadata?.name === "alasio-lake"), []);
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
    const env = Object.fromEntries((upload.env ?? []).flatMap(({ name, value }) => (value === undefined ? [] : [[name, value]])));
    assert.equal(env["WORKSPACES_BUCKET"], "workspaces");
    assert.equal(env["WORKSPACES_NAME"], "workspaces");
    // The object store, a directory of buckets, as aws s3 lists, copies to and deletes from it; the dump's volume under ROOT.
    const aws = [
      "shift 3; verb=$1; shift",
      'at() { printf "%s/%s" "$ROOT/store" "${1#s3://}"; }',
      "case $verb in",
      '  ls) [ -d "$(at "$1")" ] || exit 1; for f in "$(at "$1")"*; do [ -e "$f" ] && echo "2026-10-05 03:17:00 1 ${f##*/}"; done; true ;;',
      '  cp) from=$1; case $from in s3://*) from=$(at "$from") ;; *) from=$ROOT$from ;; esac; mkdir -p "$(dirname "$(at "$2")")"; cp "$from" "$(at "$2")" ;;',
      '  rm) rm "$(at "$1")" ;;',
      "esac",
    ].join("\n");
    const { root, run, remove } = runScript(upload, env, { aws });
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
      writeFileSync(join(meta, "dump-2026-10-04-021000.json.gz"), "older");
      writeFileSync(join(meta, "dump-2026-10-05-021000.json.gz"), "newest");
      run();
      assert.deepEqual(backups().filter((name) => name.startsWith("workspaces-")), ["workspaces-dump-2026-10-04-021000.json.gz", "workspaces-dump-2026-10-05-021000.json.gz"]);
      assert.equal(readFileSync(join(store, "backups", "workspaces-dump-2026-10-05-021000.json.gz"), "utf8"), "newest");
      assert.equal(backups().filter((name) => name.startsWith("alasio-")).length, 2);
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

describe("the lake's query endpoint", () => {
  test("reads the lake through its query endpoint, a container of the lake's pod with the reader's credentials alone", () => {
    const lake = one<V1Deployment>(install({ lake: { query: { resources: { limits: { memory: "2Gi" } } } } }), "Deployment", "alasio-lake");
    const query = lake.spec?.template.spec?.containers.find(({ name }) => name === "query");
    assert.deepEqual(query?.command, ["node", "src/endpoint.ts"]);
    assert.equal(query?.envFrom, undefined);
    const secrets = query?.env?.flatMap(({ name, valueFrom }) => (valueFrom?.secretKeyRef ? [`${name}=${valueFrom.secretKeyRef.name}/${valueFrom.secretKeyRef.key}`] : []));
    assert.deepEqual(secrets, ["LAKE_READER_PASSWORD", "LAKE_READER_S3_KEY", "LAKE_READER_S3_SECRET", "LAKE_QUERY_TOKEN"].map((key) => `${key}=alasio-lake/${key}`));
    assert.deepEqual(query?.resources?.limits, { memory: "2Gi" });
    assert.deepEqual(query?.ports, [{ name: "query", containerPort: 8090 }]);
  });

  test("has alasio make the lake reader's role while the lake runs", () => {
    const alasio = (file: Record<string, unknown>) => one<V1Deployment>(install(file), "Deployment", "alasio");
    assert.deepEqual(verifierFiles(alasio({}))?.slice(0, 2), [
      ["ALASIO_LAKE_VERIFIER_FILE", "/run/alasio/database/lake-verifier"],
      ["ALASIO_LAKE_READER_VERIFIER_FILE", "/run/alasio/database/lake-reader-verifier"],
    ]);
    const database = alasio({}).spec?.template.spec?.volumes?.find(({ name }) => name === "database");
    assert.deepEqual(database?.secret?.items?.map(({ key }) => key).slice(0, 3), ["url", "lake-verifier", "lake-reader-verifier"]);
    assert.deepEqual(verifierFiles(alasio({ lake: { enabled: false } }))?.map(([name]) => name), ["ALASIO_LAKE_VERIFIER_FILE"]);
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

  test("admits the filers alone, the collector and the backups to the filer store, which is no part of the data stack", () => {
    const objects = install();
    const store = one<V1StatefulSet>(objects, "StatefulSet", "alasio-seaweedfs-filer-store");
    assert.equal(store.spec?.template.metadata?.labels?.["alasio.dev/stack"], undefined);
    const policy = one<KubernetesObject & { spec: { ingress: unknown } }>(objects, "NetworkPolicy", "alasio-seaweedfs-filer-store");
    assert.deepEqual(policy.spec.ingress, [{
      from: [
        { podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "seaweedfs-filer" } } },
        { podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "collector" } } },
        { podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "seaweedfs-filer-store-backup" } } },
      ],
      ports: [{ protocol: "TCP", port: 6379 }],
    }]);
  });

  test("admits to each part behind the gateways what talks to it alone, and the collector, and leaves them out of the data stack's policy", () => {
    const objects = install();
    type Rule = { from: { podSelector: { matchLabels: Record<string, string> } }[]; ports: { port: number }[] };
    const admitted = (name: string) =>
      one<KubernetesObject & { spec: { ingress: Rule[] } }>(objects, "NetworkPolicy", name).spec.ingress.map(({ from, ports }) => [
        from.map(({ podSelector }) => podSelector.matchLabels["app.kubernetes.io/component"]),
        ports.map(({ port }) => port),
      ]);
    assert.deepEqual(admitted("alasio-seaweedfs-master"), [
      [["seaweedfs-master", "seaweedfs-volume", "seaweedfs-filer", "seaweedfs", "seaweedfs-lifecycle"], [9333, 19333]],
      [["collector"], [9327]],
    ]);
    assert.deepEqual(admitted("alasio-seaweedfs-volume"), [
      [["seaweedfs-volume", "seaweedfs-master", "seaweedfs-filer", "seaweedfs", "seaweedfs-lifecycle"], [8080, 18080]],
      [["collector"], [9327]],
    ]);
    assert.deepEqual(admitted("alasio-seaweedfs-filer"), [[["seaweedfs-filer", "seaweedfs", "seaweedfs-lifecycle"], [8888, 18888]], [["collector"], [9327]]]);
    // The gateways take the data stack's pods on their S3 port, the lifecycle pass alone on their gRPC port, and the collector alone on their metrics port.
    const gateway = one<KubernetesObject & { spec: { ingress: { from: { podSelector: { matchLabels: Record<string, string> } }[]; ports: { port: number }[] }[] } }>(objects, "NetworkPolicy", "alasio-seaweedfs").spec.ingress;
    assert.deepEqual(gateway.map(({ from, ports }) => [from.map(({ podSelector }) => podSelector.matchLabels), ports.map(({ port }) => port)]), [
      [[{ "app.kubernetes.io/instance": "alasio", "alasio.dev/stack": "neon" }], [8333]],
      [[{ "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "seaweedfs-lifecycle" }], [18333]],
      [[{ "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "collector" }], [9327]],
    ]);
    // The filer store takes its filers, the collector and its backups; the Valkey that serves a backup as it is restored, the Job that restores it alone.
    assert.deepEqual(admitted("alasio-seaweedfs-filer-store"), [[["seaweedfs-filer", "collector", "seaweedfs-filer-store-backup"], [6379]]]);
    assert.deepEqual(admitted("alasio-seaweedfs-filer-store-restore-source"), [[["seaweedfs-filer-store-restore"], [6379]]]);
    // The data stack's pods reach none of the parts but as these say, nor the compute but as its own policy says.
    const stack = one<KubernetesObject & { spec: { podSelector: object } }>(objects, "NetworkPolicy", "alasio-neon").spec.podSelector;
    assert.deepEqual(stack, {
      matchLabels: { "app.kubernetes.io/instance": "alasio", "alasio.dev/stack": "neon" },
      matchExpressions: [{ key: "app.kubernetes.io/component", operator: "NotIn", values: ["seaweedfs", "seaweedfs-master", "seaweedfs-volume", "seaweedfs-filer", "neon-compute"] }],
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
    assert.deepEqual(store.command, [
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
    // Its readiness probe logs in as the collector; workspace storage's Valkey, of no users of its own, as its default user.
    assert.match(store.readinessProbe?.exec?.command?.[2] ?? "", /^persistence=\$\(valkey-cli --user collector info persistence\)/u);
    const valkey = container(one<V1StatefulSet>(objects, "StatefulSet", "alasio-valkey"));
    assert.deepEqual(valkey.command, ["valkey-server", "/etc/valkey/valkey.conf", "--requirepass", "$(VALKEY_PASSWORD)"]);
    assert.match(valkey.readinessProbe?.exec?.command?.[2] ?? "", /^persistence=\$\(valkey-cli info persistence\)/u);
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
    assert.deepEqual(all(single, "PodDisruptionBudget").filter(({ metadata }) => metadata?.namespace === "alasio" && metadata.name !== "alasio-busy").map(({ metadata }) => metadata?.name), ["alasio-neon-safekeeper"]);
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
    });
    // VPA sizes its memory alone, as its CPU is what it scales on.
    assert.deepEqual(one<VerticalPodAutoscaler>(objects, "VerticalPodAutoscaler", "alasio-grafana").spec.resourcePolicy.containerPolicies.map((policy) => (policy as { controlledResources?: string[] }).controlledResources), [["memory"]]);
    assert.deepEqual(all(install(), "ScaledObject"), []);
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

  test("admits no pod to Grafana, and lets it reach DNS, its database, the lake's query endpoint and the internet's HTTPS alone", () => {
    const objects = install();
    const own = one<Policy>(objects, "NetworkPolicy", "alasio-grafana").spec;
    assert.deepEqual([own.podSelector, own.policyTypes, own.ingress], [{ matchLabels: GRAFANA }, ["Ingress", "Egress"], undefined]);
    const egress = own.egress as { to: Record<string, unknown>[]; ports: { protocol: string; port: number }[] }[];
    assert.deepEqual(egress.map(({ ports }) => ports.map(({ protocol, port }) => `${protocol}/${port}`).join(",")), ["UDP/53,TCP/53", "TCP/5432", "TCP/8090", "TCP/443"]);
    assert.deepEqual(egress[3]?.to, [{ ipBlock: { cidr: "0.0.0.0/0", except: ["0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16", "224.0.0.0/4", "240.0.0.0/4"] } }]);
    // Neither Neon's proxy nor the lake admits it as a pod of the stack: each does so on its port alone.
    assert.equal(one<V1Deployment>(objects, "Deployment", "alasio-grafana").spec?.template.metadata?.labels?.["alasio.dev/stack"], undefined);
    for (const [component, port] of [["neon-proxy", 5432], ["lake", 8090]] as const) {
      assert.deepEqual(one<Policy>(objects, "NetworkPolicy", `alasio-${component}-from-grafana`).spec.ingress, [{ from: [{ podSelector: { matchLabels: GRAFANA } }], ports: [{ protocol: "TCP", port }] }]);
    }
  });

  test("has alasio make Grafana's role while Grafana runs", () => {
    const alasio = (file: Record<string, unknown>) => one<V1Deployment>(install(file), "Deployment", "alasio");
    assert.deepEqual(verifierFiles(alasio({}))?.at(-1), ["ALASIO_GRAFANA_VERIFIER_FILE", "/run/alasio/database/grafana-verifier"]);
    const database = alasio({}).spec?.template.spec?.volumes?.find(({ name }) => name === "database");
    assert.deepEqual(database?.secret?.items?.map(({ key }) => key), ["url", "lake-verifier", "lake-reader-verifier", "grafana-verifier"]);
    assert.deepEqual(verifierFiles(alasio({ grafana: { enabled: false } }))?.map(([name]) => name), ["ALASIO_LAKE_VERIFIER_FILE", "ALASIO_LAKE_READER_VERIFIER_FILE"]);
  });
});

describe("telemetry", () => {
  /** The collector's configuration, as its ConfigMap holds it. */
  const collectorConfig = (objects: readonly KubernetesObject[]) => JSON.parse(one<V1ConfigMap>(objects, "ConfigMap", "alasio-collector").data?.["config.yaml"] ?? "");
  const COLLECTOR = "http://alasio-collector.alasio.svc:4318";
  const variable = (workload: V1Deployment, name: string) => container(workload).env?.find((env) => env.name === name);

  test("runs the collector with the lake, or with an endpoint to send to, and not with neither", () => {
    assert.ok(one(install(), "Deployment", "alasio-collector"));
    assert.ok(one(install({ lake: { enabled: false }, telemetry: { otlpEndpoint: "https://otlp.example.com" } }), "Deployment", "alasio-collector"));
    assert.deepEqual(install({ lake: { enabled: false } }).filter(({ metadata }) => metadata?.name === "alasio-collector"), []);
  });

  test("takes every signal over OTLP/HTTP, and sends all it takes and scrapes to the lake's intake", () => {
    const objects = install();
    const config = collectorConfig(objects);
    assert.deepEqual(config.receivers.otlp, { protocols: { http: { endpoint: "0.0.0.0:4318" } } });
    assert.deepEqual(config.exporters, { "otlphttp/lake": { endpoint: "http://alasio-lake:4318" } });
    for (const pipeline of ["traces", "logs"]) {
      assert.deepEqual(config.service.pipelines[pipeline], { receivers: ["otlp"], processors: ["batch"], exporters: ["otlphttp/lake"] }, pipeline);
    }
    assert.deepEqual(config.service.pipelines.metrics, { receivers: ["otlp"], processors: ["batch/metrics"], exporters: ["otlphttp/lake"] });
    assert.deepEqual(config.service.pipelines["metrics/stack"], { receivers: ["prometheus", "redis", "redis/filer-store"], processors: ["resource/stack", "batch/metrics"], exporters: ["otlphttp/lake"] });
    // Spans and log records in batches no larger than an insert the lake inlines, at once;
    // metric points a minute's at a time, which the lake writes as files.
    assert.deepEqual(config.processors.batch, { send_batch_size: 1000, send_batch_max_size: 1000, timeout: "5s" });
    assert.deepEqual(config.processors["batch/metrics"], { send_batch_size: 10_000, send_batch_max_size: 10_000, timeout: "60s" });
    assert.deepEqual(one<V1Service>(objects, "Service", "alasio-collector").spec?.ports, [{ name: "otlp-http", port: 4318, targetPort: "otlp-http" }]);
    assert.deepEqual(one<V1Service>(objects, "Service", "alasio-lake").spec?.ports?.find(({ name }) => name === "otlp-http"), { name: "otlp-http", port: 4318 });
  });

  test("sends it all to telemetry.otlpEndpoint too when one is set, in its protocol, the headers going there alone", () => {
    const telemetry = { otlpEndpoint: "https://otlp.example.com", headersSecret: "otlp-headers" };
    const objects = install({ telemetry });
    const config = collectorConfig(objects);
    assert.deepEqual(config.exporters["otlphttp/external"], { endpoint: "https://otlp.example.com", encoding: "proto", headers: "${file:/etc/otelcol/headers/headers.yaml}" });
    assert.deepEqual(config.service.pipelines.traces.exporters, ["otlphttp/lake", "otlphttp/external"]);
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

  test("has alasio, the lake and the compute export to the collector, and nothing export without it", () => {
    const objects = install({ telemetry: { otlpEndpoint: "https://otlp.example.com", resourceAttributes: "deployment.environment.name=prod" } });
    for (const name of ["alasio", "alasio-lake", "alasio-neon-compute"]) {
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

  test("keeps telemetry.retentionDays days of it in the lake", () => {
    const lake = one<V1Deployment>(install({ telemetry: { retentionDays: 7 } }), "Deployment", "alasio-lake");
    assert.deepEqual(variable(lake, "LAKE_RETENTION_DAYS"), { name: "LAKE_RETENTION_DAYS", value: "7" });
  });

  test("has the lake ask neon-control for Neon's branches, whose lakes read its files, with the token of its Secret", () => {
    const lake = one<V1Deployment>(install(), "Deployment", "alasio-lake");
    assert.deepEqual(variable(lake, "LAKE_BRANCHES_URL"), { name: "LAKE_BRANCHES_URL", value: "http://alasio-neon-control:8080/branches" });
    assert.deepEqual(container(lake).envFrom, [{ secretRef: { name: "alasio-lake" } }]);
  });

  test("admits alasio, the node agents, folder workspaces' bayma, and branch environments' alasio, compute and lake to the collector, beside the stack", () => {
    const ingress = (objects: readonly KubernetesObject[]) =>
      one<KubernetesObject & { spec: { podSelector: unknown; ingress: unknown } }>(objects, "NetworkPolicy", "alasio-collector").spec;
    const alasio = { podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "alasio" } } };
    const nodeAgents = { podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "node-agent" } } };
    const branches = { matchExpressions: [{ key: "alasio.dev/branch", operator: "Exists" }] };
    const ofBranches = [
      { namespaceSelector: branches, ...alasio },
      { namespaceSelector: branches, podSelector: { matchLabels: { "app.kubernetes.io/instance": "alasio", "alasio.dev/stack": "neon" } } },
    ];
    assert.deepEqual(ingress(install()), {
      podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "collector" } },
      policyTypes: ["Ingress"],
      ingress: [{ from: [alasio, nodeAgents, ...ofBranches], ports: [{ protocol: "TCP", port: 4318 }] }],
    });
    assert.deepEqual(ingress(install({ host: { enabled: true }, sessions: { runtimeClassName: "" } })).ingress, [{
      from: [alasio, { namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "alasio-host" } } }, ...ofBranches],
      ports: [{ protocol: "TCP", port: 4318 }],
    }]);
    // A pod of the stack's, as the compute and the lake are, which the stack's own policy admits.
    assert.equal(one<V1Deployment>(install(), "Deployment", "alasio-collector").spec?.template.metadata?.labels?.["alasio.dev/stack"], "neon");
  });

  test("scrapes every service of the stack, every minute", () => {
    const config = collectorConfig(install());
    const jobs = config.receivers.prometheus.config.scrape_configs;
    assert.deepEqual(jobs.find(({ job_name }: { job_name: string }) => job_name === "safekeeper").static_configs, [{
      targets: [0, 1, 2].map((index) => `alasio-neon-safekeeper-${index}.alasio-neon-safekeeper:7676`),
    }]);
    assert.deepEqual(jobs.find(({ job_name }: { job_name: string }) => job_name === "lake").static_configs, [{ targets: ["alasio-lake:9464"] }]);
    assert.deepEqual(jobs.find(({ job_name }: { job_name: string }) => job_name === "proxy").static_configs, [{ targets: ["alasio-neon-proxy:7001"] }]);
    // Each of neon-control's replicas, found among main's pods, as any of them wakes a compute.
    const control = jobs.find(({ job_name }: { job_name: string }) => job_name === "neon-control");
    assert.deepEqual(control.kubernetes_sd_configs, [{ role: "pod", namespaces: { names: ["alasio"] }, selectors: [{ role: "pod", label: "app.kubernetes.io/component=neon-control" }] }]);
    assert.equal(control.relabel_configs[1].replacement, "$$1:8080");
    for (const job of jobs) assert.equal(job.scrape_interval, "60s", job.job_name);
    assert.equal(config.receivers.redis.collection_interval, "60s");
  });

  test("scrapes JuiceFS's pods, found in the driver's namespace, and Valkey, with its password, when workspace storage is on", () => {
    const objects = install();
    const config = collectorConfig(objects);
    // Mount pods serve their metrics on 9567, the driver's pods theirs on 8080.
    for (const [job, name, port] of [["juicefs", "juicefs-mount", 9567], ["juicefs-csi", "juicefs-csi-driver", 8080]] as const) {
      assert.deepEqual(config.receivers.prometheus.config.scrape_configs.find(({ job_name }: { job_name: string }) => job_name === job), {
        job_name: job,
        scrape_interval: "60s",
        kubernetes_sd_configs: [{
          role: "pod",
          namespaces: { names: ["kube-system"] },
          selectors: [{ role: "pod", label: `app.kubernetes.io/name=${name}` }],
        }],
        relabel_configs: [
          { source_labels: ["__meta_kubernetes_pod_phase"], regex: "Running", action: "keep" },
          { source_labels: ["__meta_kubernetes_pod_ip"], target_label: "__address__", replacement: `$$1:${port}` },
          { source_labels: ["__meta_kubernetes_pod_name"], target_label: "pod" },
        ],
      }, job);
    }
    assert.deepEqual(config.receivers.redis, {
      endpoint: "alasio-valkey:6379",
      password: "${env:VALKEY_PASSWORD}",
      collection_interval: "60s",
      metrics: { "redis.maxmemory": { enabled: true } },
      resource_attributes: { "server.address": { enabled: true } },
    });
    assert.deepEqual(config.service.pipelines["metrics/stack"].receivers, ["prometheus", "redis", "redis/filer-store"]);
    const collector = one<V1Deployment>(objects, "Deployment", "alasio-collector");
    assert.equal(collector.spec?.template.spec?.serviceAccountName, "alasio-collector");
    assert.deepEqual(container(collector).env, [
      { name: "VALKEY_PASSWORD", valueFrom: { secretKeyRef: { name: "alasio-valkey", key: "password" } } },
      { name: "FILER_STORE_PASSWORD", valueFrom: { secretKeyRef: { name: "alasio-seaweedfs-filer-store-collector", key: "password" } } },
    ]);
    // It finds JuiceFS's pods in the driver's namespace, and neon-control's and the object store's parts in its own.
    const roles = all<V1Role>(objects, "Role").filter(({ metadata }) => metadata?.name === "alasio-collector");
    assert.deepEqual(roles.map(({ metadata }) => metadata?.namespace), ["kube-system", "alasio"]);
    for (const role of roles) assert.deepEqual(role.rules, [{ apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch"] }]);
    const bindings = all<V1RoleBinding>(objects, "RoleBinding").filter(({ metadata }) => metadata?.name === "alasio-collector");
    assert.deepEqual(bindings.map(({ metadata, subjects }) => [metadata?.namespace, subjects]), ["kube-system", "alasio"].map((namespace) => [namespace, [{ kind: "ServiceAccount", name: "alasio-collector", namespace: "alasio" }]]));
    assert.ok(one(objects, "ServiceAccount", "alasio-collector"));

    const off = install({ workspaceStorage: { enabled: false } });
    const offConfig = collectorConfig(off);
    assert.equal(offConfig.receivers.redis, undefined);
    assert.deepEqual(offConfig.service.pipelines["metrics/stack"].receivers, ["prometheus", "redis/filer-store"]);
    for (const job of ["juicefs", "juicefs-csi"]) {
      assert.equal(offConfig.receivers.prometheus.config.scrape_configs.find(({ job_name }: { job_name: string }) => job_name === job), undefined, job);
    }
    assert.deepEqual(all<V1Role>(off, "Role").filter(({ metadata }) => metadata?.name === "alasio-collector").map(({ metadata }) => metadata?.namespace), ["alasio"]);
  });

  test("scrapes each pod of each of the object store's parts, found in its own namespace, and reads the filer store, with its password", () => {
    const config = collectorConfig(install());
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

  test("scrapes nothing without Neon, only taking OTLP and sending it on, and lists no pods", () => {
    const objects = install({ neon: { enabled: false, external: { existingSecret: "db" } }, workspaceStorage: { enabled: false }, telemetry: { otlpEndpoint: "https://otlp.example.com" } });
    const config = collectorConfig(objects);
    assert.equal(one<V1Deployment>(objects, "Deployment", "alasio-collector").spec?.template.spec?.serviceAccountName, undefined);
    assert.deepEqual(objects.filter(({ kind, metadata }) => !["ConfigMap", "Service", "Deployment", "VerticalPodAutoscaler", "NetworkPolicy"].includes(kind ?? "") && metadata?.name === "alasio-collector"), []);
    assert.equal(config.receivers.prometheus, undefined);
    assert.equal(config.service.pipelines["metrics/stack"], undefined);
    assert.equal(config.processors["resource/stack"], undefined);
  });
});

describe("workspace storage", () => {
  /** The sessions template, as alasio reads it. */
  const sessions = (objects: readonly KubernetesObject[]): SessionsProfile => JSON.parse(templates(objects)).sessions;

  /** The setup Job's variable of `name`, if it has one. */
  const setupVariable = (objects: readonly KubernetesObject[], name: string) =>
    one<V1Job>(objects, "Job", "alasio-neon-setup").spec?.template.spec?.containers[0]?.env?.find((variable) => variable.name === name)?.value;

  /** Every container's image of `objects`' workloads. */
  const images = (objects: readonly KubernetesObject[]) =>
    objects
      .flatMap((object) => (object as V1StatefulSet | V1DaemonSet).spec?.template?.spec?.containers ?? [])
      .map(({ image }) => image ?? "");

  test("makes new sessions' volumes of its class, mounted as JuiceFS's are", () => {
    const profile = sessions(install());
    assert.equal(profile.volumeClaimTemplates?.[0]?.spec?.storageClassName, "alasio-workspaces");
    assert.equal(profile.podTemplate.spec?.securityContext?.fsGroupChangePolicy, "OnRootMismatch");
    assert.equal(profile.podTemplate.spec?.securityContext?.fsGroup, 1000);
    const mounts = profile.podTemplate.spec?.containers.find(({ name }) => name === "bayma")?.volumeMounts;
    assert.deepEqual(mounts?.filter(({ name }) => name === "data").map(({ mountPropagation }) => mountPropagation), ["HostToContainer", "HostToContainer"]);
  });

  test("keeps the class sessions name, and without it, the cluster's default", () => {
    assert.equal(sessions(install({ sessions: { storage: { storageClassName: "fast" } } })).volumeClaimTemplates?.[0]?.spec?.storageClassName, "fast");
    const off = templates(install({ workspaceStorage: { enabled: false } }));
    assert.doesNotMatch(off, /storageClassName|fsGroupChangePolicy|mountPropagation/u);
  });

  test("keeps its metadata in a Valkey configured as JuiceFS's Redis best practices say", () => {
    const objects = install({ workspaceStorage: { valkey: { maxmemory: "256mb" } } });
    const conf = one<V1ConfigMap>(objects, "ConfigMap", "alasio-valkey").data?.["valkey.conf"]?.split("\n") ?? [];
    for (const line of ["appendonly yes", "appendfsync everysec", "aof-use-rdb-preamble yes", "save 3600 1 300 100 60 10000", "stop-writes-on-bgsave-error yes"]) {
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

  test("installs JuiceFS's CSI driver, its images pinned by digest, provisioning volumes itself", () => {
    const objects = install();
    assert.deepEqual(one<V1CSIDriver>(objects, "CSIDriver", "csi.juicefs.com").spec, { attachRequired: false, podInfoOnMount: true });
    const controller = one<V1StatefulSet>(objects, "StatefulSet", "juicefs-csi-controller");
    const node = one<V1DaemonSet>(objects, "DaemonSet", "juicefs-csi-node");
    assert.equal(controller.metadata?.namespace, "kube-system");
    assert.equal(controller.spec?.replicas, 1);
    assert.deepEqual(controller.spec?.template.spec?.containers.map(({ name }) => name), ["juicefs-plugin", "csi-resizer", "liveness-probe"]);
    assert.ok(container(controller).args?.includes("--provisioner=true"));
    // One controller, without the elections whose renewals, after a fixed 10 s, would end it at a slow API server.
    assert.deepEqual(controller.spec?.template.spec?.containers.filter(({ args }) => args?.includes("--leader-election")), []);
    const provisioner = one<V1ClusterRole>(objects, "ClusterRole", "juicefs-external-provisioner-role");
    assert.ok(!(provisioner.rules ?? []).some(({ resources }) => resources?.includes("leases")));
    assert.deepEqual(node.spec?.template.spec?.containers.map(({ name }) => name), ["juicefs-plugin", "node-driver-registrar", "liveness-probe"]);
    for (const image of images([controller, node])) assert.match(image, /:v[0-9.]+@sha256:[0-9a-f]{64}$/u, image);
    for (const plugin of [container(controller), node.spec?.template.spec?.containers[0]]) {
      assert.deepEqual(plugin?.env?.find(({ name }) => name === "JUICEFS_MOUNT_PRIORITY_NAME"), { name: "JUICEFS_MOUNT_PRIORITY_NAME", value: "alasio-juicefs-mount" });
      assert.deepEqual(plugin?.env?.find(({ name }) => name === "JUICEFS_MOUNT_PREEMPTION_POLICY"), { name: "JUICEFS_MOUNT_PREEMPTION_POLICY", value: "Never" });
      assert.equal(plugin?.env?.find(({ name }) => name === "FS_SHARE_MOUNT"), undefined);
    }
    const shared = one<V1DaemonSet>(install({ workspaceStorage: { csi: { shareMountPod: true } } }), "DaemonSet", "juicefs-csi-node");
    assert.deepEqual(shared.spec?.template.spec?.containers[0]?.env?.find(({ name }) => name === "FS_SHARE_MOUNT"), { name: "FS_SHARE_MOUNT", value: "true" });
  });

  test("runs mount pods of its class on a pinned client, its cache bounded, its metadata backed up, ready once the mount answers", () => {
    const objects = install({ workspaceStorage: { cacheSizeMiB: 2048, backupInterval: "30m" } });
    const config = JSON.parse(one<V1ConfigMap>(objects, "ConfigMap", "juicefs-csi-driver-config").data?.["config.yaml"] ?? "");
    assert.deepEqual(config.mountPodPatch, [{
      pvcSelector: { matchStorageClassName: "alasio-workspaces" },
      ceMountImage: "juicedata/mount:ce-v1.4.1@sha256:ab99388a397fe52575fdeb84a9e017c3c594a6b07d63966cd4805bbf6f172673",
      resources: { requests: { cpu: "20m", memory: "128Mi" }, limits: { memory: "1Gi" } },
      mountOptions: ["cache-size=2048", "free-space-ratio=0.2", "backup-meta=30m"],
      readinessProbe: { exec: { command: ["sh", "-c", 'test "$(stat --file-system --format=%T ${MOUNT_POINT})" = fuseblk'] }, initialDelaySeconds: 5, periodSeconds: 10, failureThreshold: 3 },
    }]);
  });

  test("runs mount pods under a PriorityClass that never preempts", () => {
    const priority = one<V1PriorityClass>(install(), "PriorityClass", "alasio-juicefs-mount");
    assert.equal(priority.preemptionPolicy, "Never");
    assert.equal(priority.globalDefault, false);
    assert.equal(priority.value, 1_000_000_000);
  });

  test("makes each volume of its class a directory of the file system, named after its claim, deleted with it and grown with it", () => {
    const storageClass = one<V1StorageClass>(install(), "StorageClass", "alasio-workspaces");
    assert.equal(storageClass.provisioner, "csi.juicefs.com");
    assert.equal(storageClass.reclaimPolicy, "Delete");
    assert.equal(storageClass.allowVolumeExpansion, true);
    assert.equal(storageClass.parameters?.["pathPattern"], "${.pvc.namespace}-${.pvc.name}");
    for (const use of ["provisioner", "node-publish", "controller-expand"]) {
      assert.equal(storageClass.parameters?.[`csi.storage.k8s.io/${use}-secret-name`], "alasio-workspaces-juicefs", use);
      assert.equal(storageClass.parameters?.[`csi.storage.k8s.io/${use}-secret-namespace`], "alasio", use);
    }
  });

  test("checks each volume's quota daily and repairs it, as a JuiceFS admin on the pinned client, kept as the driver is", () => {
    const quotaCheck = one<V1CronJob>(install(), "CronJob", "alasio-juicefs-quota-check");
    assert.equal(quotaCheck.metadata?.namespace, "alasio");
    assert.equal(quotaCheck.metadata?.labels?.["alasio.dev/volume-driver"], "csi.juicefs.com");
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
    // one's in the trash, or by inode once it has none), users' and groups' by id.
    const juicefs = [
      'echo "$@" >> "$ROOT/calls"',
      '[ "$2" = list ] || exit 0',
      "cat <<'TABLE'",
      "+-------------------------------------------------+---------+---------+------+-----------+-------+-------+",
      "|                     Path/ID                     |   Size  |   Used  | Use% |   Inodes  | IUsed | IUse% |",
      "+-------------------------------------------------+---------+---------+------+-----------+-------+-------+",
      "| /.trash/2026-10-05-03/1-77-alasio-sessions-data | 1.0 GiB | 8.0 KiB |   0% | unchanged |     2 |       |",
      "| /alasio-sessions-data-fs-abc123                 | 1.0 GiB | 1.6 MiB |   0% | unchanged |   314 |       |",
      "| /alasio-sessions-data-fs-def456                 | 2.0 GiB |     0 B |   0% | unchanged |     0 |       |",
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
        `quota check ${metaUrl} --path /alasio-sessions-data-fs-abc123 --repair`,
        `quota check ${metaUrl} --path /alasio-sessions-data-fs-def456 --repair`,
      ]);
    } finally {
      remove();
    }
  });

  test("collects the file system daily, as a JuiceFS admin on the pinned client, kept as the driver is", () => {
    const collection = one<V1CronJob>(install(), "CronJob", "alasio-juicefs-gc");
    assert.equal(collection.metadata?.namespace, "alasio");
    assert.equal(collection.metadata?.labels?.["alasio.dev/volume-driver"], "csi.juicefs.com");
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
      assert.deepEqual(readFileSync(join(root, "calls"), "utf8").trim().split("\n"), [`gc ${metaUrl} --delete`]);
    } finally {
      remove();
    }
  });

  test("clones a session's volume into a new one's directory once its quota is set, counts its usage anew after and gives it its source's owner, privileged on a mount of the whole file system, as a JuiceFS admin", () => {
    const { clone } = sessions(install());
    assert.ok(clone);
    assert.equal(clone.claimTemplate, "data");
    const { job } = clone;
    assert.deepEqual(job.metadata, {
      generateName: "alasio-workspace-clone-",
      namespace: "alasio",
      labels: { ...job.metadata?.labels, "app.kubernetes.io/component": "workspace-clone" },
    });
    assert.equal(job.spec?.backoffLimit, 0);
    const pod = job.spec?.template;
    assert.equal(pod?.metadata?.labels?.["alasio.dev/workload"], "juicefs-admin");
    assert.equal(pod?.spec?.restartPolicy, "Never");
    assert.equal(pod?.spec?.securityContext, undefined);
    const cloner = pod?.spec?.containers[0];
    assert.ok(cloner);
    assert.equal(cloner.image, "juicedata/mount:ce-v1.4.1@sha256:ab99388a397fe52575fdeb84a9e017c3c594a6b07d63966cd4805bbf6f172673");
    assert.deepEqual(cloner.securityContext, { privileged: true });
    assert.deepEqual(cloner.env, [{ name: "META_URL", valueFrom: { secretKeyRef: { name: "alasio-workspaces-juicefs", key: "metaurl" } } }]);
    // The destination's directory has no quota at first, then the driver's. The mount, run
    // in the background, takes a while to answer, as on the box; it is up once it has, and
    // its client runs until it is unmounted.
    const juicefs = [
      '[ "$1" = mount ] && /bin/sleep 0.3',
      'echo "$@" >> "$ROOT/calls"',
      '[ "$1" = mount ] && : > "$ROOT/mounted"',
      '[ "$1" = mount ] && until [ -e "$ROOT/unmounted" ]; do /bin/sleep 0.05; done',
      'if [ "$1 $2" = "quota get" ]; then [ -e "$ROOT/quota" ] && echo "| /alasio-sessions-data-fs-def456 | 1.0 GiB |"; : > "$ROOT/quota"; fi',
      "true",
    ].join("\n");
    const recorded = (name: string) => `echo "${name} $*" >> "$ROOT/calls"`;
    // /jfs a FUSE mount once the mount is up, and a source directory the pod's group's, as kubelet made it.
    const stat = 'case $1 in --file-system) if [ -e "$ROOT/mounted" ]; then echo fuseblk; else echo overlay; fi ;; --format=%a) echo 2775 ;; *) echo 1000:1000 ;; esac';
    const env = {
      META_URL: "redis://valkey/1",
      SOURCE_NAMESPACE: "alasio-sessions",
      SOURCE_CLAIM: "data-fs-abc123",
      DESTINATION_NAMESPACE: "alasio-sessions",
      DESTINATION_CLAIM: "data-fs-def456",
    };
    const commands = { juicefs, mkdir: recorded("mkdir"), stat: stat, sleep: recorded("sleep"), umount: `${recorded("umount")}; : > "$ROOT/unmounted"`, chown: recorded("chown"), chmod: recorded("chmod") };
    const { root, run, remove } = runScript(cloner, env, commands);
    try {
      run();
      const calls = readFileSync(join(root, "calls"), "utf8").trim().split("\n");
      // It waited for the mount, a second at a time, as long as the mount took.
      assert.ok(calls.includes("sleep 1"), calls.join("\n"));
      assert.deepEqual(calls.filter((call) => call !== "sleep 1"), [
        "quota get redis://valkey/1 --path /alasio-sessions-data-fs-def456",
        "sleep 2",
        "quota get redis://valkey/1 --path /alasio-sessions-data-fs-def456",
        "mkdir -p /jfs",
        "mount --no-bgjob --cache-size 0 redis://valkey/1 /jfs",
        "clone --preserve /jfs/alasio-sessions-data-fs-abc123/workspace /jfs/alasio-sessions-data-fs-def456/workspace",
        "clone --preserve /jfs/alasio-sessions-data-fs-abc123/home /jfs/alasio-sessions-data-fs-def456/home",
        "chown 1000:1000 /jfs/alasio-sessions-data-fs-def456",
        "chmod 2775 /jfs/alasio-sessions-data-fs-def456",
        "umount /jfs",
        "quota check redis://valkey/1 --path /alasio-sessions-data-fs-def456 --repair",
      ]);
    } finally {
      remove();
    }
  });

  test("forks sessions only where their volumes are its class's", () => {
    assert.equal(sessions(install({ sessions: { storage: { storageClassName: "alasio-workspaces" } } })).clone?.claimTemplate, "data");
    assert.equal(sessions(install({ sessions: { storage: { storageClassName: "fast" } } })).clone, undefined);
    assert.equal(sessions(install({ workspaceStorage: { enabled: false } })).clone, undefined);
  });

  test("admits JuiceFS's pods and its driver's Jobs' alone to Valkey, but for the collector reading its metrics, and to the object store's S3 port, with Valkey's, which formats the file system", () => {
    const objects = install();
    const policy = (name: string) =>
      one<KubernetesObject & { spec: { podSelector: unknown; ingress: Array<{ from: unknown[]; ports: unknown }> } }>(objects, "NetworkPolicy", name).spec;
    const driver = { matchLabels: { "kubernetes.io/metadata.name": "kube-system" } };
    const peers = [
      { namespaceSelector: driver, podSelector: { matchLabels: { "app.kubernetes.io/name": "juicefs-mount" } } },
      {
        namespaceSelector: driver,
        podSelector: { matchLabels: { app: "juicefs-csi-controller", "app.kubernetes.io/name": "juicefs-csi-driver", "app.kubernetes.io/instance": "juicefs-csi-driver" } },
      },
      { namespaceSelector: driver, podSelector: { matchLabels: { app: "juicefs-csi-node", "app.kubernetes.io/name": "juicefs-csi-driver", "app.kubernetes.io/instance": "juicefs-csi-driver" } } },
      { namespaceSelector: driver, podSelector: { matchExpressions: [{ key: "batch.kubernetes.io/job-name", operator: "Exists" }] } },
      { namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "alasio" } }, podSelector: { matchLabels: { "alasio.dev/workload": "juicefs-admin" } } },
    ];
    assert.deepEqual(policy("alasio-valkey").podSelector, { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "valkey" } });
    assert.deepEqual(policy("alasio-valkey").ingress, [
      { from: peers, ports: [{ protocol: "TCP", port: 6379 }] },
      {
        from: [{ podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "collector" } } }],
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

  test("marks everything the driver needs to delete its volumes' data, so it is kept while they remain", () => {
    const objects = install();
    const needed = objects.filter(
      ({ metadata }) => metadata?.labels?.["app.kubernetes.io/name"] === "juicefs-csi-driver" || metadata?.labels?.["app.kubernetes.io/component"] === "valkey",
    );
    // What confines and sizes them is not needed for that.
    const kept = needed.filter(({ kind }) => kind !== "NetworkPolicy" && kind !== "VerticalPodAutoscaler");
    assert.deepEqual(kept.map(({ kind, metadata }) => `${kind} ${metadata?.name}`), [
      "ConfigMap alasio-valkey",
      "Service alasio-valkey",
      "StatefulSet alasio-valkey",
      "CSIDriver csi.juicefs.com",
      "PriorityClass alasio-juicefs-mount",
      "ServiceAccount juicefs-csi-controller-sa",
      "ServiceAccount juicefs-csi-node-sa",
      "ClusterRole juicefs-external-provisioner-role",
      "ClusterRole juicefs-csi-external-node-service-role",
      "ClusterRoleBinding juicefs-csi-provisioner-binding",
      "ClusterRoleBinding juicefs-csi-node-service-binding",
      "ConfigMap juicefs-csi-driver-config",
      "StatefulSet juicefs-csi-controller",
      "DaemonSet juicefs-csi-node",
    ]);
    for (const object of needed) {
      assert.equal(object.metadata?.labels?.["alasio.dev/volume-driver"], kept.includes(object) ? "csi.juicefs.com" : undefined, `${object.kind} ${object.metadata?.name}`);
    }
    // The Secrets the setup makes: Valkey's password and the file system's credentials.
    assert.deepEqual(JSON.parse(setupVariable(objects, "WORKSPACES_SECRET_LABELS") ?? ""), { "alasio.dev/volume-driver": "csi.juicefs.com" });
  });

  test("keeps its data in the operator's object store, with its keys, when it is theirs", () => {
    const objects = install({ objectStore: { bundled: { enabled: false }, external: { endpoint: "https://s3.example.com/", existingSecret: "s3" } } });
    assert.equal(setupVariable(objects, "WORKSPACES_BUCKET_URL"), "https://s3.example.com/workspaces");
    assert.equal(setupVariable(objects, "S3_EXTERNAL"), "1");
    assert.deepEqual(all(objects, "NetworkPolicy").filter((policy) => policy.metadata?.name === "alasio-seaweedfs-workspaces"), []);
    assert.ok(one(objects, "NetworkPolicy", "alasio-valkey"));
  });

  test("leaves the driver to the cluster when told", () => {
    const objects = install({ workspaceStorage: { csi: { enabled: false } } });
    assert.deepEqual([...all(objects, "CSIDriver"), ...all(objects, "DaemonSet").filter(({ metadata }) => metadata?.name !== "alasio-node-agent")], []);
    assert.deepEqual(objects.filter((object) => /^(alasio-)?juicefs-(csi|mount)/u.test(object.metadata?.name ?? "")), []);
    assert.ok(one(objects, "StorageClass", "alasio-workspaces"));
    assert.ok(one(objects, "StatefulSet", "alasio-valkey"));
  });

  test("installs nothing of it when it is off", () => {
    const objects = install({ workspaceStorage: { enabled: false } });
    for (const kind of ["CSIDriver", "StorageClass"]) assert.deepEqual(all(objects, kind), [], kind);
    assert.deepEqual(all(objects, "DaemonSet").filter(({ metadata }) => metadata?.name !== "alasio-node-agent"), []);
    assert.deepEqual(all(objects, "PriorityClass").filter(({ metadata }) => metadata?.name === "alasio-juicefs-mount"), []);
    assert.deepEqual(objects.filter(({ metadata }) => /valkey|juicefs|workspaces/u.test(metadata?.name ?? "")), []);
    assert.equal(setupVariable(objects, "WORKSPACES_NAME"), undefined);
    const buckets = one<V1Deployment>(objects, "Deployment", "alasio-seaweedfs").spec?.template.spec?.containers.find(({ name }) => name === "buckets");
    assert.match(buckets?.command?.[2] ?? "", /^for bucket in neon lake backups snapshots; do$/mu);
  });
});

describe("node agents", () => {
  const agentOf = (objects: readonly KubernetesObject[]) => one<V1DaemonSet>(objects, "DaemonSet", "alasio-node-agent");

  test("run privileged in the node's PID namespace, on every node sessions run on, with the node's paths at the same paths", () => {
    const objects = install({ sessions: { nodeSelector: { "alasio.dev/sessions": "true" }, tolerations: [{ key: "sessions", operator: "Exists" }] } });
    const agent = agentOf(objects);
    assert.equal(agent.metadata?.namespace, "alasio");
    assert.deepEqual(agent.spec?.selector.matchLabels, { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "node-agent" });
    const pod = agent.spec?.template.spec;
    assert.equal(pod?.hostPID, true);
    assert.equal(pod?.automountServiceAccountToken, false);
    assert.deepEqual(pod?.nodeSelector, { "alasio.dev/sessions": "true" });
    assert.deepEqual(pod?.tolerations, [{ key: "sessions", operator: "Exists" }]);
    assert.deepEqual(pod?.volumes, [
      { name: "run", hostPath: { path: "/run", type: "Directory" } },
      { name: "gvisor", hostPath: { path: "/usr/local/lib/alasio/gvisor", type: "DirectoryOrCreate" } },
      { name: "containerd", hostPath: { path: "/var/lib/rancher/k3s/agent/etc/containerd", type: "DirectoryOrCreate" } },
      { name: "snapshots", hostPath: { path: "/var/lib/alasio/snapshots", type: "DirectoryOrCreate" } },
    ]);
    const [container] = pod?.containers ?? [];
    assert.equal(container?.image, one<V1Deployment>(objects, "Deployment", "alasio").spec?.template.spec?.containers[0]?.image);
    assert.deepEqual(container?.command, ["node", "/opt/alasio/src/node-agent/main.ts"]);
    assert.deepEqual(container?.securityContext, { privileged: true, runAsUser: 0 });
    assert.deepEqual(container?.volumeMounts, [
      { name: "run", mountPath: "/run", mountPropagation: "HostToContainer" },
      { name: "gvisor", mountPath: "/usr/local/lib/alasio/gvisor", readOnly: true },
      { name: "containerd", mountPath: "/var/lib/rancher/k3s/agent/etc/containerd", readOnly: true },
      { name: "snapshots", mountPath: "/var/lib/alasio/snapshots" },
    ]);
    assert.deepEqual(container?.ports, [{ name: "api", containerPort: 8484 }]);
    assert.deepEqual(container?.readinessProbe, { httpGet: { path: "/healthz", port: "api" }, periodSeconds: 10 });
    assert.deepEqual(container?.resources, { requests: { cpu: "20m", memory: "96Mi" }, limits: { memory: "512Mi" } });
  });

  test("take their bucket from its Secret whole, alasio's token from theirs, their node's name and their cache's budget in bytes", () => {
    const container = (file: Record<string, unknown> = {}) => agentOf(install(file)).spec?.template.spec?.containers[0];
    const env = (file: Record<string, unknown> = {}) => Object.fromEntries((container(file)?.env ?? []).map(({ name, value, valueFrom }) => [name, value ?? valueFrom]));
    assert.deepEqual(container()?.envFrom, [{ secretRef: { name: "alasio-snapshots" } }]);
    assert.deepEqual(env(), {
      NODE_NAME: { fieldRef: { fieldPath: "spec.nodeName" } },
      ALASIO_NODE_AGENT_TOKEN: { secretKeyRef: { name: "alasio-node-agent", key: "token" } },
      ALASIO_SNAPSHOT_CACHE_BYTES: "5368709120",
      OTEL_SERVICE_NAME: "alasio-node-agent",
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://alasio-collector.alasio.svc:4318",
      OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf",
    });
    assert.equal(env({ sessions: { snapshots: { nodeCache: "1.5G" } } })["ALASIO_SNAPSHOT_CACHE_BYTES"], "1500000000");
    assert.equal(env({ sessions: { snapshots: { nodeCache: "750Mi" } } })["ALASIO_SNAPSHOT_CACHE_BYTES"], "786432000");
    // Without a collector, it exports nothing.
    assert.deepEqual(Object.keys(env({ lake: { enabled: false } })).filter((name) => name.startsWith("OTEL_")), []);
  });

  test("run only where sessions run under gVisor, beside Neon, whose setup makes their Secrets", () => {
    const agents = (file: Record<string, unknown>) => all(install(file), "DaemonSet").filter(({ metadata }) => metadata?.name === "alasio-node-agent");
    assert.equal(agents({}).length, 1);
    assert.deepEqual(agents({ sessions: { runtimeClassName: "" } }), []);
    assert.deepEqual(agents({ sessions: { enabled: false }, workspaceStorage: { enabled: false } }), []);
    assert.deepEqual(agents({ neon: { enabled: false, external: { existingSecret: "db" } }, workspaceStorage: { enabled: false } }), []);
    assert.deepEqual(all(install({ sessions: { runtimeClassName: "" } }), "NetworkPolicy").filter(({ metadata }) => /node-agent|snapshots/u.test(metadata?.name ?? "")), []);
  });

  test("take alasio's calls alone, and reach the cluster's DNS, the object store's S3 port and the collector alone", () => {
    const objects = install();
    const policies = all<NetworkPolicyObject>(objects, "NetworkPolicy");
    const podOf = (component: string, namespace = "alasio") => ({
      namespace,
      namespaceLabels: { "kubernetes.io/metadata.name": namespace },
      labels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": component },
    });
    const agent = podOf("node-agent");
    assert.equal(admits(policies, podOf("alasio"), agent, 8484), true);
    assert.equal(admits(policies, podOf("collector"), agent, 8484), false);
    assert.equal(admits(policies, podOf("alasio", "alasio-branch-try"), agent, 8484), false);
    assert.equal(admits(policies, { ...podOf("session", "alasio-sessions"), labels: { "alasio.dev/workload": "session" } }, agent, 8484), false);
    assert.equal(admits(policies, podOf("alasio"), agent, 8485), false);
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
    assert.deepEqual(egress({}).egress, [
      dns,
      { to: [selected("seaweedfs")], ports: [{ protocol: "TCP", port: 8333 }] },
      { to: [selected("collector")], ports: [{ protocol: "TCP", port: 4318 }] },
    ]);
    // An operator's object store, on its endpoint's port at any address, and no collector to reach without one.
    const external = (endpoint: string) => ({ objectStore: { bundled: { enabled: false }, external: { endpoint, existingSecret: "s3" } }, lake: { enabled: false } });
    assert.deepEqual(egress(external("https://s3.example.com")).egress, [dns, { to: [{ ipBlock: { cidr: "0.0.0.0/0" } }], ports: [{ protocol: "TCP", port: 443 }] }]);
    assert.deepEqual(egress(external("http://minio.lan:9000")).egress, [dns, { to: [{ ipBlock: { cidr: "0.0.0.0/0" } }], ports: [{ protocol: "TCP", port: 9000 }] }]);
    assert.deepEqual(all(install(external("http://minio.lan:9000")), "NetworkPolicy").filter(({ metadata }) => metadata?.name === "alasio-seaweedfs-snapshots"), []);
  });

  test("are found by alasio, which watches pods in its own namespace where they run, and nowhere else", () => {
    const own = (file: Record<string, unknown>) =>
      all<V1Role>(install(file), "Role").find(({ metadata }) => metadata?.name === "alasio" && metadata.namespace === "alasio")?.rules ?? [];
    const pods = (...verbs: string[]) => ({ apiGroups: [""], resources: ["pods"], verbs });
    assert.deepEqual(own({}), [pods("list", "watch", "patch"), { apiGroups: ["batch"], resources: ["jobs"], verbs: ["create", "get", "delete"] }]);
    assert.deepEqual(own({ workspaceStorage: { enabled: false } }), [pods("list", "watch", "patch")]);
    assert.deepEqual(own({ workspaceStorage: { enabled: false }, sessions: { runtimeClassName: "" } }), [pods("list", "patch")]);
    const binding = one<V1RoleBinding>(install({ workspaceStorage: { enabled: false } }).filter(({ metadata }) => metadata?.namespace === "alasio"), "RoleBinding", "alasio");
    assert.deepEqual(binding.subjects, [{ kind: "ServiceAccount", name: "alasio", namespace: "alasio" }]);
    // A branch environment's alasio calls none.
    const branchRules = all<V1Role>(branchObjects(configOf(), "try"), "Role").flatMap(({ rules }) => rules ?? []);
    assert.ok(!branchRules.some(({ resources, verbs }) => resources?.includes("pods") && verbs.includes("watch")));
  });
});

describe("security", () => {
  test("enforces Pod Security restricted in the sessions namespace", () => {
    const namespaces = all<V1Namespace>(install(), "Namespace");
    assert.equal(namespaces.length, 1);
    assert.equal(namespaces[0]?.metadata?.labels?.["pod-security.kubernetes.io/enforce"], "restricted");
  });

  test("makes the privileged host namespace only with the host profile", () => {
    const namespaces = all<V1Namespace>(install({ host: { enabled: true } }), "Namespace");
    assert.equal(namespaces.length, 2);
    assert.equal(namespaces[1]?.metadata?.labels?.["pod-security.kubernetes.io/enforce"], "privileged");
  });

  test("denies everything in the sessions namespace but bayma from alasio and telemetry to it", () => {
    const objects = install();
    const policy = (name: string) => one<KubernetesObject & { spec: Record<string, unknown> }>(objects, "NetworkPolicy", name).spec;
    assert.deepEqual(policy("default-deny")["policyTypes"], ["Ingress", "Egress"]);
    assert.deepEqual(policy("default-deny")["podSelector"], {});
    assert.deepEqual((policy("bayma-from-alasio")["ingress"] as Array<{ ports: unknown }>)[0]?.ports, [{ protocol: "TCP", port: 7290 }]);
    assert.deepEqual((policy("telemetry-to-alasio")["egress"] as Array<{ ports: unknown }>)[0]?.ports, [{ protocol: "TCP", port: 4318 }]);
  });

  test("keeps a session with internet off private, link-local and metadata addresses", () => {
    const policy = one<KubernetesObject & { spec: { podSelector: { matchLabels: Record<string, string> }; egress: Array<{ to: Array<{ ipBlock: { except: string[] } }> }> } }>(
      install(),
      "NetworkPolicy",
      "full-internet",
    );
    assert.equal(policy.spec.podSelector.matchLabels["alasio.dev/net-mode"], "full");
    const except = policy.spec.egress[0]?.to[0]?.ipBlock.except;
    for (const range of ["10.0.0.0/8", "169.254.0.0/16", "100.64.0.0/10"]) assert.ok(except?.includes(range), range);
  });

  test("renders no policies when they are turned off", () => {
    assert.deepEqual(all(install({ networkPolicies: { enabled: false } }), "NetworkPolicy"), []);
  });

  test("lets alasio manage only Sandboxes, their token Secrets, exec and their pods' busy labels in its namespaces, and its own pod's in its own", () => {
    const roles = all<V1Role>(install({ workspaceStorage: { enabled: false }, sessions: { runtimeClassName: "" } }), "Role").filter((role) => role.metadata?.name === "alasio");
    assert.deepEqual(Object.fromEntries(roles.map(({ metadata, rules }) => [metadata?.namespace, rules])), {
      "alasio-sessions": [
        { apiGroups: ["agents.x-k8s.io"], resources: ["sandboxes"], verbs: ["get", "list", "watch", "create", "patch", "delete"] },
        { apiGroups: [""], resources: ["secrets"], verbs: ["get", "create"] },
        { apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "patch"] },
        { apiGroups: [""], resources: ["pods/exec"], verbs: ["create", "get"] },
      ],
      alasio: [{ apiGroups: [""], resources: ["pods"], verbs: ["list", "patch"] }],
    });
    // Where node agents run, alasio finds each by its pod on the node.
    const withAgents = all<V1Role>(install({ workspaceStorage: { enabled: false } }), "Role").find(({ metadata }) => metadata?.name === "alasio" && metadata.namespace === "alasio");
    assert.deepEqual(withAgents?.rules, [{ apiGroups: [""], resources: ["pods"], verbs: ["list", "watch", "patch"] }]);
  });

  test("lets alasio read sessions' claims and run the Jobs that clone them in its own namespace, only where it forks sessions", () => {
    const rolesOf = (objects: readonly KubernetesObject[]) =>
      Object.fromEntries(all<V1Role>(objects, "Role").filter((role) => role.metadata?.name === "alasio").map(({ metadata, rules }) => [metadata?.namespace, rules]));
    const objects = install({ host: { enabled: true }, sessions: { runtimeClassName: "" } });
    const roles = rolesOf(objects);
    assert.deepEqual(roles["alasio"]?.at(-1), { apiGroups: ["batch"], resources: ["jobs"], verbs: ["create", "get", "delete"] });
    assert.deepEqual(roles["alasio-sessions"]?.at(-1), { apiGroups: [""], resources: ["persistentvolumeclaims"], verbs: ["get"] });
    assert.ok(!JSON.stringify(roles["alasio-host"]).includes("persistentvolumeclaims"));
    const binding = one<V1RoleBinding>(objects.filter(({ metadata }) => metadata?.namespace === "alasio"), "RoleBinding", "alasio");
    assert.deepEqual(binding.subjects, [{ kind: "ServiceAccount", name: "alasio", namespace: "alasio" }]);
    assert.deepEqual(binding.roleRef, { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: "alasio" });
    const unforked = rolesOf(install({ sessions: { storage: { storageClassName: "fast" } } }));
    assert.ok(!JSON.stringify(unforked["alasio"]).includes("jobs"));
    assert.ok(!JSON.stringify(unforked["alasio-sessions"]).includes("persistentvolumeclaims"));
  });

  test("lets alasio list and delete pods in JuiceFS's driver's namespace, its mount pods, only where it installs the driver, and tells it where", () => {
    const mountPodRoles = (objects: readonly KubernetesObject[]) => all<V1Role>(objects, "Role").filter((role) => role.metadata?.name === "alasio" && role.metadata.namespace === "kube-system");
    const sessions = (objects: readonly KubernetesObject[]): SessionsProfile => JSON.parse(templates(objects)).sessions;
    const objects = install();
    assert.deepEqual(mountPodRoles(objects).map(({ rules }) => rules), [[{ apiGroups: [""], resources: ["pods"], verbs: ["list", "delete"] }]]);
    const binding = one<V1RoleBinding>(objects.filter(({ metadata }) => metadata?.namespace === "kube-system"), "RoleBinding", "alasio");
    assert.deepEqual(binding.subjects, [{ kind: "ServiceAccount", name: "alasio", namespace: "alasio" }]);
    assert.deepEqual(binding.roleRef, { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: "alasio" });
    assert.equal(sessions(objects).mountPodNamespace, "kube-system");
    for (const off of [{ enabled: false }, { csi: { enabled: false } }]) {
      const without = install({ workspaceStorage: off });
      assert.deepEqual(mountPodRoles(without), []);
      assert.equal(sessions(without).mountPodNamespace, undefined);
    }
  });

  test("lets the host profile's agents restart alasio's own Deployment and nothing else", () => {
    const role = one<V1Role>(install({ host: { enabled: true } }), "Role", "alasio-restart");
    assert.deepEqual(role.rules?.[0], { apiGroups: ["apps"], resources: ["deployments"], resourceNames: ["alasio"], verbs: ["get", "patch"] });
  });
});

describe("branch environments", () => {
  const branch = branchObjects(configOf(), "try");
  const variable = (workload: V1Deployment, name: string) => container(workload).env?.find((env) => env.name === name)?.value;
  /** An object of the branch's, of `kind` and `name`, in `namespace`. */
  const of = <T extends KubernetesObject>(kind: string, name: string, namespace = "alasio-branch-try") =>
    one<T>(branch.filter(({ metadata }) => metadata?.namespace === namespace || (kind === "Namespace" && metadata?.name === name)), kind, name);

  test("are an alasio, a compute and a lake in a namespace of the branch's own, its sessions in another, everything labelled with the branch", () => {
    assert.deepEqual(all<V1Namespace>(branch, "Namespace").map(({ metadata }) => [metadata?.name, metadata?.labels?.["alasio.dev/branch"]]), [
      ["alasio-branch-try", "try"],
      ["alasio-branch-try-sessions", "try"],
    ]);
    assert.equal(of<V1Namespace>("Namespace", "alasio-branch-try-sessions").metadata?.labels?.["pod-security.kubernetes.io/enforce"], "restricted");
    assert.deepEqual(branch.filter(({ metadata }) => metadata?.labels?.["alasio.dev/branch"] !== "try"), []);
    // But its Sandboxes' admission policy, which is the cluster's.
    assert.deepEqual(branch.filter(({ kind, metadata }) => kind !== "Namespace" && !metadata?.namespace?.startsWith("alasio-branch-try")).map(({ kind, metadata }) => [kind, metadata?.name]), [
      ["ValidatingAdmissionPolicy", "alasio-branch-try-sandboxes"],
      ["ValidatingAdmissionPolicyBinding", "alasio-branch-try-sandboxes"],
    ]);
    assert.deepEqual(all<V1Deployment>(branch, "Deployment").map(({ metadata }) => metadata?.name).sort(), ["alasio", "alasio-lake", "alasio-neon-compute"]);
    // Named as main's are, as each namespace is the branch's own.
    assert.deepEqual(all<V1Service>(branch, "Service").map(({ metadata }) => metadata?.name).sort(), ["alasio-lake", "alasio-neon-compute", "alasio-telemetry"]);
  });

  test("run an alasio that knows it is the branch, asks main to fork what it inherited with its token, and sends its telemetry tagged with the branch", () => {
    const alasio = of<V1Deployment>("Deployment", "alasio");
    assert.equal(variable(alasio, "ALASIO_BRANCH"), "try");
    assert.equal(variable(alasio, "ALASIO_PARENT_FORKS_URL"), "http://alasio-branches.alasio.svc:4319");
    assert.equal(variable(alasio, "ALASIO_BRANCH_FORK_TOKEN_FILE"), "/run/alasio/branch/token");
    assert.deepEqual(alasio.spec?.template.spec?.volumes?.find(({ name }) => name === "branch"), { name: "branch", secret: { secretName: "alasio-branch-fork", items: [{ key: "token", path: "token" }] } });
    assert.equal(variable(alasio, "ALASIO_TELEMETRY_RECEIVER_SERVICE"), "alasio-telemetry.alasio-branch-try.svc");
    assert.equal(variable(alasio, "ALASIO_BRANCH_FORK_KEY_FILE"), undefined);
    // It makes no role for what reads main's Neon beside it, which a branch has none of.
    assert.equal(variable(alasio, "ALASIO_LAKE_READER_VERIFIER_FILE"), undefined);
    assert.equal(variable(alasio, "ALASIO_GRAFANA_VERIFIER_FILE"), undefined);
    assert.deepEqual(alasio.spec?.template.spec?.volumes?.find(({ name }) => name === "database")?.secret?.items?.map(({ key }) => key), ["url", "lake-verifier"]);
    for (const workload of all<V1Deployment>(branch, "Deployment")) {
      assert.equal(variable(workload, "OTEL_RESOURCE_ATTRIBUTES"), "alasio.branch=try", workload.metadata?.name);
    }
    const tagged = branchObjects(configOf({ telemetry: { resourceAttributes: "deployment.environment.name=prod" } }), "try");
    assert.equal(variable(one<V1Deployment>(tagged, "Deployment", "alasio"), "OTEL_RESOURCE_ATTRIBUTES"), "deployment.environment.name=prod,alasio.branch=try");
  });

  test("make sessions in the branch's namespace alone, which it neither clones nor reaches JuiceFS's mount pods for", () => {
    const templates = JSON.parse(of<V1ConfigMap>("ConfigMap", "alasio-sandbox-templates").data?.["templates.json"] ?? "");
    assert.equal(templates.sessions.namespace, "alasio-branch-try-sessions");
    assert.equal(templates.sessions.clone, undefined);
    assert.equal(templates.sessions.mountPodNamespace, undefined);
    assert.equal(templates.host, undefined);
    const roles = all<V1Role>(branch, "Role");
    assert.deepEqual(roles.map(({ metadata }) => [metadata?.namespace, metadata?.name]), [
      ["alasio-branch-try-sessions", "alasio"],
      // Its own pod, which it labels busy as main's does.
      ["alasio-branch-try", "alasio"],
      // main's neon-control, which wakes its compute and puts it to sleep.
      ["alasio-branch-try", "alasio-neon-control"],
      ["alasio-branch-try-sessions", "alasio-parent"],
    ]);
    assert.deepEqual(of<V1Role>("Role", "alasio", "alasio-branch-try").rules, [{ apiGroups: [""], resources: ["pods"], verbs: ["list", "patch"] }]);
    const binding = of<V1RoleBinding>("RoleBinding", "alasio", "alasio-branch-try-sessions");
    assert.deepEqual(binding.subjects, [{ kind: "ServiceAccount", name: "alasio", namespace: "alasio-branch-try" }]);
    // main's alasio, which forks the sessions the branch inherited into it.
    assert.deepEqual(of<V1Role>("Role", "alasio-parent", "alasio-branch-try-sessions").rules, [
      { apiGroups: ["agents.x-k8s.io"], resources: ["sandboxes"], verbs: ["create", "delete"] },
      { apiGroups: [""], resources: ["persistentvolumeclaims"], verbs: ["get"] },
    ]);
    assert.deepEqual(of<V1RoleBinding>("RoleBinding", "alasio-parent", "alasio-branch-try-sessions").subjects, [{ kind: "ServiceAccount", name: "alasio", namespace: "alasio" }]);
  });

  test("run a compute on the branch's timeline, from main's neon-control, which alone scales it, and a lake that is never maintained", () => {
    const compute = of<V1Deployment>("Deployment", "alasio-neon-compute");
    const args = container(compute).args ?? [];
    assert.equal(args[args.indexOf("--compute-id") + 1], "branch-try");
    assert.equal(args[args.indexOf("--control-plane-uri") + 1], "http://alasio-neon-control.alasio.svc:8080");
    const lake = of<V1Deployment>("Deployment", "alasio-lake");
    assert.equal(variable(lake, "LAKE_MAINTENANCE_HOURS"), "0");
    assert.equal(variable(lake, "LAKE_BRANCHES_URL"), undefined);
    // Nor has it a query endpoint, as it has no Grafana.
    assert.deepEqual(lake.spec?.template.spec?.containers.map(({ name }) => name), ["lake"]);
    assert.deepEqual(of<V1Service>("Service", "alasio-lake").spec?.ports?.map(({ name }) => name), ["metrics", "otlp-http"]);
    assert.deepEqual(branch.filter(({ metadata }) => metadata?.name?.includes("grafana")), []);
    // Its compute, through main's Neon proxy.
    assert.deepEqual([variable(lake, "LAKE_DATABASE_HOST"), variable(lake, "LAKE_DATABASE_PORT"), variable(lake, "LAKE_DATABASE_ENDPOINT")], ["alasio-neon-proxy.alasio.svc", "5432", "branch-try"]);
    assert.equal(compute.spec?.replicas, undefined);
    assert.deepEqual(of<V1Role>("Role", "alasio-neon-control").rules, [
      { apiGroups: ["apps"], resources: ["deployments"], resourceNames: ["alasio-neon-compute"], verbs: ["get"] },
      { apiGroups: ["apps"], resources: ["deployments/scale"], resourceNames: ["alasio-neon-compute"], verbs: ["patch"] },
      { apiGroups: [""], resources: ["pods"], verbs: ["list"] },
    ]);
    assert.deepEqual(of<V1RoleBinding>("RoleBinding", "alasio-neon-control").subjects, [{ kind: "ServiceAccount", name: "alasio-neon-control", namespace: "alasio" }]);
    assert.equal(variable(lake, "LAKE_S3_ENDPOINT"), "http://alasio-seaweedfs.alasio.svc:8333");
    const main = install();
    assert.equal(container(one<V1Deployment>(main, "Deployment", "alasio-neon-compute")).args?.at(-3), "alasio");
    assert.equal(variable(one<V1Deployment>(main, "Deployment", "alasio-neon-control"), "NEON_PAGESERVER_HOST"), "alasio-neon-pageserver.alasio.svc");
  });

  test("confine the branch's sessions to its alasio, and have a policy of its compute's", () => {
    const policy = (name: string, namespace: string) => of<KubernetesObject & { spec: Record<string, unknown> }>("NetworkPolicy", name, namespace).spec;
    const alasio = { namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "alasio-branch-try" } }, podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "alasio" } } };
    assert.deepEqual(policy("bayma-from-alasio", "alasio-branch-try-sessions")["ingress"], [{ from: [alasio], ports: [{ protocol: "TCP", port: 7290 }] }]);
    assert.deepEqual(policy("telemetry-to-alasio", "alasio-branch-try-sessions")["egress"], [{ to: [alasio], ports: [{ protocol: "TCP", port: 4318 }] }]);
    assert.deepEqual(policy("alasio-alasio", "alasio-branch-try")["ingress"], [{
      from: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "alasio-branch-try-sessions" } }, podSelector: { matchLabels: { "alasio.dev/workload": "session" } } }],
      ports: [{ protocol: "TCP", port: 4318 }],
    }]);
    assert.ok(policy("alasio-neon-compute", "alasio-branch-try"));
  });

  test("reach none of main's compute, alasio (but its fork of their sessions, from their alasio) or lake, as the policies of both admit them", () => {
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
      lake: podOf(branch, "alasio-lake", "alasio-branch-try"),
      session: { namespace: "alasio-branch-try-sessions", namespaceLabels: { ...namespaces.get("alasio-branch-try-sessions"), "kubernetes.io/metadata.name": "alasio-branch-try-sessions" }, labels: { "alasio.dev/workload": "session" } },
    };
    const targets: [string, Pod, number][] = [
      ["main's compute", podOf(main, "alasio-neon-compute", "alasio"), 55433],
      ...[4318, 4319].map((port): [string, Pod, number] => [`main's alasio on ${port}`, podOf(main, "alasio", "alasio"), port]),
      ...[4318, 8090, 9464].map((port): [string, Pod, number] => [`main's lake on ${port}`, podOf(main, "alasio-lake", "alasio"), port]),
    ];
    const reached = Object.entries(branchPods).flatMap(([from, pod]) =>
      targets.filter(([, target, port]) => admits(policies, pod, target, port)).map(([to]) => `${from} → ${to}`)
    );
    assert.deepEqual(reached, ["alasio → main's alasio on 4319"]);
  });

  test("reach every compute through main's Neon proxy alone, which with main's neon-control alone reaches one, and main's collector main's metrics alone", () => {
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
      ["main's lake", podOf(main, "alasio-lake", "alasio")],
      ["main's Grafana", podOf(main, "alasio-grafana", "alasio")],
      ["main's backup", podIn("alasio", one<V1CronJob>(main, "CronJob", "alasio-neon-backup").spec?.jobTemplate.spec?.template.metadata?.labels)],
      ["the branch's alasio", podOf(branch, "alasio", "alasio-branch-try")],
      ["the branch's lake", podOf(branch, "alasio-lake", "alasio-branch-try")],
    ];
    const proxy = podOf(main, "alasio-neon-proxy", "alasio");
    const control = podOf(main, "alasio-neon-control", "alasio");
    const computes: [string, Pod][] = [["main's compute", podOf(main, "alasio-neon-compute", "alasio")], ["the branch's compute", podOf(branch, "alasio-neon-compute", "alasio-branch-try")]];
    for (const [client, pod] of clients) {
      assert.ok(admits(policies, pod, proxy, 5432), `${client} → the proxy`);
      for (const [name, compute] of computes) assert.ok(!admits(policies, pod, compute, 55433), `${client} → ${name}`);
    }
    for (const [name, compute] of computes) {
      assert.ok(admits(policies, proxy, compute, 55433), `the proxy → ${name}`);
      assert.ok(admits(policies, control, compute, 55433), `neon-control → ${name}`);
    }
    const collector = podOf(main, "alasio-collector", "alasio");
    assert.deepEqual(computes.filter(([, compute]) => admits(policies, collector, compute, 3080)).map(([name]) => name), ["main's compute"]);
    // Sessions reach no database at all.
    assert.ok(!admits(policies, podIn("alasio-branch-try-sessions", { "alasio.dev/workload": "session" }), proxy, 5432));
  });

  test("are admitted by main to what they share of its storage alone, and to its fork of their sessions, which only main forks", () => {
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
    assert.deepEqual((policy("alasio-alasio")["ingress"] as unknown[]).at(-1), {
      from: [{ namespaceSelector: branches, podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "alasio" } } }],
      ports: [{ protocol: "TCP", port: 4319 }],
    });
    const alasio = one<V1Deployment>(main, "Deployment", "alasio");
    assert.equal(container(alasio).env?.find(({ name }) => name === "ALASIO_BRANCH_FORK_KEY_FILE")?.value, "/run/alasio/branch/fork-key");
    assert.deepEqual(alasio.spec?.template.spec?.volumes?.find(({ name }) => name === "branch"), {
      name: "branch",
      secret: { secretName: "alasio-branches", items: [{ key: "fork-key", path: "fork-key" }], optional: true },
    });
    assert.deepEqual(one<V1Service>(main, "Service", "alasio-branches").spec?.ports, [{ name: "branch-forks", port: 4319, targetPort: "branch-forks" }]);
    // Where main forks no session, it serves no fork.
    const unforked = install({ sessions: { storage: { storageClassName: "fast" } } });
    assert.equal(container(one<V1Deployment>(unforked, "Deployment", "alasio")).env?.find(({ name }) => name === "ALASIO_BRANCH_FORK_KEY_FILE"), undefined);
    assert.deepEqual(all(unforked, "Service").filter(({ metadata }) => metadata?.name === "alasio-branches"), []);
  });
});

/** Every pod spec of `objects`, by what makes it: its workload's, Job's or CronJob's name, or the templates' Sandboxes'. */
function podSpecs(objects: readonly KubernetesObject[]): [string, V1PodSpec][] {
  const workloads = [...all<V1Deployment>(objects, "Deployment"), ...all<V1StatefulSet>(objects, "StatefulSet"), ...all<V1DaemonSet>(objects, "DaemonSet"), ...all<V1Job>(objects, "Job")];
  const parsed = JSON.parse(templates(objects)) as { sessions?: SessionsProfile & { clone?: { job: V1Job } }; host?: { podTemplate: { spec: V1PodSpec } } };
  return [
    ...workloads.map(({ metadata, spec }): [string, V1PodSpec] => [metadata?.name ?? "", spec?.template.spec ?? { containers: [] }]),
    ...all<V1CronJob>(objects, "CronJob").map(({ metadata, spec }): [string, V1PodSpec] => [metadata?.name ?? "", spec?.jobTemplate.spec?.template.spec ?? { containers: [] }]),
    ...(parsed.sessions ? [["session", parsed.sessions.podTemplate.spec as V1PodSpec] as [string, V1PodSpec]] : []),
    ...(parsed.sessions?.clone ? [["clone", parsed.sessions.clone.job.spec?.template.spec ?? { containers: [] }] as [string, V1PodSpec]] : []),
    ...(parsed.host ? [["folder-bayma", parsed.host.podTemplate.spec] as [string, V1PodSpec]] : []),
  ];
}

describe("scaling", () => {
  test("runs every pod under a PriorityClass: the floor's and what runs turns given up last, of one value, so neither takes the other's place, and the floor its claims hold to a node above both; the headroom's first and preempting nothing", () => {
    const objects = install({ host: { enabled: true } });
    const classes = new Map(all<V1PriorityClass>(objects, "PriorityClass").map((each) => [each.metadata?.name ?? "", each]));
    const value = (name: string) => classes.get(name)?.value ?? Number.NaN;
    assert.equal(value("alasio-floor"), value("alasio-turn"));
    assert.ok(value("alasio-bound") > value("alasio-floor") && value("alasio-juicefs-mount") > value("alasio-bound"));
    assert.ok(value("alasio-turn") > value("alasio-component") && value("alasio-component") > value("alasio-headroom"));
    assert.ok(value("alasio-juicefs-mount") > value("alasio-floor"));
    assert.deepEqual([...classes.values()].filter(({ preemptionPolicy }) => preemptionPolicy === "Never").map(({ metadata }) => metadata?.name).sort(), ["alasio-headroom", "alasio-juicefs-mount"]);
    const tiers = new Map(podSpecs(objects).map(([name, spec]) => [name, spec.priorityClassName]));
    for (const [name, priorityClassName] of [...tiers, ...podSpecs(branchObjects(configOf(), "try")).map(([name, spec]) => [name, spec.priorityClassName])]) {
      assert.ok(priorityClassName && (classes.has(priorityClassName) || priorityClassName.startsWith("system-")), name);
    }
    assert.deepEqual(["alasio-seaweedfs", "alasio-grafana", "alasio-collector", "alasio-lake"].map((name) => tiers.get(name)), Array(4).fill("alasio-floor"));
    // Above the floor, exactly its StatefulSets whose claims hold them to a node; a branch has none.
    const held = (of: readonly KubernetesObject[]) =>
      all<V1StatefulSet>(of, "StatefulSet").filter(({ spec }) => (spec?.volumeClaimTemplates?.length ?? 0) > 0).map(({ metadata }) => metadata?.name).sort();
    assert.deepEqual(held(objects), [
      "alasio-neon-controller-db",
      "alasio-neon-pageserver",
      "alasio-neon-safekeeper",
      "alasio-seaweedfs-filer-store",
      "alasio-seaweedfs-master",
      "alasio-seaweedfs-volume",
      "alasio-valkey",
    ]);
    assert.deepEqual([...tiers].filter(([, priorityClassName]) => priorityClassName === "alasio-bound").map(([name]) => name).sort(), held(objects));
    assert.deepEqual(held(branchObjects(configOf(), "try")), []);
    // Main's compute is alasio's database, which every turn needs.
    assert.equal(tiers.get("alasio-neon-compute"), "alasio-floor");
    assert.deepEqual(["alasio-neon-setup", "alasio-neon-backup", "clone"].map((name) => tiers.get(name)), Array(3).fill("alasio-component"));
    assert.deepEqual(["alasio", "session", "folder-bayma"].map((name) => tiers.get(name)), Array(3).fill("alasio-turn"));
    // A branch's alasio runs turns; its lake serves no alert, and its compute sleeps.
    const branch = new Map(podSpecs(branchObjects(configOf(), "try")).map(([name, spec]) => [name, spec.priorityClassName]));
    assert.deepEqual(["alasio", "alasio-lake", "alasio-neon-compute"].map((name) => branch.get(name)), ["alasio-turn", "alasio-component", "alasio-component"]);
  });

  test("lets alasio's pods take others' places only on a cluster it makes, unless told, and keeps no headroom where they may not", () => {
    const policies = (objects: readonly KubernetesObject[]) =>
      all<V1PriorityClass>(objects, "PriorityClass").filter(({ metadata }) => metadata?.name !== "alasio-juicefs-mount").map(({ metadata, preemptionPolicy }) => [metadata?.name, preemptionPolicy]);
    assert.deepEqual(policies(install()), [
      ["alasio-bound", "PreemptLowerPriority"],
      ["alasio-floor", "PreemptLowerPriority"],
      ["alasio-turn", "PreemptLowerPriority"],
      ["alasio-component", "PreemptLowerPriority"],
      ["alasio-headroom", "Never"],
    ]);
    assert.deepEqual(new Set(policies(install({ autoscaling: { preemption: false } })).map(([, policy]) => policy)), new Set(["Never"]));
    const ofTarget = (shared: boolean, given: Record<string, unknown> = {}) => {
      const config = installConfigOf({ autoscaling: given }, { claude: false, shared });
      return Result.isSuccess(config) ? config.success.autoscaling.preemption : config.failure;
    };
    assert.deepEqual([ofTarget(false), ofTarget(true), ofTarget(true, { preemption: true })], [true, false, true]);
    const refused = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } }, autoscaling: { preemption: false, headroom: { scaling: { min: 1, max: 1 } } } });
    assert.ok(Result.isFailure(refused) && refused.failure.message === "autoscaling.headroom.scaling must be 0 when autoscaling.preemption is false, as no pod takes a placeholder's place then");
  });

  test("sizes every workload by a VerticalPodAutoscaler beside it: in place what keeps no state and runs no turn, the rest recommended alone, from a small floor to its memory limit", () => {
    const objects = [...install({ host: { enabled: true } }), ...branchObjects(configOf(), "try")];
    const workloads = [...all<V1Deployment>(objects, "Deployment"), ...all<V1StatefulSet>(objects, "StatefulSet"), ...all<V1DaemonSet>(objects, "DaemonSet")];
    const sized = all<VerticalPodAutoscaler>(objects, "VerticalPodAutoscaler");
    const key = ({ metadata }: KubernetesObject) => `${metadata?.namespace}/${metadata?.name}`;
    assert.deepEqual(sized.map(key).sort(), workloads.map(key).sort());
    const modes = new Map(sized.map((vpa) => [key(vpa), vpa.spec.updatePolicy.updateMode]));
    const recommended = [
      "alasio",
      "alasio-neon-compute",
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
      "juicefs-csi-controller",
      "juicefs-csi-node",
    ];
    for (const [name, mode] of modes) assert.equal(mode, recommended.includes(name.split("/")[1] ?? "") ? "Off" : "InPlace", name);
    // A branch's alasio runs turns as main's does.
    assert.equal(modes.get("alasio-branch-try/alasio"), "Off");
    for (const vpa of sized) {
      const workload = workloads.find((each) => key(each) === key(vpa));
      assert.deepEqual(vpa.spec.targetRef, { apiVersion: workload?.apiVersion, kind: workload?.kind, name: workload?.metadata?.name });
      assert.equal(vpa.spec.updatePolicy.minReplicas, vpa.spec.updatePolicy.updateMode === "Off" ? undefined : 1);
      assert.deepEqual(vpa.spec.resourcePolicy.containerPolicies, (workload?.spec?.template.spec?.containers ?? []).map(({ name, resources }) => ({
        containerName: name,
        minAllowed: { cpu: "5m", memory: "16Mi" },
        ...(resources?.limits?.["memory"] ? { maxAllowed: { memory: resources.limits["memory"] } } : {}),
        controlledValues: "RequestsOnly",
      })));
    }
  });

  test("lets no busy pod of alasio's be evicted, in its namespace and each of its workspaces', and tells alasio its pod's name to label", () => {
    const objects = [...install({ host: { enabled: true } }), ...branchObjects(configOf(), "try")];
    const budgets = all<KubernetesObject & { spec: object }>(objects, "PodDisruptionBudget").filter(({ metadata }) => metadata?.name === "alasio-busy");
    assert.deepEqual(budgets.map(({ metadata }) => metadata?.namespace).sort(), ["alasio", "alasio-branch-try", "alasio-branch-try-sessions", "alasio-host", "alasio-sessions"]);
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
    // The cluster's, made with main's alone; a branch makes its Sandboxes' alone.
    assert.deepEqual(all(branchObjects(configOf(), "try"), "ValidatingAdmissionPolicy").map(({ metadata }) => metadata?.name), ["alasio-branch-try-sandboxes"]);
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
    // It tracks what has a VerticalPodAutoscaler alone, recommends as little as a small component uses, and resizes a workload of one replica.
    assert.deepEqual(args("alasio-vpa-recommender"), ["--memory-saver", "--pod-recommendation-min-cpu-millicores=10", "--pod-recommendation-min-memory-mb=16"]);
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
  type Policy = KubernetesObject & { spec: { matchConstraints: { resourceRules: object[] }; matchConditions: { expression: string }[]; validations: { expression: string }[] } };
  /** The keys the pin of `field` in `expression` allows, of its last pin where `last`. */
  const keysOf = (expression: string, field: string, last = false) => {
    const pin = `${field}.all(key, key in [`;
    const at = last ? expression.lastIndexOf(pin) : expression.indexOf(pin);
    return at < 0 ? null : JSON.parse(expression.slice(at + pin.length - 1, expression.indexOf("]", at + pin.length) + 1));
  };
  const binding = (objects: readonly KubernetesObject[], name: string) => one<KubernetesObject & { spec: object }>(objects, "ValidatingAdmissionPolicyBinding", name).spec;

  test("lets alasio run no Job but its clone's, as rendered but for the claims it is told, each a name, where it forks sessions", () => {
    const objects = install();
    const clone = one<Policy>(objects, "ValidatingAdmissionPolicy", "alasio-clone-job");
    assert.deepEqual(clone.spec.matchConstraints, { resourceRules: [{ apiGroups: ["batch"], resources: ["jobs"], operations: ["CREATE", "UPDATE"], apiVersions: ["*"] }] });
    assert.deepEqual(clone.spec.matchConditions.map(({ expression }) => expression), ['request.userInfo.username == "system:serviceaccount:alasio:alasio"']);
    const job = clone.spec.validations[0]?.expression ?? "";
    // The Job's keys, its template's and its pod's are the clone's and what the API server defaults: no ServiceAccount, volume or host namespace of another's.
    assert.deepEqual(keysOf(job, "object.spec"), ["backoffLimit", "activeDeadlineSeconds", "ttlSecondsAfterFinished", "template", "completions", "parallelism", "completionMode", "suspend", "manualSelector", "podReplacementPolicy", "selector"]);
    assert.deepEqual(keysOf(job, 'object.spec["template"]["spec"]'), ["restartPolicy", "priorityClassName", "containers", "dnsPolicy", "schedulerName", "terminationGracePeriodSeconds", "securityContext"]);
    const rendered = (JSON.parse(one<V1ConfigMap>(objects, "ConfigMap", "alasio-sandbox-templates").data?.["templates.json"] ?? "{}") as { sessions: SessionsProfile }).sessions.clone?.job;
    assert.ok(job.includes(`object.spec["template"]["spec"]["containers"][0]["image"] == ${JSON.stringify(rendered?.spec?.template.spec?.containers[0]?.image)}`));
    for (const variable of ["SOURCE_NAMESPACE", "SOURCE_CLAIM", "DESTINATION_NAMESPACE", "DESTINATION_CLAIM"]) assert.ok(job.includes(JSON.stringify(variable)), variable);
    assert.ok(job.includes('.matches("^([a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)$")'), "each claim's a DNS label");
    assert.deepEqual(binding(objects, "alasio-clone-job"), { policyName: "alasio-clone-job", validationActions: ["Deny"] });
    // None where alasio forks no session, which runs no Job.
    assert.ok(!all(install({ workspaceStorage: { enabled: false } }), "ValidatingAdmissionPolicy").some(({ metadata }) => metadata?.name === "alasio-clone-job"));
  });

  test("lets alasio make no Sandbox but from its namespace's template, its own as a branch's, and no Secret but a Sandbox's token", () => {
    const objects = install({ host: { enabled: true } });
    const sandboxes = one<Policy>(objects, "ValidatingAdmissionPolicy", "alasio-sandboxes");
    assert.deepEqual(sandboxes.spec.matchConditions.map(({ expression }) => expression), ['request.userInfo.username == "system:serviceaccount:alasio:alasio"']);
    const sandbox = sandboxes.spec.validations[0]?.expression ?? "";
    // A Sandbox not moved onto another template is left be; one made, or moved, is its namespace's template's.
    assert.ok(sandbox.startsWith('request.operation == "UPDATE" && object.spec.podTemplate == oldObject.spec.podTemplate'));
    assert.ok(sandbox.includes('(request.namespace == "alasio-host" ? '));
    // A session's pod is as no one, under the sessions' runtime; a folder's bayma's as its own account alone.
    const sessionKeys = keysOf(sandbox, 'object.spec["podTemplate"]["spec"]', true);
    for (const key of ["serviceAccountName", "hostNetwork", "hostPID", "nodeName", "ephemeralContainers"]) assert.ok(!sessionKeys.includes(key), key);
    assert.ok(sandbox.includes('object.spec["podTemplate"]["spec"]["runtimeClassName"] == "gvisor"'));
    assert.ok(sandbox.includes('object.spec["podTemplate"]["spec"]["automountServiceAccountToken"] == false'));
    assert.ok(sandbox.includes('object.spec["podTemplate"]["spec"]["serviceAccountName"] == "alasio-host-agent"'));
    assert.deepEqual(binding(objects, "alasio-sandboxes"), { policyName: "alasio-sandboxes", validationActions: ["Deny"] });
    // A branch's is its own, of its own templates, by its own ServiceAccount.
    const branch = one<Policy>(branchObjects(configOf(), "try"), "ValidatingAdmissionPolicy", "alasio-branch-try-sandboxes");
    assert.deepEqual(branch.spec.matchConditions.map(({ expression }) => expression), ['request.userInfo.username == "system:serviceaccount:alasio-branch-try:alasio"']);

    const secrets = one<Policy>(objects, "ValidatingAdmissionPolicy", "alasio-secrets");
    assert.ok(secrets.spec.matchConditions[0]?.expression.includes("^system:serviceaccount:(alasio|alasio-branch-[a-z0-9-]+):alasio$"));
    assert.deepEqual(secrets.spec.validations.map(({ expression }) => expression), [
      'object.?type.orValue("Opaque") == "Opaque" && object.metadata.name.endsWith("-bayma-token") && object.?data.orValue({}).all(key, key == "token")',
    ]);
    assert.deepEqual(binding(objects, "alasio-secrets"), { policyName: "alasio-secrets", validationActions: ["Deny"] });
  });

  test("holds a folder's bayma's state directory under its template's state root as alasio joins it, however the root is written", () => {
    for (const stateRoot of ["/home/op/state", "/home/op//state/", "/"]) {
      const sandbox = one<Policy>(install({ host: { enabled: true, stateRoot } }), "ValidatingAdmissionPolicy", "alasio-sandboxes").spec.validations[0]?.expression ?? "";
      // The patterns of path segments: the state directory's.
      const patterns = [...sandbox.matchAll(/\.matches\(("(?:[^"\\]|\\.)*")\)/gu)]
        .map(([, quoted = '""']) => JSON.parse(quoted) as string)
        .filter((pattern) => pattern.includes("[A-Za-z0-9_][A-Za-z0-9_.-]*/"))
        .map((pattern) => new RegExp(pattern, "u"));
      assert.equal(patterns.length, 1, stateRoot);
      const profile = { stateRoot } as HostProfile;
      assert.match(hostBaymaStateDir(profile, "claude", "telegram:1"), patterns[0] ?? /^$/u, stateRoot);
      assert.doesNotMatch("/elsewhere/claude/telegram-1", patterns[0] ?? /^$/u, stateRoot);
    }
  });
});
