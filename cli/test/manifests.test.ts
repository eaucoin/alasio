/**
 * The manifests (cli/src/manifests): what they say of alasio's objects, Neon's,
 * workspace storage's, and those that confine what runs; and how the install
 * configuration is checked and defaulted.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, test } from "node:test";

import type {
  KubernetesObject,
  V1ConfigMap,
  V1Container,
  V1CronJob,
  V1CSIDriver,
  V1DaemonSet,
  V1Deployment,
  V1Job,
  V1Namespace,
  V1PriorityClass,
  V1Role,
  V1RoleBinding,
  V1Service,
  V1StatefulSet,
  V1StorageClass,
} from "@kubernetes/client-node";
import { Result } from "effect";

import type { SessionsProfile } from "../../src/kube/config.ts";
import { decodeInstallConfig, manifests } from "../src/manifests/index.ts";

/** The objects of the configuration `file` and the Telegram Secret every configuration names. */
function install(file: Record<string, unknown> = {}): readonly KubernetesObject[] {
  const { alasio, ...rest } = file;
  const decoded = decodeInstallConfig({ ...rest, alasio: { telegram: { existingSecret: "telegram" }, ...(alasio as object | undefined) } });
  if (Result.isFailure(decoded)) throw new Error(decoded.failure.message);
  return manifests(decoded.success);
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
    assert.equal(config.neon.safekeepers.replicas, 3);
    assert.equal(config.neon.image.digest, "sha256:166022a72bf9983eba96d061d794f4740edbd4c3301e66202c1180acce9a323c");
    assert.deepEqual(config.alasio.resources, { requests: { cpu: "500m", memory: "1Gi" }, limits: { memory: "8Gi" } });
  });

  test("adds the quantities given to the defaults", () => {
    const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" }, resources: { limits: { cpu: 2, memory: "4Gi" } } } });
    assert.ok(Result.isSuccess(decoded));
    assert.deepEqual(decoded.success.alasio.resources, { requests: { cpu: "500m", memory: "1Gi" }, limits: { memory: "4Gi", cpu: "2" } });
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
    assert.match(refused({ neon: { safekeepers: { replicas: 8 } } }), /^neon\.safekeepers\.replicas /u);
    assert.match(refused({ objectStore: { bundled: { volumes: 29 } } }), /^objectStore\.bundled\.volumes /u);
    assert.match(refused({ host: { mounts: [{ name: "home", hostPath: "home", mountPath: "/home" }] } }), /^host\.mounts\.0\.hostPath /u);
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
    const spec = deployment.spec?.template.spec;
    assert.deepEqual(spec?.volumes?.filter((volume) => volume.persistentVolumeClaim), []);
    assert.equal(spec?.initContainers, undefined);
    assert.deepEqual(container(deployment).volumeMounts?.filter(({ name }) => name === "state" || name === "alasio-home"), [
      { name: "state", mountPath: "/var/lib/alasio/state" },
      { name: "alasio-home", mountPath: "/var/lib/alasio/home" },
    ]);
    assert.deepEqual(spec?.volumes?.filter(({ name }) => name === "state" || name === "alasio-home"), [{ name: "state", emptyDir: {} }, { name: "alasio-home", emptyDir: {} }]);
    assert.deepEqual(all(objects, "PersistentVolumeClaim").map((claim) => claim.metadata?.name), ["alasio-neon-control"]);
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
    assert.equal(spec?.volumes?.find(({ name }) => name === "alasio-home"), undefined);
  });

  test("renders sessions under gVisor with the egress gate, and no host profile by default", () => {
    const json = templates(install());
    assert.match(json, /"runtimeClassName": "gvisor"/u);
    assert.match(json, /"egressGate": true/u);
    assert.doesNotMatch(json, /"host":/u);
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

  test("places each timeline on as many safekeepers as run", () => {
    const objects = install({ neon: { safekeepers: { replicas: 5 } } });
    assert.equal(one<V1StatefulSet>(objects, "StatefulSet", "alasio-neon-safekeeper").spec?.replicas, 5);
    assert.match(container(one<V1Deployment>(objects, "Deployment", "alasio-neon-storage-controller")).command?.[2] ?? "", /--timeline-safekeeper-count 5/u);
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

  test("pins every third-party image by digest", () => {
    const statefulSets = all<V1StatefulSet>(install({ workspaceStorage: { enabled: false } }), "StatefulSet");
    assert.equal(statefulSets.length, 4);
    for (const statefulSet of statefulSets) assert.match(container(statefulSet).image ?? "", /@sha256:[0-9a-f]{64}$/u, statefulSet.metadata?.name);
  });

  test("gives the object store a fixed count of volumes, not one made from the disk free", () => {
    const seaweedfs = one<V1StatefulSet>(install({ objectStore: { bundled: { volumes: 40 } } }), "StatefulSet", "alasio-seaweedfs");
    assert.ok(container(seaweedfs).args?.includes("-volume.max=40"));
  });

  test("uses the operator's object store and makes no SeaweedFS when told", () => {
    const objects = install({ objectStore: { bundled: { enabled: false }, external: { endpoint: "https://s3.example.com", existingSecret: "s3" } } });
    assert.deepEqual(objects.filter((object) => object.metadata?.name?.startsWith("alasio-seaweedfs")), []);
    const env = one<V1Job>(objects, "Job", "alasio-neon-setup").spec?.template.spec?.containers[0]?.env;
    assert.deepEqual(env?.find(({ name }) => name === "S3_EXTERNAL"), { name: "S3_EXTERNAL", value: "1" });
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
    for (const pipeline of ["traces", "logs", "metrics"]) {
      assert.deepEqual(config.service.pipelines[pipeline], { receivers: ["otlp"], processors: ["batch"], exporters: ["otlphttp/lake"] }, pipeline);
    }
    assert.deepEqual(config.service.pipelines["metrics/stack"].exporters, ["otlphttp/lake"]);
    // No batch larger than an insert the lake inlines.
    assert.deepEqual(config.processors.batch, { send_batch_size: 1000, send_batch_max_size: 1000, timeout: "5s" });
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

  test("admits alasio and folder workspaces' bayma to the collector, beside the stack", () => {
    const ingress = (objects: readonly KubernetesObject[]) =>
      one<KubernetesObject & { spec: { podSelector: unknown; ingress: unknown } }>(objects, "NetworkPolicy", "alasio-collector").spec;
    const alasio = { podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "alasio" } } };
    assert.deepEqual(ingress(install()), {
      podSelector: { matchLabels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "collector" } },
      policyTypes: ["Ingress"],
      ingress: [{ from: [alasio], ports: [{ protocol: "TCP", port: 4318 }] }],
    });
    assert.deepEqual(ingress(install({ host: { enabled: true } })).ingress, [{
      from: [alasio, { namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "alasio-host" } } }],
      ports: [{ protocol: "TCP", port: 4318 }],
    }]);
    // A pod of the stack's, as the compute and the lake are, which the stack's own policy admits.
    assert.equal(one<V1Deployment>(install(), "Deployment", "alasio-collector").spec?.template.metadata?.labels?.["alasio.dev/stack"], "neon");
  });

  test("scrapes every service of the stack", () => {
    const config = one<V1ConfigMap>(install(), "ConfigMap", "alasio-collector").data?.["config.yaml"] ?? "";
    assert.match(config, /alasio-neon-safekeeper-2\.alasio-neon-safekeeper:7676/u);
    assert.match(config, /alasio-lake:9464/u);
  });

  test("scrapes JuiceFS's pods, found in the driver's namespace, and Valkey, with its password, when workspace storage is on", () => {
    const objects = install();
    const config = collectorConfig(objects);
    // Mount pods serve their metrics on 9567, the driver's pods theirs on 8080.
    for (const [job, name, port] of [["juicefs", "juicefs-mount", 9567], ["juicefs-csi", "juicefs-csi-driver", 8080]] as const) {
      assert.deepEqual(config.receivers.prometheus.config.scrape_configs.find(({ job_name }: { job_name: string }) => job_name === job), {
        job_name: job,
        scrape_interval: "30s",
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
      collection_interval: "30s",
      metrics: { "redis.maxmemory": { enabled: true } },
    });
    assert.deepEqual(config.service.pipelines["metrics/stack"].receivers, ["prometheus", "redis"]);
    const collector = one<V1Deployment>(objects, "Deployment", "alasio-collector");
    assert.equal(collector.spec?.template.spec?.serviceAccountName, "alasio-collector");
    assert.deepEqual(container(collector).env, [{ name: "VALKEY_PASSWORD", valueFrom: { secretKeyRef: { name: "alasio-valkey", key: "password" } } }]);
    const role = one<V1Role>(objects, "Role", "alasio-collector");
    assert.equal(role.metadata?.namespace, "kube-system");
    assert.deepEqual(role.rules, [{ apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch"] }]);
    const binding = one<V1RoleBinding>(objects, "RoleBinding", "alasio-collector");
    assert.equal(binding.metadata?.namespace, "kube-system");
    assert.deepEqual(binding.subjects, [{ kind: "ServiceAccount", name: "alasio-collector", namespace: "alasio" }]);
    assert.ok(one(objects, "ServiceAccount", "alasio-collector"));

    const off = install({ workspaceStorage: { enabled: false } });
    const offConfig = collectorConfig(off);
    assert.equal(offConfig.receivers.redis, undefined);
    assert.deepEqual(offConfig.service.pipelines["metrics/stack"].receivers, ["prometheus"]);
    for (const job of ["juicefs", "juicefs-csi"]) {
      assert.equal(offConfig.receivers.prometheus.config.scrape_configs.find(({ job_name }: { job_name: string }) => job_name === job), undefined, job);
    }
    assert.equal(one<V1Deployment>(off, "Deployment", "alasio-collector").spec?.template.spec?.serviceAccountName, undefined);
    assert.deepEqual(off.filter(({ kind, metadata }) => !["ConfigMap", "Service", "Deployment", "NetworkPolicy"].includes(kind ?? "") && metadata?.name === "alasio-collector"), []);
  });

  test("scrapes nothing without Neon, only taking OTLP and sending it on", () => {
    const config = collectorConfig(install({ neon: { enabled: false, external: { existingSecret: "db" } }, workspaceStorage: { enabled: false }, telemetry: { otlpEndpoint: "https://otlp.example.com" } }));
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
    // The destination's directory has no quota at first, then the driver's.
    const juicefs = [
      'echo "$@" >> "$ROOT/calls"',
      'if [ "$1 $2" = "quota get" ]; then [ -e "$ROOT/quota" ] && echo "| /alasio-sessions-data-fs-def456 | 1.0 GiB |"; : > "$ROOT/quota"; fi',
      "true",
    ].join("\n");
    const recorded = (name: string) => `echo "${name} $*" >> "$ROOT/calls"`;
    // A mount that answers at once, and a source directory the pod's group's, as kubelet made it.
    const stat = 'case $1 in --file-system) echo fuseblk ;; --format=%a) echo 2775 ;; *) echo 1000:1000 ;; esac';
    const env = {
      META_URL: "redis://valkey/1",
      SOURCE_NAMESPACE: "alasio-sessions",
      SOURCE_CLAIM: "data-fs-abc123",
      DESTINATION_NAMESPACE: "alasio-sessions",
      DESTINATION_CLAIM: "data-fs-def456",
    };
    const commands = { juicefs, mkdir: recorded("mkdir"), stat: stat, sleep: recorded("sleep"), umount: recorded("umount"), chown: recorded("chown"), chmod: recorded("chmod") };
    const { root, run, remove } = runScript(cloner, env, commands);
    try {
      run();
      assert.deepEqual(readFileSync(join(root, "calls"), "utf8").trim().split("\n"), [
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
    const buckets = one<V1StatefulSet>(objects, "StatefulSet", "alasio-seaweedfs").spec?.template.spec?.containers.find(({ name }) => name === "buckets");
    assert.match(buckets?.command?.[2] ?? "", /^for bucket in neon lake backups workspaces; do$/mu);
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
    assert.deepEqual(needed.filter(({ kind }) => kind !== "NetworkPolicy").map(({ kind, metadata }) => `${kind} ${metadata?.name}`), [
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
    for (const object of needed.filter(({ kind }) => kind !== "NetworkPolicy")) {
      assert.equal(object.metadata?.labels?.["alasio.dev/volume-driver"], "csi.juicefs.com", `${object.kind} ${object.metadata?.name}`);
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
    assert.deepEqual([...all(objects, "CSIDriver"), ...all(objects, "PriorityClass"), ...all(objects, "DaemonSet")], []);
    assert.deepEqual(objects.filter((object) => object.metadata?.name?.startsWith("juicefs-")), []);
    assert.ok(one(objects, "StorageClass", "alasio-workspaces"));
    assert.ok(one(objects, "StatefulSet", "alasio-valkey"));
  });

  test("installs nothing of it when it is off", () => {
    const objects = install({ workspaceStorage: { enabled: false } });
    for (const kind of ["CSIDriver", "PriorityClass", "DaemonSet", "StorageClass"]) assert.deepEqual(all(objects, kind), [], kind);
    assert.deepEqual(objects.filter(({ metadata }) => /valkey|juicefs|workspaces/u.test(metadata?.name ?? "")), []);
    assert.equal(setupVariable(objects, "WORKSPACES_NAME"), undefined);
    const buckets = one<V1StatefulSet>(objects, "StatefulSet", "alasio-seaweedfs").spec?.template.spec?.containers.find(({ name }) => name === "buckets");
    assert.match(buckets?.command?.[2] ?? "", /^for bucket in neon lake backups; do$/mu);
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

  test("lets alasio manage only Sandboxes, their token Secrets and exec in its namespaces", () => {
    const roles = all<V1Role>(install({ workspaceStorage: { enabled: false } }), "Role").filter((role) => role.metadata?.name === "alasio");
    assert.equal(roles.length, 1);
    assert.equal(roles[0]?.metadata?.namespace, "alasio-sessions");
    assert.deepEqual(roles[0]?.rules, [
      { apiGroups: ["agents.x-k8s.io"], resources: ["sandboxes"], verbs: ["get", "list", "watch", "create", "patch", "delete"] },
      { apiGroups: [""], resources: ["secrets"], verbs: ["get", "create"] },
      { apiGroups: [""], resources: ["pods"], verbs: ["get"] },
      { apiGroups: [""], resources: ["pods/exec"], verbs: ["create", "get"] },
    ]);
  });

  test("lets alasio read sessions' claims and run the Jobs that clone them in its own namespace, only where it forks sessions", () => {
    const rolesOf = (objects: readonly KubernetesObject[]) =>
      Object.fromEntries(all<V1Role>(objects, "Role").filter((role) => role.metadata?.name === "alasio").map(({ metadata, rules }) => [metadata?.namespace, rules]));
    const objects = install({ host: { enabled: true } });
    const roles = rolesOf(objects);
    assert.deepEqual(roles["alasio"], [{ apiGroups: ["batch"], resources: ["jobs"], verbs: ["create", "get", "delete"] }]);
    assert.deepEqual(roles["alasio-sessions"]?.at(-1), { apiGroups: [""], resources: ["persistentvolumeclaims"], verbs: ["get"] });
    assert.ok(!JSON.stringify(roles["alasio-host"]).includes("persistentvolumeclaims"));
    const binding = one<V1RoleBinding>(objects.filter(({ metadata }) => metadata?.namespace === "alasio"), "RoleBinding", "alasio");
    assert.deepEqual(binding.subjects, [{ kind: "ServiceAccount", name: "alasio", namespace: "alasio" }]);
    assert.deepEqual(binding.roleRef, { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: "alasio" });
    const unforked = rolesOf(install({ sessions: { storage: { storageClassName: "fast" } } }));
    assert.equal(unforked["alasio"], undefined);
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
