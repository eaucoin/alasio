/**
 * The manifests (cli/src/manifests): what they say of alasio's objects, Neon's, and
 * those that confine what runs; and how the install configuration is checked and
 * defaulted.
 */
import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type {
  KubernetesObject,
  V1ConfigMap,
  V1Container,
  V1CronJob,
  V1Deployment,
  V1Job,
  V1Namespace,
  V1Role,
  V1Service,
  V1StatefulSet,
} from "@kubernetes/client-node";
import { Result } from "effect";

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
    assert.equal(message({ neon: { enabled: false, external: { existingSecret: "db" } }, objectStore: { bundled: { enabled: false } } }), "");
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

  test("drives Kubernetes from the rendered templates, with its state on its own volume", () => {
    const deployment = one<V1Deployment>(install(), "Deployment", "alasio");
    const env = container(deployment).env;
    assert.deepEqual(env?.find(({ name }) => name === "ALASIO_KUBE_TEMPLATES"), { name: "ALASIO_KUBE_TEMPLATES", value: "/etc/alasio/templates.json" });
    assert.deepEqual(env?.find(({ name }) => name === "ALASIO_STATE_DIR"), { name: "ALASIO_STATE_DIR", value: "/var/lib/alasio/state" });
    assert.deepEqual(env?.find(({ name }) => name === "HOME"), { name: "HOME", value: "/var/lib/alasio/home" });
    assert.deepEqual(deployment.spec?.template.spec?.volumes?.find(({ name }) => name === "state"), { name: "state", persistentVolumeClaim: { claimName: "alasio" } });
    assert.equal(deployment.spec?.template.spec?.securityContext?.runAsNonRoot, true);
  });

  test("says every object is managed by alasio", () => {
    const objects = install({ host: { enabled: true }, telemetry: { otlpEndpoint: "http://collector:4318" } });
    for (const object of objects.filter(({ kind }) => kind !== "CustomResourceDefinition")) {
      assert.equal(object.metadata?.labels?.["app.kubernetes.io/managed-by"], "alasio", `${object.kind} ${object.metadata?.name}`);
    }
  });

  test("uses an existing claim instead of making one", () => {
    const objects = install({ alasio: { persistence: { existingClaim: "mine" } } });
    assert.deepEqual(all(objects, "PersistentVolumeClaim").map((claim) => claim.metadata?.name), ["alasio-neon-control"]);
    const volumes = one<V1Deployment>(objects, "Deployment", "alasio").spec?.template.spec?.volumes;
    assert.deepEqual(volumes?.find(({ name }) => name === "state"), { name: "state", persistentVolumeClaim: { claimName: "mine" } });
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
    assert.deepEqual(container(compute).env?.find(({ name }) => name === "OTEL_SDK_DISABLED"), { name: "OTEL_SDK_DISABLED", value: "true" });
  });

  test("traces the compute where the deployment's telemetry goes, when it goes anywhere", () => {
    const compute = one<V1Deployment>(install({ telemetry: { otlpEndpoint: "http://collector:4318" } }), "Deployment", "alasio-neon-compute");
    assert.deepEqual(container(compute).env?.find(({ name }) => name === "OTEL_EXPORTER_OTLP_ENDPOINT"), {
      name: "OTEL_EXPORTER_OTLP_ENDPOINT",
      value: "http://collector:4318",
    });
  });

  test("pins every third-party image by digest", () => {
    const statefulSets = all<V1StatefulSet>(install(), "StatefulSet");
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
    const objects = install({ neon: { enabled: false, external: { existingSecret: "db" } } });
    assert.deepEqual(objects.filter((object) => object.metadata?.labels?.["alasio.dev/stack"] === "neon"), []);
    assert.deepEqual(objects.filter((object) => object.metadata?.name === "alasio-lake"), []);
  });

  test("runs the collector only with somewhere to send to", () => {
    assert.deepEqual(install().filter((object) => object.metadata?.name === "alasio-neon-collector"), []);
  });

  test("serves the compute's metrics where the collector scrapes them", () => {
    assert.ok(one<V1Service>(install(), "Service", "alasio-neon-compute").spec?.ports?.some(({ name, port }) => name === "http" && port === 3080));
  });

  test("scrapes every service of the stack when it does", () => {
    const objects = install({ telemetry: { otlpEndpoint: "http://collector:4318" } });
    const config = one<V1ConfigMap>(objects, "ConfigMap", "alasio-neon-collector").data?.["config.yaml"] ?? "";
    assert.match(config, /alasio-neon-safekeeper-2\.alasio-neon-safekeeper:7676/u);
    assert.match(config, /alasio-lake:9464/u);
  });

  test("backs the database up daily unless told not to", () => {
    assert.equal(one<V1CronJob>(install(), "CronJob", "alasio-neon-backup").spec?.schedule, "17 3 * * *");
    assert.deepEqual(install({ neon: { backup: { enabled: false } } }).filter((object) => object.metadata?.name === "alasio-neon-backup"), []);
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
    const roles = all<V1Role>(install(), "Role").filter((role) => role.metadata?.name === "alasio");
    assert.equal(roles.length, 1);
    assert.equal(roles[0]?.metadata?.namespace, "alasio-sessions");
    assert.deepEqual(roles[0]?.rules, [
      { apiGroups: ["agents.x-k8s.io"], resources: ["sandboxes"], verbs: ["get", "list", "watch", "create", "patch", "delete"] },
      { apiGroups: [""], resources: ["secrets"], verbs: ["get", "create"] },
      { apiGroups: [""], resources: ["pods"], verbs: ["get"] },
      { apiGroups: [""], resources: ["pods/exec"], verbs: ["create", "get"] },
    ]);
  });

  test("lets the host profile's agents restart alasio's own Deployment and nothing else", () => {
    const role = one<V1Role>(install({ host: { enabled: true } }), "Role", "alasio-restart");
    assert.deepEqual(role.rules?.[0], { apiGroups: ["apps"], resources: ["deployments"], resourceNames: ["alasio"], verbs: ["get", "patch"] });
  });
});
