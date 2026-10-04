/**
 * The manifests (cli/src/manifests) are the Helm chart's objects: for each of the
 * chart's CI value sets, and others that turn on and off what they leave as it is, the
 * builders' objects equal `helm template`'s, until the chart is removed.
 *
 * Helm renders a copy of the chart released at this package's version with this
 * package's images, as the builders install them. Its objects are compared as the API
 * server takes them: a key Helm rendered null is left out, and an object of alasio's
 * namespace that names none is in it. What is Helm's own is normalized away: its chart
 * label, its hooks and resource policy, and its managed-by, which is alasio. So is the
 * checksum of the sandbox templates alasio's pod carries, which the chart took of the
 * ConfigMap it wrote, labels and all, and the builders of the templates alone.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, describe, test } from "node:test";

import type { KubernetesObject } from "@kubernetes/client-node";
import { Result } from "effect";
import { parseAllDocuments } from "yaml";

import { decodeInstallConfig, manifests, NAMESPACE } from "../src/manifests/index.ts";
import { IMAGES, VERSION } from "../src/release.ts";

const CHART = new URL("../../charts/alasio/", import.meta.url).pathname;

const helmMissing = spawnSync("helm", ["version"]).error !== undefined;

/** The kinds whose objects are in no namespace. */
const CLUSTER_SCOPED = new Set(["Namespace", "ClusterRole", "ClusterRoleBinding", "CustomResourceDefinition"]);

/** A value set: Helm's values (a file of the chart's, or values written here) and the configuration that is the same. */
interface ValueSet {
  readonly name: string;
  readonly values: string | Record<string, unknown>;
  readonly config: Record<string, unknown>;
}

const TELEGRAM = { telegram: { existingSecret: "alasio-telegram" } };

const VALUE_SETS: readonly ValueSet[] = [
  { name: "ci/default-values.yaml", values: "ci/default-values.yaml", config: { alasio: TELEGRAM } },
  {
    name: "ci/host-values.yaml",
    values: "ci/host-values.yaml",
    config: {
      alasio: { ...TELEGRAM, claude: { existingSecret: "claude" } },
      telemetry: { otlpEndpoint: "http://collector.observability.svc:4318", headersSecret: "otel-headers" },
      host: {
        enabled: true,
        uid: 1000,
        gid: 1000,
        supplementalGroups: [988],
        home: "/home/operator",
        stateRoot: "/home/operator/.alasio/bayma",
        workspaceRoot: "/home/operator",
        mounts: [
          { name: "home", hostPath: "/home", mountPath: "/home" },
          { name: "docker", hostPath: "/var/run/docker.sock", mountPath: "/var/run/docker.sock", type: "Socket" },
        ],
        env: { PATH: "/home/operator/.local/bin:/usr/local/bin:/usr/bin:/bin" },
      },
    },
  },
  (() => {
    // The operator's database and none of the rest: no Neon, sessions, policies or agent-sandbox.
    const shared = {
      alasio: { ...TELEGRAM, persistence: { existingClaim: "alasio-state" }, defaultHarness: "codex" },
      neon: { enabled: false, external: { existingSecret: "database" } },
      sessions: { enabled: false },
      networkPolicies: { enabled: false },
    };
    return { name: "external database, nothing optional", values: { ...shared, "agent-sandbox": { enabled: false } }, config: { ...shared, agentSandbox: { enabled: false } } };
  })(),
  (() => {
    // An external object store, and every value of the rest that renders only when given.
    const shared = {
      imagePullSecrets: ["registry"],
      alasio: {
        ...TELEGRAM,
        env: { B_VARIABLE: "b", A_VARIABLE: "a & <b>" },
        envFrom: [{ secretRef: { name: "extra" } }],
        resources: { limits: { cpu: "4" } },
        supplementalGroups: [5, 6],
        nodeSelector: { "kubernetes.io/os": "linux" },
        tolerations: [{ key: "dedicated", operator: "Exists", effect: "NoSchedule" }],
        affinity: { nodeAffinity: { requiredDuringSchedulingIgnoredDuringExecution: { nodeSelectorTerms: [{ matchExpressions: [{ key: "a", operator: "Exists" }] }] } } },
        persistence: { storageClassName: "fast", size: "5Gi" },
      },
      telemetry: { otlpEndpoint: "https://otel.example.com", otlpProtocol: "grpc", resourceAttributes: "deployment.environment=test", receiverPort: 14318 },
      sessions: {
        namespace: "workspaces",
        createNamespace: false,
        runtimeClassName: "",
        egressGate: false,
        fullModeNameservers: ["9.9.9.9"],
        blockedCidrs: ["10.0.0.0/8"],
        storage: { size: "1Gi", storageClassName: "slow" },
        nodeSelector: { pool: "sessions" },
        tolerations: [{ key: "sessions", operator: "Exists" }],
      },
      host: {
        enabled: true,
        createNamespace: false,
        namespace: "folders",
        alasioHome: false,
        supplementalGroups: [6, 7],
        mounts: [{ name: "data", hostPath: "/srv/data", mountPath: "/data", readOnly: true, type: "Directory" }],
        // What JSON and Go quote apart: HTML's characters, and a space Go does not print.
        env: { GREETING: `<héllo> & wörld${String.fromCodePoint(0xa0)}"!"` },
      },
      neon: {
        safekeepers: { replicas: 5, storage: { storageClassName: "fast", size: "1Gi" } },
        backup: { enabled: false },
        collector: { otlpEndpoint: "http://metrics.example.com:4318" },
        nodeSelector: { pool: "data" },
        tolerations: [{ key: "data", operator: "Exists" }],
      },
      objectStore: {
        bundled: { enabled: false },
        external: { endpoint: "https://s3.example.com", region: "eu-west-1", existingSecret: "s3" },
        buckets: { neon: "neon-data", lake: "lake-data", backups: "backup-data" },
      },
      lake: { enabled: false },
    };
    const agentSandbox = { nodeSelector: { pool: "system" }, resources: { requests: { cpu: "1" } } };
    return { name: "external object store, everything given", values: { ...shared, "agent-sandbox": agentSandbox }, config: { ...shared, agentSandbox } };
  })(),
];

/** `value` without the keys Helm rendered null, at every depth. */
function withoutNulls(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(withoutNulls);
  if (typeof value !== "object" || value === null) return value;
  return Object.fromEntries(Object.entries(value).filter(([, entry]) => entry !== null).map(([key, entry]) => [key, withoutNulls(entry)]));
}

/** The annotations either side has that the other does not: Helm's, and the templates' checksum. */
const UNCOMPARED_ANNOTATIONS = new Set([
  "helm.sh/hook",
  "helm.sh/hook-weight",
  "helm.sh/hook-delete-policy",
  "helm.sh/resource-policy",
  "checksum/sandbox-templates",
]);

/** `metadata` as it is compared: managed by alasio, without Helm's chart label or the annotations not compared. */
function comparedMetadata(metadata: Record<string, unknown>): Record<string, unknown> {
  const { labels, annotations, ...rest } = metadata as { labels?: Record<string, string>; annotations?: Record<string, string> };
  const keptLabels = labels && Object.fromEntries(
    Object.entries(labels)
      .filter(([key]) => key !== "helm.sh/chart")
      .map(([key, value]) => [key, key === "app.kubernetes.io/managed-by" && value === "Helm" ? "alasio" : value]),
  );
  const keptAnnotations = annotations && Object.fromEntries(Object.entries(annotations).filter(([key]) => !UNCOMPARED_ANNOTATIONS.has(key)));
  return {
    ...rest,
    ...(keptLabels ? { labels: keptLabels } : {}),
    ...(keptAnnotations && Object.keys(keptAnnotations).length > 0 ? { annotations: keptAnnotations } : {}),
  };
}

/** `value` as it is compared: every metadata in it, at every depth, by comparedMetadata. */
function compared(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(compared);
  if (typeof value !== "object" || value === null) return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, entry]) => [
      key,
      key === "metadata" && typeof entry === "object" && entry !== null ? compared(comparedMetadata(entry as Record<string, unknown>)) : compared(entry),
    ]),
  );
}

/** `object` in alasio's namespace, unless it is cluster-scoped or names its own. */
function placed(object: KubernetesObject): KubernetesObject {
  if (CLUSTER_SCOPED.has(object.kind ?? "") || object.metadata?.namespace) return object;
  return { ...object, metadata: { ...object.metadata, namespace: NAMESPACE } };
}

/** The objects by kind, namespace and name, in that order, as JSON has them and as they are compared. */
function byIdentity(objects: readonly KubernetesObject[]): Map<string, unknown> {
  const entries = objects.map((object) => [`${object.kind}/${object.metadata?.namespace ?? ""}/${object.metadata?.name}`, compared(JSON.parse(JSON.stringify(object)))] as const);
  return new Map(entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
}

describe("the manifests are the chart's objects", { skip: helmMissing && "helm is not on PATH" }, () => {
  const work = mkdtempSync(join(tmpdir(), "alasio-parity-"));
  after(() => rmSync(work, { recursive: true, force: true }));

  // The chart released at this package's version, as the builders label their objects.
  const chart = join(work, "alasio");
  cpSync(CHART, chart, { recursive: true });
  const chartYaml = join(chart, "Chart.yaml");
  writeFileSync(chartYaml, readFileSync(chartYaml, "utf8").replace(/^version: .*$/mu, `version: ${VERSION}`).replace(/^appVersion: .*$/mu, `appVersion: "${VERSION}"`));
  // This package's images, which the chart's values leave to its appVersion, unpinned.
  const images = join(work, "images.json");
  writeFileSync(images, JSON.stringify({ images: IMAGES }));

  /** Helm's objects for `values`: the chart rendered, CRDs included. */
  const helmObjects = (values: ValueSet["values"]): KubernetesObject[] => {
    const file = typeof values === "string" ? join(CHART, values) : join(work, "values.json");
    if (typeof values !== "string") writeFileSync(file, JSON.stringify(values));
    const rendered = spawnSync("helm", ["template", "alasio", chart, "--namespace", "alasio", "--include-crds", "-f", file, "-f", images], { encoding: "utf8" });
    assert.equal(rendered.status, 0, rendered.stderr);
    return parseAllDocuments(rendered.stdout, { version: "1.1" })
      .map((document) => document.toJS() as KubernetesObject | null)
      .filter((object): object is KubernetesObject => object !== null)
      .map((object) => placed(withoutNulls(object) as KubernetesObject));
  };

  for (const { name, values, config } of VALUE_SETS) {
    test(name, () => {
      const decoded = decodeInstallConfig(config);
      assert.ok(Result.isSuccess(decoded), Result.isFailure(decoded) ? decoded.failure.message : "");
      const expected = byIdentity(helmObjects(values));
      const actual = byIdentity(manifests(decoded.success));
      assert.deepEqual([...actual.keys()], [...expected.keys()]);
      for (const [identity, object] of expected) assert.deepEqual(actual.get(identity), object, identity);
    });
  }

  test("the defaults alone, which name no Telegram Secret, install nothing", () => {
    const rendered = spawnSync("helm", ["template", "alasio", chart, "--namespace", "alasio", "-f", images], { encoding: "utf8" });
    assert.notEqual(rendered.status, 0);
    assert.match(rendered.stderr, /telegram\/existingSecret/u);
    const decoded = decodeInstallConfig({});
    assert.ok(Result.isFailure(decoded));
    assert.match(decoded.failure.message, /^alasio is required/u);
  });
});
