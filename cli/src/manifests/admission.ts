/**
 * What alasio may do beyond what RBAC can say: ValidatingAdmissionPolicies of the
 * cluster's, each matching what alasio's ServiceAccount asks (main's, or a branch
 * environment's), and holding it to what alasio up renders:
 *
 * - a Job alasio makes is the clone of a session's volume, as rendered, but for the
 *   claims it is told: RBAC lets it make any Job in its namespace, as another
 *   ServiceAccount there, a controller's whose rights reach the cluster, or of another
 *   image or command, privileged as the clone is;
 * - a Sandbox alasio makes, or moves onto another pod template, is made from the template
 *   of its namespace's workspaces, but for what alasio adds to each (src/kube/
 *   sandbox-shape.ts): RBAC lets it make Sandboxes of any pod, as any ServiceAccount, under
 *   any runtime;
 * - a Secret alasio makes is a Sandbox's token: RBAC lets it make Secrets of any type, a
 *   ServiceAccount's token among them;
 * - a pod alasio patches changes its busy label and annotations alone (src/kube/busy.ts),
 *   and in alasio's own namespace is alasio's own: RBAC lets it patch its namespaces'
 *   pods whole, their images among it;
 * - a compute's Deployment neon-control patches changes its pod template's restartedAt
 *   annotation alone, which restarts it (neon/control/computes.ts): RBAC lets it patch the
 *   Deployment whole, its image and command among it.
 *
 * Where a policy holds an object to a rendered one, `pinned` makes the CEL that says so:
 * the object's fields are the rendered ones', key for key and item for item, but for what
 * a Check allows, and what the API server defaults where the rendered one has nothing.
 */
import { posix } from "node:path";

import type { KubernetesObject, V1Container, V1PodSpec, V1PodTemplateSpec, V1Volume } from "@kubernetes/client-node";

import { BINDING_LABEL, FREE_LABEL, KEY_ANNOTATION, POOL_LABEL } from "../../../src/binder/names.ts";
import { branchNamespace, branchSessionsNamespace } from "../../../src/branch/names.ts";
import { ATTACH_VOLUME, ownDirectory, SANDBOX_POD_POLICY } from "../../../src/gateway/names.ts";
import { BUSY_ANNOTATIONS, BUSY_LABEL } from "../../../src/kube/busy-label.ts";
import { CLONE_VARIABLES, type HostProfile, type SessionsProfile } from "../../../src/kube/config.ts";
import {
  BAYMA_CONTAINER,
  DEFAULT_FULL_MODE_NAMESERVERS,
  EGRESS_GATE,
  EGRESS_GATE_SCRIPT,
  HARNESS_LABEL,
  MANAGED_BY,
  NET_MODE_LABEL,
  RESTORE_ANNOTATION,
  RESTORE_PAUSED_ANNOTATION,
  RESUMED_CONDITION,
  SANDBOX_LABEL,
  SESSION_TEMPLATE_ANNOTATION,
  SNAPSHOT_CACHE_DIR,
  SNAPSHOT_ID,
  STATE_DIR_ARG,
  TELEMETRY_TOKEN_FILE_ARGS,
  TOKEN_DIR,
  TOKEN_FILE_ARGS,
  TOKEN_KEY,
  TOKEN_MODE,
  TOKEN_VOLUME,
  tokenSecretName,
  WORKLOAD_LABEL,
} from "../../../src/kube/sandbox-shape.ts";
import { ATTACH_ROOT, RESTORE_POD } from "../../../src/node-agent/names.ts";
import { componentName, labels, NAMESPACE, neonName, RELEASE } from "./common.ts";

/** A field's test, beyond its being a rendered value: given the field's CEL expression and how deep it is, the CEL that holds where it passes. */
class Check {
  readonly cel: (expression: string, depth: number) => string;
  constructor(cel: (expression: string, depth: number) => string) {
    this.cel = cel;
  }
}

/** What a pinned field must be: its value; of a map or a list, what each of its keys or items must be; or what a Check says. */
export type Pinned = string | number | boolean | Check | readonly Pinned[] | { readonly [key: string]: Pinned | undefined };

/** Anything. */
const ANY = new Check(() => "true");
/** Any string. */
const ANY_STRING = new Check((expression) => `type(${expression}) == string`);
/** A map with nothing in it, as the API server defaults one. */
const EMPTY = new Check((expression) => `size(${expression}) == 0`);
/** A string all of which `pattern` matches. */
const matching = (pattern: string) => new Check((expression) => `type(${expression}) == string && ${expression}.matches(${JSON.stringify(`^(${pattern})$`)})`);
/** What the CEL `value` is, of the object asked about. */
const sameAs = (value: string) => new Check((expression) => `${expression} == ${value}`);
/** Any of `alternatives`. */
const oneOf = (...alternatives: Pinned[]) => new Check((expression, depth) => `(${alternatives.map((each) => pinned(expression, each, depth)).join(" || ")})`);
/** A list of `first`, then any number of items each `rest`. */
const startingWith = (first: readonly Pinned[], rest: Pinned) =>
  new Check((expression, depth) => {
    const item = `item${depth}`;
    return [
      `size(${expression}) >= ${first.length}`,
      ...first.map((each, index) => pinned(`${expression}[${index}]`, each, depth + 1)),
      `${expression}.all(${item}, ${[...first, rest].map((each) => pinned(item, each, depth + 1)).join(" || ")})`,
    ].join(" && ");
  });
/** Of a map's key: that it may be absent; where present, `value`. */
class Optional extends Check {
  readonly value: Pinned;
  constructor(value: Pinned) {
    super((expression, depth) => pinned(expression, value, depth));
    this.value = value;
  }
}
const optional = (value: Pinned): Optional => new Optional(value);

/** A map's keys: what each must be. */
type PinnedMap = { readonly [key: string]: Pinned | undefined };

/** The map `value`, but that each key `defaults` names may be there where `value` has not got it, as `defaults` says: as the API server defaults it. */
const defaulted = (value: PinnedMap, defaults: Readonly<Record<string, Pinned>>) => new Check((expression, depth) => pinnedMap(expression, value, defaults, depth));

/**
 * A DNS label: a namespace's name, and a claim's as alasio names a session's, of a claim
 * template's name and a volume id, each short enough for it (src/sandbox/names.ts). The
 * API server would take a claim's of a DNS subdomain; alasio's are held to a label.
 */
const DNS_LABEL = "[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?";

/**
 * A CEL expression that holds where `expression` is `expected`: of a map, no key but its,
 * and each as it is; of a list, its items alone, in order; of a Check, what it says.
 */
export function pinned(expression: string, expected: Pinned, depth = 0): string {
  if (expected instanceof Check) return expected.cel(expression, depth);
  if (typeof expected !== "object") return `${expression} == ${JSON.stringify(expected)}`;
  if (Array.isArray(expected)) {
    return [`size(${expression}) == ${expected.length}`, ...expected.map((item: Pinned, index) => pinned(`${expression}[${index}]`, item, depth))].join(" && ");
  }
  return pinnedMap(expression, expected as PinnedMap, {}, depth);
}

/** pinned of the map `expected`, and of the keys `defaults` names that it has not got (defaulted). */
function pinnedMap(expression: string, expected: PinnedMap, defaults: Readonly<Record<string, Pinned>>, depth: number): string {
  const keys = Object.keys(expected).filter((key) => expected[key] !== undefined);
  const extra = Object.keys(defaults).filter((key) => !keys.includes(key));
  const at = (key: string) => `${expression}[${JSON.stringify(key)}]`;
  return [
    `${expression}.all(key, key in ${JSON.stringify([...keys, ...extra])})`,
    ...keys.map((key) => {
      const value = expected[key];
      if (value instanceof Optional) return `(!(${JSON.stringify(key)} in ${expression}) || ${pinned(at(key), value.value, depth)})`;
      return `${JSON.stringify(key)} in ${expression} && ${pinned(at(key), value as Pinned, depth)}`;
    }),
    ...extra.map((key) => `(!(${JSON.stringify(key)} in ${expression}) || ${pinned(at(key), defaults[key] as Pinned, depth)})`),
  ].join(" && ");
}

/** The name the ServiceAccount of the alasio of the namespace `namespace` asks as. */
export const alasioAccount = (namespace: string) => `system:serviceaccount:${namespace}:${RELEASE}`;

/** Whether the request is by main's alasio or a branch environment's, as CEL. */
const BY_ANY_ALASIO = `request.userInfo.username.matches('^system:serviceaccount:(${NAMESPACE}|${branchNamespace("")}[a-z0-9-]+):${RELEASE}$')`;

/** A ValidatingAdmissionPolicy `name` of alasio's, on `rules`, of what `by` asks, refusing what `validations` do not hold for, and its binding. */
function admissionPolicy(
  name: string,
  rules: readonly { readonly apiGroups: string[]; readonly resources: string[]; readonly operations: string[] }[],
  by: string,
  validations: readonly { readonly expression: string; readonly message: string }[],
  variables?: readonly { readonly name: string; readonly expression: string }[],
): KubernetesObject[] {
  const metadata = { name, labels: labels("alasio") };
  const policy = {
    apiVersion: "admissionregistration.k8s.io/v1",
    kind: "ValidatingAdmissionPolicy",
    metadata,
    spec: {
      failurePolicy: "Fail",
      matchConstraints: { resourceRules: rules.map((rule) => ({ ...rule, apiVersions: ["*"] })) },
      matchConditions: [{ name: "by-alasio", expression: by }],
      ...(variables ? { variables } : {}),
      validations,
    },
  };
  const binding = { apiVersion: "admissionregistration.k8s.io/v1", kind: "ValidatingAdmissionPolicyBinding", metadata, spec: { policyName: name, validationActions: ["Deny"] } };
  return [policy, binding];
}

/** CEL that holds where the maps `variables.old<field>` and `variables.new<field>` differ in `keys` alone. */
const unchanged = (field: string, keys: readonly string[]) =>
  `variables.new${field}.all(key, key in ${JSON.stringify(keys)} || (key in variables.old${field} && variables.old${field}[key] == variables.new${field}[key])) && ` +
  `variables.old${field}.all(key, key in ${JSON.stringify(keys)} || key in variables.new${field})`;

/**
 * What alasio may change of a pod: its busy label and the annotations that go with it
 * (src/kube/busy.ts), and nothing else; and in its own namespace, of its own pod alone,
 * not the floor's beside it. Main's alasio's and each branch environment's.
 */
export function busyLabelPolicy(): KubernetesObject[] {
  const component = "app.kubernetes.io/component";
  return admissionPolicy(componentName("busy-label"), [{ apiGroups: [""], resources: ["pods"], operations: ["UPDATE"] }], BY_ANY_ALASIO, [
    { expression: "object.spec == oldObject.spec", message: "alasio changes no pod's spec" },
    {
      expression: "object.metadata.?finalizers.orValue([]) == oldObject.metadata.?finalizers.orValue([]) && " +
        "object.metadata.?ownerReferences.orValue([]) == oldObject.metadata.?ownerReferences.orValue([])",
      message: "alasio changes no pod's finalizers or owners",
    },
    {
      expression: `request.userInfo.username != "system:serviceaccount:" + request.namespace + ":${RELEASE}" || ` +
        `("${component}" in variables.oldLabels && variables.oldLabels["${component}"] == "alasio")`,
      message: "alasio labels no pod of its own namespace but its own",
    },
    { expression: unchanged("Labels", [BUSY_LABEL]), message: `alasio changes no label of a pod's but ${BUSY_LABEL}` },
    { expression: unchanged("Annotations", Object.keys(BUSY_ANNOTATIONS)), message: `alasio changes no annotation of a pod's but ${Object.keys(BUSY_ANNOTATIONS).join(" and ")}` },
  ], [
    { name: "oldLabels", expression: "oldObject.metadata.?labels.orValue({})" },
    { name: "newLabels", expression: "object.metadata.?labels.orValue({})" },
    { name: "oldAnnotations", expression: "oldObject.metadata.?annotations.orValue({})" },
    { name: "newAnnotations", expression: "object.metadata.?annotations.orValue({})" },
  ]);
}

/** The annotation of a pod template whose change restarts its pods, as `kubectl rollout restart` sets it. */
const RESTARTED_AT = "kubectl.kubernetes.io/restartedAt";

/** The fields of a Deployment's spec but its pod template. */
const DEPLOYMENT_FIELDS = ["replicas", "selector", "strategy", "minReadySeconds", "revisionHistoryLimit", "paused", "progressDeadlineSeconds"];

/**
 * What neon-control may change of a Deployment, which RBAC lets it patch whole, of the
 * computes alone (./neon.ts's computeScaling): its pod template's RESTARTED_AT, which
 * restarts it (neon/control/computes.ts), and nothing else. Its scale it changes through
 * the scale subresource, which this does not match.
 */
export function computeRestartPolicy(): KubernetesObject[] {
  const same = (field: string) => `has(object.spec.${field}) == has(oldObject.spec.${field}) && (!has(object.spec.${field}) || object.spec.${field} == oldObject.spec.${field})`;
  const account = `system:serviceaccount:${NAMESPACE}:${neonName("control")}`;
  return admissionPolicy(componentName("compute-restarts"), [{ apiGroups: ["apps"], resources: ["deployments"], operations: ["UPDATE"] }], `request.userInfo.username == "${account}"`, [
    { expression: "object.spec.template.spec == oldObject.spec.template.spec", message: "neon-control changes no compute's pod spec" },
    { expression: DEPLOYMENT_FIELDS.map(same).join(" && "), message: "neon-control changes nothing of a compute's Deployment's spec but its pod template's annotation" },
    {
      expression: "object.metadata.?labels.orValue({}) == oldObject.metadata.?labels.orValue({}) && " +
        "object.metadata.?annotations.orValue({}) == oldObject.metadata.?annotations.orValue({}) && " +
        "object.metadata.?finalizers.orValue([]) == oldObject.metadata.?finalizers.orValue([]) && " +
        "object.metadata.?ownerReferences.orValue([]) == oldObject.metadata.?ownerReferences.orValue([])",
      message: "neon-control changes no compute Deployment's labels, annotations, finalizers or owners",
    },
    {
      expression: Object.entries({ labels: "{}", finalizers: "[]", ownerReferences: "[]", name: '""', generateName: '""' })
        .map(([field, none]) => `object.spec.template.metadata.?${field}.orValue(${none}) == oldObject.spec.template.metadata.?${field}.orValue(${none})`)
        .join(" && "),
      message: "neon-control changes no label, finalizer, owner or name of a compute's pod template",
    },
    { expression: unchanged("Annotations", [RESTARTED_AT]), message: `neon-control changes no annotation of a compute's pod template but ${RESTARTED_AT}` },
  ], [
    { name: "oldAnnotations", expression: "oldObject.spec.template.metadata.?annotations.orValue({})" },
    { name: "newAnnotations", expression: "object.spec.template.metadata.?annotations.orValue({})" },
  ]);
}

/**
 * What the API server gives a Job, its template, and the template's pod and containers,
 * when its maker does not: a pinned Job may have them so. Its selector and its template's
 * labels of its own are the Job's; a pod's scheduling and termination are no matter of
 * its rights; and a pod's security context, defaulted, is empty.
 */
const JOB_DEFAULTS = {
  spec: { completions: 1, parallelism: 1, completionMode: "NonIndexed", suspend: false, manualSelector: false, podReplacementPolicy: ANY_STRING, selector: ANY },
  metadata: { creationTimestamp: ANY },
  labels: {
    "batch.kubernetes.io/controller-uid": ANY_STRING,
    "controller-uid": ANY_STRING,
    "batch.kubernetes.io/job-name": sameAs("object.metadata.name"),
    "job-name": sameAs("object.metadata.name"),
  },
  pod: { dnsPolicy: ANY, schedulerName: ANY, terminationGracePeriodSeconds: ANY, securityContext: EMPTY },
  container: { terminationMessagePath: ANY, terminationMessagePolicy: ANY },
} satisfies Record<string, Record<string, Pinned>>;

/**
 * What alasio may run as a Job: the clone of a session's volume (src/sandbox/index.ts),
 * `job`, its spec and its pod's as rendered, but for the claims each of its containers is
 * told, as alasio adds them (cloneJobManifest), each a DNS label (DNS_LABEL). Main's
 * alasio's, the one that clones.
 */
export function cloneJobPolicy(job: KubernetesObject & { readonly spec?: { readonly template?: { readonly spec?: V1PodSpec } } }): KubernetesObject[] {
  const spec = job.spec ?? {};
  const pod = spec.template?.spec;
  const claims = CLONE_VARIABLES.map((variable) => ({ name: variable, value: matching(DNS_LABEL) }));
  const template = spec.template as { readonly metadata?: { readonly labels?: PinnedMap } & PinnedMap } | undefined;
  const containers = (pod?.containers ?? []).map((container) =>
    defaulted({ ...(container as unknown as PinnedMap), env: [...((container.env ?? []) as unknown as Pinned[]), ...claims] }, JOB_DEFAULTS.container)
  );
  const expected = defaulted({
    ...(spec as unknown as PinnedMap),
    template: {
      metadata: defaulted({ ...template?.metadata, labels: defaulted(template?.metadata?.labels ?? {}, JOB_DEFAULTS.labels) }, JOB_DEFAULTS.metadata),
      spec: defaulted({ ...(pod as unknown as PinnedMap), containers }, JOB_DEFAULTS.pod),
    },
  }, JOB_DEFAULTS.spec);
  return admissionPolicy(componentName("clone-job"), [{ apiGroups: ["batch"], resources: ["jobs"], operations: ["CREATE", "UPDATE"] }], `request.userInfo.username == "${alasioAccount(NAMESPACE)}"`, [{
    expression: pinned("object.spec", expected),
    message: "alasio runs no Job but its clone of a session's volume, as alasio up renders it",
  }]);
}

/** A container of a Sandbox's pod template as its CRD stores it: its ports' protocol defaulted. */
const stored = (container: PinnedMap): PinnedMap => {
  const ports = container["ports"];
  return Array.isArray(ports) ? { ...container, ports: ports.map((port: PinnedMap) => defaulted(port, { protocol: "TCP" })) } : container;
};

/** The token Secret of the Sandbox asked about, as CEL. */
const OWN_TOKEN = sameAs(`object.metadata.name + ${JSON.stringify(tokenSecretName(""))}`);

/** A path segment as sanitizePathToken makes one. */
const PATH_TOKEN = "[A-Za-z0-9_][A-Za-z0-9_.-]*";

/** `text`, as a pattern that matches it alone. */
const literally = (text: string) => text.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");

/** A variable of the environment, set to a string. */
const SET: Pinned = { name: ANY_STRING, value: ANY_STRING };

/** A node's name: a DNS subdomain. */
const NODE_NAME = "[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?";

/** A pod template's hash, as podTemplateHash makes one (src/kube/sandboxes.ts). */
const TEMPLATE_HASH = "[0-9a-f]{64}";

/**
 * Where a node agent prepares a snapshot of the Sandbox asked about, of the sessions of
 * `namespace`, for its pod to be restored from: under the snapshot cache, in its
 * environment's directory, in its workspace's, the Sandbox's own, a snapshot's.
 */
const restoredFrom = (namespace: string) =>
  new Check((expression) => {
    const own = `${JSON.stringify(`${SNAPSHOT_CACHE_DIR}/${namespace}/`)} + object.metadata.name + "/"`;
    return `type(${expression}) == string && ${expression}.startsWith(${own}) && ${expression}.substring(size(${own})).matches(${JSON.stringify(`^${SNAPSHOT_ID}$`)})`;
  });

/** What a session's Sandbox is made of: its template, and the profile its snapshots are restored by, the current one's. */
interface SessionWorkspace {
  readonly session: SessionsProfile;
  readonly current: SessionsProfile;
}

/**
 * The Sandbox of a workspace alasio makes from `template`: a session's (`session`, with
 * the session's profile, and `current`, the profile its snapshots are restored by) or a
 * folder's bayma's (`folder`, with its state root), as sandboxManifest and
 * sessionSandboxManifest or hostBaymaManifest make it.
 *
 * Of a session's, what its snapshots add (src/sandbox/snapshots.ts): the hash of the
 * template its pod is made from, which they record; and, restored from one, the image of the Sandbox's own that its
 * node prepared, left paused until it is resumed, under a release `current` keeps, on
 * that node, and ready once resumed.
 */
function sandboxOf(template: SessionsProfile | HostProfile, workspace: SessionWorkspace | { readonly folder: HostProfile }): Pinned {
  const podTemplate: V1PodTemplateSpec = template.podTemplate as V1PodTemplateSpec;
  const spec: V1PodSpec = podTemplate.spec ?? { containers: [] };
  const session = "session" in workspace ? workspace.session : null;
  const snapshots = "current" in workspace ? (workspace.current.snapshots ?? null) : null;
  const bayma = spec.containers.find(({ name }) => name === BAYMA_CONTAINER);
  const tokenMount = { name: TOKEN_VOLUME, mountPath: TOKEN_DIR, readOnly: true };
  // Under the state root as hostBaymaStateDir joins it: normalized, its one slash after it.
  const stateDir = "folder" in workspace ? [STATE_DIR_ARG, matching(`${literally(posix.join(workspace.folder.stateRoot, "/"))}${PATH_TOKEN}/${PATH_TOKEN}`)] : [];
  // A session's bayma that exports telemetry reads the token it exports with from its token's file too.
  const args = (each: V1Container): Pinned => {
    const own = [...(each.args ?? []), ...TOKEN_FILE_ARGS, ...stateDir];
    return session ? oneOf(own, [...own, ...TELEMETRY_TOKEN_FILE_ARGS]) : own;
  };
  const container = (each: V1Container): Pinned =>
    stored(
      (each.name !== BAYMA_CONTAINER ? each : {
        ...each,
        args: args(each),
        volumeMounts: [...(each.volumeMounts ?? []), tokenMount],
        env: optional(startingWith((each.env ?? []) as unknown as Pinned[], SET)),
      }) as unknown as PinnedMap,
    );
  const gate = session && session.egressGate !== false
    ? [{
      name: EGRESS_GATE.name,
      ...(bayma?.image === undefined ? {} : { image: bayma.image }),
      command: ["node", "-e", EGRESS_GATE_SCRIPT],
      ...(bayma?.securityContext === undefined ? {} : { securityContext: bayma.securityContext }),
      resources: EGRESS_GATE.resources,
    }]
    : [];
  const initContainers = ([...gate, ...(spec.initContainers ?? [])] as unknown as PinnedMap[]).map(stored);
  const volumes: Pinned[] = [...((spec.volumes ?? []) as V1Volume[] as unknown as Pinned[]), { name: TOKEN_VOLUME, secret: { secretName: OWN_TOKEN, defaultMode: TOKEN_MODE } }];
  const own = {
    ...MANAGED_BY,
    [SANDBOX_LABEL]: sameAs("object.metadata.name"),
    ...(session ? { [WORKLOAD_LABEL]: "session", [NET_MODE_LABEL]: oneOf("none", "full") } : { [WORKLOAD_LABEL]: "folder", [HARNESS_LABEL]: matching(PATH_TOKEN) }),
  };
  const annotations = (podTemplate.metadata?.annotations ?? {}) as PinnedMap;
  const added: PinnedMap = session
    ? {
      [SESSION_TEMPLATE_ANNOTATION]: optional(matching(TEMPLATE_HASH)),
      ...(snapshots && session
        ? {
          [RESTORE_ANNOTATION]: optional(restoredFrom(session.namespace)),
          [RESTORE_PAUSED_ANNOTATION]: optional("true"),
        }
        : {}),
    }
    : {};
  // The template's annotations, and those added where they may be; a template of none has a map of them only where some are added.
  const metadataAnnotations = Object.keys(added).length === 0 ? {} : { annotations: Object.keys(annotations).length > 0 ? { ...annotations, ...added } : optional({ ...added }) };
  const restoredOn: Pinned = {
    ...((spec.affinity ?? {}) as PinnedMap),
    nodeAffinity: {
      requiredDuringSchedulingIgnoredDuringExecution: { nodeSelectorTerms: [{ matchFields: [{ key: "metadata.name", operator: "In", values: [matching(NODE_NAME)] }] }] },
    },
  };
  const readinessGates = (spec.readinessGates ?? []) as unknown as Pinned[];
  const restorable = snapshots
    ? {
      runtimeClassName: oneOf(...new Set([spec.runtimeClassName ?? "", ...snapshots.releases.map(({ runtimeClassName }) => runtimeClassName)].filter((each) => each !== ""))),
      readinessGates: spec.readinessGates ? oneOf(readinessGates, [...readinessGates, { conditionType: RESUMED_CONDITION }]) : optional([{ conditionType: RESUMED_CONDITION }]),
      affinity: spec.affinity ? oneOf(spec.affinity as unknown as PinnedMap, restoredOn) : optional(restoredOn),
    }
    : {};
  const nameservers = session ? [...(session.fullModeNameservers ?? DEFAULT_FULL_MODE_NAMESERVERS)] : [];
  const podSpec = {
    ...spec,
    containers: spec.containers.map(container),
    ...(initContainers.length > 0 ? { initContainers } : {}),
    volumes,
    // A session's pod speaks to the cluster as no one, and resolves names as its internet says.
    ...(session
      ? { automountServiceAccountToken: false, enableServiceLinks: false, dnsPolicy: "None", dnsConfig: { nameservers: oneOf(nameservers, ["127.0.0.1"]) } }
      : {}),
    ...restorable,
  };
  // A Sandbox restored onto the template before the current one keeps the claims it was made with, of either.
  const claims = [...new Set([session, "current" in workspace ? workspace.current : null].map((each) => JSON.stringify(each?.volumeClaimTemplates ?? [])))]
    .map((each) => JSON.parse(each) as Pinned[])
    .filter((each) => each.length > 0);
  // Its CRD defaults what is done with it at its shutdown time, which alasio gives none.
  return defaulted({
    operatingMode: oneOf("Running", "Suspended"),
    service: true,
    podTemplate: { ...podTemplate, metadata: { ...podTemplate.metadata, labels: { ...podTemplate.metadata?.labels, ...own }, ...metadataAnnotations }, spec: podSpec },
    ...(claims.length > 0 ? { volumeClaimTemplates: claims.length === 1 ? claims[0] : oneOf(...(claims as Pinned[])) } : {}),
  } as unknown as PinnedMap, { shutdownPolicy: "Retain" });
}

/** Any number. */
const ANY_NUMBER = new Check((expression) => `type(${expression}) == int || type(${expression}) == double`);

/** The tolerations the API server gives a pod of a node not ready or unreachable, for five minutes, where it has none of its own of them (DefaultTolerationSeconds). */
const DEFAULT_TOLERATIONS: readonly Pinned[] = ["node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"].map((key) => ({ key, operator: "Exists", effect: "NoExecute", tolerationSeconds: 300 }));

/**
 * What the API server gives a sandbox pool's pod, of its template, where the template has
 * not: its own defaults, its priority from its class, its ServiceAccount the namespace's
 * default, which it mounts no token of; of each container, where its termination message
 * goes, and its resources as VPA sizes them; of a probe, its timeout and thresholds; of a
 * port, its protocol; of a field reference, its API version.
 */
const POOL_POD_DEFAULTS = {
  pod: {
    restartPolicy: "Always",
    terminationGracePeriodSeconds: 30,
    schedulerName: "default-scheduler",
    serviceAccountName: "default",
    serviceAccount: "default",
    priority: ANY_NUMBER,
    preemptionPolicy: oneOf("PreemptLowerPriority", "Never"),
  },
  container: { terminationMessagePath: "/dev/termination-log", terminationMessagePolicy: "File", resources: ANY },
  probe: { timeoutSeconds: 1, successThreshold: 1, failureThreshold: 3 },
} satisfies Record<string, Record<string, Pinned>>;

/** `container` of a pool's template, as the API server stores it in a pod (POOL_POD_DEFAULTS). */
function poolContainer(container: V1Container): Pinned {
  const each = container as unknown as PinnedMap;
  const probe = each["readinessProbe"] as PinnedMap | undefined;
  return defaulted({
    ...each,
    // Its resources are VPA's, as it sizes the pod as it is made, and resizes it after (pods/resize).
    resources: ANY,
    ...(container.ports ? { ports: container.ports.map((port) => defaulted(port as unknown as PinnedMap, { protocol: "TCP" })) } : {}),
    ...(container.env
      ? {
        env: container.env.map((variable) =>
          variable.valueFrom?.fieldRef
            ? { ...variable, valueFrom: { fieldRef: defaulted(variable.valueFrom.fieldRef as unknown as PinnedMap, { apiVersion: "v1" }) } }
            : variable
        ) as unknown as Pinned[],
      }
      : {}),
    ...(probe ? { readinessProbe: defaulted(probe, POOL_POD_DEFAULTS.probe) } : {}),
  }, POOL_POD_DEFAULTS.container);
}

/** The spec of a pod of the pool template `template`, as the API server stores it: what POOL_POD_DEFAULTS adds, and its tolerations, its template's then the API server's own. */
function poolPodSpec(template: V1PodTemplateSpec): Pinned {
  const spec = template.spec ?? { containers: [] };
  const tolerations = (spec.tolerations ?? []) as unknown as Pinned[];
  return defaulted({
    ...(spec as unknown as PinnedMap),
    containers: spec.containers.map(poolContainer),
    ...(spec.initContainers ? { initContainers: spec.initContainers.map(poolContainer) } : {}),
    // The API server's of a node not ready and unreachable, in either order.
    tolerations: oneOf([...tolerations, ...DEFAULT_TOLERATIONS], [...tolerations, ...[...DEFAULT_TOLERATIONS].reverse()]),
  }, POOL_POD_DEFAULTS.pod);
}

/**
 * Where the sandbox pods' policy holds: main's sessions' namespace, by its name, and every
 * branch environment's; and who makes the pods of the session Sandboxes there are, beside the
 * pools', until they are gone: agent-sandbox's controller, by its user's name.
 */
interface SandboxPodScope {
  readonly sessionsNamespace: string;
  readonly sandboxController: string;
  /**
   * Of main's snapshotted sessions, who makes a restore's pod, the sandbox gateway, by its
   * ServiceAccount's user's name; the templates a snapshot restores onto, each pool's current
   * one and the one before it; and the RuntimeClasses of the releases it restores under.
   */
  readonly restores?: { readonly restorer: string; readonly templates: readonly V1PodTemplateSpec[]; readonly runtimeClasses: readonly string[] } | null;
}

/**
 * The image a restore's pod of main's sessions, `namespace`'s, is restored from, as the
 * gateway's node agent prepared it: of the workspace its binding's key names, in the node's
 * snapshot cache, under a snapshot's id.
 */
const restoredFromKey = (namespace: string) =>
  new Check((expression) => {
    const own = `${JSON.stringify(`${SNAPSHOT_CACHE_DIR}/${namespace}/`)} + variables.annotations[${JSON.stringify(KEY_ANNOTATION)}].substring(${"sessionfs:".length}) + "/"`;
    return `type(${expression}) == string && ${expression}.startsWith(${own}) && ${expression}.substring(size(${own})).matches(${JSON.stringify(`^${SNAPSHOT_ID}$`)})`;
  });

/** A restore's pod's own directory on its node: its own name's, under every pod's, its name a restore's pod's, as its node's agent makes a directory for no other. */
const OWN_DIRECTORY = new Check((expression) => `${expression} == ${JSON.stringify(`${ATTACH_ROOT}/`)} + object.metadata.name && object.metadata.name.matches(${JSON.stringify(RESTORE_POD.source)})`);

/**
 * A restore's pod of the pool template `template`, as the gateway makes it (src/gateway/snapshots.ts)
 * and the API server stores it: its template's, bound from the first, never free, its sandbox
 * restored paused from the image its node's agent prepared, under one of `runtimeClasses`, on that
 * node, its own directory there its volume's, and ready once resumed.
 */
function restorePod(template: V1PodTemplateSpec, namespace: string, runtimeClasses: readonly string[]): string {
  // Its own directory named by its volume's path, its own name's (ownDirectory).
  const owned = ownDirectory(template.spec ?? { containers: [] }, "");
  const volumes = (owned.volumes ?? []).map((volume) => (volume.name === ATTACH_VOLUME ? { name: ATTACH_VOLUME, hostPath: { path: OWN_DIRECTORY, type: "Directory" } } : volume));
  const restoredOn: Pinned = {
    nodeAffinity: { requiredDuringSchedulingIgnoredDuringExecution: { nodeSelectorTerms: [{ matchFields: [{ key: "metadata.name", operator: "In", values: [matching(NODE_NAME)] }] }] } },
  };
  // Its template's spec, as the API server stores a pod of it, but what a restore gives it.
  const restoring = poolPodSpec({
    ...template,
    spec: {
      ...owned,
      volumes,
      runtimeClassName: oneOf(...runtimeClasses),
      readinessGates: [...((template.spec?.readinessGates ?? []) as unknown as Pinned[]), { conditionType: RESUMED_CONDITION }],
      affinity: restoredOn,
    } as unknown as V1PodSpec,
  });
  const { [FREE_LABEL]: _free, ...own } = (template.metadata?.labels ?? {}) as PinnedMap;
  const labels = { ...own, [BINDING_LABEL]: matching("[0-9a-f]{40}") } as PinnedMap;
  const annotations = {
    ...(template.metadata?.annotations ?? {}),
    [KEY_ANNOTATION]: matching(`sessionfs:${DNS_LABEL}`),
    ...BUSY_ANNOTATIONS,
    [RESTORE_ANNOTATION]: restoredFromKey(namespace),
    [RESTORE_PAUSED_ANNOTATION]: "true",
    vpaObservedContainers: optional(ANY_STRING),
    vpaUpdates: optional(ANY_STRING),
  } as PinnedMap;
  return `(${pinned("object.spec", restoring)} && ${pinned("variables.labels", labels)} && ${pinned("variables.annotations", annotations)})`;
}

/**
 * Every pod of a namespace of sessions, main's or a branch environment's, made or changed by
 * anyone, held to what the pools' templates, `templates`, make, as Pod Security's restricted
 * level, which the namespaces keep warning and auditing of, cannot: a pool's pod mounts its
 * own directory on its node, a hostPath no level but privileged allows, which the namespaces
 * enforce, so this policy is their boundary.
 *
 * - A pod made is a pod of one of `templates`, as the API server stores it (poolPodSpec), its
 *   template's labels and annotations, its ReplicaSet's template hash beside them, and what
 *   VPA notes; or a restore's of main's sessions, of a template a snapshot restores onto
 *   (restorePod), made by the gateway alone; or, of the session Sandboxes there are until they
 *   go, one agent-sandbox's controller makes, of a Sandbox the Sandbox policy holds to its template.
 * - A pod changed keeps its spec whole (its resources change by pods/resize, which this does
 *   not match), and of its labels and annotations changes those a binding has alone (its free
 *   label taken off, its binding's put on) and those that keep its node, and of its owners lets
 *   go alone.
 * - No pod gets an ephemeral container.
 *
 * It fails closed: where it cannot say, the API server refuses.
 */
export function sandboxPodPolicy(templates: readonly V1PodTemplateSpec[], { sessionsNamespace, sandboxController, restores = null }: SandboxPodScope): KubernetesObject[] {
  const name = SANDBOX_POD_POLICY;
  const metadata = { name, labels: labels("session") };
  const made = templates.map((template) => {
    const labels = { ...(template.metadata?.labels ?? {}), "pod-template-hash": ANY_STRING } as PinnedMap;
    const annotations = { ...(template.metadata?.annotations ?? {}), vpaObservedContainers: optional(ANY_STRING), vpaUpdates: optional(ANY_STRING) } as PinnedMap;
    return `(${pinned("object.spec", poolPodSpec(template))} && ${pinned("variables.labels", labels)} && ${pinned("variables.annotations", annotations)})`;
  });
  const restored = restores
    ? [`(request.namespace == ${JSON.stringify(sessionsNamespace)} && request.userInfo.username == ${JSON.stringify(restores.restorer)} && (${restores.templates.map((template) => restorePod(template, sessionsNamespace, restores.runtimeClasses)).join(" || ")}))`]
    : [];
  const bound = [BINDING_LABEL, FREE_LABEL];
  const policy = {
    apiVersion: "admissionregistration.k8s.io/v1",
    kind: "ValidatingAdmissionPolicy",
    metadata,
    spec: {
      failurePolicy: "Fail",
      matchConstraints: {
        resourceRules: [
          { apiGroups: [""], apiVersions: ["*"], resources: ["pods"], operations: ["CREATE", "UPDATE"] },
          { apiGroups: [""], apiVersions: ["*"], resources: ["pods/ephemeralcontainers"], operations: ["CREATE", "UPDATE"] },
        ],
      },
      matchConditions: [{
        name: "of-sessions",
        expression: `request.namespace == ${JSON.stringify(sessionsNamespace)} || request.namespace.matches(${JSON.stringify(`^${literally(branchSessionsNamespace("\u0000")).replace("\u0000", DNS_LABEL)}$`)})`,
      }],
      variables: [
        { name: "labels", expression: "object.metadata.?labels.orValue({})" },
        { name: "annotations", expression: "object.metadata.?annotations.orValue({})" },
        { name: "oldLabels", expression: "request.operation == 'UPDATE' ? oldObject.metadata.?labels.orValue({}) : {}" },
        { name: "newLabels", expression: "object.metadata.?labels.orValue({})" },
        { name: "oldAnnotations", expression: "request.operation == 'UPDATE' ? oldObject.metadata.?annotations.orValue({}) : {}" },
        { name: "newAnnotations", expression: "object.metadata.?annotations.orValue({})" },
        { name: "pooled", expression: `${JSON.stringify(POOL_LABEL)} in variables.labels || ${JSON.stringify(POOL_LABEL)} in variables.oldLabels` },
      ],
      validations: [
        { expression: "request.?subResource.orValue('') != 'ephemeralcontainers'", message: "no pod of a namespace of sessions gets an ephemeral container" },
        {
          expression: `request.operation != 'CREATE' || !variables.pooled || ${[...made, ...restored].join(" || ")}`,
          message: "a pod of a sandbox pool is its pool's template, as alasio up renders it, or a restore's of one, as the sandbox gateway makes it, and nothing else",
        },
        {
          expression: `request.operation != 'CREATE' || variables.pooled || request.userInfo.username == ${JSON.stringify(sandboxController)}`,
          message: "no pod is made in a namespace of sessions but a sandbox pool's, and a session Sandbox's",
        },
        { expression: "request.operation != 'UPDATE' || object.spec == oldObject.spec", message: "no pod's spec changes in a namespace of sessions" },
        {
          expression: `request.operation != 'UPDATE' || !variables.pooled || (${unchanged("Labels", bound)})`,
          message: `no label of a sandbox pool's pod changes but ${bound.join(" and ")}, as it is bound`,
        },
        {
          expression: `request.operation != 'UPDATE' || !variables.pooled || (${unchanged("Annotations", [KEY_ANNOTATION, ...Object.keys(BUSY_ANNOTATIONS)])})`,
          message: `no annotation of a sandbox pool's pod changes but ${KEY_ANNOTATION} and what keeps its node, as it is bound`,
        },
        {
          expression: "request.operation != 'UPDATE' || object.metadata.?ownerReferences.orValue([]).all(owner, owner in oldObject.metadata.?ownerReferences.orValue([]))",
          message: "a pod of a namespace of sessions gets no owner it had not",
        },
      ],
    },
  };
  const binding = { apiVersion: "admissionregistration.k8s.io/v1", kind: "ValidatingAdmissionPolicyBinding", metadata, spec: { policyName: name, validationActions: ["Deny"] } };
  return [policy, binding];
}

/** The name of the Sandbox policy of the alasio of the namespace `namespace`. */
export const sandboxPolicyName = (namespace: string): string => `${namespace}-sandboxes`;

/**
 * What alasio may make as a Sandbox, by `account`, the ServiceAccount of the alasio of
 * `name`: a session's from `sessions`, or from `previousSessions`, the sessions' template
 * before it, which `alasio up` carries forward until the template changes again, so a
 * snapshot taken on it restores onto it whole (src/sandbox/snapshots.ts), in any namespace
 * but `host`'s; and a folder's bayma's from `host`, in its namespace. A Sandbox moved onto
 * no other pod template, as alasio suspends and resumes it, is left as it was made. Each
 * template is a variable of its own, as each is checked whole.
 */
export function sandboxPolicy(
  name: string,
  account: string,
  templates: { readonly sessions: SessionsProfile | null; readonly previousSessions: SessionsProfile | null; readonly host: HostProfile | null },
): KubernetesObject[] {
  const { sessions, previousSessions, host } = templates;
  const ofSessions = sessions
    ? [
      { name: "sessions", expression: pinned("object.spec", sandboxOf(sessions, { session: sessions, current: sessions })) },
      ...(previousSessions ? [{ name: "previousSessions", expression: pinned("object.spec", sandboxOf(previousSessions, { session: previousSessions, current: sessions })) }] : []),
    ]
    : [];
  const ofHost = host ? [{ name: "host", expression: pinned("object.spec", sandboxOf(host, { folder: host })) }] : [];
  const session = ofSessions.length > 0 ? `(${ofSessions.map((each) => `variables.${each.name}`).join(" || ")})` : "false";
  const made = host ? `(request.namespace == ${JSON.stringify(host.namespace)} ? variables.host : ${session})` : session;
  const unmoved = "request.operation == \"UPDATE\" && object.spec.podTemplate == oldObject.spec.podTemplate && " +
    "object.spec.?volumeClaimTemplates.orValue([]) == oldObject.spec.?volumeClaimTemplates.orValue([])";
  // A restore's pod is left paused, and ready only once resumed; no other pod is paused.
  const annotations = "object.spec.podTemplate.?metadata.?annotations.orValue({})";
  const restoring = `(${JSON.stringify(RESTORE_ANNOTATION)} in ${annotations})`;
  const gated = `object.spec.podTemplate.?spec.?readinessGates.orValue([]).exists(gate, gate.conditionType == ${JSON.stringify(RESUMED_CONDITION)})`;
  return admissionPolicy(name, [{ apiGroups: ["agents.x-k8s.io"], resources: ["sandboxes"], operations: ["CREATE", "UPDATE"] }], `request.userInfo.username == "${account}"`, [
    { expression: `${unmoved} || ${made}`, message: "alasio makes no Sandbox but from its workspaces' templates, as alasio up renders them" },
    {
      expression: `${restoring} == (${JSON.stringify(RESTORE_PAUSED_ANNOTATION)} in ${annotations}) && (!${restoring} || ${gated})`,
      message: "alasio restores a Sandbox from a snapshot only paused, its pod ready once resumed, and pauses none it does not restore",
    },
  ], [...ofSessions, ...ofHost]);
}

/**
 * The fields of a pod's status but its conditions that every Kubernetes alasio runs on
 * has (1.28 on), as CEL refuses a field its schema lacks: its addresses, its node's, and
 * its containers' and its own state.
 */
const POD_STATUS_FIELDS = [
  "containerStatuses",
  "ephemeralContainerStatuses",
  "hostIP",
  "hostIPs",
  "initContainerStatuses",
  "message",
  "nominatedNodeName",
  "phase",
  "podIP",
  "podIPs",
  "qosClass",
  "reason",
  "startTime",
] as const;

/**
 * What the alasio of `account` may change of a pod's status, as it lets a restored session's
 * pod be ready once its node's agent resumed its sandbox (src/sandbox/snapshots.ts): the
 * RESUMED_CONDITION of a session's pod in `namespace` that is gated on it, set true, and
 * nothing else, its addresses above all, which network policies are programmed from.
 */
export function resumedConditionPolicy(name: string, account: string, namespace: string): KubernetesObject[] {
  const resumed = JSON.stringify(RESUMED_CONDITION);
  const others = (object: string) => `${object}.status.?conditions.orValue([]).filter(condition, condition.type != ${resumed})`;
  return admissionPolicy(name, [{ apiGroups: [""], resources: ["pods/status"], operations: ["UPDATE"] }], `request.userInfo.username == "${account}"`, [
    {
      expression: `request.namespace == ${JSON.stringify(namespace)} && ${JSON.stringify(WORKLOAD_LABEL)} in object.metadata.?labels.orValue({}) && ` +
        `object.metadata.labels[${JSON.stringify(WORKLOAD_LABEL)}] == "session" && ` +
        `object.spec.?readinessGates.orValue([]).exists(gate, gate.conditionType == ${resumed})`,
      message: "alasio sets the status of no pod but a restored session's",
    },
    {
      expression: `${others("object")} == ${others("oldObject")} && ` +
        `object.status.?conditions.orValue([]).all(condition, condition.type != ${resumed} || condition.status == "True")`,
      message: `alasio sets no condition of a pod's but ${RESUMED_CONDITION}, and that true`,
    },
    {
      expression: POD_STATUS_FIELDS.map((field) =>
        `(has(object.status.${field}) ? has(oldObject.status.${field}) && object.status.${field} == oldObject.status.${field} : !has(oldObject.status.${field}))`
      ).join(" && ") +
        " && object.metadata.?labels.orValue({}) == oldObject.metadata.?labels.orValue({}) && object.metadata.?annotations.orValue({}) == oldObject.metadata.?annotations.orValue({})",
      message: "alasio changes nothing of a pod's status but its conditions, nor its labels or annotations",
    },
  ]);
}

/** What alasio may make as a Secret: a Sandbox's token, of its one key, as tokenSecretManifest makes it. Main's alasio's and each branch environment's. */
export function secretPolicy(): KubernetesObject[] {
  return admissionPolicy(componentName("secrets"), [{ apiGroups: [""], resources: ["secrets"], operations: ["CREATE", "UPDATE"] }], BY_ANY_ALASIO, [{
    expression: `object.?type.orValue("Opaque") == "Opaque" && object.metadata.name.endsWith(${JSON.stringify(tokenSecretName(""))}) && ` +
      `object.?data.orValue({}).all(key, key == ${JSON.stringify(TOKEN_KEY)})`,
    message: "alasio makes no Secret but a Sandbox's token",
  }]);
}
