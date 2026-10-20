/**
 * What alasio's components may do beyond what RBAC can say: ValidatingAdmissionPolicies of
 * the cluster's, each matching what one of their ServiceAccounts asks, and holding it to
 * what alasio up renders:
 *
 * - a pod made in the folder namespace is a folder instance's, which the sandbox gateway
 *   alone makes, of the folder template, but for what the gateway adds to each
 *   (src/gateway/names.ts folderPodSpec), and a Secret the gateway makes there is an
 *   instance's token: RBAC lets it make pods of any spec, as any ServiceAccount, under any
 *   runtime, and Secrets of any type, a ServiceAccount's token among them;
 * - a pod alasio patches changes its busy label and annotations alone (src/kube/busy.ts),
 *   and in alasio's own namespace is alasio's own: RBAC lets it patch its namespaces'
 *   pods whole, their images among it;
 * - a compute's Deployment neon-control patches changes its pod template's restartedAt
 *   annotation alone, which restarts it (neon/control/computes.ts): RBAC lets it patch the
 *   Deployment whole, its image and command among it;
 * - a pod neon-control patches is a compute's, and changes its serving label alone, which
 *   takes it off its Service as a sleep stops it: RBAC lets it patch any pod of a
 *   compute's namespace whole.
 *
 * Where a policy holds an object to a rendered one, `pinned` makes the CEL that says so:
 * the object's fields are the rendered ones', key for key and item for item, but for what
 * a Check allows, and what the API server defaults where the rendered one has nothing.
 */
import { posix } from "node:path";

import type { KubernetesObject, V1Container, V1PodSpec, V1PodTemplateSpec, V1ResourceRequirements } from "@kubernetes/client-node";

import { SERVING_LABEL } from "../../../neon/control/endpoints.ts";
import { BINDING_LABEL, FREE_LABEL, KEY_ANNOTATION, POOL_LABEL } from "../../../src/binder/names.ts";
import { branchNamespace, branchSessionsNamespace } from "../../../src/branch/names.ts";
import { ATTACH_VOLUME, FOLDER_KEY_PREFIX, FOLDER_POOL, FOLDER_POD, folderPodSpec, folderTokenSecret, ownDirectory, SANDBOX_POD_POLICY } from "../../../src/gateway/names.ts";
import { BUSY_ANNOTATIONS, BUSY_LABEL } from "../../../src/kube/busy-label.ts";
import { RESTORE_ANNOTATION, RESTORE_PAUSED_ANNOTATION, RESUMED_CONDITION, SNAPSHOT_CACHE_DIR, SNAPSHOT_ID } from "../../../src/kube/sandbox-shape.ts";
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
/** A string all of which `pattern` matches. */
const matching = (pattern: string) => new Check((expression) => `type(${expression}) == string && ${expression}.matches(${JSON.stringify(`^(${pattern})$`)})`);
/** What the CEL `value` is, of the object asked about. */
const sameAs = (value: string) => new Check((expression) => `${expression} == ${value}`);
/** A quantity, as the API server writes one, of the amount `value` is. */
const quantityOf = (value: string) => new Check((expression) => `isQuantity(${expression}) && quantity(${expression}).compareTo(quantity(${JSON.stringify(value)})) == 0`);
/** A quantity, as the API server writes one, of at least `least`'s amount, and at most `most`'s where given. */
const quantityWithin = (least: string, most: string | undefined) =>
  new Check((expression) =>
    [`isQuantity(${expression})`, `quantity(${expression}).compareTo(quantity(${JSON.stringify(least)})) >= 0`, ...(most ? [`quantity(${expression}).compareTo(quantity(${JSON.stringify(most)})) <= 0`] : [])].join(" && ")
  );
/** Any of `alternatives`. */
const oneOf = (...alternatives: Pinned[]) => new Check((expression, depth) => `(${alternatives.map((each) => pinned(expression, each, depth)).join(" || ")})`);
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

/** The components of the computes' pods: a primary's, and a read replica's. */
const COMPUTE_COMPONENTS = ["neon-compute", "neon-compute-ro"];

/**
 * What neon-control may change of a pod, which RBAC lets it patch whole, of any in a
 * compute's namespace (./neon.ts's computeScaling): of a compute's pod, its serving label
 * (neon/control/endpoints.ts's SERVING_LABEL), which takes it off its Service as a sleep
 * stops it, or gives it back, and nothing else.
 */
export function computeServingPolicy(): KubernetesObject[] {
  const component = "app.kubernetes.io/component";
  const account = `system:serviceaccount:${NAMESPACE}:${neonName("control")}`;
  return admissionPolicy(componentName("compute-serving"), [{ apiGroups: [""], resources: ["pods"], operations: ["UPDATE"] }], `request.userInfo.username == "${account}"`, [
    { expression: "object.spec == oldObject.spec", message: "neon-control changes no pod's spec" },
    {
      expression: `"${component}" in variables.oldLabels && variables.oldLabels["${component}"] in ${JSON.stringify(COMPUTE_COMPONENTS)}`,
      message: "neon-control labels no pod but a compute's",
    },
    {
      expression: "object.metadata.?annotations.orValue({}) == oldObject.metadata.?annotations.orValue({}) && " +
        "object.metadata.?finalizers.orValue([]) == oldObject.metadata.?finalizers.orValue([]) && " +
        "object.metadata.?ownerReferences.orValue([]) == oldObject.metadata.?ownerReferences.orValue([])",
      message: "neon-control changes no pod's annotations, finalizers or owners",
    },
    { expression: unchanged("Labels", [SERVING_LABEL]), message: `neon-control changes no label of a pod's but ${SERVING_LABEL}` },
    {
      expression: `!("${SERVING_LABEL}" in variables.newLabels) || variables.newLabels["${SERVING_LABEL}"] == "true"`,
      message: `neon-control sets ${SERVING_LABEL} to true alone, or takes it off`,
    },
  ], [
    { name: "oldLabels", expression: "oldObject.metadata.?labels.orValue({})" },
    { name: "newLabels", expression: "object.metadata.?labels.orValue({})" },
  ]);
}

/** A path segment as sanitizePathToken makes one. */
const PATH_TOKEN = "[A-Za-z0-9_][A-Za-z0-9_.-]*";

/** `text`, as a pattern that matches it alone. */
const literally = (text: string) => text.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");

/** A node's name: a DNS subdomain. */
const NODE_NAME = "[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?";

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

/**
 * `container` of a pool's template, as the API server stores it in a pod (POOL_POD_DEFAULTS):
 * its resources any, as VPA sizes a pool's pod as it is made, and resizes it after
 * (pods/resize).
 */
const poolContainer = (container: V1Container): Pinned => templateContainer(container, ANY);

/** `container` of a template, as the API server stores it in a pod (POOL_POD_DEFAULTS), its resources as `resources` says. */
function templateContainer(container: V1Container, resources: Pinned): Pinned {
  const each = container as unknown as PinnedMap;
  const probe = each["readinessProbe"] as PinnedMap | undefined;
  return defaulted({
    ...each,
    resources,
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

/** Where the sandbox pods' policy holds: main's sessions' namespace, by its name, and every branch environment's. */
interface SandboxPodScope {
  readonly sessionsNamespace: string;
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

/** A restore's pod's own directory on its node: its namespace's and its own name's, under every pod's, its name a restore's pod's, as its node's agent makes a directory for no other. */
const OWN_DIRECTORY = new Check((expression) => `${expression} == ${JSON.stringify(`${ATTACH_ROOT}/`)} + object.metadata.namespace + "/" + object.metadata.name && object.metadata.name.matches(${JSON.stringify(RESTORE_POD.source)})`);

/**
 * A restore's pod of the pool template `template`, as the gateway makes it (src/gateway/snapshots.ts)
 * and the API server stores it: its template's, bound from the first, never free, its sandbox
 * restored paused from the image its node's agent prepared, under one of `runtimeClasses`, on that
 * node, its own directory there its volume's, ready once resumed, and never started again.
 */
function restorePod(template: V1PodTemplateSpec, namespace: string, runtimeClasses: readonly string[]): string {
  // Its own directory named by its volume's path, its own name's (ownDirectory).
  const owned = ownDirectory(template.spec ?? { containers: [] }, "", "");
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
      restartPolicy: "Never",
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
 *   (restorePod), made by the gateway alone; and no other pod is made there.
 * - A pod changed keeps its spec whole (its resources change by pods/resize, which this does
 *   not match), and of its labels and annotations changes those a binding has alone (its free
 *   label taken off, its binding's put on) and those that keep its node, and of its owners lets
 *   go alone.
 * - No pod gets an ephemeral container.
 *
 * It fails closed: where it cannot say, the API server refuses.
 */
export function sandboxPodPolicy(templates: readonly V1PodTemplateSpec[], { sessionsNamespace, restores = null }: SandboxPodScope): KubernetesObject[] {
  const name = SANDBOX_POD_POLICY;
  const metadata = { name, labels: labels("session") };
  // Each template's pin a variable of its own, as CEL holds each expression to 100,000 code points, which all of them together pass.
  const made = templates.map((template, index) => {
    const labels = { ...(template.metadata?.labels ?? {}), "pod-template-hash": ANY_STRING } as PinnedMap;
    const annotations = { ...(template.metadata?.annotations ?? {}), vpaObservedContainers: optional(ANY_STRING), vpaUpdates: optional(ANY_STRING) } as PinnedMap;
    return { name: `made${index}`, expression: `${pinned("object.spec", poolPodSpec(template))} && ${pinned("variables.labels", labels)} && ${pinned("variables.annotations", annotations)}` };
  });
  const restoring = (restores?.templates ?? []).map((template, index) => ({ name: `restored${index}`, expression: restorePod(template, sessionsNamespace, restores?.runtimeClasses ?? []) }));
  const restored = restores
    ? [`(request.namespace == ${JSON.stringify(sessionsNamespace)} && request.userInfo.username == ${JSON.stringify(restores.restorer)} && (${restoring.map(({ name }) => `variables.${name}`).join(" || ")}))`]
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
        ...made,
        ...restoring,
      ],
      validations: [
        { expression: "request.?subResource.orValue('') != 'ephemeralcontainers'", message: "no pod of a namespace of sessions gets an ephemeral container" },
        {
          expression: `request.operation != 'CREATE' || !variables.pooled || ${[...made.map(({ name }) => `variables.${name}`), ...restored].join(" || ")}`,
          message: "a pod of a sandbox pool is its pool's template, as alasio up renders it, or a restore's of one, as the sandbox gateway makes it, and nothing else",
        },
        { expression: "request.operation != 'CREATE' || variables.pooled", message: "no pod is made in a namespace of sessions but a sandbox pool's" },
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

/** The folder pods' policy, which holds every pod of the folder namespace to the folder template (folderPodPolicy). */
export const FOLDER_POD_POLICY = "alasio-folder-pods";

/** What stands for a folder instance's pod's own name, and its state directory, in its spec as folderPodSpec makes it, which the policy pins by what they must be. */
const POD_NAME = "\u0000pod";
const STATE_DIR = "\u0000state-dir";

/**
 * The spec `spec` of a folder instance's pod, as the API server stores it: as a pool's pod's
 * (poolPodSpec), and what the API server gives a pod that the folder template, unlike the
 * pools', leaves out: its DNS policy, its Service links, and each of the machine's paths'
 * type where its mount names none.
 */
function folderPodPinned(spec: V1PodSpec): Pinned {
  const { map, defaults } = folderPodParts(spec);
  return defaulted(map, defaults);
}

/**
 * A folder instance's container's `resources`, its template's: its limits theirs, its CPU
 * request its, and its memory request, where it has one, the gateway's, as it sizes each pod
 * of its folder's instances' use (src/gateway/folders.ts SIZING), between its template's and
 * its limit.
 */
function folderResources(resources: V1ResourceRequirements | undefined): Pinned {
  const quantities = (of: Readonly<Record<string, string>> | undefined) => Object.fromEntries(Object.entries(of ?? {}).map(([name, value]) => [name, quantityOf(value)]));
  const memory = resources?.requests?.["memory"];
  return {
    ...(resources?.limits ? { limits: quantities(resources.limits) } : {}),
    ...(resources?.requests ? { requests: { ...quantities(resources.requests), ...(memory ? { memory: quantityWithin(memory, resources.limits?.["memory"]) } : {}) } } : {}),
  };
}

/** folderPodPinned's map and the defaults it is pinned with. */
export function folderPodParts(spec: V1PodSpec): { readonly map: PinnedMap; readonly defaults: Readonly<Record<string, Pinned>> } {
  const tolerations = (spec.tolerations ?? []) as unknown as Pinned[];
  const volumes = (spec.volumes ?? []).map((volume) =>
    volume.hostPath && volume.hostPath.type === undefined ? { ...(volume as unknown as PinnedMap), hostPath: defaulted(volume.hostPath as unknown as PinnedMap, { type: "" }) } : volume
  ) as unknown as Pinned[];
  return {
    map: {
      ...(spec as unknown as PinnedMap),
      containers: spec.containers.map((container) => templateContainer(container, folderResources(container.resources))),
      ...(spec.volumes ? { volumes } : {}),
      tolerations: oneOf([...tolerations, ...DEFAULT_TOLERATIONS], [...tolerations, ...[...DEFAULT_TOLERATIONS].reverse()]),
    },
    defaults: {
      ...POOL_POD_DEFAULTS.pod,
      // The account the template names, as the API server copies it to the field it deprecated.
      ...(spec.serviceAccountName ? { serviceAccountName: spec.serviceAccountName, serviceAccount: spec.serviceAccountName } : {}),
      dnsPolicy: "ClusterFirst",
      enableServiceLinks: true,
    },
  };
}

/** `value`, each string of it `stands` names the Check of, as that Check. */
function standingFor(value: Pinned, stands: Readonly<Record<string, Check>>): Pinned {
  if (typeof value === "string") return stands[value] ?? value;
  if (value instanceof Check || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map((item: Pinned) => standingFor(item, stands));
  return Object.fromEntries(Object.entries(value as PinnedMap).map(([key, item]) => [key, item === undefined ? undefined : standingFor(item, stands)]));
}

/**
 * Every pod of the folder namespace, `namespace`, made or changed by anyone, held to what the
 * folder template, `template`, makes (src/gateway/folders.ts): a folder instance's pod is the
 * operator's, of the machine's own mounts and the capabilities bayma's process snapshots need,
 * which no Pod Security level but privileged allows, as the namespace enforces, so this policy
 * is its boundary.
 *
 * - A pod made is made by `maker`, the gateway, alone, and is the template's, as the API server
 *   stores it (poolPodSpec), with what folderPodSpec adds, alone: its token's Secret, named for
 *   it, and its state directory under `stateRoot`, of a harness's and a conversation's names;
 *   its bayma's memory request the gateway's, between its template's and its limit
 *   (folderResources); bound from the first, to a folder instance's key, its node kept; and
 *   named as one.
 * - A pod changed keeps its spec, its labels, and its owners, and of its annotations changes
 *   those that keep its node alone.
 * - No pod gets an ephemeral container.
 *
 * It fails closed: where it cannot say, the API server refuses.
 */
export function folderPodPolicy(template: V1PodTemplateSpec, { namespace, maker, stateRoot }: { readonly namespace: string; readonly maker: string; readonly stateRoot: string }): KubernetesObject[] {
  const metadata = { name: FOLDER_POD_POLICY, labels: labels("folder-bayma") };
  // The spec as folderPodSpec makes it, its own name's and state directory's places standing for what they must be, then pinned.
  const spec = folderPodPinned(standingFor(folderPodSpec(template.spec ?? { containers: [] }, POD_NAME, STATE_DIR) as unknown as Pinned, {
    [folderTokenSecret(POD_NAME)]: sameAs(`object.metadata.name + ${JSON.stringify(folderTokenSecret(""))}`),
    [STATE_DIR]: matching(`${literally(posix.join(stateRoot, "/"))}${PATH_TOKEN}/${PATH_TOKEN}`),
  }) as unknown as V1PodSpec);
  const podLabels = { ...(template.metadata?.labels ?? {}), [POOL_LABEL]: FOLDER_POOL, [BINDING_LABEL]: matching("[0-9a-f]{40}") } as PinnedMap;
  const podAnnotations = {
    ...(template.metadata?.annotations ?? {}),
    [KEY_ANNOTATION]: matching(`${literally(FOLDER_KEY_PREFIX)}[a-z][a-z0-9-]{0,30}:.{1,512}`),
    ...BUSY_ANNOTATIONS,
    vpaObservedContainers: optional(ANY_STRING),
    vpaUpdates: optional(ANY_STRING),
  } as PinnedMap;
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
      matchConditions: [{ name: "of-folders", expression: `request.namespace == ${JSON.stringify(namespace)}` }],
      variables: [
        { name: "oldAnnotations", expression: "request.operation == 'UPDATE' ? oldObject.metadata.?annotations.orValue({}) : {}" },
        { name: "newAnnotations", expression: "object.metadata.?annotations.orValue({})" },
        // Each pin a variable of its own, as CEL holds each expression to 100,000 code points.
        { name: "folderSpec", expression: pinned("object.spec", spec) },
        { name: "folderLabels", expression: pinned("object.metadata.?labels.orValue({})", podLabels) },
        { name: "folderAnnotations", expression: pinned("object.metadata.?annotations.orValue({})", podAnnotations) },
      ],
      validations: [
        { expression: "request.?subResource.orValue('') != 'ephemeralcontainers'", message: "no pod of the folder namespace gets an ephemeral container" },
        { expression: `request.operation != 'CREATE' || request.userInfo.username == ${JSON.stringify(maker)}`, message: "no pod is made in the folder namespace but by the sandbox gateway" },
        { expression: `request.operation != 'CREATE' || object.metadata.name.matches(${JSON.stringify(FOLDER_POD.source)})`, message: "a pod made in the folder namespace is named as a folder instance's" },
        { expression: "request.operation != 'CREATE' || variables.folderSpec", message: "a pod made in the folder namespace is a folder instance's, of the folder template, as alasio up renders it" },
        { expression: "request.operation != 'CREATE' || variables.folderLabels", message: "a pod made in the folder namespace is labelled as a folder instance bound" },
        { expression: "request.operation != 'CREATE' || variables.folderAnnotations", message: "a pod made in the folder namespace is annotated with a folder instance's key, its node kept" },
        { expression: "request.operation != 'UPDATE' || object.spec == oldObject.spec", message: "no pod's spec changes in the folder namespace" },
        { expression: "request.operation != 'UPDATE' || object.metadata.?labels.orValue({}) == oldObject.metadata.?labels.orValue({})", message: "no pod's labels change in the folder namespace" },
        {
          expression: `request.operation != 'UPDATE' || (${unchanged("Annotations", Object.keys(BUSY_ANNOTATIONS))})`,
          message: "no annotation of a folder instance's pod changes but what keeps its node",
        },
        {
          expression: "request.operation != 'UPDATE' || object.metadata.?ownerReferences.orValue([]).all(owner, owner in oldObject.metadata.?ownerReferences.orValue([]))",
          message: "a pod of the folder namespace gets no owner it had not",
        },
      ],
    },
  };
  const binding = { apiVersion: "admissionregistration.k8s.io/v1", kind: "ValidatingAdmissionPolicyBinding", metadata, spec: { policyName: FOLDER_POD_POLICY, validationActions: ["Deny"] } };
  return [policy, binding];
}

/** What the sandbox gateway, `maker`, may make as a Secret in the folder namespace, `namespace`: a folder instance's token, of its one key, named for its pod. */
export function folderSecretPolicy({ namespace, maker }: { readonly namespace: string; readonly maker: string }): KubernetesObject[] {
  return admissionPolicy(componentName("folder-secrets"), [{ apiGroups: [""], resources: ["secrets"], operations: ["CREATE", "UPDATE"] }], `request.userInfo.username == ${JSON.stringify(maker)}`, [{
    expression: `request.namespace == ${JSON.stringify(namespace)} && object.?type.orValue("Opaque") == "Opaque" && ` +
      `object.metadata.name.matches(${JSON.stringify(`^${FOLDER_POD.source.slice(1, -1)}${literally(folderTokenSecret(""))}$`)}) && ` +
      `object.?data.orValue({}).all(key, key == "token")`,
    message: "the sandbox gateway makes no Secret but a folder instance's token, in the folder namespace",
  }]);
}

/**
 * What the sandbox gateway, `maker`, may delete in the folder namespace, `namespace`, as RBAC
 * lets it delete any pod and Secret there: a folder instance's pod, of the folder pool, and a
 * folder instance's token's Secret, alone.
 */
export function folderDeletionPolicy({ namespace, maker }: { readonly namespace: string; readonly maker: string }): KubernetesObject[] {
  const pool = `oldObject.metadata.?labels.orValue({})`;
  return admissionPolicy(componentName("folder-deletions"), [{ apiGroups: [""], resources: ["pods", "secrets"], operations: ["DELETE"] }], `request.userInfo.username == ${JSON.stringify(maker)} && request.namespace == ${JSON.stringify(namespace)}`, [
    {
      expression: `request.resource.resource != "pods" || (oldObject.metadata.name.matches(${JSON.stringify(FOLDER_POD.source)}) && ${JSON.stringify(POOL_LABEL)} in ${pool} && ${pool}[${JSON.stringify(POOL_LABEL)}] == ${JSON.stringify(FOLDER_POOL)})`,
      message: "the sandbox gateway deletes no pod in the folder namespace but a folder instance's",
    },
    {
      expression: `request.resource.resource != "secrets" || oldObject.metadata.name.matches(${JSON.stringify(`^${FOLDER_POD.source.slice(1, -1)}${literally(folderTokenSecret(""))}$`)})`,
      message: "the sandbox gateway deletes no Secret in the folder namespace but a folder instance's token",
    },
  ]);
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
 * What `account`, the sandbox gateway's, may change of a pod's status, as it lets a restore's
 * pod be ready once its node's agent resumed its sandbox (src/gateway/snapshots.ts): the
 * RESUMED_CONDITION of a pool's pod in `namespace` that is gated on it, set true, and nothing
 * else, its addresses above all, which network policies are programmed from. RBAC lets it
 * patch the status of any pod of the namespace, whole.
 */
export function resumedConditionPolicy(name: string, account: string, namespace: string): KubernetesObject[] {
  const resumed = JSON.stringify(RESUMED_CONDITION);
  const others = (object: string) => `${object}.status.?conditions.orValue([]).filter(condition, condition.type != ${resumed})`;
  return admissionPolicy(name, [{ apiGroups: [""], resources: ["pods/status"], operations: ["UPDATE"] }], `request.userInfo.username == "${account}"`, [
    {
      expression: `request.namespace == ${JSON.stringify(namespace)} && ${JSON.stringify(POOL_LABEL)} in object.metadata.?labels.orValue({}) && ` +
        `object.spec.?readinessGates.orValue([]).exists(gate, gate.conditionType == ${resumed})`,
      message: "the sandbox gateway sets the status of no pod but a restore's of its pools'",
    },
    {
      expression: `${others("object")} == ${others("oldObject")} && ` +
        `object.status.?conditions.orValue([]).all(condition, condition.type != ${resumed} || condition.status == "True")`,
      message: `the sandbox gateway sets no condition of a pod's but ${RESUMED_CONDITION}, and that true`,
    },
    {
      expression: POD_STATUS_FIELDS.map((field) =>
        `(has(object.status.${field}) ? has(oldObject.status.${field}) && object.status.${field} == oldObject.status.${field} : !has(oldObject.status.${field}))`
      ).join(" && ") +
        " && object.metadata.?labels.orValue({}) == oldObject.metadata.?labels.orValue({}) && object.metadata.?annotations.orValue({}) == oldObject.metadata.?annotations.orValue({})",
      message: "the sandbox gateway changes nothing of a pod's status but its conditions, nor its labels or annotations",
    },
  ]);
}
