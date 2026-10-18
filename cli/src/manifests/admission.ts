/**
 * What alasio may do beyond what RBAC can say: ValidatingAdmissionPolicies of the
 * cluster's, each matching what alasio's ServiceAccount asks (main's, or a branch
 * environment's), and holding it to what alasio up renders:
 *
 * - a Sandbox alasio makes, or moves onto another pod template, is a folder's bayma's, made
 *   from the template of its namespace's, but for what alasio adds to each (src/kube/
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
import type { HostProfile } from "../../../src/kube/config.ts";
import {
  BAYMA_CONTAINER,
  HARNESS_LABEL,
  MANAGED_BY,
  RESTORE_ANNOTATION,
  RESTORE_PAUSED_ANNOTATION,
  RESUMED_CONDITION,
  SANDBOX_LABEL,
  SNAPSHOT_CACHE_DIR,
  SNAPSHOT_ID,
  STATE_DIR_ARG,
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

/**
 * The Sandbox of a folder's bayma alasio makes from `host`, its profile, with its state
 * root, as sandboxManifest and hostBaymaManifest make it: its template's, its token
 * mounted and its state directory under the state root, labelled its harness's.
 */
function sandboxOf(host: HostProfile): Pinned {
  const podTemplate: V1PodTemplateSpec = host.podTemplate as V1PodTemplateSpec;
  const spec: V1PodSpec = podTemplate.spec ?? { containers: [] };
  const tokenMount = { name: TOKEN_VOLUME, mountPath: TOKEN_DIR, readOnly: true };
  // Under the state root as hostBaymaStateDir joins it: normalized, its one slash after it.
  const stateDir = [STATE_DIR_ARG, matching(`${literally(posix.join(host.stateRoot, "/"))}${PATH_TOKEN}/${PATH_TOKEN}`)];
  const container = (each: V1Container): Pinned =>
    stored(
      (each.name !== BAYMA_CONTAINER ? each : {
        ...each,
        args: [...(each.args ?? []), ...TOKEN_FILE_ARGS, ...stateDir],
        volumeMounts: [...(each.volumeMounts ?? []), tokenMount],
        env: optional(startingWith((each.env ?? []) as unknown as Pinned[], SET)),
      }) as unknown as PinnedMap,
    );
  const initContainers = ((spec.initContainers ?? []) as unknown as PinnedMap[]).map(stored);
  const volumes: Pinned[] = [...((spec.volumes ?? []) as V1Volume[] as unknown as Pinned[]), { name: TOKEN_VOLUME, secret: { secretName: OWN_TOKEN, defaultMode: TOKEN_MODE } }];
  const own = { ...MANAGED_BY, [SANDBOX_LABEL]: sameAs("object.metadata.name"), [WORKLOAD_LABEL]: "folder", [HARNESS_LABEL]: matching(PATH_TOKEN) };
  const podSpec = { ...spec, containers: spec.containers.map(container), ...(initContainers.length > 0 ? { initContainers } : {}), volumes };
  // Its CRD defaults what is done with it at its shutdown time, which alasio gives none.
  return defaulted({
    operatingMode: oneOf("Running", "Suspended"),
    service: true,
    podTemplate: { ...podTemplate, metadata: { ...podTemplate.metadata, labels: { ...podTemplate.metadata?.labels, ...own } }, spec: podSpec },
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

/** A restore's pod's own directory on its node: its own name's, under every pod's, its name a restore's pod's, as its node's agent makes a directory for no other. */
const OWN_DIRECTORY = new Check((expression) => `${expression} == ${JSON.stringify(`${ATTACH_ROOT}/`)} + object.metadata.name && object.metadata.name.matches(${JSON.stringify(RESTORE_POD.source)})`);

/**
 * A restore's pod of the pool template `template`, as the gateway makes it (src/gateway/snapshots.ts)
 * and the API server stores it: its template's, bound from the first, never free, its sandbox
 * restored paused from the image its node's agent prepared, under one of `runtimeClasses`, on that
 * node, its own directory there its volume's, ready once resumed, and never started again.
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

/** The name of the Sandbox policy of the alasio of the namespace `namespace`. */
export const sandboxPolicyName = (namespace: string): string => `${namespace}-sandboxes`;

/**
 * What alasio may make as a Sandbox, by `account`, the ServiceAccount of the alasio of
 * `name`: a folder's bayma's from `host`, in its namespace, and nothing where there is no
 * host profile. A Sandbox moved onto no other pod template, as alasio suspends and resumes
 * it, is left as it was made.
 */
export function sandboxPolicy(name: string, account: string, host: HostProfile | null): KubernetesObject[] {
  const made = host ? `(request.namespace == ${JSON.stringify(host.namespace)} && variables.host)` : "false";
  const unmoved = "request.operation == \"UPDATE\" && object.spec.podTemplate == oldObject.spec.podTemplate && " +
    "object.spec.?volumeClaimTemplates.orValue([]) == oldObject.spec.?volumeClaimTemplates.orValue([])";
  return admissionPolicy(name, [{ apiGroups: ["agents.x-k8s.io"], resources: ["sandboxes"], operations: ["CREATE", "UPDATE"] }], `request.userInfo.username == "${account}"`, [
    { expression: `${unmoved} || ${made}`, message: "alasio makes no Sandbox but from its folder workspaces' template, as alasio up renders it" },
  ], host ? [{ name: "host", expression: pinned("object.spec", sandboxOf(host)) }] : []);
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

/** What alasio may make as a Secret: a Sandbox's token, of its one key, as tokenSecretManifest makes it. Main's alasio's and each branch environment's. */
export function secretPolicy(): KubernetesObject[] {
  return admissionPolicy(componentName("secrets"), [{ apiGroups: [""], resources: ["secrets"], operations: ["CREATE", "UPDATE"] }], BY_ANY_ALASIO, [{
    expression: `object.?type.orValue("Opaque") == "Opaque" && object.metadata.name.endsWith(${JSON.stringify(tokenSecretName(""))}) && ` +
      `object.?data.orValue({}).all(key, key == ${JSON.stringify(TOKEN_KEY)})`,
    message: "alasio makes no Secret but a Sandbox's token",
  }]);
}
