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
 *   ServiceAccount's token among them.
 *
 * Where a policy holds an object to a rendered one, `pinned` makes the CEL that says so:
 * the object's fields are the rendered ones', key for key and item for item, but for what
 * a Check allows, and what the API server defaults where the rendered one has nothing.
 */
import { posix } from "node:path";

import type { KubernetesObject, V1Container, V1PodSpec, V1PodTemplateSpec, V1Volume } from "@kubernetes/client-node";

import { branchNamespace } from "../../../src/branch/names.ts";
import { CLONE_VARIABLES, type HostProfile, type SessionsProfile } from "../../../src/kube/config.ts";
import {
  BAYMA_CONTAINER,
  DEFAULT_FULL_MODE_NAMESERVERS,
  EGRESS_GATE,
  EGRESS_GATE_SCRIPT,
  HARNESS_LABEL,
  MANAGED_BY,
  NET_MODE_LABEL,
  SANDBOX_LABEL,
  STATE_DIR_ARG,
  TOKEN_DIR,
  TOKEN_ENV,
  TOKEN_FILE_ARGS,
  TOKEN_KEY,
  TOKEN_MODE,
  TOKEN_VOLUME,
  tokenSecretName,
  WORKLOAD_LABEL,
} from "../../../src/kube/sandbox-shape.ts";
import { componentName, labels, NAMESPACE, RELEASE } from "./common.ts";

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
      validations,
    },
  };
  const binding = { apiVersion: "admissionregistration.k8s.io/v1", kind: "ValidatingAdmissionPolicyBinding", metadata, spec: { policyName: name, validationActions: ["Deny"] } };
  return [policy, binding];
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

/**
 * The Sandbox of a workspace alasio makes from `template`: a session's (`session`, with
 * the session's profile) or a folder's bayma's (`folder`, with its state root), as
 * sandboxManifest and sessionSandboxManifest or hostBaymaManifest make it.
 */
function sandboxOf(template: SessionsProfile | HostProfile, workspace: { readonly session: SessionsProfile } | { readonly folder: HostProfile }): Pinned {
  const podTemplate: V1PodTemplateSpec = template.podTemplate as V1PodTemplateSpec;
  const spec: V1PodSpec = podTemplate.spec ?? { containers: [] };
  const session = "session" in workspace ? workspace.session : null;
  const bayma = spec.containers.find(({ name }) => name === BAYMA_CONTAINER);
  const tokenMount = { name: TOKEN_VOLUME, mountPath: TOKEN_DIR, readOnly: true };
  const variables: Pinned = session
    ? oneOf(SET, { name: TOKEN_ENV, valueFrom: { secretKeyRef: { name: OWN_TOKEN, key: TOKEN_KEY } } })
    : SET;
  // Under the state root as hostBaymaStateDir joins it: normalized, its one slash after it.
  const stateDir = "folder" in workspace ? [STATE_DIR_ARG, matching(`${literally(posix.join(workspace.folder.stateRoot, "/"))}${PATH_TOKEN}/${PATH_TOKEN}`)] : [];
  const container = (each: V1Container): Pinned =>
    stored(
      (each.name !== BAYMA_CONTAINER ? each : {
        ...each,
        args: [...(each.args ?? []), ...TOKEN_FILE_ARGS, ...stateDir],
        volumeMounts: [...(each.volumeMounts ?? []), tokenMount],
        env: optional(startingWith((each.env ?? []) as unknown as Pinned[], variables)),
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
  };
  // Its CRD defaults what is done with it at its shutdown time, which alasio gives none.
  return defaulted({
    operatingMode: oneOf("Running", "Suspended"),
    service: true,
    podTemplate: { ...podTemplate, metadata: { ...podTemplate.metadata, labels: { ...podTemplate.metadata?.labels, ...own } }, spec: podSpec },
    ...(session?.volumeClaimTemplates?.length ? { volumeClaimTemplates: session.volumeClaimTemplates } : {}),
  } as unknown as PinnedMap, { shutdownPolicy: "Retain" });
}

/** The name of the Sandbox policy of the alasio of the namespace `namespace`. */
export const sandboxPolicyName = (namespace: string): string => `${namespace}-sandboxes`;

/**
 * What alasio may make as a Sandbox, by `account`, the ServiceAccount of the alasio of
 * `name`: a session's from `sessions`, in any namespace but `host`'s, and a folder's bayma's
 * from `host`, in its namespace. A Sandbox moved onto no other pod template, as alasio
 * suspends and resumes it, is left as it was made.
 */
export function sandboxPolicy(
  name: string,
  account: string,
  templates: { readonly sessions: SessionsProfile | null; readonly host: HostProfile | null },
): KubernetesObject[] {
  const { sessions, host } = templates;
  const ofSessions = sessions ? pinned("object.spec", sandboxOf(sessions, { session: sessions })) : "false";
  const made = host ? `(request.namespace == ${JSON.stringify(host.namespace)} ? ${pinned("object.spec", sandboxOf(host, { folder: host }))} : ${ofSessions})` : ofSessions;
  const unmoved = "request.operation == \"UPDATE\" && object.spec.podTemplate == oldObject.spec.podTemplate && " +
    "object.spec.?volumeClaimTemplates.orValue([]) == oldObject.spec.?volumeClaimTemplates.orValue([])";
  return admissionPolicy(name, [{ apiGroups: ["agents.x-k8s.io"], resources: ["sandboxes"], operations: ["CREATE", "UPDATE"] }], `request.userInfo.username == "${account}"`, [{
    expression: `${unmoved} || ${made}`,
    message: "alasio makes no Sandbox but from its workspaces' templates, as alasio up renders them",
  }]);
}

/** What alasio may make as a Secret: a Sandbox's token, of its one key, as tokenSecretManifest makes it. Main's alasio's and each branch environment's. */
export function secretPolicy(): KubernetesObject[] {
  return admissionPolicy(componentName("secrets"), [{ apiGroups: [""], resources: ["secrets"], operations: ["CREATE", "UPDATE"] }], BY_ANY_ALASIO, [{
    expression: `object.?type.orValue("Opaque") == "Opaque" && object.metadata.name.endsWith(${JSON.stringify(tokenSecretName(""))}) && ` +
      `object.?data.orValue({}).all(key, key == ${JSON.stringify(TOKEN_KEY)})`,
    message: "alasio makes no Secret but a Sandbox's token",
  }]);
}
