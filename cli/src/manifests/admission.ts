/**
 * What alasio may do beyond what RBAC can say: ValidatingAdmissionPolicies of the
 * cluster's, each matching what alasio's ServiceAccount asks (main's, or a branch
 * environment's), and holding it to what alasio up renders:
 *
 * - a Job alasio makes is the clone of a session's volume, as rendered, but for the
 *   claims it is told: RBAC lets it make any Job in its namespace, as another
 *   ServiceAccount there, a controller's whose rights reach the cluster, or of another
 *   image or command, privileged as the clone is.
 *
 * Where a policy holds an object to a rendered one, `pinned` makes the CEL that says so:
 * the object's fields are the rendered ones', key for key and item for item, but for what
 * a Check allows, and what the API server defaults where the rendered one has nothing.
 */
import type { KubernetesObject, V1PodSpec } from "@kubernetes/client-node";

import { CLONE_VARIABLES } from "../../../src/kube/config.ts";
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
      return `${JSON.stringify(key)} in ${expression} && ${pinned(at(key), expected[key] as Pinned, depth)}`;
    }),
    ...extra.map((key) => `(!(${JSON.stringify(key)} in ${expression}) || ${pinned(at(key), defaults[key] as Pinned, depth)})`),
  ].join(" && ");
}

/** The name the ServiceAccount of the alasio of the namespace `namespace` asks as. */
export const alasioAccount = (namespace: string) => `system:serviceaccount:${namespace}:${RELEASE}`;

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
