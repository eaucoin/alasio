/**
 * The bundled store's admission of the cluster's nodes, whose restores' checkpoint gofers read
 * the snapshots bucket straight from it by URLs (../node-agent/store-urls.ts), from each node's
 * own network, which no pod selector of a NetworkPolicy names: the installation's policy of the
 * store's S3 port for them (cli/src/manifests/network-policies.ts storeFromNodesPolicy) admits
 * each node's own addresses, and no other. The gateway is its one writer: the node agents only
 * read it, as each waits for its node to be admitted before it prepares a restore by URLs
 * (../node-agent/store-admission.ts), so a node's agent cannot change what the policy admits.
 *
 * As the gateway starts and every ADMISSION_EVERY_MS after, it lists the cluster's nodes and
 * writes the policy, as of the version it read, where its spec is other than the gateway's own:
 * the store's pods selected, by the labels it is given, for ingress alone, admitted on the S3 port
 * from each address of each node's, as its Node object says them (its InternalIP and ExternalIP
 * addresses), and the first two of each of its pods' ranges (spec.podCIDRs); none where there are
 * none, as a rule of no peer would admit everyone. Its whole spec is the gateway's, so that one
 * changed by anyone else, its selector or its types as its rule, is set back. What it keeps of
 * each node is in the policy too (NODE_ADDRESSES_ANNOTATION), which the agents read. A session's
 * pod, of another address of its node's range, is not among them; nor is a node gone from the
 * cluster. Each replica of the gateway writes the same, so two at once agree; one whose write
 * finds the policy written since reads it again. What fails is said, and tried again
 * ADMISSION_EVERY_MS on.
 *
 * The addresses of a node's pods' range assume the CNI alasio installs with k3s, flannel, whose
 * host-side interfaces take them: the overlay's (flannel.1) the range's first, from which the node
 * reaches another node's pods, a Service's ClusterIP among them; the bridge's (cni0) the second,
 * from which it reaches its own. Under another CNI, whose host-side addresses are others, a
 * restore by URLs reaches the store on no node but the store's own, as the store's pods refuse
 * the rest: so the node agents' restores by URLs are not the default (sessions.snapshots
 * restoreReads), and not to be made it before a test of two nodes, on a release that reads by
 * URLs, has shown a restore read the store across nodes.
 */
import { isIPv6 } from "node:net";

import type { KubernetesObject, V1Node } from "@kubernetes/client-node";
import { Effect, Schedule } from "effect";

import type { KubeApiError, KubeClient } from "../kube/client.ts";
import { NODE_ADDRESSES_ANNOTATION } from "../node-agent/names.ts";

/** The S3 port of the bundled store, which the policy admits the nodes to. */
export const STORE_S3_PORT = 8333;

/** How often the gateway sets the nodes' addresses in the policy again, as nodes join and go. */
export const ADMISSION_EVERY_MS = 15_000;

/** How many times a write of the policy that finds it written since is tried again at once. */
const WRITE_TRIES = 5;

/** The first two addresses of the range `cidr` (as `10.42.1.0/24`, `fd00:42:1::/64`): its network address and the one after it. */
export function rangeStart(cidr: string): readonly string[] {
  const [base = ""] = cidr.split("/");
  if (isIPv6(base)) {
    const groups = expandIPv6(base);
    const last = groups.at(-1) ?? 0;
    return [last, last + 1].map((value) => compressIPv6([...groups.slice(0, 7), value]));
  }
  const parts = base.split(".").map(Number);
  if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) return [];
  const value = ((parts[0]! << 24) >>> 0) + (parts[1]! << 16) + (parts[2]! << 8) + parts[3]!;
  return [value, value + 1].map((each) => [24, 16, 8, 0].map((shift) => (each >>> shift) & 255).join("."));
}

/** The eight groups of the IPv6 address `address`. */
function expandIPv6(address: string): number[] {
  const [head = "", tail] = address.split("::");
  const parse = (part: string) => (part === "" ? [] : part.split(":").map((group) => Number.parseInt(group, 16)));
  const front = parse(head);
  const back = tail === undefined ? [] : parse(tail);
  return [...front, ...new Array<number>(8 - front.length - back.length).fill(0), ...back];
}

/** The IPv6 address of the eight groups `groups`, its longest run of zeros compressed, as Node writes one. */
function compressIPv6(groups: readonly number[]): string {
  let [start, length] = [-1, 0];
  for (let index = 0; index < 8;) {
    if (groups[index] !== 0) {
      index++;
      continue;
    }
    let end = index;
    while (end < 8 && groups[end] === 0) end++;
    if (end - index > length && end - index > 1) [start, length] = [index, end - index];
    index = end;
  }
  const hex = groups.map((group) => group.toString(16));
  if (start === -1) return hex.join(":");
  return `${hex.slice(0, start).join(":")}::${hex.slice(start + length).join(":")}`;
}

/** The addresses `node` holds, as the module says: its InternalIP and ExternalIP addresses, and the first two of each of its pods' ranges; sorted. */
export function nodeAddressesOf(node: V1Node): readonly string[] {
  const own = (node.status?.addresses ?? []).filter(({ type }) => type === "InternalIP" || type === "ExternalIP").map(({ address }) => address);
  const ranges = node.spec?.podCIDRs ?? (node.spec?.podCIDR === undefined ? [] : [node.spec.podCIDR]);
  return [...new Set([...own, ...ranges.flatMap(rangeStart)])].sort();
}

/**
 * The policy `policy`, of each node's addresses `kept`, as the gateway has it: its spec its own,
 * of the store's pods, by their labels `storeLabels`, for ingress alone, admitted on the S3 port
 * from each address any node holds, as a host's of its own; from none where there are none.
 */
export function admitting(policy: KubernetesObject, kept: Readonly<Record<string, readonly string[]>>, storeLabels: Readonly<Record<string, string>>): KubernetesObject {
  const addresses = [...new Set(Object.values(kept).flat())].sort();
  return {
    ...policy,
    metadata: { ...policy.metadata, annotations: { ...policy.metadata?.annotations, [NODE_ADDRESSES_ANNOTATION]: JSON.stringify(Object.fromEntries(Object.entries(kept).sort(([a], [b]) => a.localeCompare(b)))) } },
    spec: {
      podSelector: { matchLabels: { ...storeLabels } },
      policyTypes: ["Ingress"],
      ingress: addresses.length === 0 ? [] : [{
        ports: [{ protocol: "TCP", port: STORE_S3_PORT }],
        from: addresses.map((address) => ({ ipBlock: { cidr: `${address}/${isIPv6(address) ? 128 : 32}` } })),
      }],
    },
  } as KubernetesObject;
}

/** `value` as JSON, each object's keys sorted: what two policies' specs are compared by, whatever order the API server gives their fields in. */
function canonical(value: unknown): string {
  return JSON.stringify(value, (_key, each: unknown) =>
    each !== null && typeof each === "object" && !Array.isArray(each)
      ? Object.fromEntries(Object.entries(each as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b)))
      : each);
}

/**
 * Has the policy `name` of `namespace` admit the cluster's nodes as they are now, as the module
 * says: whether it wrote it, or found it as it would have; null where there is no policy.
 */
export const admitNodes = (kube: Pick<KubeClient["Service"], "read" | "list" | "replace">, namespace: string, name: string, storeLabels: Readonly<Record<string, string>>): Effect.Effect<"written" | "unchanged" | null, KubeApiError> =>
  Effect.gen(function*() {
    for (let tries = 1;; tries++) {
      const policy = yield* kube.read("networking.k8s.io/v1", "NetworkPolicy", namespace, name);
      if (policy === null) return null;
      const nodes = (yield* kube.list("v1", "Node", "", "")) as readonly V1Node[];
      const kept = Object.fromEntries(nodes.map((node) => [node.metadata?.name ?? "", nodeAddressesOf(node)] as const).filter(([named]) => named !== ""));
      const after = admitting(policy, kept, storeLabels);
      // As it would be written: its kept addresses, and its whole spec, as read.
      const same = after.metadata?.annotations?.[NODE_ADDRESSES_ANNOTATION] === policy.metadata?.annotations?.[NODE_ADDRESSES_ANNOTATION] &&
        canonical((after as { readonly spec?: unknown }).spec) === canonical({ ingress: [], ...(policy as { readonly spec?: object }).spec });
      if (same) return "unchanged";
      const written = yield* kube.replace(after).pipe(
        Effect.as(true),
        Effect.catchIf((error) => error.status === 409 && tries < WRITE_TRIES, () => Effect.succeed(false)),
      );
      if (written) return "written";
    }
  });

/** Keeps the policy `name` of `namespace` admitting the cluster's nodes, as the module says, for as long as the gateway runs. */
export const keepNodesAdmitted = (kube: Pick<KubeClient["Service"], "read" | "list" | "replace">, namespace: string, name: string, storeLabels: Readonly<Record<string, string>>, everyMs: number = ADMISSION_EVERY_MS): Effect.Effect<never> =>
  admitNodes(kube, namespace, name, storeLabels).pipe(
    Effect.tap((done) =>
      done === null
        ? Effect.logWarning(`the bundled store's policy ${namespace}/${name}, which admits the nodes whose restores read it by URLs, is not there: their restores reach it on no node but their own`)
        : done === "written"
        ? Effect.logInfo(`admitted the cluster's nodes, as they are now, to the bundled store, their restores reading it by URLs`)
        : Effect.void
    ),
    Effect.catch((error) => Effect.logWarning(`could not admit the cluster's nodes to the bundled store, which their restores read by URLs, so a node that joined reaches it on no node but its own until it is tried again: ${error.message}`)),
    Effect.repeat(Schedule.spaced(everyMs)),
    Effect.andThen(Effect.never),
  );
