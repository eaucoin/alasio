/**
 * The bundled store's admission of its nodes, whose restores' checkpoint gofers read the
 * snapshots bucket straight from it by URLs (./store-urls.ts), from the node's own network, which
 * no pod selector of a NetworkPolicy names: the installation's policy of the store's S3 port for
 * them (cli/src/manifests/network-policies.ts storeFromNodesPolicy) admits each node's own
 * addresses, and no other, as each node's agent says them. A session's pod, of an address of the
 * pods' ranges no node holds, is not among them; nor is a node gone from the cluster.
 *
 * Each agent, as it starts and every ADMISSION_EVERY_MS after, as a node's addresses may change,
 * reads the policy, sets its node's addresses in what the policy keeps of each node's
 * (NODE_ADDRESSES_ANNOTATION), drops those of nodes the cluster has no more, and writes the
 * policy back as of the version it read, so that the agents of nodes that join, or go, at once
 * leave none of each other's out: one whose write finds the policy written since reads it again.
 * The policy admits, on the S3 port, each address any node holds; none where none is kept, as a
 * rule of no peer would admit everyone. A node's addresses are those of its interfaces but its
 * loopback's and link-local ones: the one its own processes reach a pod by is among them,
 * whatever its route, the node's own address, or its pods' bridge's or overlay's. What fails is
 * said, and tried again ADMISSION_EVERY_MS on.
 */
import { isIPv6 } from "node:net";

import type { KubernetesObject } from "@kubernetes/client-node";
import { Effect, Schedule, Schema } from "effect";

import type { KubeApiError, KubeClient } from "../kube/client.ts";
import { HostCommandError, type HostCommands } from "./host.ts";

/** The annotation of the policy that keeps each node's addresses, as JSON: `{ "<node>": ["<address>", ...] }`. */
export const NODE_ADDRESSES_ANNOTATION = "alasio.dev/node-addresses";

/** The S3 port of the bundled store, which the policy admits the nodes to. */
export const STORE_S3_PORT = 8333;

/** How often each agent sets its node's addresses in the policy again. */
export const ADMISSION_EVERY_MS = 5 * 60_000;

/** How many times a write of the policy that finds it written since is tried again at once. */
const WRITE_TRIES = 5;

/** The addresses kept of each node, as the policy's annotation says them. */
const NodeAddresses = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Array(Schema.String)));

/** The interfaces of the node's network as Node's os.networkInterfaces() says them, printed by the agent's own Node in that network. */
const INTERFACES_SCRIPT = "process.stdout.write(JSON.stringify(require('os').networkInterfaces()))";

/** What os.networkInterfaces() says of an interface's address. */
const Interfaces = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Array(Schema.Struct({ address: Schema.String, family: Schema.String, internal: Schema.Boolean }))));

/** Whether `address` is link-local, which no other node reaches: 169.254.0.0/16, fe80::/10. */
const isLinkLocal = (address: string): boolean => (isIPv6(address) ? /^fe[89ab]/iu.test(address) : address.startsWith("169.254."));

/**
 * The addresses the node holds, as its interfaces say them in its own network, entered by the
 * agent's own Node (HostCommands.inNetwork): but its loopback's and link-local ones, sorted.
 */
export const nodeAddresses = (host: HostCommands, node: string = process.execPath): Effect.Effect<readonly string[], HostCommandError> =>
  host.inNetwork([node, "-e", INTERFACES_SCRIPT]).pipe(
    Effect.flatMap(({ exitCode, stdout, stderr }) =>
      exitCode === 0
        ? Schema.decodeUnknownEffect(Interfaces)(stdout).pipe(Effect.mapError((error) => new HostCommandError({ command: "node -e networkInterfaces", reason: error.message })))
        : Effect.fail(new HostCommandError({ command: "node -e networkInterfaces", reason: stderr.trim() || `it exited ${exitCode}` }))
    ),
    Effect.map((interfaces) =>
      [...new Set(Object.values(interfaces).flat().filter(({ address, internal }) => !internal && !isLinkLocal(address)).map(({ address }) => address))].sort()
    ),
  );

/** The policy `policy`, its kept addresses `kept`, as it admits them: on the S3 port, each address any node holds, as a host's of its own; none where there are none. */
export function admitting(policy: KubernetesObject, kept: Readonly<Record<string, readonly string[]>>): KubernetesObject {
  const addresses = [...new Set(Object.values(kept).flat())].sort();
  const spec = (policy as { readonly spec?: Record<string, unknown> }).spec ?? {};
  return {
    ...policy,
    metadata: { ...policy.metadata, annotations: { ...policy.metadata?.annotations, [NODE_ADDRESSES_ANNOTATION]: JSON.stringify(Object.fromEntries(Object.entries(kept).sort(([a], [b]) => a.localeCompare(b)))) } },
    spec: {
      ...spec,
      // Its fields in the order the API server gives them back, which a policy read is compared in.
      ingress: addresses.length === 0 ? [] : [{
        ports: [{ protocol: "TCP", port: STORE_S3_PORT }],
        from: addresses.map((address) => ({ ipBlock: { cidr: `${address}/${isIPv6(address) ? 128 : 32}` } })),
      }],
    },
  } as KubernetesObject;
}

/**
 * Sets the addresses of the node `node`, `addresses`, in the policy `name` of `namespace`, and
 * drops those of nodes the cluster has no more, as the module says: whether it wrote it, or
 * found it as it would have; null where there is no policy, as of an installation whose
 * restores read through the agent.
 */
export const admitNode = (kube: Pick<KubeClient["Service"], "read" | "replace">, namespace: string, name: string, node: string, addresses: readonly string[]): Effect.Effect<"written" | "unchanged" | null, KubeApiError | Schema.SchemaError> =>
  Effect.gen(function*() {
    for (let tries = 1;; tries++) {
      const policy = yield* kube.read("networking.k8s.io/v1", "NetworkPolicy", namespace, name);
      if (policy === null) return null;
      const said = policy.metadata?.annotations?.[NODE_ADDRESSES_ANNOTATION];
      const before = said === undefined ? {} : yield* Schema.decodeUnknownEffect(NodeAddresses)(said);
      const kept: Record<string, readonly string[]> = { [node]: addresses };
      for (const [other, held] of Object.entries(before)) {
        if (other === node) continue;
        // A node the cluster has no more is dropped, its addresses another's to hold.
        if ((yield* kube.read("v1", "Node", "", other)) !== null) kept[other] = held;
      }
      const after = admitting(policy, kept);
      // As it would be written: its kept addresses and its rule, as read.
      const same = after.metadata?.annotations?.[NODE_ADDRESSES_ANNOTATION] === said &&
        JSON.stringify((after as { readonly spec?: { readonly ingress?: unknown } }).spec?.ingress) === JSON.stringify((policy as { readonly spec?: { readonly ingress?: unknown } }).spec?.ingress ?? []);
      if (same) return "unchanged";
      const written = yield* kube.replace(after).pipe(
        Effect.as(true),
        Effect.catchIf((error) => error.status === 409 && tries < WRITE_TRIES, () => Effect.succeed(false)),
      );
      if (written) return "written";
    }
  });

/**
 * Has the node `node` admitted to the bundled store by the policy `name` of `namespace`, as the
 * module says, as the agent starts and every ADMISSION_EVERY_MS after, for as long as it runs.
 */
export const keepNodeAdmitted = (kube: Pick<KubeClient["Service"], "read" | "replace">, host: HostCommands, namespace: string, name: string, node: string, everyMs: number = ADMISSION_EVERY_MS): Effect.Effect<never> =>
  Effect.gen(function*() {
    const addresses = yield* nodeAddresses(host);
    const done = yield* admitNode(kube, namespace, name, node, addresses);
    if (done === null) yield* Effect.logWarning(`the bundled store's policy ${namespace}/${name}, which admits the nodes whose restores read it by URLs, is not there: the restores of ${node} reach the store on no node but their own`);
    else if (done === "written") yield* Effect.logInfo(`admitted ${node}'s addresses, ${addresses.join(", ")}, to the bundled store, its restores reading it by URLs`);
  }).pipe(
    Effect.catch((error) => Effect.logWarning(`could not admit ${node}'s addresses to the bundled store, which its restores read by URLs, so they reach it on no node but their own until it is tried again: ${error.message}`)),
    Effect.repeat(Schedule.spaced(everyMs)),
    Effect.andThen(Effect.never),
  );
