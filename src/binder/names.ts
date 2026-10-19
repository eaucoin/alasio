/**
 * What the binder's bindings are known by (./bindings.ts): the labels and annotations of a
 * pool's pods and of their Leases, and how a key names them. Free of anything but Node's own,
 * so the command line, which renders the pools, takes it without the binder.
 */
import { createHash, createHmac } from "node:crypto";

import { BUSY_ANNOTATIONS } from "../kube/busy-label.ts";

/** The label of a pool's pods, its name. */
export const POOL_LABEL = "alasio.dev/pool";
/** The label of a pool's free pods, which its Deployment's selector selects with POOL_LABEL. */
export const FREE_LABEL = "alasio.dev/free";
/** The label of a bound pod, and of its Lease: its key's hash. */
export const BINDING_LABEL = "alasio.dev/binding";
/** The annotation of a bound pod, and of its Lease: its key. */
export const KEY_ANNOTATION = "alasio.dev/binding-key";

/** A pool: its name and namespace. */
export interface Pool {
  readonly name: string;
  readonly namespace: string;
}

/**
 * The key of the environment `environment`'s callers, of the gateway's caller key `master`:
 * its HMAC of the environment's name, hex, a key as the text its Secret holds is, as the
 * master is; so each environment's alasio, given its own alone, signs tokens of its own
 * environment's sessions alone (./tokens.ts).
 */
export const environmentKey = (master: Buffer, environment: string): Buffer =>
  Buffer.from(createHmac("sha256", master).update(`environment:${environment}`).digest("hex"));

/** The hash of `key` its Lease and pod are named and labelled by: a label's value at most. */
export const keyHash = (key: string): string => createHash("sha256").update(key).digest("hex").slice(0, 40);

/** The name of the Lease of `key`'s binding. */
export const leaseName = (key: string): string => `binding-${keyHash(key)}`;

/** The labels of a pool's free pods, as its Deployment's template and selector give them. */
export const freeLabels = (pool: Pool): Readonly<Record<string, string>> => ({ [POOL_LABEL]: pool.name, [FREE_LABEL]: "true" });

/** The labels of a pod of `pool` bound to `key`. */
export const boundLabels = (pool: Pool, key: string): Readonly<Record<string, string>> => ({ [POOL_LABEL]: pool.name, [BINDING_LABEL]: keyHash(key) });

/** The annotations of a pod bound to `key`: the key, and what keeps its node from being removed while it is. */
export const boundAnnotations = (key: string): Readonly<Record<string, string>> => ({ [KEY_ANNOTATION]: key, ...BUSY_ANNOTATIONS });

/**
 * The header of an answer the front gives a request it passed to no pod, nothing of it sent
 * past the front (./server.ts), so that its caller may send it again, after its Retry-After: an
 * MCP call's among them, which a pod given it may have run.
 */
export const NOT_PASSED_HEADER = "x-alasio-not-passed";
