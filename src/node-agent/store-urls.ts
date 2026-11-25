/**
 * The URLs the node agent gives a restore's checkpoint gofer to read the snapshots bucket by,
 * straight from the store (./agent.ts goferUrlOptions): each of an object, presigned for GET by
 * the agent's own keys (./s3.ts presignedUrl), at the store's endpoint as the gofer reaches it
 * from the node's network, not the cluster's. A Service's name of the cluster's
 * (`<service>.<namespace>.svc`, or under the cluster's domain, `.svc.<domain>`), as the bundled
 * store's endpoint is, the node's resolver does not know: the agent, which resolves it in the
 * cluster's DNS, signs its URLs for the Service's address, as the node reaches a Service there.
 * Any other endpoint, an operator's store, is signed as it is, which the node resolves itself. A
 * Service's address is resolved once in a while (RESOLVED_FOR_MS), as a Service keeps its address
 * for as long as it lives.
 */
import { lookup } from "node:dns/promises";
import { isIPv6 } from "node:net";

import { Effect } from "effect";

import { presignedUrl, S3Error, type S3Options } from "./s3.ts";

/** How long the agent signs URLs for the address it resolved a Service's name to before it resolves it again. */
export const RESOLVED_FOR_MS = 60_000;

/** Whether `hostname` is a Service's name of the cluster's, which the node's resolver does not know. */
export const isServiceName = (hostname: string): boolean => hostname.endsWith(".svc") || hostname.includes(".svc.");

/** What makeStoreUrls is given: the bucket and as whom, and, for a test, how a name is resolved. */
export interface StoreUrlsOptions {
  readonly store: Pick<S3Options, "endpoint" | "region" | "bucket" | "accessKeyId" | "secretAccessKey">;
  readonly resolve?: (hostname: string) => Promise<string>;
}

/**
 * The URL of the object `key` of the bucket, presigned for GET by the agent's keys for
 * `expiresSeconds`, at the store's endpoint as the node reaches it, as the module says.
 */
export function makeStoreUrls({ store, resolve = async (hostname) => (await lookup(hostname)).address }: StoreUrlsOptions): (key: string, expiresSeconds: number) => Effect.Effect<string, S3Error> {
  const endpoint = new URL(store.endpoint);
  let resolved: { readonly endpoint: string; readonly at: number } | null = null;

  /** The store's endpoint as the node reaches it: a Service's, by its address, resolved anew past RESOLVED_FOR_MS. */
  const reached: Effect.Effect<string, S3Error> = Effect.suspend(() => {
    if (!isServiceName(endpoint.hostname)) return Effect.succeed(store.endpoint);
    const now = Date.now();
    if (resolved !== null && now - resolved.at < RESOLVED_FOR_MS) return Effect.succeed(resolved.endpoint);
    return Effect.tryPromise({
      try: () => resolve(endpoint.hostname),
      catch: (cause) => new S3Error({ operation: "signing of a URL", key: endpoint.hostname, reason: `the store's Service did not resolve: ${cause instanceof Error ? cause.message : String(cause)}` }),
    }).pipe(
      Effect.map((address) => {
        const at = new URL(store.endpoint);
        at.hostname = isIPv6(address) ? `[${address}]` : address;
        const signed = at.href;
        resolved = { endpoint: signed, at: now };
        return signed;
      }),
    );
  });

  return (key, expiresSeconds) =>
    Effect.flatMap(reached, (at) =>
      Effect.try({
        try: () => presignedUrl(at, store.bucket, key, "GET", store, new Date(), expiresSeconds),
        catch: (cause) => new S3Error({ operation: "signing of a URL", key, reason: cause instanceof Error ? cause.message : String(cause) }),
      }));
}
