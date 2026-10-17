/**
 * Where the cluster's API server is, for what may reach it alone: the addresses and ports
 * the default namespace's `kubernetes` Service sends its clients on to, as its
 * EndpointSlices say. A pod's connection to the Service reaches one of those, and a
 * NetworkPolicy, which names no Service, reaches it by their address blocks alone
 * (./manifests/network-policies.ts). `alasio up` reads them each time it applies, so the
 * policies follow them as they change.
 */
import type { V1EndpointSlice } from "@kubernetes/client-node";
import { Effect } from "effect";

import { kind, KubeApi } from "./kube/api.ts";

/** The API server's addresses (IPv4) and ports (TCP), each sorted, none twice. */
export interface ApiServer {
  readonly addresses: readonly string[];
  readonly ports: readonly number[];
}

/**
 * Where `slices`, the `kubernetes` Service's, say the API server is: the addresses of their
 * IPv4 endpoints not said to be unready, and their TCP ports; null where they say no address
 * or no port.
 */
export function apiServerOf(slices: readonly V1EndpointSlice[]): ApiServer | null {
  const ipv4 = slices.filter(({ addressType }) => addressType === "IPv4");
  const addresses = ipv4.flatMap(({ endpoints }) => (endpoints ?? []).filter(({ conditions }) => conditions?.ready !== false).flatMap(({ addresses }) => addresses));
  const ports = ipv4.flatMap(({ ports }) => (ports ?? []).flatMap(({ port, protocol }) => (port !== undefined && (protocol ?? "TCP") === "TCP" ? [port] : [])));
  if (addresses.length === 0 || ports.length === 0) return null;
  return { addresses: [...new Set(addresses)].sort(), ports: [...new Set(ports)].sort((a, b) => a - b) };
}

/** Where the cluster's API server is, as the module says; null where its EndpointSlices cannot be read, or say nowhere. */
export const readApiServer: Effect.Effect<ApiServer | null, never, KubeApi> = Effect.flatMap(KubeApi, (kube) =>
  kube.list<V1EndpointSlice>(kind("EndpointSlice"), { namespace: "default", labelSelector: "kubernetes.io/service-name=kubernetes" }).pipe(
    Effect.map(apiServerOf),
    Effect.catch(() => Effect.succeed(null)),
  ));
