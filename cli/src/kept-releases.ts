/**
 * The gVisor releases alasio keeps: those it pins (cluster/node/pins.json, the current
 * one first, which new sandboxes run), and every other one installed that a snapshot
 * still names, as only the release that took a snapshot restores it
 * (src/sandbox/snapshots.ts). A session's Sandbox released into a snapshot names it, and
 * the release that took it (src/kube/sandboxes.ts snapshotBinding), so what the cluster's
 * Sandboxes name is every release a snapshot may be restored with.
 *
 * Where what they name cannot be read (the cluster is down, or a Sandbox was bound before
 * the release was recorded), every release installed is kept. A release no snapshot
 * names, and that is not pinned, is removed, its RuntimeClass with it. On a cluster in
 * Docker, the releases are its node image's, the pinned ones: a snapshot naming another
 * falls back, as one whose release is not kept does. On a cluster of a kubeconfig, the
 * operator keeps the releases. A release before gvisor.restoresFromBucketSince, as the
 * fork's .6, does not end a restore it refuses, which stalls the next pod's teardown for
 * minutes before the fallback (progress/live-lazy.md); its snapshots, released again under
 * a later release, leave it unkept over time.
 */
import type { V1RuntimeClass } from "@kubernetes/client-node";
import { Effect } from "effect";

import { SNAPSHOT_ANNOTATION, SNAPSHOT_RELEASE_ANNOTATION } from "../../src/kube/sandbox-shape.ts";
import { GVISOR_RELEASE_ANNOTATION } from "./cluster/k3s.ts";
import { kind, KubeApi } from "./kube/api.ts";

/** What the cluster's sessions are released into snapshots of: the snapshots each release names, by release. */
export type SnapshotReleases = ReadonlyMap<string, readonly string[]>;

/** The Sandboxes of every namespace, of sessions, as `list` sees them: their annotations. */
interface Bound {
  readonly metadata?: { readonly namespace?: string; readonly name?: string; readonly annotations?: Readonly<Record<string, string>> };
}

/**
 * The snapshots of the cluster's sessions, by the release that took each, as their
 * Sandboxes say; null where they cannot be read, or a Sandbox bound to a snapshot names
 * no release. Pure, for tests.
 */
export function snapshotReleasesOf(sandboxes: readonly Bound[]): SnapshotReleases | null {
  const named = new Map<string, string[]>();
  for (const { metadata } of sandboxes) {
    const snapshot = metadata?.annotations?.[SNAPSHOT_ANNOTATION];
    if (!snapshot) continue;
    const release = metadata?.annotations?.[SNAPSHOT_RELEASE_ANNOTATION];
    if (!release) return null;
    named.set(release, [...(named.get(release) ?? []), `${metadata?.namespace}/${metadata?.name}`]);
  }
  return named;
}

/** What the cluster's Sandboxes name, as snapshotReleasesOf says; null where they cannot be read. */
export const snapshotReleases: Effect.Effect<SnapshotReleases | null, never, KubeApi> = Effect.flatMap(KubeApi, (kube) =>
  kube.list<Bound>(kind("Sandbox"), { labelSelector: "alasio.dev/workload=session" }).pipe(
    Effect.map(snapshotReleasesOf),
    Effect.timeout("15 seconds"),
    Effect.catch((error) => Effect.logInfo(`what the cluster's snapshots name is not known, so every gVisor release installed is kept: ${error.message}`).pipe(Effect.as(null))),
  ));

/**
 * The releases kept, of those `installed`, beside the `pinned` ones, as what the
 * snapshots name (`named`, or null where it is not known) says: the pinned first, in
 * their order, then those installed that a snapshot names, in theirs. Pure, for tests.
 */
export function keptReleases(pinned: readonly string[], installed: readonly string[], named: SnapshotReleases | null): string[] {
  return [...new Set([...pinned, ...installed.filter((release) => named === null || named.has(release))])];
}

/**
 * The releases the cluster keeps, as its RuntimeClasses of alasio's say (the ones
 * HostCluster and DockerCluster apply), beside the `pinned` ones, in keptReleases' order;
 * the pinned alone where it has none, or they cannot be read, as on a cluster of a
 * kubeconfig.
 */
export const clusterReleases = (pinned: readonly string[]): Effect.Effect<string[], never, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) =>
    kube.list<V1RuntimeClass>(kind("RuntimeClass")).pipe(
      Effect.map((classes) => keptReleases(pinned, classes.flatMap(({ metadata }) => metadata?.annotations?.[GVISOR_RELEASE_ANNOTATION] ?? []), null)),
      Effect.catch(() => Effect.succeed([...pinned])),
    ));

/** `count` snapshots, as a sentence says it. */
const snapshotsOf = (count: number): string => (count === 1 ? "1 snapshot names it" : `${count} snapshots name it`);

/**
 * What `alasio status` says of the gVisor releases `kept` (the current one first) and of
 * those the snapshots name, `named` (null where they cannot be read), a line each: each
 * release kept, with how many snapshots name it, then each release named but not kept,
 * whose snapshots fall back. Pure, for tests.
 */
export function describeKept(kept: readonly string[], named: SnapshotReleases | null): string[] {
  const count = (release: string) => (named === null ? null : named.get(release)?.length ?? 0);
  const said = (release: string) => {
    const counted = count(release);
    return counted === null ? "what snapshots name it is not seen" : snapshotsOf(counted);
  };
  return [
    ...kept.map((release, index) => `gVisor ${release}, kept${index === 0 ? ", which new sessions run" : ""}: ${said(release)}`),
    ...[...(named?.keys() ?? [])].filter((release) => !kept.includes(release)).map((release) =>
      `gVisor ${release}, not kept: ${said(release)}, restored from bayma's checkpoints instead`
    ),
  ];
}
