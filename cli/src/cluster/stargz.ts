/**
 * Lazy pulling on the nodes alasio makes: stargz-snapshotter's own daemon,
 * containerd-stargz-grpc, beside k3s's containerd, as its snapshotter, under a name of
 * its own (SNAPSHOTTER), as k3s always registers its builtin `stargz`. An eStargz layer
 * is mounted from the registry as it is read, and fetched whole in the background; any
 * other layer is pulled whole, as overlayfs would.
 *
 * The daemon, not k3s's builtin plugin: k3s's containerd dies with k3s, and the builtin
 * plugin's mounts with it, so every lazily pulled pod would stop at each restart of k3s,
 * which alasio makes whenever the node's files change. The daemon's FUSE manager keeps the
 * mounts through a restart of k3s and of the daemon alike, as proven in CI.
 *
 * The daemon reads layers from the registries itself: from each image's own registry, and
 * through the node's mirrors (`registries`) that it can reach as they are, a host and a
 * port, plain HTTP where the mirror is, no path rewritten. It takes no registry's login,
 * as alasio's registries ask for none; an image it cannot read lazily is pulled whole.
 */
import type { Registries } from "./k3s.ts";

/** The snapshotter's name, which containerd's CRI pulls with. */
export const SNAPSHOTTER = "alasio-stargz";
/** Where the daemon keeps its snapshots and caches. */
export const STARGZ_ROOT = "/var/lib/containerd-stargz-grpc";
/** The daemon's socket, which containerd reaches it at. */
export const STARGZ_SOCKET = "/run/containerd-stargz-grpc/containerd-stargz-grpc.sock";
/** The daemon's configuration. */
export const STARGZ_CONFIG = "/etc/containerd-stargz-grpc/config.toml";
/** The daemon's executables, as its release has them: the daemon and its FUSE manager. */
export const STARGZ_EXECUTABLES = ["containerd-stargz-grpc", "stargz-fuse-manager"] as const;

/** `value` as a TOML string, quoted. */
const tomlString = (value: string): string => JSON.stringify(value);

/**
 * The daemon's configuration on a node pulling through `registries`: its background fetch,
 * which fetches every layer whole so a pod reads nothing from the registry once it is done,
 * one layer at a time, so it takes less of the network a pod's start reads its layers
 * over (in CI, a 2.1 GB image started in 0.7–1.3 s so, and 1.0–3.2 s with the
 * default two); its FUSE manager on, which keeps its mounts; and each mirror it can reach
 * as it is, ahead of the registry itself, which it reads last. Pure, for tests.
 */
export function stargzConfig(registries?: Registries): string {
  const mirrors = Object.entries(registries?.mirrors ?? {})
    // `*` stands for every registry, which the daemon has no way to say.
    .filter(([host, { rewrite }]) => host !== "*" && rewrite === undefined)
    .flatMap(([host, { endpoint }]) => {
      const reached = endpoint.map((url) => new URL(url)).filter(({ pathname }) => pathname === "/" || pathname === "");
      return reached.length === 0 ? [] : [[host, reached] as const];
    });
  return [
    "# stargz-snapshotter's daemon on a node alasio makes: alasio writes this file.",
    "max_concurrency = 1",
    "",
    "[fuse_manager]",
    "enable = true",
    `address = ${tomlString("/run/containerd-stargz-grpc/fuse-manager.sock")}`,
    `path = ${tomlString("/usr/local/bin/stargz-fuse-manager")}`,
    ...mirrors.flatMap(([host, reached]) =>
      reached.flatMap((url) => [
        "",
        `[[resolver.host.${tomlString(host)}.mirrors]]`,
        `host = ${tomlString(url.host)}`,
        ...(url.protocol === "http:" ? ["insecure = true"] : []),
      ])
    ),
    "",
  ].join("\n");
}
