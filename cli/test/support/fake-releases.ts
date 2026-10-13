/**
 * Releases of k3s, gVisor and stargz-snapshotter for the tests of what makes a machine a
 * node: a k3s binary, its install script, an archive of each gVisor release (a tar
 * archive, compressed with zstd, of runsc, its shim and gvisor-bin/), and
 * stargz-snapshotter's (a tar archive, gzipped, of its executables), each a few bytes,
 * pinned by their digests as cluster/node/pins.json pins the real ones, and served where
 * the real ones are, from memory. What is fetched is recorded.
 */
import { createHash } from "node:crypto";
import { gzipSync, zstdCompressSync } from "node:zlib";

import { Effect, Layer } from "effect";

import { DownloadFailed, NodeReleases } from "../../src/cluster/host.ts";
import { downloads, NODE_PINS, type NodePins } from "../../src/cluster/node.ts";
import { tarArchive, type TarEntry } from "../../src/cluster/tar.ts";

export interface FakeReleases {
  readonly pins: NodePins;
  /** The files of stargz-snapshotter's release, by their names in its archive. */
  readonly stargzFiles: ReadonlyMap<string, string>;
  /** What is served, by URL, which a test may change. */
  readonly served: Map<string, Buffer>;
  /** The URLs fetched, in order. */
  readonly fetched: string[];
  readonly layer: Layer.Layer<NodeReleases>;
}

/** The archive of gVisor's `release`: each of its binaries says its name and the release. */
function gvisorArchive(release: string): Buffer {
  const executable = (name: string): TarEntry => ({ name, type: "file", mode: 0o755, content: Buffer.from(`${name.split("/").at(-1)} ${release}\n`) });
  return zstdCompressSync(tarArchive([
    executable("containerd-shim-runsc-v1"),
    executable("runsc"),
    { name: "gvisor-bin/", type: "directory", mode: 0o755, content: Buffer.alloc(0) },
    executable("gvisor-bin/gvisor_sentry"),
    { name: "README", type: "file", mode: 0o644, content: Buffer.from("not one of gVisor's binaries\n") },
  ]));
}

/** Releases of k3s `k3s`, of gVisor `gvisor`, the current first, and of stargz-snapshotter `stargz`, pinned. */
export function fakeReleases(
  { k3s = "v1.99.0+k3s1", gvisor = ["20990101.0"], stargz = "v9.9.0" }: { readonly k3s?: string; readonly gvisor?: readonly string[]; readonly stargz?: string } = {},
): FakeReleases {
  const sha = (algorithm: "sha256" | "sha512", content: Buffer) => createHash(algorithm).update(content).digest("hex");
  const binary = Buffer.from(`k3s ${k3s}\n`);
  const script = Buffer.from(`#!/bin/sh\n# k3s's install script, of ${k3s}\n`);
  const archives = new Map(gvisor.map((release) => [release, gvisorArchive(release)]));
  const stargzFiles = new Map(["containerd-stargz-grpc", "stargz-fuse-manager", "ctr-remote"].map((name) => [name, `${name} ${stargz}\n`]));
  const stargzArchive = gzipSync(tarArchive([...stargzFiles].map(([name, content]): TarEntry => ({ name, type: "file", mode: 0o755, content: Buffer.from(content) }))));
  const pins: NodePins = {
    k3s: { ...NODE_PINS.k3s, version: k3s, sha256: sha("sha256", binary), installScript: { commit: `commit-of-${k3s}`, sha256: sha("sha256", script) } },
    gvisor: { releases: [...archives].map(([release, archive]) => ({ release, sha512: sha("sha512", archive) })) },
    stargz: { version: stargz, sha256: sha("sha256", stargzArchive) },
  };
  const from = downloads(pins);
  const served = new Map([
    [from.k3s.url, binary],
    [from.installScript.url, script],
    ...[...from.gvisor].map(([release, { url }]) => [url, archives.get(release) ?? Buffer.alloc(0)] as const),
    [from.stargz.url, stargzArchive],
  ]);
  const fetched: string[] = [];
  return {
    pins,
    stargzFiles,
    served,
    fetched,
    layer: Layer.succeed(
      NodeReleases,
      NodeReleases.of({
        pins,
        fetch: (url) =>
          Effect.suspend(() => {
            fetched.push(url);
            const content = served.get(url);
            return content ? Effect.succeed(new Uint8Array(content)) : Effect.fail(new DownloadFailed({ url, reason: "it answered 404 Not Found" }));
          }),
      }),
    ),
  };
}
