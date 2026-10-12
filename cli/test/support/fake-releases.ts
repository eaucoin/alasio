/**
 * Releases of k3s and gVisor for the tests of what makes a machine a node: a k3s binary,
 * its install script, and an archive of each gVisor release (a tar archive, compressed
 * with zstd, of runsc, its shim and gvisor-bin/), each a few bytes, pinned by their
 * digests as cluster/node/pins.json pins the real ones, and served where the real ones
 * are, from memory. What is fetched is recorded.
 */
import { createHash } from "node:crypto";
import { zstdCompressSync } from "node:zlib";

import { Effect, Layer } from "effect";

import { DownloadFailed, NodeReleases } from "../../src/cluster/host.ts";
import { downloads, NODE_PINS, type NodePins } from "../../src/cluster/node.ts";
import { tarArchive, type TarEntry } from "../../src/cluster/tar.ts";

export interface FakeReleases {
  readonly pins: NodePins;
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

/** Releases of k3s `k3s` and of gVisor `gvisor`, the current first, pinned. */
export function fakeReleases({ k3s = "v1.99.0+k3s1", gvisor = ["20990101.0"] }: { readonly k3s?: string; readonly gvisor?: readonly string[] } = {}): FakeReleases {
  const sha = (algorithm: "sha256" | "sha512", content: Buffer) => createHash(algorithm).update(content).digest("hex");
  const binary = Buffer.from(`k3s ${k3s}\n`);
  const script = Buffer.from(`#!/bin/sh\n# k3s's install script, of ${k3s}\n`);
  const archives = new Map(gvisor.map((release) => [release, gvisorArchive(release)]));
  const pins: NodePins = {
    k3s: { ...NODE_PINS.k3s, version: k3s, sha256: sha("sha256", binary), installScript: { commit: `commit-of-${k3s}`, sha256: sha("sha256", script) } },
    gvisor: { releases: [...archives].map(([release, archive]) => ({ release, sha512: sha("sha512", archive) })) },
  };
  const from = downloads(pins);
  const served = new Map([
    [from.k3s.url, binary],
    [from.installScript.url, script],
    ...[...from.gvisor].map(([release, { url }]) => [url, archives.get(release) ?? Buffer.alloc(0)] as const),
  ]);
  const fetched: string[] = [];
  return {
    pins,
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
