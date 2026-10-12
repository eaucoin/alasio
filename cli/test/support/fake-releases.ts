/**
 * Releases of k3s and gVisor for the tests of what makes a machine a node: a k3s binary,
 * its install script, and a gVisor release archive (a tar archive, compressed with zstd,
 * of runsc, its shim and gvisor-bin/), each a few bytes, pinned by their digests as
 * cluster/node/pins.json pins the real ones, and served where the real ones are, from
 * memory. What is fetched is recorded.
 */
import { createHash } from "node:crypto";
import { zstdCompressSync } from "node:zlib";

import { Effect, Layer } from "effect";

import { DownloadFailed, NodeReleases } from "../../src/cluster/host.ts";
import { downloads, NODE_PINS, type NodePins } from "../../src/cluster/node.ts";
import { tarArchive, type TarEntry } from "../../src/cluster/tar.ts";

export interface FakeReleases {
  readonly pins: NodePins;
  /** The files of gVisor's release, by their paths in its archive. */
  readonly gvisorFiles: ReadonlyMap<string, string>;
  /** What is served, by URL, which a test may change. */
  readonly served: Map<string, Buffer>;
  /** The URLs fetched, in order. */
  readonly fetched: string[];
  readonly layer: Layer.Layer<NodeReleases>;
}

/** Releases of k3s `k3s` and gVisor `gvisor`, pinned. */
export function fakeReleases({ k3s = "v1.99.0+k3s1", gvisor = "20990101.0" } = {}): FakeReleases {
  const sha = (algorithm: "sha256" | "sha512", content: Buffer) => createHash(algorithm).update(content).digest("hex");
  const binary = Buffer.from(`k3s ${k3s}\n`);
  const script = Buffer.from(`#!/bin/sh\n# k3s's install script, of ${k3s}\n`);
  const gvisorFiles = new Map([
    ["runsc", `runsc ${gvisor}\n`],
    ["containerd-shim-runsc-v1", `containerd-shim-runsc-v1 ${gvisor}\n`],
    ["gvisor-bin/gvisor_sentry", `gvisor_sentry ${gvisor}\n`],
  ]);
  const executable = (name: string): TarEntry => ({ name, type: "file", mode: 0o755, content: Buffer.from(gvisorFiles.get(name) ?? "") });
  const archive = zstdCompressSync(tarArchive([
    executable("containerd-shim-runsc-v1"),
    executable("runsc"),
    { name: "gvisor-bin/", type: "directory", mode: 0o755, content: Buffer.alloc(0) },
    executable("gvisor-bin/gvisor_sentry"),
    { name: "README", type: "file", mode: 0o644, content: Buffer.from("not one of gVisor's binaries\n") },
  ]));
  const pins: NodePins = {
    k3s: { ...NODE_PINS.k3s, version: k3s, sha256: sha("sha256", binary), installScript: { commit: `commit-of-${k3s}`, sha256: sha("sha256", script) } },
    gvisor: { release: gvisor, sha512: sha("sha512", archive) },
  };
  const from = downloads(pins);
  const served = new Map([[from.k3s.url, binary], [from.installScript.url, script], [from.gvisor.url, archive]]);
  const fetched: string[] = [];
  return {
    pins,
    gvisorFiles,
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
