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

/** An entry of a tar archive: a file with its content and mode, or a directory. */
interface Entry {
  readonly name: string;
  readonly content?: Buffer;
  readonly mode?: number;
}

/** A POSIX tar archive of `entries`, those without content directories. */
export function tar(entries: readonly Entry[]): Buffer {
  const blocks = entries.flatMap(({ name, content, mode = 0o755 }) => {
    const header = Buffer.alloc(512);
    const octal = (value: number, width: number) => value.toString(8).padStart(width - 1, "0");
    header.write(name, 0, "latin1");
    header.write(octal(mode, 8), 100, "latin1");
    header.write(octal(0, 8), 108, "latin1");
    header.write(octal(0, 8), 116, "latin1");
    header.write(octal(content?.length ?? 0, 12), 124, "latin1");
    header.write(octal(0, 12), 136, "latin1");
    header.write(content ? "0" : "5", 156, "latin1");
    header.write("ustar\u000000", 257, "latin1");
    header.write("        ", 148, "latin1");
    header.write(`${octal(header.reduce((sum, byte) => sum + byte, 0), 7)}\0`, 148, "latin1");
    const padded = content ? Buffer.concat([content, Buffer.alloc((512 - (content.length % 512)) % 512)]) : Buffer.alloc(0);
    return [header, padded];
  });
  return Buffer.concat([...blocks, Buffer.alloc(1024)]);
}

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
  const archive = zstdCompressSync(tar([
    { name: "containerd-shim-runsc-v1", content: Buffer.from(gvisorFiles.get("containerd-shim-runsc-v1") ?? "") },
    { name: "runsc", content: Buffer.from(gvisorFiles.get("runsc") ?? "") },
    { name: "gvisor-bin/" },
    { name: "gvisor-bin/gvisor_sentry", content: Buffer.from(gvisorFiles.get("gvisor-bin/gvisor_sentry") ?? "") },
    { name: "README", content: Buffer.from("not one of gVisor's binaries\n"), mode: 0o644 },
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
