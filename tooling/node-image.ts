/**
 * The node image's build arguments (cluster/node/Dockerfile): the k3s image it is built
 * from, the gVisor releases it adds, each as RELEASE:SHA512, separated by spaces, and
 * stargz-snapshotter's release with its sha256, as cluster/node/pins.json pins them for
 * the image and the host target alike. Every build of the image passes them; run, it
 * says them one a line, as a shell's array takes them:
 *
 *   mapfile -t args < <(node tooling/node-image.ts) && docker build "${args[@]}" cluster/node
 */
import pins from "../cluster/node/pins.json" with { type: "json" };

/** `docker build`'s arguments that pass the pins to the node image's build. */
export const NODE_BUILD_ARGS: readonly string[] = [
  `--build-arg=K3S_IMAGE=${pins.k3s.image}`,
  `--build-arg=GVISOR_RELEASES=${pins.gvisor.releases.map(({ release, sha512 }) => `${release}:${sha512}`).join(" ")}`,
  `--build-arg=STARGZ_VERSION=${pins.stargz.version}`,
  `--build-arg=STARGZ_SHA256=${pins.stargz.sha256}`,
];

if (import.meta.main) console.log(NODE_BUILD_ARGS.join("\n"));
