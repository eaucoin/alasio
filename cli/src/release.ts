/**
 * What this version of the package installs. The release workflow writes the released
 * version and the digests of the images it published here, with tooling/pin-release.ts,
 * before it builds the package; this module holds nothing else.
 */
export const VERSION = "0.0.0-development";

/** alasio's own images, tagged with the version and pinned by digest once released. */
export const IMAGES = {
  alasio: { repository: "ghcr.io/eaucoin/alasio", tag: VERSION, digest: "" },
  agent: { repository: "ghcr.io/eaucoin/alasio-agent", tag: VERSION, digest: "" },
  lake: { repository: "ghcr.io/eaucoin/alasio-lake", tag: VERSION, digest: "" },
};

/** The node image of the cluster in Docker: k3s with gVisor (cluster/node), pinned like the others. */
export const NODE_IMAGE = { repository: "ghcr.io/eaucoin/alasio-node", tag: VERSION, digest: "" };
