/**
 * Pins alasio's command line to a release: writes the released version, and the digests
 * of the images the release published, into cli/src/release.ts, which holds nothing
 * else. The release workflow runs it before it builds the package:
 *
 *   node tooling/pin-release.ts <version> alasio=<digest> alasio-agent=<digest> alasio-lake=<digest> alasio-node=<digest>
 */
import { writeFileSync } from "node:fs";

/** The images a release publishes, by the name of their repository under ghcr.io/eaucoin. */
export const RELEASED_IMAGES = ["alasio", "alasio-agent", "alasio-lake", "alasio-node"] as const;

export type ReleasedImage = (typeof RELEASED_IMAGES)[number];

/** A release: its version, and its images' digests; a digest is empty for an image not pinned. */
export interface ReleasePin {
  readonly version: string;
  readonly digests: Readonly<Record<ReleasedImage, string>>;
}

/** What the repository's release.ts holds between releases: a development version, no image pinned. */
export const DEVELOPMENT: ReleasePin = {
  version: "0.0.0-development",
  digests: { "alasio": "", "alasio-agent": "", "alasio-lake": "", "alasio-node": "" },
};

const VERSION = /^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/u;
const DIGEST = /^sha256:[0-9a-f]{64}$/u;

/** The release `args` (a version, then `image=digest` for each released image) say; it throws, saying why, when they are not one. */
export function parsePin(args: readonly string[]): ReleasePin {
  const [version, ...pairs] = args;
  if (version === undefined || !VERSION.test(version)) throw new Error(`${version ?? "no version"} is not a version such as 4.0.0`);
  const given = new Map(pairs.map((pair) => {
    const [image = "", digest = ""] = pair.split("=");
    if (!RELEASED_IMAGES.some((name) => name === image)) throw new Error(`${image} is not one of the released images, ${RELEASED_IMAGES.join(", ")}`);
    if (!DIGEST.test(digest)) throw new Error(`${digest || "nothing"}, the digest given for ${image}, is not a sha256 digest`);
    return [image, digest] as const;
  }));
  if (given.size !== pairs.length) throw new Error("an image is given more than one digest");
  const missing = RELEASED_IMAGES.filter((image) => !given.has(image));
  if (missing.length > 0) throw new Error(`no digest is given for ${missing.join(", ")}`);
  const digest = (image: ReleasedImage): string => given.get(image) ?? "";
  return { version, digests: { "alasio": digest("alasio"), "alasio-agent": digest("alasio-agent"), "alasio-lake": digest("alasio-lake"), "alasio-node": digest("alasio-node") } };
}

/** cli/src/release.ts as it is for `pin`. */
export function releaseModule({ version, digests }: ReleasePin): string {
  const image = (name: ReleasedImage) => `{ repository: "ghcr.io/eaucoin/${name}", tag: VERSION, digest: ${JSON.stringify(digests[name])} }`;
  return `/**
 * What this version of the package installs. The release workflow writes the released
 * version and the digests of the images it published here, with tooling/pin-release.ts,
 * before it builds the package; this module holds nothing else.
 */
export const VERSION = ${JSON.stringify(version)};

/** alasio's own images, tagged with the version and pinned by digest once released. */
export const IMAGES = {
  alasio: ${image("alasio")},
  agent: ${image("alasio-agent")},
  lake: ${image("alasio-lake")},
};

/** The node image of the cluster in Docker: k3s with gVisor (cluster/node), pinned like the others. */
export const NODE_IMAGE = ${image("alasio-node")};
`;
}

/** Where release.ts is. */
export const RELEASE_MODULE = new URL("../cli/src/release.ts", import.meta.url);

if (import.meta.main) {
  const pin = parsePin(process.argv.slice(2));
  writeFileSync(RELEASE_MODULE, releaseModule(pin));
  console.log(`pinned alasio ${pin.version}: ${RELEASED_IMAGES.map((image) => `${image}@${pin.digests[image]}`).join(", ")}`);
}
