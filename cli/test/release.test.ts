/**
 * The release the package installs (cli/src/release.ts), which the release workflow pins
 * with tooling/pin-release.ts: the module is exactly what the tool writes, so pinning it
 * changes the version and the digests and nothing else.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import { DEVELOPMENT, parsePin, RELEASE_MODULE, releaseModule } from "../../tooling/pin-release.ts";

const DIGEST = (digit: string) => `sha256:${digit.repeat(64)}`;
const IMAGES = [`alasio=${DIGEST("1")}`, `alasio-agent=${DIGEST("2")}`, `alasio-lake=${DIGEST("3")}`, `alasio-grafana=${DIGEST("5")}`, `alasio-node=${DIGEST("4")}`];

test("release.ts is what the tool writes for a development version, with no image pinned", () => {
  assert.equal(readFileSync(RELEASE_MODULE, "utf8"), releaseModule(DEVELOPMENT));
});

test("a release is pinned to its version and each image's digest, and to nothing else", async () => {
  const pinned = releaseModule(parsePin(["4.0.0", ...IMAGES]));
  const lines = (text: string) => text.split("\n");
  const changed = lines(pinned).filter((line, index) => line !== lines(releaseModule(DEVELOPMENT))[index]);
  assert.deepEqual(changed, [
    "export const VERSION = \"4.0.0\";",
    `  alasio: { repository: "ghcr.io/eaucoin/alasio", tag: VERSION, digest: "${DIGEST("1")}" },`,
    `  agent: { repository: "ghcr.io/eaucoin/alasio-agent", tag: VERSION, digest: "${DIGEST("2")}" },`,
    `  lake: { repository: "ghcr.io/eaucoin/alasio-lake", tag: VERSION, digest: "${DIGEST("3")}" },`,
    `  grafana: { repository: "ghcr.io/eaucoin/alasio-grafana", tag: VERSION, digest: "${DIGEST("5")}" },`,
    `export const NODE_IMAGE = { repository: "ghcr.io/eaucoin/alasio-node", tag: VERSION, digest: "${DIGEST("4")}" };`,
  ]);
  const module: typeof import("../src/release.ts") = await import(`data:text/javascript,${encodeURIComponent(pinned)}`);
  assert.equal(module.VERSION, "4.0.0");
  assert.deepEqual(module.IMAGES.agent, { repository: "ghcr.io/eaucoin/alasio-agent", tag: "4.0.0", digest: DIGEST("2") });
  assert.deepEqual(module.NODE_IMAGE, { repository: "ghcr.io/eaucoin/alasio-node", tag: "4.0.0", digest: DIGEST("4") });
});

test("a pin that is not a release's is refused, saying why", () => {
  assert.throws(() => parsePin([]), /^Error: no version is not a version such as 4\.0\.0$/u);
  assert.throws(() => parsePin(["v4.0.0", ...IMAGES]), /^Error: v4\.0\.0 is not a version such as 4\.0\.0$/u);
  assert.throws(() => parsePin(["4.0.0", ...IMAGES.slice(1)]), /^Error: no digest is given for alasio$/u);
  assert.throws(() => parsePin(["4.0.0", ...IMAGES, IMAGES[0] ?? ""]), /^Error: an image is given more than one digest$/u);
  assert.throws(() => parsePin(["4.0.0", ...IMAGES, `alasio-bot=${DIGEST("6")}`]), /^Error: alasio-bot is not one of the released images, alasio, alasio-agent, alasio-lake, alasio-grafana, alasio-node$/u);
  assert.throws(() => parsePin(["4.0.0", "alasio=sha256:abc", ...IMAGES.slice(1)]), /^Error: sha256:abc, the digest given for alasio, is not a sha256 digest$/u);
});
