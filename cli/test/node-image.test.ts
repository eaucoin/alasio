/**
 * The node image's pins (cluster/node/pins.json), which its every build passes as build
 * arguments (tooling/node-image.ts): the Dockerfile takes each, and has none of its own.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import pins from "../../cluster/node/pins.json" with { type: "json" };
import { NODE_BUILD_ARGS } from "../../tooling/node-image.ts";

const DOCKERFILE = readFileSync(new URL("../../cluster/node/Dockerfile", import.meta.url), "utf8");

test("the node image is built from the k3s image and the gVisor release pins.json pins", () => {
  assert.deepEqual(NODE_BUILD_ARGS, [
    `--build-arg=K3S_IMAGE=${pins.k3s.image}`,
    `--build-arg=GVISOR_RELEASE=${pins.gvisor.release}`,
    `--build-arg=GVISOR_SHA512=${pins.gvisor.sha512}`,
  ]);
  assert.match(pins.k3s.image, new RegExp(`^rancher/k3s:${pins.k3s.version.replace("+", "-")}@sha256:[0-9a-f]{64}$`, "u"));
});

test("the Dockerfile declares every pin passed, and pins nothing itself", () => {
  const declared = [...DOCKERFILE.matchAll(/^ARG (\S+)$/gmu)].map(([, name]) => name);
  assert.deepEqual(declared, NODE_BUILD_ARGS.map((arg) => /^--build-arg=([A-Z0-9_]+)=/u.exec(arg)?.[1]));
  assert.doesNotMatch(DOCKERFILE, /^ARG \S+=/mu);
  assert.match(DOCKERFILE, /^FROM \$\{K3S_IMAGE\}$/mu);
});
