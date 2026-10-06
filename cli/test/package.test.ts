/**
 * What alasio's npm package may hold (tooling/cli-package.ts): the built command line,
 * with the CRD it applies and the node's pins and containerd template, and nothing else; CI's package job checks the package npm
 * packs against it.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { packageProblems, REQUIRED } from "../../tooling/cli-package.ts";

test("a package of the built command line alone has no problem", () => {
  assert.deepEqual(packageProblems(["package.json", ...REQUIRED, "dist/cli/src/main.js.map", "dist/src/kube/config.js"]), []);
});

test("sources, tests and what is not built are refused, and what it needs is required", () => {
  assert.deepEqual(packageProblems(["package.json", "dist/cli/src/main.js", "src/main.ts", "dist/cli/test/commands.test.js", "dist/cli/src/config.d.ts"]), [
    "src/main.ts is not of the built command line",
    "dist/cli/src/config.d.ts is not of the built command line",
    "dist/cli/test/commands.test.js is a test's",
    "dist/cli/src/manifests/sandboxes.agents.x-k8s.io.json is missing",
    "dist/cluster/node/pins.json is missing",
    "dist/cluster/node/config-v3.toml.tmpl is missing",
  ]);
});
