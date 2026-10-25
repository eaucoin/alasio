/** What alasio's telemetry says of its service (src/telemetry/start.ts). */
import assert from "node:assert/strict";
import { test } from "node:test";

import { REVISION_VARIABLE, serviceAttributes } from "../src/telemetry/start.ts";

test("alasio's telemetry says the commit its image was built from, and asks git of none, in a checkout or not", () => {
  // Run in this checkout, whose HEAD git would tell: what says the commit is the image's build alone.
  assert.deepEqual(serviceAttributes({}), { "service.name": "alasio" });
  assert.deepEqual(serviceAttributes({ [REVISION_VARIABLE]: " " }), { "service.name": "alasio" });
  assert.deepEqual(serviceAttributes({ [REVISION_VARIABLE]: "2ba2b9bb" }), { "service.name": "alasio", "vcs.ref.head.revision": "2ba2b9bb" });
});
