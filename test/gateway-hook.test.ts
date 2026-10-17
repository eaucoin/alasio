import assert from "node:assert/strict";
import { test } from "node:test";

import { hookOf } from "../src/gateway/hook.ts";

test("bayma's hook is bayma as its container runs it, after its init, told the state directory it serves", () => {
  assert.deepEqual(
    hookOf({ name: "bayma", command: ["/usr/bin/tini", "-s", "--", "node", "/opt/bayma/bayma.js"], args: ["mcp-http", "--state-dir", "/home/agent/.bayma"] }),
    ["node", "/opt/bayma/bayma.js", "sandbox-hook", "--state-dir", "/home/agent/.bayma"],
  );
  assert.deepEqual(hookOf({ name: "bayma", command: ["bayma"], args: ["mcp-http", "--state-dir", "/state"] }), ["bayma", "sandbox-hook", "--state-dir", "/state"]);
  assert.equal(hookOf({ name: "bayma", command: ["bayma"], args: ["mcp-http"] }), null);
});
