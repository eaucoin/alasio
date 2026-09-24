import assert from "node:assert/strict";
import { test } from "node:test";

import { isBlockedDbCommand } from "../src/policy/db-guardrail.js";
import { extractShellCommands } from "../src/policy/embedded-shell.js";
import { looksLikeSelfRestartCommand } from "../src/policy/restart-command.js";

const restarts = (code) => extractShellCommands(code).some(looksLikeSelfRestartCommand);
const blocked = (code) => extractShellCommands(code).some(isBlockedDbCommand);

test("shell commands are recovered from Bun shell, spawn and exec calls and bare command lines", () => {
  assert.equal(restarts("await $`./restart-alasio-standalone.sh`"), true);
  assert.equal(restarts('import { $ } from "bun";\nconst r = await $`sudo systemctl restart alasio-standalone.service`.quiet();'), true);
  assert.equal(restarts('execSync("/home/operator/alasio/restart-alasio-standalone.sh")'), true);
  assert.equal(restarts("./restart-alasio-standalone.sh"), true);
  assert.equal(blocked('Bun.spawnSync(["psql", "-c", "drop table users"])'), true);
  assert.equal(blocked('await $`psql -c "drop table users"`'), true);
  assert.equal(restarts("const x = 1 + 1"), false);
  assert.equal(blocked("await $`ls ${dir}`"), false);
  assert.deepEqual(extractShellCommands(""), []);
  assert.deepEqual(extractShellCommands(undefined), []);
});
