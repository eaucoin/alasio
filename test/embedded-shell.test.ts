import assert from "node:assert/strict";
import { test } from "node:test";

import { isBlockedDbCommand } from "../src/policy/db-guardrail.ts";
import { extractShellCommands } from "../src/policy/embedded-shell.ts";
import { looksLikeSelfRestartCommand } from "../src/policy/restart-command.ts";

const restarts = (code: string) => extractShellCommands(code).some((command) => looksLikeSelfRestartCommand(command, { env: {} }));
const blocked = (code: string) =>extractShellCommands(code).some(isBlockedDbCommand);

test("shell commands are recovered from Bun shell, spawn and exec calls and bare command lines", () => {
  assert.equal(restarts("await $`kubectl -n alasio rollout restart deployment/alasio`"), true);
  assert.equal(restarts('import { $ } from "bun";\nconst r = await $`kubectl rollout restart deploy/alasio`.quiet();'), true);
  assert.equal(restarts('execSync("kubectl rollout restart deployment/alasio")'), true);
  assert.equal(restarts("kubectl rollout restart deployment/alasio"), true);
  assert.equal(blocked('Bun.spawnSync(["psql", "-c", "drop table users"])'), true);
  assert.equal(blocked('await $`psql -c "drop table users"`'), true);
  assert.equal(restarts("const x = 1 + 1"), false);
  assert.equal(blocked("await $`ls ${dir}`"), false);
  assert.deepEqual(extractShellCommands(""), []);
  assert.deepEqual(extractShellCommands(undefined), []);
});
