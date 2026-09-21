import assert from "node:assert/strict";
import { test } from "node:test";

import { looksLikeSelfRestartCommand, looksLikeSelfRestartNearMiss } from "../src/policy/restart-command.js";

test("self-restart detector accepts the Alasio restart wrapper", () => {
  const commands = [
    "/home/operator/monorepo-alasio-runtime/bots/alasio/restart-alasio-operator.sh",
    "cd /home/operator/monorepo-alasio-runtime/bots/alasio && ./restart-alasio-operator.sh",
    "cd /home/operator/monorepo-alasio-runtime && bots/alasio/restart-alasio-operator.sh",
    "bash -lc 'cd /home/operator/monorepo-alasio-runtime/bots/alasio && ./restart-alasio-operator.sh'",
    "bash /home/operator/monorepo-alasio-runtime/bots/alasio/restart-alasio-operator.sh",
    "/bin/bash -lc ./restart-alasio-operator.sh",
  ];

  for (const command of commands) {
    assert.equal(looksLikeSelfRestartCommand(command), true, command);
    assert.equal(looksLikeSelfRestartNearMiss(command), false, command);
  }
});

test("self-restart detector accepts direct alasio systemd restarts", () => {
  const commands = [
    "sudo -n systemctl restart alasio.service",
    "/usr/bin/sudo -n /usr/bin/systemctl restart alasio",
    "bash -lc 'sudo -n systemctl restart alasio.service'",
  ];

  for (const command of commands) {
    assert.equal(looksLikeSelfRestartCommand(command), true, command);
    assert.equal(looksLikeSelfRestartNearMiss(command), false, command);
  }
});

test("self-restart detector flags restart-shaped commands with extra args", () => {
  const commands = [
    "sudo -n systemctl restart alasio.service --now",
    "cd /home/operator/monorepo-alasio-runtime/bots/alasio && ./restart-alasio-operator.sh --bad",
    "bash -lc 'cd /home/operator/monorepo-alasio-runtime && bots/alasio/restart-alasio-operator.sh --bad'",
  ];

  for (const command of commands) {
    assert.equal(looksLikeSelfRestartCommand(command), false, command);
    assert.equal(looksLikeSelfRestartNearMiss(command), true, command);
  }
});
