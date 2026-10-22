/**
 * A member of a group on the log (src/log/topic.ts), as alasio's three groups join theirs: its
 * place kept by heartbeats often enough that the broker ends it within seconds of its last.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { memberOptions } from "../src/log/topic.ts";

test("a member's session ends ten seconds after its last heartbeat, which come every three, and a rebalance waits ten seconds at most", () => {
  const directory = mkdtempSync(join(tmpdir(), "alasio-log-topic-"));
  try {
    const passwordFile = join(directory, "password");
    writeFileSync(passwordFile, "secret\n");
    const options = memberOptions({ brokers: ["log:9092"], username: "alasio-main", passwordFile, topic: "telegram.main", group: "alasio.main" });
    assert.deepEqual([options.sessionTimeout, options.heartbeatInterval, options.rebalanceTimeout], [10_000, 3_000, 10_000]);
    // Three heartbeats may go missing before the broker ends the session.
    assert.ok(options.heartbeatInterval * 3 <= options.sessionTimeout);
    // No session the broker refuses: none under its default group.min.session.timeout.ms.
    assert.ok(options.sessionTimeout >= 6_000);
    assert.deepEqual([options.groupId, options.sasl.password], ["alasio.main", "secret"]);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
