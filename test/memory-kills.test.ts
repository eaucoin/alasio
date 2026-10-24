/**
 * What tells a harness's process the kernel killed for its memory (src/shared/memory-kills.ts),
 * of a cgroup laid out as a container's own under cgroup v2: its kills counted from a process's
 * start, and the limit it ran out of; and Codex's app-server ending so (src/codex/app-server/process.ts).
 */
import assert from "node:assert/strict";
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Cause, Effect, Exit, Option } from "effect";

import { makeSpawnAppServer } from "../src/codex/app-server/process.ts";
import { asQuantity, ranOutOf, watchMemoryKills } from "../src/shared/memory-kills.ts";

/** A cgroup's directory of the test's own, its kills counted at `kills` and its limit `max`, as memory.events and memory.max say them. */
function cgroup(kills: number, max: string): { readonly dir: string; readonly killed: (kills: number) => void; readonly remove: () => void } {
  const dir = mkdtempSync(join(tmpdir(), "alasio-cgroup-"));
  const killed = (count: number) => writeFileSync(join(dir, "memory.events"), `low 0\nhigh 0\nmax 4\noom 2\noom_kill ${count}\noom_group_kill 0\n`);
  killed(kills);
  writeFileSync(join(dir, "memory.max"), `${max}\n`);
  return { dir, killed, remove: () => rmSync(dir, { recursive: true, force: true }) };
}

test("a kill for memory is a process's own only where the cgroup counts one since it started, said with the limit the cgroup has, and none where its files cannot be read", () => {
  const limited = cgroup(3, String(1536 * 2 ** 20));
  const unlimited = cgroup(0, "max");
  try {
    // Counted before it started: not its own.
    const started = watchMemoryKills(limited.dir);
    assert.equal(started(), null);
    limited.killed(4);
    assert.deepEqual(started(), { limit: "1536Mi" });
    // Told once: asked again with none counted since, none.
    assert.equal(started(), null);
    assert.equal(watchMemoryKills(limited.dir)(), null);
    // Of a cgroup with no limit, as the machine running out kills one.
    const free = watchMemoryKills(unlimited.dir);
    unlimited.killed(1);
    assert.deepEqual(free(), { limit: null });
    // Under cgroup v1, or none: nothing said.
    const unread = watchMemoryKills(join(limited.dir, "none"));
    assert.equal(unread(), null);
  } finally {
    limited.remove();
    unlimited.remove();
  }
  assert.deepEqual([asQuantity(4 * 2 ** 30), asQuantity(1536 * 2 ** 20), asQuantity(1000)], ["4Gi", "1536Mi", "1000"]);
  assert.deepEqual([ranOutOf({ limit: "4Gi" }), ranOutOf({ limit: null })], ["ran out of its memory (4Gi)", "ran out of memory"]);
});

test("Codex's app-server killed as its cgroup counts a kill for memory ends saying it ran out of its memory, and one killed otherwise as it did", async () => {
  const counted = cgroup(0, String(1536 * 2 ** 20));
  const bin = join(counted.dir, "codex");
  const before = process.env["ALASIO_CODEX_BIN"];
  try {
    process.env["ALASIO_CODEX_BIN"] = bin;
    /** How an app-server that kills itself ends, its cgroup's kills counted at `kills` first. */
    const ending = async (kills: number) => {
      writeFileSync(bin, `#!/bin/sh\nprintf 'low 0\\noom_kill ${kills}\\n' > '${join(counted.dir, "memory.events")}'\nkill -9 $$\n`);
      chmodSync(bin, 0o755);
      const exit = await Effect.runPromiseExit(Effect.scoped(Effect.flatMap(makeSpawnAppServer(counted.dir)({ cwd: counted.dir, env: { PATH: process.env["PATH"] ?? "" } }), (server) => server.ended)));
      assert.ok(Exit.isFailure(exit));
      return Exit.isFailure(exit) ? Option.match(Cause.findErrorOption(exit.cause), { onNone: () => "", onSome: (error) => error.message }) : "";
    };
    assert.equal(await ending(1), "Codex app-server ran out of its memory (1536Mi) and was killed by the kernel");
    // Killed again with no kill counted since it started.
    assert.equal(await ending(1), "Codex app-server exited code=null signal=SIGKILL");
  } finally {
    if (before === undefined) delete process.env["ALASIO_CODEX_BIN"];
    else process.env["ALASIO_CODEX_BIN"] = before;
    counted.remove();
  }
});
