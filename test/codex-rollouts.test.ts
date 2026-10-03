import assert from "node:assert/strict";
import { appendFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, statSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { after, before, describe, test } from "node:test";

import pg from "pg";

import { listRolloutFiles, parseRolloutName } from "../src/codex/rollouts/files.ts";
import { startCodexRollouts } from "../src/codex/rollouts/index.ts";
import { type KnownRollouts, mirrorRollout } from "../src/codex/rollouts/mirror.ts";
import { restoreRollouts } from "../src/codex/rollouts/restore.ts";
import { NeonRolloutStore } from "../src/codex/rollouts/store.ts";
import { dockerAvailable, startPostgres, type TestPostgres } from "./support/postgres.ts";

const skip = !dockerAvailable() && "needs Docker for a throwaway Postgres";

const THREAD_A = "01a0cadd-b753-7d42-84a0-15a98e372686";
const THREAD_B = "01a0e957-a6ff-7691-8d31-ed8d7315fa68";
const REVISION = "01a0e93a-28f1-7af0-92b6-2ac47098a852";
const THREAD_C = "01a0e93a-37f1-7342-b4ae-5e02dd4dba61";
const fileName = (threadId: string, rolloutId?: string) =>`rollout-2026-09-28T18-47-10-${threadId}${rolloutId ? `_${rolloutId}` : ""}.jsonl`;
const DAY = "sessions/2026/09/28";

/** A rollout's first line, Codex's session_meta, with the one field the store reads. */
const meta = (id: string, historyBase: string | null = null) =>
  `${JSON.stringify({ type: "session_meta", payload: { id, ...(historyBase ? { history_base: { thread_id: historyBase, end_ordinal_exclusive: 1, end_byte_offset: 10 } } : {}) } })}\n`;

test("rollout names are Codex's: a thread id, and a reverted thread's rollout id after it", () => {
  assert.deepEqual(parseRolloutName(fileName(THREAD_A)), { threadId: THREAD_A, rolloutId: THREAD_A });
  assert.deepEqual(parseRolloutName(fileName(THREAD_A, REVISION)), { threadId: THREAD_A, rolloutId: REVISION });
  assert.equal(parseRolloutName(`${fileName(THREAD_A)}.restoring`), null);
  assert.equal(parseRolloutName("rollout-notes.jsonl"), null);
});

test("rollout files are found under sessions and archived_sessions, compressed ones by their plain name", () => {
  const home = mkdtempSync(join(tmpdir(), "alasio-rollouts-"));
  try {
    mkdirSync(join(home, DAY), { recursive: true });
    mkdirSync(join(home, "archived_sessions"), { recursive: true });
    writeFileSync(join(home, DAY, fileName(THREAD_A)), meta(THREAD_A));
    writeFileSync(join(home, "archived_sessions", `${fileName(THREAD_B)}.zst`), "compressed");
    writeFileSync(join(home, DAY, "notes.txt"), "not a rollout");
    const files = listRolloutFiles(home).map(({ name, path, compressed }) => ({ name, path, compressed }));
    assert.deepEqual(files.sort((a, b) => a.name.localeCompare(b.name)), [
      { name: fileName(THREAD_A), path: join(DAY, fileName(THREAD_A)), compressed: false },
      { name: fileName(THREAD_B), path: join("archived_sessions", `${fileName(THREAD_B)}.zst`), compressed: true },
    ]);
    assert.deepEqual(listRolloutFiles(join(home, "missing")), []);
  } finally {
    rmSync(home, { recursive: true, force: true });
  }
});

describe("the rollout store", { skip }, () => {
  // Set by the first hook, which runs unless the suite is skipped.
  let database: TestPostgres | undefined;
  let pool: pg.Pool;
  let schemas = 0;
  const homes: string[] = [];

  before(async () => {
    database = await startPostgres();
    pool = new pg.Pool({ connectionString: database.url, max: 4 });
  });

  after(async () => {
    await pool?.end();
    await database?.stop();
    for (const home of homes) rmSync(home, { recursive: true, force: true });
  });

  /** A store on a schema of its own, a Codex home, and the mirror's pass over them. */
  async function makeMirror() {
    const schema = `rollouts_${process.pid}_${++schemas}`;
    const store = new NeonRolloutStore(pool, { schema });
    await store.ensureSchema();
    await store.ensureSchema();
    const home = mkdtempSync(join(tmpdir(), "alasio-rollouts-"));
    homes.push(home);
    const known: KnownRollouts = new Map();
    /** Mirrors every file, as the check does. Returns how many had bytes mirrored. */
    const pass = async () => {
      let mirrored = 0;
      for (const file of listRolloutFiles(home)) {
        if (await mirrorRollout({ store, home, known, file })) mirrored += 1;
      }
      return mirrored;
    };
    const write = (path: string, text: string) => {
      mkdirSync(dirname(join(home, path)), { recursive: true });
      writeFileSync(join(home, path), text);
    };
    const chunkCount = async () =>
      (await pool.query<{ count: number }>(`select count(*)::int as count from ${schema}.rollout_chunks`)).rows[0]?.count;
    return { store, home, known, pass, write, chunkCount };
  }

  const read = (home: string, path: string) =>readFileSync(join(home, path));

  test("a new file is kept whole, and a grown one gets only its new bytes", async () => {
    const { store, home, known, pass, write, chunkCount } = await makeMirror();
    const path = join(DAY, fileName(THREAD_A));
    write(path, meta(THREAD_A));
    assert.equal(await pass(), 1);
    assert.equal(await pass(), 0);
    appendFileSync(join(home, path), '{"type":"response_item"}\n{"type":"event_');
    assert.equal(await pass(), 1);
    assert.deepEqual(await store.read(fileName(THREAD_A)), read(home, path));
    assert.equal(known.get(fileName(THREAD_A))?.size,statSync(join(home, path)).size);
    assert.equal(await chunkCount(), 2);
  });

  test("a file rewritten with another first line, or shorter, is kept whole again", async () => {
    const { store, home, pass, write, chunkCount } = await makeMirror();
    const path = join(DAY, fileName(THREAD_A));
    write(path, `${meta(THREAD_A)}{"type":"turn"}\n`);
    await pass();
    write(path, `${meta(THREAD_A, THREAD_B)}{"type":"turn"}\n{"type":"more"}\n`);
    assert.equal(await pass(), 1);
    assert.deepEqual(await store.read(fileName(THREAD_A)), read(home, path));
    write(path, meta(THREAD_A));
    assert.equal(await pass(), 1);
    assert.deepEqual(await store.read(fileName(THREAD_A)), read(home, path));
    assert.equal(await chunkCount(), 1);
  });

  test("an archived file only has its place updated", async () => {
    const { store, home, pass, write, chunkCount } = await makeMirror();
    write(join(DAY, fileName(THREAD_A)), meta(THREAD_A));
    await pass();
    mkdirSync(join(home, "archived_sessions"));
    renameSync(join(home, DAY, fileName(THREAD_A)), join(home, "archived_sessions", fileName(THREAD_A)));
    assert.equal(await pass(), 0);
    assert.deepEqual((await store.list()).map(({ name, path }) => ({ name, path })), [
      { name: fileName(THREAD_A), path: join("archived_sessions", fileName(THREAD_A)) },
    ]);
    assert.equal(await chunkCount(), 1);
  });

  test("a file without a whole first line yet, and a compressed file, are left alone", async () => {
    const { store, pass, write } = await makeMirror();
    write(join(DAY, fileName(THREAD_A)), '{"type":"session_meta","payl');
    write(join(DAY, `${fileName(THREAD_B)}.zst`), "compressed");
    assert.equal(await pass(), 0);
    assert.deepEqual(await store.list(), []);
  });

  test("restore writes back a thread's missing files and the files its history starts in, as they were", async () => {
    const { store, home, pass, write } = await makeMirror();
    const base = join(DAY, fileName(THREAD_A));
    const revert = join(DAY, fileName(THREAD_A, REVISION));
    const fork = join(DAY, fileName(THREAD_B));
    const other = join(DAY, fileName(THREAD_C));
    write(base, `${meta(THREAD_A)}{"turn":1}\n`);
    write(revert, meta(THREAD_A, THREAD_A));
    write(fork, `${meta(THREAD_B, REVISION)}{"turn":2}\n`);
    write(other, meta(THREAD_C));
    utimesSync(join(home, fork), new Date(1_790_000_000_000), new Date(1_790_000_000_000));
    await pass();
    const originals = Object.fromEntries([base, revert, fork, other].map((path) => [path, read(home, path)]));
    const fresh = mkdtempSync(join(tmpdir(), "alasio-rollouts-"));
    homes.push(fresh);

    // The fork's history starts in the reverted thread's newer file, and that
    // one's in the thread's first: all three, and nothing else.
    const written = await restoreRollouts({ store, threadIds: [THREAD_B], home: fresh });
    assert.deepEqual(written.sort(), [base, revert, fork].sort());
    for (const path of written) assert.deepEqual(read(fresh, path), originals[path]);
    assert.equal(statSync(join(fresh, fork)).mtimeMs, 1_790_000_000_000);
    assert.equal(existsSync(join(fresh, other)), false);
    assert.deepEqual(await restoreRollouts({ store, threadIds: [THREAD_B], home: fresh }), []);
    assert.deepEqual(await restoreRollouts({ store, threadIds: [], home: fresh }), []);
  });

  test("a change is mirrored as it is written, long before the half-minute check, in a new home and day folders made later too", async () => {
    const { store, home, write } = await makeMirror();
    const rollouts = startCodexRollouts({ store, home });
    // Codex makes sessions/ with a new home's first thread, after the mirror started.
    await new Promise((resolve) => setTimeout(resolve, 100));
    write(join(DAY, fileName(THREAD_A)), meta(THREAD_A));
    try {
      const mirrored = async (name: string, path: string) => {
        const deadline = Date.now() + 3_000;
        while (Date.now() < deadline) {
          const kept = (await store.list()).find((row) => row.name === name);
          if (kept && kept.size === statSync(join(home, path)).size) return (await store.read(name)).equals(read(home, path));
          await new Promise((resolve) => setTimeout(resolve, 20));
        }
        return false;
      };
      assert.ok(await mirrored(fileName(THREAD_A), join(DAY, fileName(THREAD_A))));
      appendFileSync(join(home, DAY, fileName(THREAD_A)), '{"type":"turn"}\n');
      assert.ok(await mirrored(fileName(THREAD_A), join(DAY, fileName(THREAD_A))));
      const nextDay = join("sessions/2026/09/29", fileName(THREAD_B));
      write(nextDay, meta(THREAD_B));
      assert.ok(await mirrored(fileName(THREAD_B), nextDay));
    } finally {
      await rollouts.close();
    }
  });

  test("flush mirrors a thread's files before it returns, and restore writes them back", async () => {
    const { store, home, write } = await makeMirror();
    mkdirSync(join(home, DAY), { recursive: true });
    const rollouts = startCodexRollouts({ store, home });
    try {
      write(join(DAY, fileName(THREAD_A)), `${meta(THREAD_A)}{"turn":1}\n`);
      write(join(DAY, fileName(THREAD_A, REVISION)), meta(THREAD_A, THREAD_A));
      await rollouts.flush(THREAD_A);
      assert.deepEqual((await store.list()).map(({ name }) => name).sort(), [fileName(THREAD_A), fileName(THREAD_A, REVISION)].sort());
      const fresh = mkdtempSync(join(tmpdir(), "alasio-rollouts-"));
      homes.push(fresh);
      const restored = startCodexRollouts({ store, home: fresh });
      try {
        assert.equal((await restored.restore([THREAD_A])).length, 2);
      } finally {
        await restored.close();
      }
    } finally {
      await rollouts.close();
    }
  });

  test("restore leaves a file present, or present compressed, as it is", async () => {
    const { store, home, pass, write } = await makeMirror();
    const path = join(DAY, fileName(THREAD_A));
    write(path, meta(THREAD_A));
    await pass();
    writeFileSync(join(home, path), "changed here");
    assert.deepEqual(await restoreRollouts({ store, threadIds: [THREAD_A], home }), []);
    assert.equal(readFileSync(join(home, path), "utf8"), "changed here");
    renameSync(join(home, path), join(home, `${path}.zst`));
    assert.deepEqual(await restoreRollouts({ store, threadIds: [THREAD_A], home }), []);
    assert.equal(existsSync(join(home, path)), false);
  });
});
