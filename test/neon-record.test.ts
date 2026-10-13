/**
 * neon-control's record in Postgres (neon/control/record.ts), as two replicas share it:
 * made once by either, its branches changed by one at a time, read by both, and led by
 * one; its locks on connections of their own, which reads never wait behind, and given up
 * after their patience.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import type { Branch } from "../neon/control/branches.ts";
import pg from "pg";

import { BranchRecord, RecordBusy } from "../neon/control/record.ts";
import { startPostgres } from "./support/postgres.ts";

const main: Branch = {
  name: "main",
  parent: null,
  timelineId: "1".repeat(32),
  lsn: null,
  createdAt: "2026-10-12T00:00:00.000Z",
  compute: null,
  state: "ready",
  safekeepers: { generation: 1, ids: [1, 2, 3] },
};
const dev: Branch = { ...main, name: "dev", parent: "main", timelineId: "2".repeat(32), lsn: "0/16B5A58", createdAt: "2026-10-12T01:00:00.000Z", state: "creating" };

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test("two replicas make the record once, and each reads what the other saved", async () => {
  const database = await startPostgres();
  const [one, two] = [new BranchRecord(database.url), new BranchRecord(database.url)];
  try {
    await Promise.all([one.ensureSchema(), two.ensureSchema()]);
    await one.ensureSchema();
    assert.deepEqual(await two.branches(), []);
    await one.changing(({ save }) => save([main]));
    await two.changing(async ({ branches, save }) => {
      assert.deepEqual(branches, [main]);
      await save([...branches, dev]);
    });
    assert.deepEqual(await one.branches(), [main, dev]);
    await one.changing(({ branches, save }) => save(branches.filter(({ name }) => name !== "dev")));
    assert.deepEqual(await two.branches(), [main]);
  } finally {
    await Promise.all([one.end(), two.end()]);
    await database.stop();
  }
});

test("two replicas change the branches one at a time, each from what the other recorded", async () => {
  const database = await startPostgres();
  const [one, two] = [new BranchRecord(database.url), new BranchRecord(database.url)];
  try {
    await one.ensureSchema();
    const order: string[] = [];
    let release = () => {};
    const held = new Promise<void>((resolve) => (release = resolve));
    const first = one.changing(async ({ save }) => {
      order.push("one starts");
      // Saved at once, as a creation records its timeline before the storage controller is asked.
      await save([main, dev]);
      await held;
      order.push("one ends");
    });
    await sleep(200);
    const second = two.changing(async ({ branches }) => {
      order.push("two starts");
      assert.deepEqual(branches, [main, dev], "it finds what the first saved, as it was then");
    });
    await sleep(500);
    assert.deepEqual(order, ["one starts"], "the second waits");
    release();
    await Promise.all([first, second]);
    assert.deepEqual(order, ["one starts", "one ends", "two starts"]);
  } finally {
    await Promise.all([one.end(), two.end()]);
    await database.stop();
  }
});

test("a change that fails leaves the lock to the next", async () => {
  const database = await startPostgres();
  const record = new BranchRecord(database.url);
  try {
    await record.ensureSchema();
    await assert.rejects(record.changing(() => Promise.reject(new Error("failed"))), /failed/u);
    assert.equal(await Promise.race([record.changing(async () => "changed"), sleep(5000).then(() => "waited")]), "changed");
  } finally {
    await record.end();
    await database.stop();
  }
});

test("one replica leads, until it ends, or its connection is lost, and then another", async () => {
  const database = await startPostgres();
  const [one, two, three] = [new BranchRecord(database.url), new BranchRecord(database.url), new BranchRecord(database.url)];
  const admin = new pg.Client({ connectionString: database.url });
  await admin.connect();
  try {
    assert.ok(await one.leads());
    assert.ok(!(await two.leads()));
    assert.ok(await one.leads(), "and goes on leading");
    await one.end();
    assert.ok(await two.leads());
    // Its connection ended by the server, as a network gone would: it finds out as it asks, and another leads.
    await admin.query("select pg_terminate_backend(pid) from pg_locks where locktype = 'advisory' and granted and pid <> pg_backend_pid()");
    assert.ok(await three.leads());
    assert.ok(!(await two.leads()), "it asks its connection, which is gone, and does not lead");
  } finally {
    await admin.end();
    await Promise.all([two.end(), three.end()]);
    await database.stop();
  }
});

test("each lock is held on a connection of its own, so reads go on however many wait, and is given up after its patience", async () => {
  const database = await startPostgres();
  const [one, two] = [new BranchRecord(database.url), new BranchRecord(database.url)];
  try {
    await one.ensureSchema();
    let release = () => {};
    const held = new Promise<void>((resolve) => (release = resolve));
    const holding = one.changing(() => held);
    await sleep(200);
    // More waiting than the pool has connections.
    const waiting = Array.from({ length: 6 }, () => two.changing(async () => "changed"));
    await sleep(200);
    assert.deepEqual(await Promise.race([two.branches(), sleep(5000).then(() => "waited")]), []);
    assert.equal(await two.tryChanging(async () => "changed"), null, "one that only tries does not wait");
    release();
    await holding;
    assert.deepEqual(await Promise.all(waiting), Array(6).fill("changed"));
    assert.deepEqual(await two.tryChanging(async () => "changed"), { value: "changed" });

    // Given up after its patience, of a lock never released.
    const patient = new BranchRecord(database.url, { patienceMs: 1000 });
    const forever = new Promise<void>(() => {});
    void one.changing(() => forever);
    await sleep(200);
    const started = Date.now();
    await assert.rejects(patient.changing(async () => "changed"), RecordBusy);
    assert.ok(Date.now() - started >= 900);
    await patient.end();
  } finally {
    await Promise.all([one.end(), two.end()]);
    await database.stop();
  }
});
