/**
 * neon-control as the control plane of Neon's proxy (neon/control/endpoints.ts): which
 * role's verifier it answers on which compute, the errors the proxy reads, when a
 * compute is idle, and how one is woken; and a compute's activity, counted as neon-control counts it
 * (neon/control/computes.ts), on a real Postgres.
 */
import assert from "node:assert/strict";
import { describe, test } from "node:test";

import pg from "pg";

import type { ComputeCredentials } from "../neon/control/branches.ts";
import { computeActivity, livePods } from "../neon/control/computes.ts";
import { controlPlaneError, idleness, lsnDistance, proxyQuery, roleSecret, suspendDue, wakeCompute } from "../neon/control/endpoints.ts";
import { scramVerifier } from "../neon/control/scram.ts";
import { startPostgres } from "./support/postgres.ts";

/** dev's compute's own credentials, as its request gave them. */
const dev: ComputeCredentials = { verifiers: { alasio: scramVerifier("dev-password"), lake: scramVerifier("dev-lake") }, tokenSha256: "0".repeat(64) };
const mainVerifiers = {
  alasio: scramVerifier("main-password"),
  lake: scramVerifier("main-lake"),
  lake_reader: scramVerifier("main-reader"),
  grafana: scramVerifier("main-grafana"),
};

describe("the proxy's questions", () => {
  test("name the endpoint, and for a secret the role", () => {
    assert.deepEqual(proxyQuery("/get_endpoint_access_control?session_id=s&application_name=psql&endpointish=branch-dev&role=lake"), { endpoint: "branch-dev", role: "lake" });
    assert.deepEqual(proxyQuery("/wake_compute?session_id=s&application_name=&endpointish=alasio"), { endpoint: "alasio", role: null });
    assert.equal(proxyQuery("/wake_compute?session_id=s"), null);
  });

  test("are answered, on main's compute, with the stack's verifiers of main's clients' roles", () => {
    for (const [role, verifier] of Object.entries(mainVerifiers)) assert.equal(roleSecret(null, role, mainVerifiers), verifier, role);
  });

  test("are answered, on a branch's, with the verifiers its request gave, and nothing of main's", () => {
    assert.equal(roleSecret(dev, "alasio", mainVerifiers), dev.verifiers.alasio);
    assert.equal(roleSecret(dev, "lake", mainVerifiers), dev.verifiers.lake);
    assert.equal(roleSecret(dev, "grafana", mainVerifiers), null);
    assert.equal(roleSecret(dev, "lake_reader", mainVerifiers), null);
  });

  test("are refused for a role no client connects as: neon-control's own, a compute's superuser, or none", () => {
    for (const role of ["neon_control", "cloud_admin", "postgres", "constructor", "toString"]) {
      assert.equal(roleSecret(null, role, mainVerifiers), null, role);
      assert.equal(roleSecret(dev, role, mainVerifiers), null, role);
    }
  });

  test("are refused with Neon's errors, which say whether, and when, to ask again", () => {
    assert.deepEqual(controlPlaneError("RUNNING_OPERATIONS", "waking", 1000), {
      error: "waking",
      status: {
        code: "RUNNING_OPERATIONS",
        message: "waking",
        details: { error_info: { reason: "RUNNING_OPERATIONS" }, retry_info: { retry_delay_ms: 1000 }, user_facing_message: { message: "waking" } },
      },
    });
    assert.equal((controlPlaneError("ENDPOINT_NOT_FOUND", "none") as { status: { details: { retry_info: unknown } } }).status.details.retry_info, null);
  });
});

describe("a compute", () => {
  const minute = 60_000;

  test("is idle from the first count of no session on, and used while one is open, or one was opened, or a wake asked for, since the last count", () => {
    const first = idleness(undefined, { sessions: 0, established: 4, wokenAt: null }, 0);
    assert.deepEqual(first, { since: 0, established: 4, wokenAt: null });
    assert.deepEqual(idleness(first, { sessions: 0, established: 4, wokenAt: null }, minute), { since: 0, established: 4, wokenAt: null });
    // A session opened and closed between two counts.
    const between = idleness(first, { sessions: 0, established: 5, wokenAt: null }, minute);
    assert.deepEqual(between, { since: null, established: 5, wokenAt: null });
    assert.deepEqual(idleness(between, { sessions: 0, established: 5, wokenAt: null }, 2 * minute), { since: 2 * minute, established: 5, wokenAt: null });
    // An idle session, holding a lock, open.
    assert.deepEqual(idleness(first, { sessions: 1, established: 4, wokenAt: null }, minute), { since: null, established: 4, wokenAt: null });
    assert.deepEqual(idleness(undefined, { sessions: 1, established: 4, wokenAt: null }, 0), { since: null, established: 4, wokenAt: null });
    // A wake asked for, as the command line asks before it works in the compute, with no session yet.
    assert.deepEqual(idleness(first, { sessions: 0, established: 4, wokenAt: 30_000 }, minute), { since: null, established: 4, wokenAt: 30_000 });
  });

  test("runs while a pod of it runs, starts or stops, but not one that has ended, as an evicted pod lingers", () => {
    assert.equal(livePods([]), 0);
    assert.equal(livePods([{ status: { phase: "Running" } }, { status: { phase: "Pending" } }, {}]), 3);
    assert.equal(livePods([{ status: { phase: "Failed" } }, { status: { phase: "Succeeded" } }]), 0);
  });

  test("sleeps once idle for as long as it may be, and never while used", () => {
    assert.ok(!suspendDue({ since: 0, established: 4, wokenAt: null }, 4 * minute, 5 * minute));
    assert.ok(suspendDue({ since: 0, established: 4, wokenAt: null }, 5 * minute, 5 * minute));
    assert.ok(!suspendDue({ since: null, established: 4, wokenAt: null }, 60 * minute, 5 * minute));
  });

  /**
   * A compute of `replicas` replicas, with as many pods, woken as wakeCompute wakes one; what
   * the wake did, in `done`. `asked` runs as the wake is recorded, `locking` as the wake
   * waits for the compute's lock. It takes connections while it is scaled up and a pod of it
   * runs, and the wake is out of time after a hundred asks.
   */
  const waking = (replicas: number, asked: () => void = () => {}, locking: () => void = () => {}) => {
    const compute = { replicas, pods: replicas };
    const done: string[] = [];
    let asks = 0;
    const woke = wakeCompute({
      deployment: () => Promise.resolve({ replicas: compute.replicas }),
      asked: () => Promise.resolve(asked()),
      locked: async (scaling) => {
        locking();
        done.push("locked");
        return scaling();
      },
      untilGone: () => Promise.resolve(done.push(`gone at ${compute.pods} pods`)).then(() => {}),
      scaleUp: () => Promise.resolve(Object.assign(compute, { replicas: 1, pods: 1 })).then(() => void done.push("scaled up")),
      accepts: () => {
        if (++asks > 100) return Promise.reject(new Error("out of time"));
        return Promise.resolve(compute.replicas > 0 && compute.pods > 0);
      },
      pause: () => Promise.resolve(),
    });
    return { compute, done, woke };
  };

  test("woken while it runs, takes no lock; while it sleeps, is scaled up under its lock once its pod is gone", async () => {
    const running = waking(1);
    assert.equal(await running.woke, false);
    assert.deepEqual(running.done, []);
    const asleep = waking(0);
    assert.equal(await asleep.woke, true);
    assert.deepEqual(asleep.done, ["locked", "gone at 0 pods", "scaled up"]);
  });

  test("woken as a sleep that counted it before the wake was recorded scales it down, is woken once that sleep lets go of its lock", async () => {
    // The wake reads it running; the sleep, which holds the lock, its count made, then
    // scales it to none as the wake is recorded, and lets go of the lock once its pod is gone.
    const raced = waking(1, () => Object.assign(raced.compute, { replicas: 0 }), () => Object.assign(raced.compute, { pods: 0 }));
    assert.equal(await raced.woke, true);
    assert.deepEqual(raced.done, ["locked", "gone at 0 pods", "scaled up"]);
  });

  test("counts its clients' sessions, an idle one holding a lock among them, and those opened and closed since, but neon-control's own and on its database alone", async () => {
    const database = await startPostgres();
    const url = new URL(database.url);
    const admin = new pg.Client({ connectionString: database.url });
    await admin.connect();
    const as = (username: string, password: string) => new pg.Client({ connectionString: Object.assign(new URL(url), { username, password }).toString() });
    const count = () => computeActivity(url.hostname, Number(url.port), "neon_control", "monitor-password");
    try {
      await admin.query("create role neon_control login password 'monitor-password'");
      await admin.query("create role client login password 'client-password'");
      await admin.query(`grant connect on database ${url.pathname.slice(1)} to client`);
      // The test's own superuser's session is a client's too, as a compute's is not.
      const before = await count();
      assert.equal(before.sessions, 1);

      const holder = as("client", "client-password");
      await holder.connect();
      await holder.query("select pg_advisory_lock(1)");
      const holding = await count();
      assert.equal(holding.sessions, 2, "an idle session, holding a lock, is a client's");
      assert.ok(holding.established > before.established);
      await holder.end();

      // pg_stat_database counts a session as its backend reports, which an ending one does at once.
      const after = await count();
      assert.equal(after.sessions, 1);
      assert.equal((await count()).established, after.established, "neon-control's own sessions, on the postgres database, count for nothing");
      await assert.rejects(computeActivity(url.hostname, Number(url.port), "neon_control", "wrong"), /password authentication failed/u);
    } finally {
      await admin.query(`revoke connect on database ${url.pathname.slice(1)} from client`);
      await admin.query("drop role if exists client");
      await admin.query("drop role if exists neon_control");
      await admin.end();
      await database.stop();
    }
  });
});

test("a read replica's lag is the WAL between what it replayed and what its primary flushed, never under none", () => {
  assert.equal(lsnDistance("0/16B5A50", "0/16B5A58"), 8);
  assert.equal(lsnDistance("0/FFFFFFF8", "1/8"), 16);
  assert.equal(lsnDistance("1/8", "0/FFFFFFF8"), 0, "a replica ahead, read a moment after its primary");
});
