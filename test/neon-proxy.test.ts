/**
 * neon-control as the control plane of Neon's proxy (neon/control/endpoints.ts): which
 * role's verifier it answers on which compute, the errors the proxy reads, when a
 * compute is idle, and how one is woken and put to sleep; and a compute's pods and
 * Service, as a sleep takes them off it, and its activity, counted as neon-control counts
 * it (neon/control/computes.ts), on a real Postgres.
 */
import assert from "node:assert/strict";
import { type AddressInfo, createServer } from "node:net";
import { describe, test } from "node:test";

import pg from "pg";

import type { ComputeCredentials } from "../neon/control/branches.ts";
import { computeActivity, livePods, podsLeft, reaches, slicedPods } from "../neon/control/computes.ts";
import { controlPlaneError, idleness, lsnDistance, proxyQuery, roleSecret, SERVING_LABEL, suspendCompute, suspendDue, wakeCompute, withdrawCompute } from "../neon/control/endpoints.ts";
import { scramVerifier } from "../neon/control/scram.ts";
import { startPostgres } from "./support/postgres.ts";

/** dev's compute's own credentials, as its request gave them. */
const dev: ComputeCredentials = { verifiers: { alasio: scramVerifier("dev-password"), lake: scramVerifier("dev-lake"), harness: scramVerifier("dev-harness") }, tokenSha256: "0".repeat(64) };
const mainVerifiers = {
  alasio: scramVerifier("main-password"),
  lake: scramVerifier("main-lake"),
  lake_reader: scramVerifier("main-reader"),
  grafana: scramVerifier("main-grafana"),
  harness: scramVerifier("main-harness"),
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
    assert.equal(roleSecret(dev, "harness", mainVerifiers), dev.verifiers.harness);
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

  /** A compute: its replicas, its pods, whether a sleep has marked it stopping, and whether its Service selects its pod. */
  interface Fake {
    replicas: number;
    pods: number;
    stopping: boolean;
    serving: boolean;
  }

  /**
   * A compute as `compute` says, woken as wakeCompute wakes one; what the wake did, in
   * `done`. `asked` runs as the wake is recorded, `locking` as the wake waits for the
   * compute's lock. It takes connections while it is scaled up and a pod of it runs that its
   * Service selects, and the wake is out of time after a hundred asks.
   */
  const waking = (compute: Fake, asked: () => void = () => {}, locking: () => void = () => {}) => {
    const done: string[] = [];
    let asks = 0;
    const woke = wakeCompute({
      deployment: () => Promise.resolve({ replicas: compute.replicas }),
      asked: () => Promise.resolve(asked()),
      stopping: () => Promise.resolve(compute.stopping),
      locked: async (scaling) => {
        locking();
        done.push("locked");
        return scaling();
      },
      untilGone: () => Promise.resolve(done.push(`gone at ${compute.pods} pods`)).then(() => {}),
      resume: () => Promise.resolve(Object.assign(compute, { stopping: false, serving: true })).then(() => void done.push("resumed")),
      scaleUp: () => Promise.resolve(Object.assign(compute, { replicas: 1, pods: 1, serving: true })).then(() => void done.push("scaled up")),
      accepts: () => {
        if (++asks > 100) return Promise.reject(new Error("out of time"));
        return Promise.resolve(compute.replicas > 0 && compute.pods > 0 && compute.serving);
      },
      pause: () => Promise.resolve(),
    });
    return { done, woke };
  };

  const running = (): Fake => ({ replicas: 1, pods: 1, stopping: false, serving: true });
  const asleep = (): Fake => ({ replicas: 0, pods: 0, stopping: false, serving: true });

  test("woken while it runs, takes no lock; while it sleeps, is scaled up under its lock once its pod is gone", async () => {
    const awake = waking(running());
    assert.equal(await awake.woke, false);
    assert.deepEqual(awake.done, []);
    const slept = waking(asleep());
    assert.equal(await slept.woke, true);
    assert.deepEqual(slept.done, ["locked", "gone at 0 pods", "scaled up"]);
  });

  test("woken as a sleep stops it, its pod taken off its Service and still running, is woken once that sleep lets go of its lock, its pod gone", async () => {
    // Before the wake reads it, or as the wake is recorded: either way it finds it stopping after.
    for (const marked of ["before", "as asked"]) {
      const compute: Fake = marked === "before" ? { ...running(), stopping: true, serving: false } : running();
      const stopped = waking(
        compute,
        () => void (marked === "as asked" && Object.assign(compute, { stopping: true, serving: false })),
        () => void Object.assign(compute, { replicas: 0, pods: 0, stopping: false }),
      );
      assert.equal(await stopped.woke, true, marked);
      assert.deepEqual(stopped.done, ["locked", "gone at 0 pods", "scaled up"], marked);
    }
  });

  test("woken as a sleep stops it that its count then gives up, as the wake was recorded, is left running", async () => {
    const compute = running();
    const given = waking(compute, () => void Object.assign(compute, { stopping: true, serving: false }), () => void Object.assign(compute, { stopping: false, serving: true }));
    assert.equal(await given.woke, false);
    assert.deepEqual(given.done, ["locked"]);
  });

  test("woken where a stop was left unfinished, its sleep's neon-control stopped in it, ends it: its pod given back to its Service, or, scaled to none, once its pod is gone", async () => {
    const left = waking({ ...running(), stopping: true, serving: false });
    assert.equal(await left.woke, false);
    assert.deepEqual(left.done, ["locked", "resumed"]);
    const scaled = waking({ ...asleep(), stopping: true });
    assert.equal(await scaled.woke, true);
    assert.deepEqual(scaled.done, ["locked", "gone at 0 pods", "resumed", "scaled up"]);
  });

  /** A sleep of a compute, as suspendCompute puts one to sleep, used again by its count where `used`, failing at `failing`: what it did, in `done`. */
  const sleeping = (used: boolean, failing?: string) => {
    const done: string[] = [];
    const step = <T = void>(name: string, value?: T) => (): Promise<T> => {
      done.push(name);
      return name === failing ? Promise.reject(new Error(`${name} failed`)) : Promise.resolve(value as T);
    };
    const slept = suspendCompute<string[]>({
      mark: step("marked"),
      withdraw: step("withdrawn", ["pod"]),
      used: (pods) => step(`counted on ${pods.join(", ")}`, used)(),
      restore: step("restored"),
      scaleDown: step("scaled down"),
      untilGone: step("gone"),
      unmark: step("unmarked"),
    });
    return { done, slept };
  };

  test("put to sleep is marked stopping and taken off its Service before it is counted again, on its pod, and scaled to none once still idle", async () => {
    const idle = sleeping(false);
    assert.equal(await idle.slept, true);
    assert.deepEqual(idle.done, ["marked", "withdrawn", "counted on pod", "scaled down", "gone", "unmarked"]);
  });

  test("put to sleep, used again by its count, or failing before it scales, is given back to its Service; failing after, is left marked, for a wake to end", async () => {
    const used = sleeping(true);
    assert.equal(await used.slept, false);
    assert.deepEqual(used.done, ["marked", "withdrawn", "counted on pod", "restored"]);
    const unwithdrawn = sleeping(false, "withdrawn");
    await assert.rejects(unwithdrawn.slept, /withdrawn failed/u);
    assert.deepEqual(unwithdrawn.done, ["marked", "withdrawn", "restored"]);
    const uncounted = sleeping(false, "counted on pod");
    await assert.rejects(uncounted.slept, /counted on pod failed/u);
    assert.deepEqual(uncounted.done, ["marked", "withdrawn", "counted on pod", "restored"]);
    const ungone = sleeping(false, "gone");
    await assert.rejects(ungone.slept, /gone failed/u);
    assert.deepEqual(ungone.done, ["marked", "withdrawn", "counted on pod", "scaled down", "gone"]);
  });

  /**
   * Its pods taken off its Service, as withdrawCompute takes them, its EndpointSlices
   * listing them for `listed` asks, and a connection to it let through for `reached`, the
   * pause out of time after `patience`: what it did, in `done`.
   */
  const withdrawing = (listed: number, reached: number, patience = Infinity) => {
    const done: string[] = [];
    let pauses = 0;
    const withdrawn = withdrawCompute({
      pods: () => Promise.resolve(["pod-a", "pod-b"]),
      takeOff: (pod) => Promise.resolve(void done.push(`took ${pod} off`)),
      listed: (pods) => {
        done.push(`listed ${pods.join(", ")}?`);
        return Promise.resolve(listed-- > 0);
      },
      reached: () => {
        done.push("reached?");
        return Promise.resolve(reached-- > 0);
      },
      pause: (still) => (++pauses > patience ? Promise.reject(new Error(`out of time: ${still}`)) : Promise.resolve(void done.push("paused"))),
    });
    return { done, withdrawn };
  };

  test("taken off its Service by a sleep, is waited for until its EndpointSlices list none of its pods, and then until a connection to it is refused, as kube-proxy follows them", async () => {
    const { done, withdrawn } = withdrawing(1, 2);
    assert.deepEqual(await withdrawn, ["pod-a", "pod-b"]);
    assert.deepEqual(done, [
      "took pod-a off",
      "took pod-b off",
      "listed pod-a, pod-b?",
      "paused",
      "listed pod-a, pod-b?",
      "reached?",
      "paused",
      "reached?",
      "paused",
      "reached?",
    ]);
  });

  test("taken off its Service by a sleep, fails once out of time, saying what is still so", async () => {
    await assert.rejects(withdrawing(5, 0, 2).withdrawn, /^Error: out of time: its EndpointSlices list its pods taken off it$/u);
    await assert.rejects(withdrawing(1, 5, 2).withdrawn, /^Error: out of time: a connection to it is let through to its pods taken off it$/u);
  });

  test("is taken off its Service, and given back to it, on its pods that run or may, none being deleted already, as the one a sleep scaled down is", () => {
    const pod = (name: string, { phase = "Running", ip = "10.42.0.7" as string | null, serving = true, deleting = false } = {}) => ({
      metadata: { name, labels: serving ? { [SERVING_LABEL]: "true" } : {}, ...(deleting ? { deletionTimestamp: "2026-10-20T00:00:00Z" } : {}) },
      status: { phase, ...(ip ? { podIP: ip } : {}) },
    });
    assert.deepEqual(podsLeft([pod("running"), pod("withdrawn", { serving: false }), pod("deleted", { deleting: true }), pod("evicted", { phase: "Failed" }), pod("starting", { phase: "Pending", ip: null })]), [
      { name: "running", ip: "10.42.0.7", serving: true },
      { name: "withdrawn", ip: "10.42.0.7", serving: false },
      { name: "starting", ip: null, serving: true },
    ]);
  });

  test("is reached at its Service while a connection there is taken, and no longer once one is refused", async () => {
    const server = createServer((socket) => socket.destroy());
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    // A server listening on a TCP port has an address of its own.
    const { port } = server.address() as AddressInfo;
    assert.equal(await reaches("127.0.0.1", port), true);
    await new Promise((resolve) => server.close(resolve));
    assert.equal(await reaches("127.0.0.1", port), false);
  });

  test("is not taken for refused where it cannot be asked, its name resolving to nothing or nothing answering in time, but fails", async () => {
    await assert.rejects(reaches("nothing.invalid", 1), { code: "ENOTFOUND" });
    // TEST-NET-1's, which routes nowhere.
    await assert.rejects(reaches("192.0.2.1", 9), /neither took nor refused a connection in \d+ ms|EHOSTUNREACH|ENETUNREACH/u);
  });

  test("is routed to on each pod its Service's EndpointSlices list, whatever the endpoint's conditions", () => {
    const endpoint = (name: string | undefined, ready: boolean) => ({ ...(name ? { targetRef: { name } } : {}), conditions: { ready } });
    assert.deepEqual(slicedPods([{ endpoints: [endpoint("pod-a", true), endpoint(undefined, true)] }, { endpoints: [endpoint("pod-b", false)] }, { endpoints: null }, {}]), new Set(["pod-a", "pod-b"]));
    assert.deepEqual(slicedPods([]), new Set());
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
