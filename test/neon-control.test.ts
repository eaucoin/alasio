import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  attachNotified,
  type Branch,
  BranchError,
  BRANCHES_SCOPE,
  branchOfCompute,
  branchPoint,
  branchRequest,
  branchView,
  callScopes,
  type ComputeStack,
  computeConfig,
  computeId,
  computeOfToken,
  creationRefused,
  deleting,
  type DescribedShard,
  existingBranch,
  layoutOf,
  nameProblem,
  pageserversServing,
  placementOf,
  PROXY_SCOPE,
  type ReadyBranch,
  safekeepersNotified,
  sameLayout,
  type ShardLayout,
  shardMoves,
  type StorageNode,
  tokenSha256,
  withNotifiedPlacement,
} from "../neon/control/branches.ts";
import { bearsScope, computeCtlToken, generateKeyPair, signToken, verifyToken } from "../neon/control/jwt.ts";
import { createControlMetrics } from "../neon/control/metrics.ts";
import { containerRuns } from "../neon/control/computes.ts";
import { reconcile, type Reconciling, type Reconfigured, reconfigure, type Reconfiguring, settledLayout, tookLayout } from "../neon/control/reconfigure.ts";
import { scramVerifier } from "../neon/control/scram.ts";

const MAIN_TIMELINE = "11111111111111111111111111111111";
const DEV_TIMELINE = "22222222222222222222222222222222";

/** dev's compute's own credentials: its roles' passwords' verifiers, and its token's hash. */
const DEV_COMPUTE = {
  verifiers: { alasio: scramVerifier("dev-password"), lake: scramVerifier("dev-lake-password") },
  tokenSha256: tokenSha256("dev-token"),
};

const main: ReadyBranch = {
  name: "main",
  parent: null,
  timelineId: MAIN_TIMELINE,
  lsn: null,
  createdAt: "2026-10-07T00:00:00.000Z",
  compute: null,
  state: "ready",
  safekeepers: { generation: 1, ids: [1, 2, 3] },
};
const dev: ReadyBranch = {
  name: "dev",
  parent: "main",
  timelineId: DEV_TIMELINE,
  lsn: "0/16B5A58",
  createdAt: "2026-10-07T01:00:00.000Z",
  compute: DEV_COMPUTE,
  state: "ready",
  safekeepers: { generation: 1, ids: [2, 3, 1] },
};

/** The status of the BranchError `run` throws. */
function refusal(run: () => unknown): number {
  try {
    run();
  } catch (error) {
    assert.ok(error instanceof BranchError, String(error));
    return error.status;
  }
  assert.fail("nothing was refused");
}

describe("a branch's name", () => {
  test("is a DNS-1123 label of at most 30 characters", () => {
    for (const name of ["dev", "a", "try-2", "0day", "x".repeat(30)]) assert.equal(nameProblem(name), null, name);
    for (const name of ["", "Dev", "dev_1", "-dev", "dev-", "dév", "a.b", "x".repeat(31)]) assert.ok(nameProblem(name), name);
  });

  test("is never main, alasio's own", () => {
    assert.match(nameProblem("main") ?? "", /alasio's own/u);
  });
});

describe("a branch asked for", () => {
  test("is of main at its end unless it says otherwise, with its compute's credentials", () => {
    assert.deepEqual(branchRequest({ name: "dev", compute: DEV_COMPUTE }), { name: "dev", parent: "main", lsn: null, compute: DEV_COMPUTE });
    assert.deepEqual(branchRequest({ name: "dev", parent: "other", lsn: "0/16b5a50", compute: DEV_COMPUTE }), { name: "dev", parent: "other", lsn: "0/16B5A50", compute: DEV_COMPUTE });
  });

  test("is at a branch point aligned as the pageserver aligns it", () => {
    assert.equal(branchPoint("0/16B5A51"), "0/16B5A58");
    assert.equal(branchPoint("0/16B5A58"), "0/16B5A58");
    assert.equal(branchPoint("1/FFFFFFF9"), "2/0");
    assert.equal(branchPoint("16B5A58"), null);
    assert.equal(branchPoint("0/16B5A58/1"), null);
  });

  test("is refused when it is not one", () => {
    const compute = DEV_COMPUTE;
    for (
      const body of [
        null,
        "dev",
        { parent: "main", compute },
        { name: "Dev", compute },
        { name: "main", compute },
        { name: "dev", parent: 1, compute },
        { name: "dev", lsn: "head", compute },
        { name: "dev", lsn: 1, compute },
        { name: "dev", ancestor: "main", compute },
        // Its compute's credentials, which are its own, and hashes alone.
        { name: "dev" },
        { name: "dev", compute: { ...compute, password: "dev-password" } },
        { name: "dev", compute: { ...compute, verifiers: { ...compute.verifiers, alasio: "dev-password" } } },
        { name: "dev", compute: { ...compute, verifiers: { alasio: compute.verifiers.alasio } } },
        { name: "dev", compute: { ...compute, verifiers: { ...compute.verifiers, grafana: compute.verifiers.lake } } },
        { name: "dev", compute: { ...compute, tokenSha256: "dev-token" } },
      ]
    ) {
      assert.equal(refusal(() => branchRequest(body)), 400, JSON.stringify(body));
    }
  });
});

describe("the branches", () => {
  const request = { name: "dev", parent: "main", lsn: null, compute: DEV_COMPUTE };

  test("create a branch of a ready parent once, and answer it when asked for again", () => {
    assert.equal(existingBranch([main], request), null);
    assert.equal(existingBranch([main, dev], request), dev);
    assert.equal(existingBranch([main, dev], { ...request, lsn: dev.lsn }), dev);
    const creating: Branch = { ...dev, state: "creating" };
    assert.equal(existingBranch([main, creating], request), creating);
  });

  test("refuse a name taken otherwise, a branch being deleted, and a parent not ready or not there", () => {
    assert.equal(refusal(() => existingBranch([main, dev], { ...request, parent: "other" })), 409);
    assert.equal(refusal(() => existingBranch([main, dev], { ...request, lsn: "0/1000000" })), 409);
    assert.equal(refusal(() => existingBranch([main, { ...dev, state: "deleting" }], request)), 409);
    assert.equal(refusal(() => existingBranch([main, { ...dev, state: "creating" }], { ...request, name: "child", parent: "dev" })), 409);
    assert.equal(refusal(() => existingBranch([main], { ...request, name: "child", parent: "dev" })), 404);
  });

  test("delete a branch with none of its own, never main", () => {
    const [, deleted] = deleting([main, dev], "dev");
    assert.deepEqual(deleted, { name: "dev", parent: "main", timelineId: DEV_TIMELINE, lsn: dev.lsn, createdAt: dev.createdAt, compute: DEV_COMPUTE, state: "deleting" });
    assert.equal(refusal(() => deleting([main, dev], "main")), 400);
    assert.equal(refusal(() => deleting([main], "dev")), 404);
    const child: Branch = { ...dev, name: "child", parent: "dev", timelineId: "3".repeat(32) };
    assert.equal(refusal(() => deleting([main, dev, child], "dev")), 412);
  });

  test("serve a ready branch's compute by its id, main's as alasio", () => {
    assert.equal(computeId("main"), "alasio");
    assert.equal(computeId("dev"), "branch-dev");
    assert.equal(branchOfCompute([main, dev], "alasio"), main);
    assert.equal(branchOfCompute([main, dev], "branch-dev"), dev);
    assert.equal(branchOfCompute([main, { ...dev, state: "creating" }], "branch-dev"), undefined);
    assert.equal(branchOfCompute([main, dev], "dev"), undefined);
  });

  test("know each compute by its own token alone: main's the stack's, a branch's the one it was created with", () => {
    const holder = (authorization: string | undefined) => computeOfToken([main, dev], authorization, "stack-token");
    assert.equal(holder("Bearer stack-token"), "alasio");
    assert.equal(holder("Bearer dev-token"), "branch-dev");
    assert.equal(holder("Bearer other-token"), null);
    assert.equal(holder("dev-token"), null);
    assert.equal(holder(undefined), null);
    // Its hash, which neon-control keeps, is not its token.
    assert.equal(holder(`Bearer ${DEV_COMPUTE.tokenSha256}`), null);
    assert.equal(computeOfToken([main, dev], "Bearer ", ""), null);
  });

  test("answer a branch without its compute's credentials", () => {
    assert.deepEqual(Object.keys(branchView(dev)).sort(), ["computeId", "createdAt", "lsn", "name", "parent", "safekeepers", "state", "timelineId"]);
  });
});

describe("the storage controller's answer to creating a branch", () => {
  const branch = { name: "dev", timelineId: DEV_TIMELINE, lsn: "0/16B5A58" };
  const created = {
    tenant_id: "t",
    timeline_id: DEV_TIMELINE,
    ancestor_timeline_id: MAIN_TIMELINE,
    ancestor_lsn: "0/16B5A58",
    last_record_lsn: "0/16B5A58",
    safekeepers: { tenant_id: "t", timeline_id: DEV_TIMELINE, generation: 1, safekeepers: [{ id: 2, hostname: "sk-1" }, { id: 3, hostname: "sk-2" }, { id: 1, hostname: "sk-0" }] },
  };

  test("places it on the safekeepers it names", () => {
    assert.deepEqual(placementOf(branch, MAIN_TIMELINE, created), { generation: 1, ids: [2, 3, 1] });
  });

  test("is refused when it is not of the branch asked for, as a timeline bootstrapped instead is not", () => {
    assert.equal(refusal(() => placementOf(branch, MAIN_TIMELINE, { ...created, ancestor_timeline_id: null, ancestor_lsn: null })), 502);
    assert.equal(refusal(() => placementOf(branch, MAIN_TIMELINE, { ...created, ancestor_lsn: "0/16B5A60" })), 502);
    assert.equal(refusal(() => placementOf(branch, MAIN_TIMELINE, { ...created, timeline_id: MAIN_TIMELINE })), 502);
    assert.equal(refusal(() => placementOf(branch, MAIN_TIMELINE, { ...created, safekeepers: null })), 502);
  });

  test("of main, bootstrapped, names no ancestor", () => {
    const bootstrapped = { ...created, timeline_id: MAIN_TIMELINE, ancestor_timeline_id: null, ancestor_lsn: null };
    assert.deepEqual(placementOf({ name: "main", timelineId: MAIN_TIMELINE, lsn: null }, null, bootstrapped), { generation: 1, ids: [2, 3, 1] });
    assert.equal(refusal(() => placementOf({ name: "main", timelineId: MAIN_TIMELINE, lsn: null }, null, created)), 502);
  });

  test("refusing a branch point its parent lacks is the caller's to change, saying how far back its history goes; any other refusal, to ask again", () => {
    const asked = { name: "dev", parent: "main", lsn: "0/8" };
    const wrapped = '{"msg":"pageserver 1 406 Not Acceptable: 406 Not Acceptable invalid branch start lsn: less than latest GC cutoff 0/14EE2B0"}';
    assert.equal(creationRefused(asked, 409, wrapped).status, 406);
    assert.match(creationRefused(asked, 409, wrapped).message, /^main has no 0\/8 to branch dev from: it is before the history it keeps, which goes a day back, or past its end /u);
    assert.equal(creationRefused(asked, 406, '{"msg":"invalid branch start lsn"}').status, 406);
    assert.equal(creationRefused(asked, 409, '{"msg":"pageserver 1 429 Too Many Requests"}').status, 503);
    assert.equal(creationRefused(asked, null, "fetch failed").status, 503);
  });
});

describe("safekeepers notified", () => {
  const body = { tenant_id: "t", timeline_id: DEV_TIMELINE, generation: 3, safekeepers: [{ id: 1, hostname: "sk-0" }, { id: 4, hostname: "sk-3" }] };

  test("place the branch of their timeline anew, from a later generation on", () => {
    const notified = safekeepersNotified(body);
    assert.deepEqual(notified, { timelineId: DEV_TIMELINE, placement: { generation: 3, ids: [1, 4] } });
    const placed = withNotifiedPlacement([main, dev], notified);
    assert.deepEqual(placed, [main, { ...dev, safekeepers: { generation: 3, ids: [1, 4] } }]);
    assert.equal(withNotifiedPlacement(placed ?? [], notified), null);
    assert.equal(withNotifiedPlacement([main], notified), null);
  });

  test("are refused when the notification is not one", () => {
    for (const malformed of [null, { ...body, generation: "3" }, { ...body, safekeepers: [] }, { ...body, safekeepers: [{ hostname: "sk-0" }] }]) {
      assert.equal(refusal(() => safekeepersNotified(malformed)), 400, JSON.stringify(malformed));
    }
  });
});

/** What every compute's spec shares in these tests. */
const stack: ComputeStack = {
  tenantId: "t",
  safekeepers: [1, 2, 3].map((id) => ({ id, host: `sk-${id - 1}`, pgPort: 5454, httpPort: 7676 })),
  verifiers: { alasio: "SCRAM-SHA-256$verifier" },
  monitor: { role: "neon_control", verifier: (id) => `SCRAM-SHA-256$monitor-${id}` },
  storageAuthToken: "token",
  jwks: { keys: [] },
};

/** One shard, on one pageserver. */
const ONE_SHARD: ShardLayout = { pageservers: ["alasio-neon-pageserver.alasio.svc.cluster.local:6400"], stripeSize: null };

test("a branch's compute spec is main's but for its timeline, its safekeepers and its roles' passwords", () => {
  const ofMain = computeConfig(main, stack, ONE_SHARD);
  const ofDev = computeConfig({ ...dev, safekeepers: { generation: 2, ids: [2, 3] } }, stack, ONE_SHARD);
  assert.equal(ofDev.spec.timeline_id, DEV_TIMELINE);
  assert.deepEqual(ofDev.spec.safekeeper_connstrings, ["sk-1:5454", "sk-2:5454"]);
  assert.equal(ofDev.spec.safekeepers_generation, 2);
  assert.deepEqual(ofMain.spec.safekeeper_connstrings, ["sk-0:5454", "sk-1:5454", "sk-2:5454"]);
  const { timeline_id: _main, safekeeper_connstrings: _mainSafekeepers, safekeepers_generation: _mainGeneration, ...mainRest } = ofMain.spec;
  const { timeline_id: _dev, safekeeper_connstrings: _devSafekeepers, safekeepers_generation: _devGeneration, ...devRest } = ofDev.spec;
  const { cluster: { roles: mainRoles, ...mainCluster }, ...mainSpec } = mainRest;
  const { cluster: { roles: devRoles, ...devCluster }, ...devSpec } = devRest;
  assert.deepEqual([devSpec, devCluster], [mainSpec, mainCluster]);
  // neon-control's own role last, with a password of the compute's own, so it logs in once the spec's roles are applied.
  assert.deepEqual(mainRoles, [
    { name: "alasio", encrypted_password: "SCRAM-SHA-256$verifier", options: null },
    { name: "neon_control", encrypted_password: "SCRAM-SHA-256$monitor-alasio", options: null },
  ]);
  assert.deepEqual(devRoles, [
    { name: "alasio", encrypted_password: DEV_COMPUTE.verifiers.alasio, options: null },
    { name: "neon_control", encrypted_password: "SCRAM-SHA-256$monitor-branch-dev", options: null },
  ]);
});

test("a compute reads each shard from the pageserver the storage controller placed it on, in shard order, by the stripe", () => {
  const one = computeConfig(main, stack, ONE_SHARD).spec;
  assert.equal(one.pageserver_connstring, "postgresql://no_user@alasio-neon-pageserver.alasio.svc.cluster.local:6400");
  assert.equal(one.shard_stripe_size, undefined, "one shard has no stripe");
  const two = computeConfig(main, stack, { pageservers: ["ps-1:6400", "ps-0:6400"], stripeSize: 2048 }).spec;
  assert.equal(two.pageserver_connstring, "postgresql://no_user@ps-1:6400,postgresql://no_user@ps-0:6400");
  assert.equal(two.shard_stripe_size, 2048);
  // The tenant's own id, never a shard's.
  assert.equal(two.tenant_id, "t");
});

describe("the shards", () => {
  /** Four shards at rest, attached to the pageservers `on` says, in shard order. */
  const shards = (on: readonly (number | null)[]): DescribedShard[] =>
    on.map((node, index) => ({ tenant_shard_id: `t-0${index}04`, node_attached: node, is_reconciling: false, is_splitting: false }));

  test("all on one pageserver, as a split leaves them, are spread in shard order, one on each in turn", () => {
    assert.deepEqual(shardMoves(shards([1, 1, 1, 1]), 2), [{ shardId: "t-0104", from: 1, to: 2 }, { shardId: "t-0304", from: 1, to: 2 }]);
    assert.deepEqual(shardMoves(shards([2, 2, 2, 2]), 3), [{ shardId: "t-0004", from: 2, to: 1 }, { shardId: "t-0204", from: 2, to: 3 }, { shardId: "t-0304", from: 2, to: 1 }]);
  });

  test("are left where the storage controller placed them otherwise, while any moves or splits, and where there is nothing to spread", () => {
    assert.deepEqual(shardMoves(shards([2, 1, 2, 1]), 2), [], "a drain and fill's own placement");
    assert.deepEqual(shardMoves(shards([1, 1, 1, null]), 2), [], "one shard attached nowhere");
    assert.deepEqual(shardMoves(shards([1, 1, 1, 1]).map((shard, index) => ({ ...shard, is_reconciling: index === 3 })), 2), []);
    assert.deepEqual(shardMoves(shards([1, 1, 1, 1]).map((shard) => ({ ...shard, is_splitting: true })), 2), []);
    assert.deepEqual(shardMoves(shards([1, 1, 1, 1]), 1), []);
    assert.deepEqual(shardMoves(shards([1]), 2), []);
  });

  test("are split and spread while every pageserver is registered, available and takes shards", () => {
    const node = (id: number, availability = "Active", scheduling = "Active"): StorageNode => ({ id, availability, scheduling, listen_pg_addr: `ps-${id}`, listen_pg_port: 6400 });
    assert.equal(pageserversServing([node(1), node(2)], 2), true);
    assert.equal(pageserversServing([node(1)], 2), false, "one not registered yet");
    assert.equal(pageserversServing([node(1), node(2, "Offline")], 2), false);
    assert.equal(pageserversServing([node(1), node(2, "WarmingUp")], 2), false);
    assert.equal(pageserversServing([node(1, "Active", "PauseForRestart"), node(2)], 2), false, "one drained");
    assert.equal(pageserversServing([node(1), node(2, "Active", "Filling")], 2), false);
  });
});

describe("shards notified", () => {
  const body = { tenant_id: "t", preferred_az: "az-pageserver", stripe_size: 2048, shards: [{ node_id: 2, shard_number: 1 }, { node_id: 1, shard_number: 0 }] };

  test("place each shard, in shard order, on a pageserver whose address the storage controller lists", () => {
    const notified = attachNotified(body, "t");
    assert.deepEqual(notified, { nodeIds: [1, 2], stripeSize: 2048 });
    assert.deepEqual(layoutOf(notified, new Map([[1, "ps-0:6400"], [2, "ps-1:6400"]])), { pageservers: ["ps-0:6400", "ps-1:6400"], stripeSize: 2048 });
    assert.equal(layoutOf(notified, new Map([[1, "ps-0:6400"]])), null, "a pageserver not yet listed");
    // One shard has no stripe, which the storage controller gives as null.
    assert.deepEqual(attachNotified({ ...body, stripe_size: null, shards: [{ node_id: 1, shard_number: 0 }] }, "t"), { nodeIds: [1], stripeSize: null });
  });

  test("are refused when the notification is not one, or of another tenant", () => {
    for (const malformed of [null, { ...body, shards: [] }, { ...body, shards: [{ node_id: "1", shard_number: 0 }] }, { ...body, shards: [{ node_id: 1, shard_number: 1 }] }, { ...body, stripe_size: "2048" }]) {
      assert.equal(refusal(() => attachNotified(malformed, "t")), 400, JSON.stringify(malformed));
    }
    assert.equal(refusal(() => attachNotified(body, "another")), 400);
  });

  test("tell a layout from another by where each shard is, and the stripe", () => {
    const two = { pageservers: ["ps-0:6400", "ps-1:6400"], stripeSize: 2048 };
    assert.ok(sameLayout(two, { pageservers: ["ps-0:6400", "ps-1:6400"], stripeSize: 2048 }));
    assert.ok(!sameLayout(two, { pageservers: ["ps-1:6400", "ps-0:6400"], stripeSize: 2048 }));
    assert.ok(!sameLayout(two, { pageservers: ["ps-0:6400", "ps-1:6400"], stripeSize: 4096 }));
    assert.ok(!sameLayout(two, { pageservers: ["ps-0:6400"], stripeSize: null }));
  });
});

describe("a running compute given a new layout", () => {
  const layout: ShardLayout = { pageservers: ["ps-0:6400", "ps-1:6400"], stripeSize: 2048 };
  /** A compute compute_ctl says is `status` of, answering `configured` to a spec, as `awake` says; what was done with it. */
  const compute = ({ awake = true, running = true, configured = 200, status = "running" as string | null, failing = "" } = {}) => {
    const done: string[] = [];
    const through: Reconfiguring = {
      awake: async () => awake,
      running: async () => running,
      configure: async () => {
        done.push("configure");
        if (failing === "configure") throw new Error("connect ECONNREFUSED");
        return configured;
      },
      status: async () => {
        done.push("status");
        return status;
      },
      restart: async () => {
        done.push("restart");
        if (failing === "restart") throw new Error("restarting alasio-branch-dev/alasio-neon-compute: 403");
      },
      gave: async (given) => {
        assert.deepEqual(given, layout);
        done.push("gave");
      },
    };
    return { done, through };
  };
  const given = async (options: Parameters<typeof compute>[0]): Promise<[Reconfigured, string[]]> => {
    const { done, through } = compute(options);
    return [await reconfigure(layout, through), done];
  };

  test("takes it live, and is recorded given it", async () => {
    assert.deepEqual(await given({}), [{ outcome: "given" }, ["configure", "gave"]]);
  });

  test("is left asleep, as it fetches the layout when woken", async () => {
    assert.deepEqual(await given({ awake: false }), [{ outcome: "asleep" }, []]);
  });

  test("is restarted while it starts on the spec it fetched, or has failed, as it never takes one then", async () => {
    assert.deepEqual(await given({ configured: 412, status: "init" }), [{ outcome: "restarted" }, ["configure", "status", "restart"]]);
    assert.deepEqual(await given({ configured: 412, status: "failed" }), [{ outcome: "restarted" }, ["configure", "status", "restart"]]);
  });

  test("is left alone while it configures or stops, or compute_ctl says nothing, as it may hold its clients' sessions", async () => {
    for (const status of ["configuration_pending", "configuration", "termination_pending_fast", "running", null]) {
      assert.deepEqual(await given({ configured: 412, status }), [{ outcome: "waiting", status }, ["configure", "status"]]);
    }
  });

  test("takes it at its next start where no container of it runs, pulling its image or crashing", async () => {
    assert.deepEqual(await given({ configured: 0, running: false }), [{ outcome: "starting" }, ["configure"]]);
  });

  test("is asked again later where compute_ctl is not reached, or refuses otherwise, and never fails the others", async () => {
    assert.deepEqual(await given({ configured: 0 }), [{ outcome: "unreached", answered: 0 }, ["configure"]]);
    assert.deepEqual(await given({ configured: 401 }), [{ outcome: "unreached", answered: 401 }, ["configure"]]);
    assert.deepEqual(await given({ failing: "configure" }), [{ outcome: "failed", error: "connect ECONNREFUSED" }, ["configure"]]);
    assert.deepEqual(await given({ configured: 412, status: "init", failing: "restart" }), [
      { outcome: "failed", error: "restarting alasio-branch-dev/alasio-neon-compute: 403" },
      ["configure", "status", "restart"],
    ]);
  });

  test("has the layout once given it, or asleep; the storage controller notifies again otherwise", () => {
    const outcomes: Reconfigured[] = [
      { outcome: "given" },
      { outcome: "asleep" },
      { outcome: "starting" },
      { outcome: "restarted" },
      { outcome: "waiting", status: "configuration" },
      { outcome: "unreached", answered: 0 },
      { outcome: "failed", error: "" },
    ];
    assert.deepEqual(outcomes.map(tookLayout), [true, true, true, false, false, false, false]);
  });
});

describe("the leader's round", () => {
  const one: ShardLayout = { pageservers: ["ps-0:6400"], stripeSize: null };
  const two: ShardLayout = { pageservers: ["ps-0:6400", "ps-1:6400"], stripeSize: 2048 };
  /** Two shards, moving or not, located as `two`, the layout `recorded`, and each compute last given `given`; what was recorded and given, each in turn. */
  const round = (recorded: ShardLayout | null, given: ShardLayout[], moving = false) => {
    const done: { recorded: ShardLayout[]; given: ShardLayout[] } = { recorded: [], given: [] };
    const through: Reconciling = {
      shards: async () => [0, 1].map((index) => ({ tenant_shard_id: `t-0${index}02`, node_attached: index + 1, is_reconciling: moving && index === 1, is_splitting: false })),
      located: async () => two,
      recorded: async () => recorded,
      record: async (layout) => void done.recorded.push(layout),
      configure: async (layout) => {
        const stale = given.filter((had) => !sameLayout(had, layout));
        done.given.push(...stale.map(() => layout));
        return 0;
      },
    };
    return { done, through };
  };

  test("alone sets a stale recorded layout right, as the storage controller locates the shards, and gives every compute on another it", async () => {
    const { done, through } = round(one, [one, two]);
    assert.deepEqual(await reconcile(through), { layout: two, recorded: true, pending: 0 });
    assert.deepEqual(done, { recorded: [two], given: [two] });
  });

  test("records a layout where none is, records none where the record is right, and still gives a compute left on another it", async () => {
    assert.deepEqual(await reconcile(round(null, []).through), { layout: two, recorded: true, pending: 0 });
    const { done, through } = round(two, [one]);
    assert.deepEqual(await reconcile(through), { layout: two, recorded: false, pending: 0 });
    assert.deepEqual(done, { recorded: [], given: [two] });
  });

  test("does nothing while a shard moves or splits, as a notification follows", async () => {
    const { done, through } = round(one, [one], true);
    assert.equal(await reconcile(through), null);
    assert.deepEqual(done, { recorded: [], given: [] });
  });
});

test("a compute's spec gives it the layout recorded once it is recorded given it, read again until it stays", async () => {
  const one: ShardLayout = { pageservers: ["ps-0:6400"], stripeSize: null };
  const two: ShardLayout = { pageservers: ["ps-0:6400", "ps-1:6400"], stripeSize: 2048 };
  // A notification records two as the compute's row is written with one.
  const recorded = [one, two, two];
  const given: ShardLayout[] = [];
  const read = async () => recorded.shift() ?? two;
  assert.deepEqual(await settledLayout(read, async (layout) => void given.push(layout)), two);
  assert.deepEqual(given, [one, two], "the last recorded given is the one it runs on");
  assert.deepEqual(await settledLayout(async () => one, async () => {}), one);
});

test("a compute's pod runs it while its container runs, and not once it is being deleted", () => {
  const pod = (state: object, deleting = false) => ({ metadata: deleting ? { deletionTimestamp: "2026-10-14T00:00:00Z" } : {}, status: { containerStatuses: [{ name: "compute", state }] } });
  assert.equal(containerRuns([pod({ running: { startedAt: "x" } })], "compute"), true);
  assert.equal(containerRuns([pod({ waiting: { reason: "ImagePullBackOff" } })], "compute"), false);
  assert.equal(containerRuns([pod({ waiting: { reason: "CrashLoopBackOff" } }), pod({ running: {} }, true)], "compute"), false);
  assert.equal(containerRuns([], "compute"), false);
});

test("every compute checkpoints hourly, unless an hour's WAL comes to a gigabyte first", () => {
  for (const branch of [main, dev]) {
    const settings = new Map(computeConfig(branch, stack, ONE_SHARD).spec.cluster.settings.map(({ name, value }) => [name, value]));
    assert.deepEqual([settings.get("checkpoint_timeout"), settings.get("max_wal_size")], ["1h", "2GB"], branch.name);
  }
});

test("neon-control takes a token of the admin scope only, not the tenant's every compute holds", () => {
  const { privateKeyPem, publicKeyPem } = generateKeyPair();
  const bearer = (token: string) => `Bearer ${token}`;
  assert.ok(bearsScope(publicKeyPem, bearer(signToken(privateKeyPem, "admin")), "admin"));
  assert.ok(!bearsScope(publicKeyPem, bearer(signToken(privateKeyPem, "tenant", "t")), "admin"));
  assert.ok(!bearsScope(publicKeyPem, bearer(signToken(privateKeyPem, "pageserverapi")), "admin"));
  assert.ok(!bearsScope(publicKeyPem, bearer(signToken(generateKeyPair().privateKeyPem, "admin")), "admin"));
  assert.ok(!bearsScope(publicKeyPem, signToken(privateKeyPem, "admin"), "admin"));
  assert.ok(!bearsScope(publicKeyPem, undefined, "admin"));
});

test("compute_ctl's token names its compute alone, of no scope, so nothing else of Neon's takes it, and ends in five minutes", () => {
  const { privateKeyPem, publicKeyPem } = generateKeyPair();
  const token = computeCtlToken(privateKeyPem, "branch-dev", Date.UTC(2026, 9, 13, 12));
  assert.deepEqual(verifyToken(publicKeyPem, token), { compute_id: "branch-dev", exp: Date.UTC(2026, 9, 13, 12, 5) / 1000 });
  for (const scope of ["admin", "tenant", "proxy", BRANCHES_SCOPE]) assert.ok(!bearsScope(publicKeyPem, `Bearer ${token}`, scope), scope);
});

test("the lake's token of the branches scope lists the branches, and calls nothing else", () => {
  const { privateKeyPem, publicKeyPem } = generateKeyPair();
  const lake = `Bearer ${signToken(privateKeyPem, BRANCHES_SCOPE)}`;
  const takes = (method: string, url: string) => bearsScope(publicKeyPem, lake, ...callScopes(method, url));
  assert.ok(takes("GET", "/branches"));
  assert.ok(!takes("POST", "/branches"));
  assert.ok(!takes("DELETE", "/branches/dev"));
  assert.ok(!takes("PUT", "/notify-safekeepers"));
  assert.ok(!takes("GET", "/get_endpoint_access_control?endpointish=alasio&role=alasio"));
  assert.ok(bearsScope(publicKeyPem, `Bearer ${signToken(privateKeyPem, "admin")}`, ...callScopes("POST", "/branches")));
});

test("the proxy's token of the proxy scope asks for roles' secrets and wakes computes, and calls nothing else; nothing else calls those", () => {
  const { privateKeyPem, publicKeyPem } = generateKeyPair();
  const as = (scope: string) => (method: string, url: string) => bearsScope(publicKeyPem, `Bearer ${signToken(privateKeyPem, scope)}`, ...callScopes(method, url));
  const proxy = as(PROXY_SCOPE);
  const admin = as("admin");
  assert.ok(proxy("GET", "/get_endpoint_access_control?session_id=s&application_name=&endpointish=alasio&role=lake"));
  assert.ok(proxy("GET", "/wake_compute?session_id=s&application_name=&endpointish=branch-dev"));
  assert.ok(!admin("GET", "/wake_compute?endpointish=alasio"));
  for (const [method, url] of [["GET", "/branches"], ["POST", "/branches"], ["DELETE", "/branches/dev"], ["POST", "/computes/alasio/wake"], ["PUT", "/notify-attach"]] as const) {
    assert.ok(!proxy(method, url), `${method} ${url}`);
  }
  assert.ok(admin("POST", "/computes/alasio/wake"));
});

test("neon-control's metrics count its wakes, by how they started and in buckets of how long they took, its failures, sleeps and lock timeouts, and the replicas of each compute while it leads", () => {
  const metrics = createControlMetrics();
  const rendered = () => metrics.render().split("\n");
  // Every counter, at 0, before anything has happened: what an increase is reckoned from.
  for (const line of ['neon_control_wakes_total{start="cold"} 0', "neon_control_wake_failures_total 0", "neon_control_suspends_total 0", 'neon_control_lock_timeouts_total{lock="changes"} 0']) {
    assert.ok(rendered().includes(line), line);
  }
  metrics.woke("cold", 6.5);
  metrics.woke("warm", 0.1);
  metrics.wakeFailed();
  metrics.suspended();
  metrics.lockTimedOut("compute");
  metrics.replicas(new Map([["alasio", 1], ["branch-dev", 0]]));
  const lines = rendered();
  for (const line of [
    'neon_control_wakes_total{start="cold"} 1',
    'neon_control_wakes_total{start="warm"} 1',
    'neon_control_wake_duration_seconds_bucket{start="cold",le="5"} 0',
    'neon_control_wake_duration_seconds_bucket{start="cold",le="10"} 1',
    'neon_control_wake_duration_seconds_bucket{start="cold",le="+Inf"} 1',
    'neon_control_wake_duration_seconds_sum{start="cold"} 6.5',
    'neon_control_wake_duration_seconds_bucket{start="warm",le="0.25"} 1',
    "neon_control_wake_failures_total 1",
    "neon_control_suspends_total 1",
    'neon_control_lock_timeouts_total{lock="compute"} 1',
    'neon_control_compute_replicas{compute="alasio"} 1',
    'neon_control_compute_replicas{compute="branch-dev"} 0',
    "# TYPE neon_control_wake_duration_seconds histogram",
  ]) {
    assert.ok(lines.includes(line), line);
  }
  // A replica that no longer leads says nothing of the computes.
  metrics.replicas(new Map());
  assert.ok(!metrics.render().includes("neon_control_compute_replicas{"));
});
