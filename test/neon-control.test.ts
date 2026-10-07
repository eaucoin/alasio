import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
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
  existingBranch,
  nameProblem,
  placementOf,
  type ReadyBranch,
  safekeepersNotified,
  tokenSha256,
  withNotifiedPlacement,
} from "../neon/control/branches.ts";
import { bearsScope, generateKeyPair, signToken } from "../neon/control/jwt.ts";
import { scramVerifier } from "../neon/control/scram.ts";

const MAIN_TIMELINE = "11111111111111111111111111111111";
const DEV_TIMELINE = "22222222222222222222222222222222";

/** dev's compute's own credentials: its role's password's verifier, and its token's hash. */
const DEV_COMPUTE = { passwordVerifier: scramVerifier("dev-password"), tokenSha256: tokenSha256("dev-token") };

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
        { name: "dev", compute: { ...compute, passwordVerifier: "dev-password" } },
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

  test("refusing a branch point its parent lacks is the caller's to change; any other refusal, to ask again", () => {
    const asked = { name: "dev", parent: "main", lsn: "0/8" };
    const wrapped = '{"msg":"pageserver 1 406 Not Acceptable: 406 Not Acceptable invalid branch start lsn: less than latest GC cutoff 0/14EE2B0"}';
    assert.equal(creationRefused(asked, 409, wrapped).status, 406);
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

test("a branch's compute spec is main's but for its timeline, its safekeepers and its role's password", () => {
  const stack: ComputeStack = {
    tenantId: "t",
    pageserverHost: "pageserver",
    safekeepers: [1, 2, 3].map((id) => ({ id, host: `sk-${id - 1}`, pgPort: 5454, httpPort: 7676 })),
    passwordVerifier: "SCRAM-SHA-256$verifier",
    storageAuthToken: "token",
    jwks: { keys: [] },
  };
  const ofMain = computeConfig(main, stack);
  const ofDev = computeConfig({ ...dev, safekeepers: { generation: 2, ids: [2, 3] } }, stack);
  assert.equal(ofDev.spec.timeline_id, DEV_TIMELINE);
  assert.deepEqual(ofDev.spec.safekeeper_connstrings, ["sk-1:5454", "sk-2:5454"]);
  assert.equal(ofDev.spec.safekeepers_generation, 2);
  assert.deepEqual(ofMain.spec.safekeeper_connstrings, ["sk-0:5454", "sk-1:5454", "sk-2:5454"]);
  const { timeline_id: _main, safekeeper_connstrings: _mainSafekeepers, safekeepers_generation: _mainGeneration, ...mainRest } = ofMain.spec;
  const { timeline_id: _dev, safekeeper_connstrings: _devSafekeepers, safekeepers_generation: _devGeneration, ...devRest } = ofDev.spec;
  const { cluster: { roles: mainRoles, ...mainCluster }, ...mainSpec } = mainRest;
  const { cluster: { roles: devRoles, ...devCluster }, ...devSpec } = devRest;
  assert.deepEqual([devSpec, devCluster], [mainSpec, mainCluster]);
  assert.deepEqual(mainRoles, [{ name: "alasio", encrypted_password: "SCRAM-SHA-256$verifier", options: null }]);
  assert.deepEqual(devRoles, [{ name: "alasio", encrypted_password: DEV_COMPUTE.passwordVerifier, options: null }]);
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

test("the lake's token of the branches scope lists the branches, and calls nothing else", () => {
  const { privateKeyPem, publicKeyPem } = generateKeyPair();
  const lake = `Bearer ${signToken(privateKeyPem, BRANCHES_SCOPE)}`;
  const takes = (method: string, url: string) => bearsScope(publicKeyPem, lake, ...callScopes(method, url));
  assert.ok(takes("GET", "/branches"));
  assert.ok(!takes("POST", "/branches"));
  assert.ok(!takes("DELETE", "/branches/dev"));
  assert.ok(!takes("PUT", "/notify-safekeepers"));
  assert.ok(bearsScope(publicKeyPem, `Bearer ${signToken(privateKeyPem, "admin")}`, ...callScopes("POST", "/branches")));
});
