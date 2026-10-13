/**
 * neon-control as the control plane of Neon's proxy (neon/control/endpoints.ts): which
 * role's verifier it answers on which compute, and the errors the proxy reads; and when a
 * compute takes connections, as neon-control logs in to it (neon/control/computes.ts),
 * on a real Postgres.
 */
import assert from "node:assert/strict";
import { describe, test } from "node:test";

import pg from "pg";

import type { ComputeCredentials } from "../neon/control/branches.ts";
import { computeAccepts } from "../neon/control/computes.ts";
import { controlPlaneError, proxyQuery, roleSecret } from "../neon/control/endpoints.ts";
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

test("a compute is taken to accept connections once neon-control's own role logs in to it", async () => {
  const database = await startPostgres();
  const url = new URL(database.url);
  const admin = new pg.Client({ connectionString: database.url });
  await admin.connect();
  try {
    await assert.rejects(computeAccepts(url.hostname, Number(url.port), "neon_control", "monitor-password"), /neon_control/u, "not before its role is there");
    await admin.query("create role neon_control login password 'monitor-password'");
    await computeAccepts(url.hostname, Number(url.port), "neon_control", "monitor-password");
    await assert.rejects(computeAccepts(url.hostname, Number(url.port), "neon_control", "wrong"), /password authentication failed/u);
  } finally {
    await admin.query("drop role if exists neon_control");
    await admin.end();
    await database.stop();
  }
});
