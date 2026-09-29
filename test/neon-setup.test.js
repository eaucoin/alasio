import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { generateKeyPair } from "../neon/control/jwt.js";
import { controlRevision, DEFAULT_BRIDGE, neonLayout, setupNeon } from "../neon/control/setup.js";
import { NEON_PROJECT, neonBridgeName } from "../src/neon/stack.js";

function withDir(fn) {
  const dir = mkdtempSync(join(tmpdir(), "alasio-neon-setup-"));
  try {
    return fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test("neon-control's revision changes with its code, and only with it", () => {
  withDir((dir) => {
    writeFileSync(join(dir, "service.js"), "a");
    const before = controlRevision(dir);
    writeFileSync(join(dir, "notes.txt"), "not code");
    assert.equal(controlRevision(dir), before);
    writeFileSync(join(dir, "service.js"), "a, changed");
    assert.notEqual(controlRevision(dir), before);
  });
});

test("alasio's stack gets the stable bridge name and any other project a distinct valid one", () => {
  assert.equal(neonBridgeName(NEON_PROJECT), DEFAULT_BRIDGE);
  const test1 = neonBridgeName("alasio-neon-test-1");
  assert.notEqual(test1, DEFAULT_BRIDGE);
  assert.notEqual(test1, neonBridgeName("alasio-neon-test-2"));
  assert.match(test1, /^[A-Za-z0-9_.-]{1,15}$/); // a Linux interface name
  withDir((dir) => {
    setupNeon(dir, { bridgeName: test1 });
    assert.match(readFileSync(neonLayout(dir).composeEnv, "utf8"), new RegExp(`^ALASIO_NEON_BRIDGE='${test1}'$`, "m"));
    assert.throws(() => setupNeon(dir, { bridgeName: "a-name-far-too-long" }), /Linux interface name/);
  });
});

test("the session-filesystem secrets are rendered for the stack and as files for alasio", () => {
  withDir((dir) => {
    const layout = setupNeon(dir);
    const secrets = JSON.parse(readFileSync(layout.secretsFile, "utf8"));
    const s3 = JSON.parse(readFileSync(join(layout.seaweedfs, "s3.json"), "utf8"));
    const sessions = s3.identities.find((identity) => identity.name === "sessions");
    assert.deepEqual(sessions.actions, ["Read:sessions", "List:sessions", "Tagging:sessions", "Write:sessions"]);
    assert.equal(readFileSync(layout.sandboxS3KeyFile, "utf8"), secrets.s3.sessions.accessKey);
    assert.equal(readFileSync(layout.sandboxS3SecretFile, "utf8"), secrets.s3.sessions.secretKey);
    assert.equal(readFileSync(layout.sandboxMetadataPasswordFile, "utf8"), secrets.sandboxMetadataPassword);
    assert.match(readFileSync(layout.composeEnv, "utf8"), /^SANDBOX_METADATA_PASSWORD='.+'$/m); // Valkey's requirepass
  });
});

test("a deployment made before a secret existed gains it on the next setup, keeping the rest", () => {
  withDir((dir) => {
    const layout = neonLayout(dir);
    mkdirSync(layout.secrets, { recursive: true });
    mkdirSync(layout.keys, { recursive: true });
    const { privateKeyPem, publicKeyPem } = generateKeyPair();
    writeFileSync(layout.privateKey, privateKeyPem);
    writeFileSync(layout.publicKey, publicKeyPem);
    const old = {
      tenantId: "t",
      timelineId: "tl",
      s3: { neon: { accessKey: "n", secretKey: "ns" }, admin: { accessKey: "a", secretKey: "as" } },
      controllerDbPassword: "c",
      alasioPassword: "q",
    };
    writeFileSync(layout.secretsFile, JSON.stringify(old));
    setupNeon(dir);
    const secrets = JSON.parse(readFileSync(layout.secretsFile, "utf8"));
    assert.ok(secrets.s3.sessions.accessKey && secrets.sandboxMetadataPassword); // gained
    assert.deepEqual(secrets.s3.neon, old.s3.neon); // kept
    assert.equal(secrets.alasioPassword, "q");
    assert.equal(secrets.tenantId, "t");
    const again = readFileSync(layout.secretsFile, "utf8");
    setupNeon(dir);
    assert.equal(readFileSync(layout.secretsFile, "utf8"), again); // then stable
  });
});
