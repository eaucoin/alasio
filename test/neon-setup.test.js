import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { generateKeyPair } from "../neon/control/jwt.js";
import { controlRevision, DEFAULT_BRIDGE, lakeRevision, neonLayout, setupNeon } from "../neon/control/setup.js";
import { NEON_PROJECT, neonBridgeName, neonTelemetry } from "../src/neon/stack.js";

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
    assert.ok(secrets.s3.lake.accessKey && secrets.lakePassword);
    assert.deepEqual(secrets.s3.neon, old.s3.neon); // kept
    assert.equal(secrets.alasioPassword, "q");
    assert.equal(secrets.tenantId, "t");
    const again = readFileSync(layout.secretsFile, "utf8");
    setupNeon(dir);
    assert.equal(readFileSync(layout.secretsFile, "utf8"), again); // then stable
  });
});

test("the stack's telemetry collector runs only with an endpoint, and is recreated when its configuration changes", () => {
  assert.equal(neonTelemetry({}), null);
  assert.deepEqual(
    neonTelemetry({ ALASIO_NEON_OTLP_ENDPOINT: "http://collector:4318", OTEL_EXPORTER_OTLP_HEADERS: "authorization=Bearer%20x" }),
    { endpoint: "http://collector:4318", headers: { authorization: "Bearer x" } },
  );
  withDir((dir) => {
    const layout = setupNeon(dir);
    assert.match(readFileSync(layout.composeEnv, "utf8"), /^COMPOSE_PROFILES=''$/m);
    assert.equal(existsSync(layout.otelCollectorConfig), false);

    setupNeon(dir, { otlp: { endpoint: "http://collector:4318", headers: { authorization: "Bearer x" } } });
    const composeEnv = readFileSync(layout.composeEnv, "utf8");
    assert.match(composeEnv, /^COMPOSE_PROFILES='telemetry'$/m);
    const revision = composeEnv.match(/^ALASIO_NEON_TELEMETRY_REVISION='(\w+)'$/m)[1];
    const config = JSON.parse(readFileSync(layout.otelCollectorConfig, "utf8"));
    assert.equal(statSync(layout.otelCollectorConfig).mode & 0o777, 0o600); // it holds the backend's headers
    assert.deepEqual(config.exporters.otlphttp, { endpoint: "http://collector:4318", headers: { authorization: "Bearer x" } });
    const targets = config.receivers.prometheus.config.scrape_configs.flatMap((job) => job.static_configs[0].targets);
    assert.deepEqual(targets, ["pageserver:9898", "safekeeper-1:7676", "safekeeper-2:7676", "safekeeper-3:7676", "storage-controller:1234", "storage-broker:50051", "compute:3080", "seaweedfs:9327"]);

    setupNeon(dir, { otlp: { endpoint: "http://elsewhere:4318", headers: {} } });
    assert.notEqual(readFileSync(layout.composeEnv, "utf8").match(/^ALASIO_NEON_TELEMETRY_REVISION='(\w+)'$/m)[1], revision);
  });
});

const envValue = (layout, name) => readFileSync(layout.composeEnv, "utf8").match(new RegExp(`^${name}='(.*)'$`, "m"))?.[1];

test("the analytics lake's role, database password, and storage identity exist whether or not it runs", () => {
  withDir((dir) => {
    const layout = setupNeon(dir);
    const secrets = JSON.parse(readFileSync(layout.secretsFile, "utf8"));
    const s3 = JSON.parse(readFileSync(join(layout.seaweedfs, "s3.json"), "utf8"));
    const lake = s3.identities.find((identity) => identity.name === "lake");
    assert.deepEqual(lake.actions, ["Read:lake", "List:lake", "Tagging:lake", "Write:lake"]); // its bucket alone
    assert.deepEqual(lake.credentials, [secrets.s3.lake]);
    assert.equal(envValue(layout, "LAKE_DATABASE_PASSWORD"), secrets.lakePassword);
    assert.equal(envValue(layout, "LAKE_S3_ACCESS_KEY"), secrets.s3.lake.accessKey);
    assert.equal(envValue(layout, "LAKE_S3_SECRET_KEY"), secrets.s3.lake.secretKey);
    assert.match(envValue(layout, "ALASIO_NEON_LAKE_IMAGE"), /^alasio-neon-lake:[0-9a-f]{16}$/);
    assert.equal(envValue(layout, "COMPOSE_PROFILES"), ""); // but it does not run
  });
});

test("the lake runs only when on, its metrics scraped with the stack's", () => {
  withDir((dir) => {
    const layout = setupNeon(dir, { lake: true });
    assert.equal(envValue(layout, "COMPOSE_PROFILES"), "lake");
    setupNeon(dir, { lake: true, otlp: { endpoint: "http://collector:4318", headers: {} } });
    assert.equal(envValue(layout, "COMPOSE_PROFILES"), "telemetry,lake");
    const config = JSON.parse(readFileSync(layout.otelCollectorConfig, "utf8"));
    const jobs = Object.fromEntries(config.receivers.prometheus.config.scrape_configs.map((job) => [job.job_name, job.static_configs[0].targets]));
    assert.deepEqual(jobs.lake, ["lake:9464"]);
    setupNeon(dir, { otlp: { endpoint: "http://collector:4318", headers: {} } });
    assert.equal(JSON.parse(readFileSync(layout.otelCollectorConfig, "utf8")).receivers.prometheus.config.scrape_configs.some((job) => job.job_name === "lake"), false);
  });
});

test("the lake's image revision changes with what it is built from, and only with it", () => {
  withDir((dir) => {
    mkdirSync(join(dir, "src"));
    for (const name of ["Dockerfile", ".dockerignore", "package.json", "package-lock.json", "src/service.js"]) writeFileSync(join(dir, name), name);
    const before = lakeRevision(dir);
    writeFileSync(join(dir, "README.md"), "not built from");
    assert.equal(lakeRevision(dir), before);
    writeFileSync(join(dir, "src", "service.js"), "changed");
    assert.notEqual(lakeRevision(dir), before);
  });
});

test("SeaweedFS is recreated when its S3 identities change, and only then", () => {
  withDir((dir) => {
    const layout = setupNeon(dir);
    const revision = envValue(layout, "ALASIO_NEON_S3_REVISION");
    setupNeon(dir, { lake: true });
    assert.equal(envValue(layout, "ALASIO_NEON_S3_REVISION"), revision);
    const secrets = JSON.parse(readFileSync(layout.secretsFile, "utf8"));
    secrets.s3.lake.secretKey = "rotated";
    writeFileSync(layout.secretsFile, JSON.stringify(secrets));
    setupNeon(dir);
    assert.notEqual(envValue(layout, "ALASIO_NEON_S3_REVISION"), revision);
  });
});
