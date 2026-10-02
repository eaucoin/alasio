import assert from "node:assert/strict";
import { test } from "node:test";

import { generateKeyPair, verifyToken } from "../neon/control/jwt.js";
import { renderSecrets, secretNames, setupConfig, setupKube } from "../neon/control/kube-setup.js";

const ENV = {
  NAMESPACE: "alasio",
  SECRET_PREFIX: "q",
  NEON_BROKER_URL: "http://q-neon-storage-broker:50051",
  NEON_CONTROLLER_URL: "http://q-neon-storage-controller:1234",
  NEON_PAGESERVER_HOST: "q-neon-pageserver",
  NEON_COMPUTE_HOST: "q-neon-compute",
  NEON_CONTROLLER_DB_HOST: "q-neon-controller-db",
  S3_ENDPOINT: "http://q-seaweedfs:8333",
};

/** Secrets in memory, as the API server keeps them: stringData becomes base64 data. */
function fakeKube() {
  const secrets = new Map();
  const stored = (object, version) => ({
    ...object,
    metadata: { ...object.metadata, resourceVersion: String(version) },
    data: Object.fromEntries(Object.entries(object.stringData).map(([k, v]) => [k, Buffer.from(v).toString("base64")])),
    stringData: undefined,
  });
  let version = 0;
  return {
    secrets,
    writes: [],
    async read(apiVersion, kind, namespace, name) {
      return structuredClone(secrets.get(name) ?? null);
    },
    async create(object) {
      if (secrets.has(object.metadata.name)) throw Object.assign(new Error("exists"), { code: 409 });
      this.writes.push(["create", object.metadata.name]);
      secrets.set(object.metadata.name, stored(object, ++version));
    },
    async replace(object) {
      const current = secrets.get(object.metadata.name);
      assert.equal(object.metadata.resourceVersion, current.metadata.resourceVersion, "a replace names the version it read");
      this.writes.push(["replace", object.metadata.name]);
      secrets.set(object.metadata.name, stored(object, ++version));
    },
  };
}

const value = (kube, name, key) => Buffer.from(kube.secrets.get(name).data[key], "base64").toString();

test("the setup's configuration comes from the environment, checked", () => {
  const config = setupConfig(ENV);
  assert.equal(config.prefix, "q");
  assert.equal(config.s3Region, "us-east-1");
  assert.equal(config.neonBucket, "neon");
  assert.equal(config.external, false);
  assert.throws(() => setupConfig({ ...ENV, NEON_COMPUTE_HOST: "" }), /NEON_COMPUTE_HOST must be set/);
  assert.throws(() => setupConfig({ ...ENV, S3_EXTERNAL: "1" }), /S3_ACCESS_KEY must be set/);
  assert.equal(setupConfig({ ...ENV, S3_EXTERNAL: "1", S3_ACCESS_KEY: "a", S3_SECRET_KEY: "b" }).accessKey, "a");
});

test("the stack's secrets are made once, and every service's rendered from them on each run", async () => {
  const kube = fakeKube();
  const config = setupConfig(ENV);
  await setupKube({ kube, config, log: () => {} });
  const names = secretNames("q");
  assert.deepEqual([...kube.secrets.keys()].sort(), Object.values(names).sort());
  const root = JSON.parse(value(kube, names.root, "secrets.json"));
  const publicKey = value(kube, names.root, "auth_public_key.pem");

  // Each service gets what it needs, signed with the stack's key.
  assert.deepEqual(verifyToken(publicKey, value(kube, names.storageController, "CONTROL_PLANE_JWT_TOKEN")), { scope: "admin" });
  assert.deepEqual(verifyToken(publicKey, value(kube, names.safekeeper, "safekeeper_peer_token")), { scope: "safekeeperdata" });
  assert.equal(value(kube, names.compute, "NEON_CONTROL_PLANE_TOKEN"), root.computeControlToken);
  assert.equal(value(kube, names.database, "url"), `postgresql://alasio:${encodeURIComponent(root.alasioPassword)}@q-neon-compute:55433/alasio`);
  assert.equal(value(kube, names.lake, "LAKE_S3_KEY"), root.s3.lake.accessKey);
  assert.match(value(kube, names.pageserver, "pageserver.toml"), /control_plane_api='http:\/\/q-neon-storage-controller:1234\/upcall\/v1\/'/u);
  assert.match(value(kube, names.pageserver, "pageserver.toml"), /endpoint="http:\/\/q-seaweedfs:8333", bucket_name="neon"/u);
  assert.equal(JSON.parse(value(kube, names.pageserver, "metadata.json")).host, "q-neon-pageserver");
  assert.deepEqual(JSON.parse(value(kube, names.seaweedfs, "s3.json")).identities.map((identity) => identity.name), ["neon", "admin", "sessions", "lake"]);

  // A second run keeps every secret and the key, and rewrites what derives from them.
  await setupKube({ kube, config, log: () => {} });
  assert.deepEqual(JSON.parse(value(kube, names.root, "secrets.json")), root);
  assert.equal(value(kube, names.root, "auth_public_key.pem"), publicKey);
  assert.equal(kube.writes.filter(([verb]) => verb === "replace").length, Object.keys(names).length);
});

test("a root predating a secret gains it without losing the rest", async () => {
  const kube = fakeKube();
  const config = setupConfig(ENV);
  await setupKube({ kube, config, log: () => {} });
  const names = secretNames("q");
  const root = JSON.parse(value(kube, names.root, "secrets.json"));
  delete root.computeControlToken;
  const current = kube.secrets.get(names.root);
  current.data["secrets.json"] = Buffer.from(JSON.stringify(root)).toString("base64");
  await setupKube({ kube, config, log: () => {} });
  const completed = JSON.parse(value(kube, names.root, "secrets.json"));
  assert.equal(completed.alasioPassword, root.alasioPassword);
  assert.match(completed.computeControlToken, /^[\w-]{32}$/u);
});

test("with an external object store, every service uses its credentials and no SeaweedFS identities are made", () => {
  const config = setupConfig({ ...ENV, S3_EXTERNAL: "1", S3_ACCESS_KEY: "AK", S3_SECRET_KEY: "SK", S3_REGION: "eu-west-1", S3_BUCKET_NEON: "my-neon" });
  const rendered = renderSecrets({
    secrets: { s3: {}, controllerDbPassword: "c", alasioPassword: "q", lakePassword: "l", computeControlToken: "t" },
    privateKeyPem: generateKeyPair().privateKeyPem,
    publicKeyPem: "pk",
    config,
  });
  const names = secretNames("q");
  assert.equal(rendered[names.seaweedfs], undefined);
  assert.equal(rendered[names.safekeeper].AWS_ACCESS_KEY_ID, "AK");
  assert.equal(rendered[names.lake].LAKE_S3_SECRET, "SK");
  assert.match(rendered[names.safekeeper].REMOTE_STORAGE, /bucket_name="my-neon", bucket_region="eu-west-1"/u);
});
