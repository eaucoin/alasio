import assert from "node:assert/strict";
import { test } from "node:test";

import type { KubernetesObject, V1Secret } from "@kubernetes/client-node";

import { generateKeyPair, verifyToken } from "../neon/control/jwt.ts";
import { renderSecrets, secretNames, setupConfig, setupKube, type StoredSecret } from "../neon/control/kube-setup.ts";
import type { SeaweedS3Config, StackSecrets, StoredSecrets } from "../neon/control/secrets.ts";

const ENV = {
  NAMESPACE: "alasio",
  SECRET_PREFIX: "q",
  NEON_BROKER_URL: "http://q-neon-storage-broker:50051",
  NEON_CONTROLLER_URL: "http://q-neon-storage-controller:1234",
  NEON_PAGESERVER_HOST: "q-neon-pageserver",
  NEON_COMPUTE_HOST: "q-neon-compute",
  NEON_CONTROLLER_DB_HOST: "q-neon-controller-db",
  S3_ENDPOINT: "http://q-seaweedfs:8333",
  WORKSPACES_NAME: "workspaces",
  WORKSPACES_BUCKET: "workspaces",
  WORKSPACES_BUCKET_URL: "http://q-seaweedfs.alasio.svc.cluster.local:8333/workspaces",
  WORKSPACES_TRASH_DAYS: "1",
  VALKEY_ADDRESS: "q-valkey.alasio.svc.cluster.local:6379",
  WORKSPACES_SECRET_LABELS: '{"alasio.dev/volume-driver":"csi.juicefs.com"}',
};

/** Secrets in memory, as the API server keeps them: stringData becomes base64 data. */
function nameOf(object: V1Secret): string {
  const name = object.metadata?.name;
  assert.ok(name, "a Secret is written by name");
  return name;
}

function fakeKube() {
  const secrets = new Map<string, StoredSecret>();
  const writes: [verb: "create" | "replace", name: string][] = [];
  const stored = ({ stringData, ...object }: V1Secret, version: number): StoredSecret => {
    assert.ok(stringData, "a Secret is written as string data");
    return {
      ...object,
      metadata: { ...object.metadata, resourceVersion: String(version) },
      data: Object.fromEntries(Object.entries(stringData).map(([k, v]) => [k, Buffer.from(v).toString("base64")])),
    };
  };
  let version = 0;
  return {
    secrets,
    writes,
    async read(_apiVersion: string, _kind: string, _namespace: string, name: string): Promise<KubernetesObject | null> {
      return structuredClone(secrets.get(name) ?? null);
    },
    async create(object: V1Secret) {
      if (secrets.has(nameOf(object))) throw Object.assign(new Error("exists"), { code: 409 });
      writes.push(["create", nameOf(object)]);
      secrets.set(nameOf(object), stored(object, ++version));
    },
    async replace(object: V1Secret) {
      const current = secrets.get(nameOf(object));
      assert.equal(object.metadata?.resourceVersion, current?.metadata.resourceVersion, "a replace names the version it read");
      writes.push(["replace", nameOf(object)]);
      secrets.set(nameOf(object), stored(object, ++version));
    },
  };
}

function value(kube: ReturnType<typeof fakeKube>, name: string, key: string): string {
  const data = kube.secrets.get(name)?.data?.[key];
  assert.ok(data !== undefined, `${name} has ${key}`);
  return Buffer.from(data, "base64").toString();
}

test("the setup's configuration comes from the environment, checked", () => {
  const config = setupConfig(ENV);
  assert.equal(config.prefix, "q");
  assert.equal(config.s3Region, "us-east-1");
  assert.equal(config.neonBucket, "neon");
  assert.equal(config.external, false);
  assert.throws(() => setupConfig({ ...ENV, NEON_COMPUTE_HOST: "" }), /NEON_COMPUTE_HOST must be set/);
  assert.throws(() => setupConfig({ ...ENV, S3_EXTERNAL: "1" }), /S3_ACCESS_KEY must be set/);
  const external = setupConfig({ ...ENV, S3_EXTERNAL: "1", S3_ACCESS_KEY: "a", S3_SECRET_KEY: "b" });
  assert.ok(external.external);
  assert.equal(external.accessKey, "a");
  assert.equal(config.workspaces?.valkeyAddress, "q-valkey.alasio.svc.cluster.local:6379");
  assert.equal(setupConfig({ ...ENV, WORKSPACES_NAME: "" }).workspaces, null);
  assert.throws(() => setupConfig({ ...ENV, WORKSPACES_BUCKET_URL: "" }), /WORKSPACES_BUCKET_URL must be set/);
});

test("the stack's secrets are made once, and every service's rendered from them on each run", async () => {
  const kube = fakeKube();
  const config = setupConfig(ENV);
  await setupKube({ kube, config, log: () => {} });
  const names = secretNames("q");
  assert.deepEqual([...kube.secrets.keys()].sort(), Object.values(names).sort());
  const root: StackSecrets = JSON.parse(value(kube, names.root, "secrets.json"));
  const publicKey = value(kube, names.root, "auth_public_key.pem");

  // Each service gets what it needs, signed with the stack's key.
  assert.deepEqual(verifyToken(publicKey, value(kube, names.storageController, "CONTROL_PLANE_JWT_TOKEN")), { scope: "admin" });
  assert.deepEqual(verifyToken(publicKey, value(kube, names.safekeeper, "safekeeper_peer_token")), { scope: "safekeeperdata" });
  assert.equal(value(kube, names.compute, "NEON_CONTROL_PLANE_TOKEN"), root.computeControlToken);
  assert.equal(value(kube, names.database, "url"), `postgresql://alasio:${encodeURIComponent(root.alasioPassword)}@q-neon-compute:55433/alasio`);
  assert.equal(value(kube, names.lake, "LAKE_S3_KEY"), root.s3.lake.accessKey);
  assert.deepEqual(verifyToken(publicKey, value(kube, names.lake, "LAKE_BRANCHES_TOKEN")), { scope: "branches" });
  assert.match(value(kube, names.pageserver, "pageserver.toml"), /control_plane_api='http:\/\/q-neon-storage-controller:1234\/upcall\/v1\/'/u);
  assert.match(value(kube, names.pageserver, "pageserver.toml"), /endpoint="http:\/\/q-seaweedfs:8333", bucket_name="neon"/u);
  const metadata: { host: string } = JSON.parse(value(kube, names.pageserver, "metadata.json"));
  assert.equal(metadata.host, "q-neon-pageserver");
  const seaweed: SeaweedS3Config = JSON.parse(value(kube, names.seaweedfs, "s3.json"));
  assert.deepEqual(seaweed.identities.map((identity) => identity.name), ["neon", "admin", "lake", "lakeReader", "workspaces"]);

  // The lake's query endpoint reads as a reader, in the catalog and in the object store, and Grafana queries it with its token.
  assert.equal(value(kube, names.lake, "LAKE_READER_PASSWORD"), root.lakeReaderPassword);
  assert.equal(value(kube, names.lake, "LAKE_READER_S3_KEY"), root.s3.lakeReader.accessKey);
  assert.deepEqual(seaweed.identities.find((identity) => identity.name === "lakeReader")?.actions, ["Read:lake", "List:lake"]);
  assert.equal(value(kube, names.database, "lake-reader-password"), root.lakeReaderPassword);
  assert.equal(value(kube, names.grafana, "LAKE_QUERY_TOKEN"), value(kube, names.lake, "LAKE_QUERY_TOKEN"));
  // alasio makes Grafana's role with the password Grafana connects with.
  assert.equal(value(kube, names.grafana, "GF_DATABASE_PASSWORD"), value(kube, names.database, "grafana-password"));
  assert.match(value(kube, names.grafana, "GF_SECURITY_ADMIN_PASSWORD"), /^[\w-]{32}$/u);
  assert.equal(value(kube, names.grafana, "GF_SECURITY_SECRET_KEY"), root.grafanaSecretKey);

  // Workspace storage's JuiceFS reaches Valkey with its password, and its own bucket alone.
  assert.equal(value(kube, names.valkey, "password"), root.valkeyPassword);
  assert.equal(value(kube, names.workspaces, "metaurl"), `redis://:${encodeURIComponent(root.valkeyPassword)}@q-valkey.alasio.svc.cluster.local:6379/1`);
  assert.equal(value(kube, names.workspaces, "name"), "workspaces");
  assert.equal(value(kube, names.workspaces, "storage"), "s3");
  assert.equal(value(kube, names.workspaces, "bucket"), "http://q-seaweedfs.alasio.svc.cluster.local:8333/workspaces");
  assert.equal(value(kube, names.workspaces, "access-key"), root.s3.workspaces.accessKey);
  assert.equal(value(kube, names.workspaces, "secret-key"), root.s3.workspaces.secretKey);
  assert.equal(value(kube, names.workspaces, "format-options"), "trash-days=1");
  const driverOf = (name: string) => kube.secrets.get(name)?.metadata.labels?.["alasio.dev/volume-driver"];
  assert.equal(driverOf(names.valkey), "csi.juicefs.com");
  assert.equal(driverOf(names.workspaces), "csi.juicefs.com");
  assert.equal(driverOf(names.database), undefined);
  assert.deepEqual(seaweed.identities.find((identity) => identity.name === "workspaces")?.actions, [
    "Read:workspaces",
    "List:workspaces",
    "Tagging:workspaces",
    "Write:workspaces",
  ]);

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
  const root: StoredSecrets = JSON.parse(value(kube, names.root, "secrets.json"));
  delete root.computeControlToken;
  const current = kube.secrets.get(names.root);
  assert.ok(current?.data);
  current.data["secrets.json"] = Buffer.from(JSON.stringify(root)).toString("base64");
  await setupKube({ kube, config, log: () => {} });
  const completed: StackSecrets = JSON.parse(value(kube, names.root, "secrets.json"));
  assert.equal(completed.alasioPassword, root.alasioPassword);
  assert.match(completed.computeControlToken, /^[\w-]{32}$/u);
});

/** Secrets as the root holds them, every S3 identity's credentials `s3`. */
function stackSecrets(s3: { accessKey: string; secretKey: string }): StackSecrets {
  return {
    tenantId: "tenant",
    timelineId: "timeline",
    s3: { neon: s3, admin: s3, lake: s3, lakeReader: s3, workspaces: s3 },
    controllerDbPassword: "c",
    alasioPassword: "q",
    lakePassword: "l",
    computeControlToken: "t",
    valkeyPassword: "v",
    lakeReaderPassword: "r",
    lakeQueryToken: "k",
    grafanaPassword: "g",
    grafanaAdminPassword: "a",
    grafanaSecretKey: "s",
  };
}

test("with an external object store, every service uses its credentials and no SeaweedFS identities are made", () => {
  const config = setupConfig({
    ...ENV,
    S3_EXTERNAL: "1",
    S3_ACCESS_KEY: "AK",
    S3_SECRET_KEY: "SK",
    S3_REGION: "eu-west-1",
    S3_BUCKET_NEON: "my-neon",
    WORKSPACES_BUCKET_URL: "https://s3.example.com/my-workspaces",
  });
  // The bundled store's credentials, which the external store's are used instead of.
  const rendered = renderSecrets({
    secrets: stackSecrets({ accessKey: "bundled-key", secretKey: "bundled-secret" }),
    privateKeyPem: generateKeyPair().privateKeyPem,
    publicKeyPem: "pk",
    config,
  });
  const names = secretNames("q");
  assert.equal(rendered[names.seaweedfs], undefined);
  assert.equal(rendered[names.safekeeper]?.["AWS_ACCESS_KEY_ID"], "AK");
  assert.equal(rendered[names.lake]?.["LAKE_S3_SECRET"], "SK");
  assert.match(rendered[names.safekeeper]?.["REMOTE_STORAGE"] ?? "",/bucket_name="my-neon", bucket_region="eu-west-1"/u);
  assert.equal(rendered[names.workspaces]?.["bucket"], "https://s3.example.com/my-workspaces");
  assert.equal(rendered[names.workspaces]?.["access-key"], "AK");
  assert.equal(rendered[names.workspaces]?.["secret-key"], "SK");
});

test("without workspace storage, neither its Secrets nor its identity are made", () => {
  const rendered = renderSecrets({
    secrets: stackSecrets({ accessKey: "key", secretKey: "secret" }),
    privateKeyPem: generateKeyPair().privateKeyPem,
    publicKeyPem: "pk",
    config: setupConfig({ ...ENV, WORKSPACES_NAME: "" }),
  });
  const names = secretNames("q");
  assert.equal(rendered[names.valkey], undefined);
  assert.equal(rendered[names.workspaces], undefined);
  const seaweed: SeaweedS3Config = JSON.parse(rendered[names.seaweedfs]?.["s3.json"] ?? "");
  assert.deepEqual(seaweed.identities.map((identity) => identity.name), ["neon", "admin", "lake", "lakeReader"]);
});
