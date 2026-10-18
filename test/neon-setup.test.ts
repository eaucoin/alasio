import assert from "node:assert/strict";
import { X509Certificate } from "node:crypto";
import { test } from "node:test";

import type { KubernetesObject, V1Secret } from "@kubernetes/client-node";

import { makeCertificate, RENEW_DAYS, VALID_DAYS, webhookCertificate } from "../neon/control/certificate.ts";
import { generateKeyPair, verifyToken } from "../neon/control/jwt.ts";
import { refuseMainKeyOnSnapshots, renderSecrets, secretNames, setupConfig, setupKube, type StoredSecret } from "../neon/control/kube-setup.ts";
import { scramVerifier } from "../neon/control/scram.ts";
import { clusterId, type SeaweedS3Config, type StackSecrets, type StoredSecrets } from "../neon/control/secrets.ts";
import { environmentKey } from "../src/binder/names.ts";
import { makeS3Bucket } from "../src/node-agent/s3.ts";
import { startFakeS3 } from "./support/fake-s3.ts";

/** Whether `verifier` is a SCRAM verifier of `password`, made with its own salt. */
function verifies(verifier: string | undefined, password: string | undefined): boolean {
  const salt = /^SCRAM-SHA-256\$4096:([^$]+)\$/u.exec(verifier ?? "")?.[1];
  return salt !== undefined && password !== undefined && scramVerifier(password, Buffer.from(salt, "base64")) === verifier;
}

const ENV = {
  NAMESPACE: "alasio",
  SECRET_PREFIX: "q",
  NEON_BROKER_URL: "http://q-neon-storage-broker:50051",
  NEON_CONTROLLER_URL: "http://q-neon-storage-controller:1234",
  NEON_PROXY_HOST: "q-neon-proxy.alasio.svc",
  NEON_CONTROLLER_DB_HOST: "q-neon-controller-db",
  S3_ENDPOINT: "http://q-seaweedfs:8333",
  WORKSPACES_NAME: "workspaces",
  WORKSPACES_BUCKET: "workspaces",
  WORKSPACES_BUCKET_URL: "http://q-seaweedfs.alasio.svc.cluster.local:8333/workspaces",
  WORKSPACES_TRASH_DAYS: "1",
  VALKEY_ADDRESS: "q-valkey.alasio.svc.cluster.local:6379",
  SNAPSHOTS_BUCKET: "snapshots",
  WEBHOOK_ADDRESS: "203.0.113.10",
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
  // Without Neon, none of its hosts; with it, every one.
  assert.equal(setupConfig({ ...ENV, NEON_PROXY_HOST: "" }).neon, null);
  assert.throws(() => setupConfig({ ...ENV, NEON_BROKER_URL: "" }), /NEON_BROKER_URL must be set/);
  assert.throws(() => setupConfig({ ...ENV, S3_EXTERNAL: "1" }), /S3_ACCESS_KEY must be set/);
  // Where sessions are snapshotted, the external store's key for the snapshots bucket alone is required.
  assert.throws(() => setupConfig({ ...ENV, S3_EXTERNAL: "1", S3_ACCESS_KEY: "a", S3_SECRET_KEY: "b" }), /S3_SNAPSHOTS_ACCESS_KEY must be set/);
  const external = setupConfig({ ...ENV, S3_EXTERNAL: "1", S3_ACCESS_KEY: "a", S3_SECRET_KEY: "b", S3_SNAPSHOTS_ACCESS_KEY: "c", S3_SNAPSHOTS_SECRET_KEY: "d" });
  assert.ok(external.external);
  assert.equal(external.accessKey, "a");
  assert.deepEqual(external.snapshots, { accessKey: "c", secretKey: "d" });
  const unsnapshotted = setupConfig({ ...ENV, S3_EXTERNAL: "1", S3_ACCESS_KEY: "a", S3_SECRET_KEY: "b", SNAPSHOTS_BUCKET: "" });
  assert.ok(unsnapshotted.external && unsnapshotted.snapshots === null);
  assert.equal(config.workspaces?.valkeyAddress, "q-valkey.alasio.svc.cluster.local:6379");
  assert.equal(setupConfig({ ...ENV, WORKSPACES_NAME: "" }).workspaces, null);
  assert.throws(() => setupConfig({ ...ENV, WORKSPACES_BUCKET_URL: "" }), /WORKSPACES_BUCKET_URL must be set/);
  assert.equal(config.snapshotsBucket, "snapshots");
  assert.equal(setupConfig({ ...ENV, SNAPSHOTS_BUCKET: "" }).snapshotsBucket, null);
});

test("the stack's secrets are made once, and every service's rendered from them on each run", async () => {
  const kube = fakeKube();
  const config = setupConfig(ENV);
  await setupKube({ kube, config, log: () => {} });
  const names = secretNames("q");
  // Every Secret's name, each log principal's among them.
  const every = Object.values(names).flatMap((name) => (typeof name === "string" ? [name] : Object.values(name)));
  assert.deepEqual([...kube.secrets.keys()].sort(), every.sort());
  const root: StackSecrets = JSON.parse(value(kube, names.root, "secrets.json"));
  const publicKey = value(kube, names.root, "auth_public_key.pem");

  // Each service gets what it needs, signed with the stack's key.
  assert.deepEqual(verifyToken(publicKey, value(kube, names.storageController, "CONTROL_PLANE_JWT_TOKEN")), { scope: "admin" });
  assert.deepEqual(verifyToken(publicKey, value(kube, names.safekeeper, "safekeeper_peer_token")), { scope: "safekeeperdata" });
  assert.equal(value(kube, names.compute, "NEON_CONTROL_PLANE_TOKEN"), root.computeControlToken);
  // Main's compute, through Neon's proxy, which names it by the option; the proxy with a token that does nothing else.
  assert.equal(value(kube, names.database, "url"), `postgresql://alasio:${encodeURIComponent(root.alasioPassword)}@q-neon-proxy.alasio.svc:5432/alasio?options=endpoint%3Dalasio`);
  assert.deepEqual(verifyToken(publicKey, value(kube, names.proxy, "NEON_PROXY_TO_CONTROLPLANE_TOKEN")), { scope: "proxy" });
  assert.equal(value(kube, names.lake, "LAKE_S3_KEY"), root.s3.lake.accessKey);
  assert.deepEqual(verifyToken(publicKey, value(kube, names.lake, "LAKE_BRANCHES_TOKEN")), { scope: "branches" });
  assert.deepEqual(verifyToken(publicKey, value(kube, names.branches, "control-token")), { scope: "admin" });
  assert.equal(value(kube, names.branches, "fork-key"), root.branchForkKey);
  // neon-control's own, which it draws its role's passwords on the computes from, rendered nowhere but the root.
  assert.match(root.monitorKey ?? "", /^[\w-]{32}$/u);
  assert.ok(![...kube.secrets.entries()].some(([name, secret]) => name !== names.root && Object.values(secret.data ?? {}).some((data) => Buffer.from(data, "base64").toString("utf8").includes(root.monitorKey ?? "-"))));
  assert.match(value(kube, names.pageserver, "pageserver.toml"), /control_plane_api='http:\/\/q-neon-storage-controller:1234\/upcall\/v1\/'/u);
  assert.match(value(kube, names.pageserver, "pageserver.toml"), /endpoint="http:\/\/q-seaweedfs:8333", bucket_name="neon"/u);
  // It evicts layers only once the node's disk, which it shares, is nearly full: at 94 % used or under 8 GiB free.
  assert.match(value(kube, names.pageserver, "pageserver.toml"), /^disk_usage_based_eviction=\{ max_usage_pct=94, min_avail_bytes=8589934592 \}$/mu);
  // Its identity is each pageserver's own, which it renders itself (cli/src/manifests/neon.ts), not the stack's.
  const pageserverKeys = Object.keys(kube.secrets.get(names.pageserver)?.data ?? {});
  assert.ok(!pageserverKeys.includes("identity.toml") && !pageserverKeys.includes("metadata.json"), pageserverKeys.join(", "));
  const seaweed: SeaweedS3Config = JSON.parse(value(kube, names.seaweedfs, "s3.json"));
  assert.deepEqual(seaweed.identities.map((identity) => identity.name), ["neon", "admin", "backups", "lake", "lakeReader", "workspaces", "snapshots"]);

  // The log's: its cluster's id and its admin's password; every principal's for its
  // access applier; and each principal's, KEDA's with its mechanism.
  assert.match(value(kube, names.log, "KAFKA_CLUSTER_ID"), /^[\w-]{22}$/u);
  assert.equal(value(kube, names.log, "LOG_ADMIN_PASSWORD"), root.log.admin);
  assert.deepEqual(Object.keys(kube.secrets.get(names.logAccess)?.data ?? {}).sort(), ["alasio-main", "gateway", "intake", "loader-main", "sandbox-gateway", "scaler", "webhook"]);
  assert.deepEqual(names.logPrincipals, {
    "gateway": "q-log-gateway",
    "intake": "q-log-intake",
    "scaler": "q-log-scaler",
    "webhook": "q-log-webhook",
    "sandbox-gateway": "q-log-sandbox-gateway",
    "alasio-main": "q-log-alasio-main",
    "loader-main": "q-log-loader-main",
  });
  for (const principal of ["gateway", "intake", "scaler", "webhook", "alasio-main", "loader-main"] as const) {
    assert.equal(value(kube, names.logAccess, principal), root.log[principal]);
    assert.equal(value(kube, names.logPrincipals[principal], "username"), principal);
    assert.equal(value(kube, names.logPrincipals[principal], "password"), root.log[principal]);
  }
  assert.equal(value(kube, names.logPrincipals.scaler, "sasl"), "scram_sha512");
  assert.equal(kube.secrets.get(names.logPrincipals.gateway)?.data?.["sasl"], undefined);
  assert.equal(new Set(Object.values(root.log)).size, 8);
  // The filers' store's users' passwords, each its own: the filers', the collector's and the backups'.
  assert.equal(value(kube, names.filerStoreFiler, "password"), root.filerStoreFilerPassword);
  assert.equal(value(kube, names.filerStoreCollector, "password"), root.filerStoreCollectorPassword);
  assert.equal(value(kube, names.filerStoreBackup, "password"), root.filerStoreBackupPassword);
  assert.equal(new Set([root.filerStoreFilerPassword, root.filerStoreCollectorPassword, root.filerStoreBackupPassword]).size, 3);
  assert.deepEqual([names.filerStoreFiler, names.filerStoreCollector, names.filerStoreBackup], ["q-seaweedfs-filer-store-filer", "q-seaweedfs-filer-store-collector", "q-seaweedfs-filer-store-backup"]);

  // The lake's query endpoint reads as a reader, in the catalog and in the object store, and Grafana queries it with its token.
  assert.equal(value(kube, names.lake, "LAKE_READER_PASSWORD"), root.lakeReaderPassword);
  assert.equal(value(kube, names.lake, "LAKE_READER_S3_KEY"), root.s3.lakeReader.accessKey);
  assert.deepEqual(seaweed.identities.find((identity) => identity.name === "lakeReader")?.actions, ["Read:lake", "List:lake"]);
  // alasio sets each role it makes to the verifier made once with its password, the one neon-control answers Neon's proxy with.
  for (const [key, role, password] of [["lake-verifier", "lake", root.lakePassword], ["lake-reader-verifier", "lake_reader", root.lakeReaderPassword], ["grafana-verifier", "grafana", root.grafanaPassword]] as const) {
    const verifier = value(kube, names.database, key);
    assert.equal(verifier, root.verifiers?.[role], key);
    assert.ok(verifies(verifier, password), key);
  }
  assert.ok(verifies(root.verifiers?.alasio, root.alasioPassword));
  // Each salted at random.
  assert.equal(new Set(Object.values(root.verifiers ?? {}).map((verifier) => verifier.split("$")[1])).size, 4);
  assert.equal(value(kube, names.grafana, "LAKE_QUERY_TOKEN"), value(kube, names.lake, "LAKE_QUERY_TOKEN"));
  // Grafana's password, which its database's URL takes as it is.
  assert.equal(value(kube, names.grafana, "GF_DATABASE_PASSWORD"), root.grafanaPassword);
  assert.match(value(kube, names.grafana, "GF_DATABASE_PASSWORD"), /^[\w-]{32}$/u);
  assert.match(value(kube, names.grafana, "GF_SECURITY_ADMIN_PASSWORD"), /^[\w-]{32}$/u);
  assert.equal(value(kube, names.grafana, "GF_SECURITY_SECRET_KEY"), root.grafanaSecretKey);

  // Workspace storage's JuiceFS reaches Valkey with its password, and its own bucket alone.
  assert.equal(value(kube, names.valkey, "password"), root.valkeyPassword);
  assert.equal(value(kube, names.workspaces, "metaurl"), `redis://:${encodeURIComponent(root.valkeyPassword)}@q-valkey.alasio.svc.cluster.local:6379/1`);
  // The node agents', a Valkey user of their own, of a password of its own.
  assert.equal(value(kube, names.workspaceQuotas, "password"), root.valkeyQuotasPassword);
  assert.equal(value(kube, names.workspaceQuotas, "metaurl"), `redis://quotas:${encodeURIComponent(root.valkeyQuotasPassword)}@q-valkey.alasio.svc.cluster.local:6379/1`);
  assert.notEqual(root.valkeyQuotasPassword, root.valkeyPassword);
  assert.equal(value(kube, names.workspaces, "name"), "workspaces");
  assert.equal(value(kube, names.workspaces, "storage"), "s3");
  assert.equal(value(kube, names.workspaces, "bucket"), "http://q-seaweedfs.alasio.svc.cluster.local:8333/workspaces");
  assert.equal(value(kube, names.workspaces, "access-key"), root.s3.workspaces.accessKey);
  assert.equal(value(kube, names.workspaces, "secret-key"), root.s3.workspaces.secretKey);
  assert.equal(value(kube, names.workspaces, "format-options"), "trash-days=1");
  assert.deepEqual(seaweed.identities.find((identity) => identity.name === "workspaces")?.actions, [
    "Read:workspaces",
    "List:workspaces",
    "Tagging:workspaces",
    "Write:workspaces",
  ]);

  // The node agents keep session sandboxes' snapshots in their bucket alone, which they find, with their keys, in their
  // Secret, and give its lifecycle rules as its admin; no other identity may do anything with it.
  assert.deepEqual(seaweed.identities.find((identity) => identity.name === "snapshots")?.actions, [
    "Read:snapshots",
    "List:snapshots",
    "Tagging:snapshots",
    "Write:snapshots",
    "Admin:snapshots",
  ]);
  for (const { name, actions } of seaweed.identities) {
    if (name !== "snapshots") assert.ok(actions.every((action) => action.includes(":") && !action.endsWith(":snapshots")), `${name}: ${actions.join(", ")}`);
  }
  // The admin, of Neon's bucket alone, gives it its lifecycle rules; the backups' writes the backups and reads the workspaces' metadata.
  assert.deepEqual(seaweed.identities.find((identity) => identity.name === "admin")?.actions, ["Admin:neon"]);
  assert.deepEqual(seaweed.identities.find((identity) => identity.name === "backups")?.actions, [
    "Read:backups",
    "List:backups",
    "Tagging:backups",
    "Write:backups",
    "Read:workspaces",
    "List:workspaces",
  ]);
  assert.equal(value(kube, names.s3Backups, "AWS_ACCESS_KEY_ID"), root.s3.backups.accessKey);
  assert.match(root.s3.backups.accessKey, /^backups[0-9a-f]{12}$/u);
  assert.deepEqual(Object.fromEntries(Object.keys(kube.secrets.get(names.snapshots)?.data ?? {}).map((key) => [key, value(kube, names.snapshots, key)])), {
    S3_ENDPOINT: "http://q-seaweedfs:8333",
    S3_REGION: "us-east-1",
    S3_BUCKET: "snapshots",
    AWS_ACCESS_KEY_ID: root.s3.snapshots.accessKey,
    AWS_SECRET_ACCESS_KEY: root.s3.snapshots.secretKey,
  });
  // alasio and the node agents read the token alasio presents them from one Secret.
  assert.equal(value(kube, names.nodeAgent, "token"), root.nodeAgentToken);
  assert.match(root.nodeAgentToken, /^[\w-]{32}$/u);
  // The sandbox gateway's keys, each its own.
  assert.deepEqual([value(kube, names.sandboxGateway, "caller-key"), value(kube, names.sandboxGateway, "binding-key")], [root.gatewayCallerKey, root.gatewayBindingKey]);
  assert.notEqual(root.gatewayCallerKey, root.gatewayBindingKey);
  // main's alasio's key to it, of the caller key, not the caller key itself.
  assert.equal(value(kube, names.sessionsCaller, "caller-key"), environmentKey(Buffer.from(root.gatewayCallerKey), "main").toString());
  assert.notEqual(value(kube, names.sessionsCaller, "caller-key"), root.gatewayCallerKey);

  // A second run keeps every secret and the key, and rewrites what derives from them.
  await setupKube({ kube, config, log: () => {} });
  assert.deepEqual(JSON.parse(value(kube, names.root, "secrets.json")), root);
  assert.equal(value(kube, names.root, "auth_public_key.pem"), publicKey);
  assert.equal(kube.writes.filter(([verb]) => verb === "replace").length, every.length);
});

test("the log's cluster id is Kafka's kind, and never begins with what its command line takes for an option", () => {
  // One in 64 would begin with "-" drawn alone.
  const ids = Array.from({ length: 10_000 }, clusterId);
  assert.deepEqual(ids.filter((id) => !/^[A-Za-z0-9_][A-Za-z0-9_-]{21}$/u.test(id)), []);
});

test("the webhook intake's certificate is made for its address, kept while it serves it, and made anew for another", async () => {
  const kube = fakeKube();
  const names = secretNames("q");
  await setupKube({ kube, config: setupConfig(ENV), log: () => {} });
  const first = value(kube, names.webhookTls, "tls.crt");
  assert.equal(new X509Certificate(first).subjectAltName, "IP Address:203.0.113.10");
  assert.match(value(kube, names.webhookTls, "tls.key"), /^-----BEGIN PRIVATE KEY-----/u);
  await setupKube({ kube, config: setupConfig(ENV), log: () => {} });
  assert.equal(value(kube, names.webhookTls, "tls.crt"), first);
  await setupKube({ kube, config: setupConfig({ ...ENV, WEBHOOK_ADDRESS: "bot.example.com" }), log: () => {} });
  assert.equal(new X509Certificate(value(kube, names.webhookTls, "tls.crt")).subjectAltName, "DNS:bot.example.com");
});

test("a certificate is made anew within its last days, and one that does not parse at once", () => {
  const made = makeCertificate("203.0.113.10");
  const certificate = new X509Certificate(made.cert);
  assert.equal(certificate.subject, "CN=203.0.113.10");
  assert.equal(Math.round((Date.parse(certificate.validTo) - Date.parse(certificate.validFrom)) / 86_400_000), VALID_DAYS);
  const fresh = () => ({ cert: "new", key: "new" });
  const at = (days: number) => new Date(Date.parse(certificate.validTo) - days * 86_400_000);
  assert.equal(webhookCertificate("203.0.113.10", made, at(RENEW_DAYS + 1), fresh).made, false);
  assert.equal(webhookCertificate("203.0.113.10", made, at(RENEW_DAYS - 1), fresh).made, true);
  assert.equal(webhookCertificate("203.0.113.11", made, at(RENEW_DAYS + 1), fresh).made, true);
  assert.deepEqual(webhookCertificate("203.0.113.10", { cert: "x", key: "y" }, new Date(), fresh), { certificate: fresh(), made: true });
});

test("a root predating a secret gains it without losing the rest", async () => {
  const kube = fakeKube();
  const config = setupConfig(ENV);
  await setupKube({ kube, config, log: () => {} });
  const names = secretNames("q");
  const root: StoredSecrets = JSON.parse(value(kube, names.root, "secrets.json"));
  delete root.computeControlToken;
  delete root.verifiers?.lake;
  const current = kube.secrets.get(names.root);
  assert.ok(current?.data);
  current.data["secrets.json"] = Buffer.from(JSON.stringify(root)).toString("base64");
  await setupKube({ kube, config, log: () => {} });
  const completed: StackSecrets = JSON.parse(value(kube, names.root, "secrets.json"));
  assert.equal(completed.alasioPassword, root.alasioPassword);
  assert.match(completed.computeControlToken, /^[\w-]{32}$/u);
  // A verifier missing is made of the password kept; those there are kept.
  assert.equal(completed.verifiers.alasio, root.verifiers?.alasio);
  assert.ok(verifies(completed.verifiers.lake, root.lakePassword));
});

test("a root predating an S3 identity gains its credentials, and the store the identity, every other secret kept", async () => {
  const kube = fakeKube();
  const config = setupConfig(ENV);
  await setupKube({ kube, config, log: () => {} });
  const names = secretNames("q");
  const root: StoredSecrets = JSON.parse(value(kube, names.root, "secrets.json"));
  delete root.s3?.snapshots;
  const current = kube.secrets.get(names.root);
  assert.ok(current?.data);
  current.data["secrets.json"] = Buffer.from(JSON.stringify(root)).toString("base64");
  await setupKube({ kube, config, log: () => {} });
  const completed: StackSecrets = JSON.parse(value(kube, names.root, "secrets.json"));
  const { snapshots, ...kept } = completed.s3;
  assert.deepEqual({ ...completed, s3: kept }, root);
  assert.match(snapshots.accessKey, /^snapshots[0-9a-f]{12}$/u);
  const seaweed: SeaweedS3Config = JSON.parse(value(kube, names.seaweedfs, "s3.json"));
  assert.deepEqual(seaweed.identities.find((identity) => identity.name === "snapshots")?.credentials, [snapshots]);
});

/** Secrets as the root holds them, every S3 identity's credentials `s3`. */
function stackSecrets(s3: { accessKey: string; secretKey: string }): StackSecrets {
  return {
    tenantId: "tenant",
    timelineId: "timeline",
    s3: { neon: s3, admin: s3, backups: s3, lake: s3, lakeReader: s3, workspaces: s3, snapshots: s3 },
    controllerDbPassword: "c",
    alasioPassword: "q",
    lakePassword: "l",
    computeControlToken: "t",
    valkeyQuotasPassword: "vq",
    valkeyPassword: "v",
    lakeReaderPassword: "r",
    lakeQueryToken: "k",
    grafanaPassword: "g",
    grafanaAdminPassword: "a",
    grafanaSecretKey: "s",
    branchForkKey: "f",
    filerStoreFilerPassword: "p",
    filerStoreCollectorPassword: "c",
    filerStoreBackupPassword: "b",
    monitorKey: "m",
    verifiers: { alasio: "SCRAM-SHA-256$q", lake: "SCRAM-SHA-256$l", lake_reader: "SCRAM-SHA-256$r", grafana: "SCRAM-SHA-256$g" },
    nodeAgentToken: "n",
    gatewayCallerKey: "gc",
    gatewayBindingKey: "gb",
    logClusterId: "cluster",
    log: { "admin": "la", "gateway": "lg", "intake": "li", "scaler": "ls", "webhook": "lw", "sandbox-gateway": "lb", "alasio-main": "lm", "loader-main": "ll" },
  };
}

test("with an external object store, every service uses its credentials, but the node agents their key for the snapshots bucket, and no SeaweedFS identities are made", () => {
  const config = setupConfig({
    ...ENV,
    S3_EXTERNAL: "1",
    S3_ACCESS_KEY: "AK",
    S3_SECRET_KEY: "SK",
    S3_SNAPSHOTS_ACCESS_KEY: "SNAP-AK",
    S3_SNAPSHOTS_SECRET_KEY: "SNAP-SK",
    S3_REGION: "eu-west-1",
    S3_BUCKET_NEON: "my-neon",
    WORKSPACES_BUCKET_URL: "https://s3.example.com/my-workspaces",
  });
  // The bundled store's credentials, which the external store's are used instead of.
  const rendered = renderSecrets({
    secrets: stackSecrets({ accessKey: "bundled-key", secretKey: "bundled-secret" }),
    privateKeyPem: generateKeyPair().privateKeyPem,
    publicKeyPem: "pk",
    webhookTls: { cert: "c", key: "k" },
    config,
  });
  const names = secretNames("q");
  assert.equal(rendered[names.seaweedfs], undefined);
  assert.equal(rendered[names.filerStoreFiler], undefined);
  assert.equal(rendered[names.filerStoreCollector], undefined);
  assert.equal(rendered[names.filerStoreBackup], undefined);
  assert.equal(rendered[names.safekeeper]?.["AWS_ACCESS_KEY_ID"], "AK");
  assert.equal(rendered[names.lake]?.["LAKE_S3_SECRET"], "SK");
  assert.match(rendered[names.safekeeper]?.["REMOTE_STORAGE"] ?? "",/bucket_name="my-neon", bucket_region="eu-west-1"/u);
  assert.equal(rendered[names.workspaces]?.["bucket"], "https://s3.example.com/my-workspaces");
  assert.equal(rendered[names.workspaces]?.["access-key"], "AK");
  assert.equal(rendered[names.workspaces]?.["secret-key"], "SK");
  assert.deepEqual(rendered[names.snapshots], {
    S3_ENDPOINT: "http://q-seaweedfs:8333",
    S3_REGION: "eu-west-1",
    S3_BUCKET: "snapshots",
    AWS_ACCESS_KEY_ID: "SNAP-AK",
    AWS_SECRET_ACCESS_KEY: "SNAP-SK",
  });
  // No other Secret holds the snapshots bucket's key.
  for (const [name, data] of Object.entries(rendered)) {
    if (name !== names.snapshots) assert.ok(!JSON.stringify(data).includes("SNAP-"), name);
  }
});

test("without Neon, the log's and the webhook intake's Secrets are made, and none of Neon's, the object store's or workspace storage's", () => {
  const rendered = renderSecrets({
    secrets: stackSecrets({ accessKey: "key", secretKey: "secret" }),
    privateKeyPem: generateKeyPair().privateKeyPem,
    publicKeyPem: "pk",
    webhookTls: { cert: "c", key: "k" },
    config: setupConfig({ ...ENV, NEON_BROKER_URL: "", NEON_CONTROLLER_URL: "", NEON_PAGESERVER_HOST: "", NEON_PROXY_HOST: "", NEON_CONTROLLER_DB_HOST: "" }),
  });
  const names = secretNames("q");
  assert.deepEqual(Object.keys(rendered).sort(), [names.root, names.log, names.logAccess, ...Object.values(names.logPrincipals), names.webhookTls].sort());
});

test("without workspace storage, neither its Secrets nor its identity are made", () => {
  const rendered = renderSecrets({
    secrets: stackSecrets({ accessKey: "key", secretKey: "secret" }),
    privateKeyPem: generateKeyPair().privateKeyPem,
    publicKeyPem: "pk",
    webhookTls: { cert: "c", key: "k" },
    config: setupConfig({ ...ENV, WORKSPACES_NAME: "" }),
  });
  const names = secretNames("q");
  assert.equal(rendered[names.valkey], undefined);
  assert.equal(rendered[names.workspaces], undefined);
  const seaweed: SeaweedS3Config = JSON.parse(rendered[names.seaweedfs]?.["s3.json"] ?? "");
  assert.deepEqual(seaweed.identities.map((identity) => identity.name), ["neon", "admin", "backups", "lake", "lakeReader", "snapshots"]);
});

test("without session sandboxes snapshotted, neither the snapshots' Secret, their identity nor the node agents' token's Secret are made", () => {
  const rendered = renderSecrets({
    secrets: stackSecrets({ accessKey: "key", secretKey: "secret" }),
    privateKeyPem: generateKeyPair().privateKeyPem,
    publicKeyPem: "pk",
    webhookTls: { cert: "c", key: "k" },
    config: setupConfig({ ...ENV, SNAPSHOTS_BUCKET: "" }),
  });
  const names = secretNames("q");
  assert.equal(rendered[names.snapshots], undefined);
  assert.equal(rendered[names.nodeAgent], undefined);
  const seaweed: SeaweedS3Config = JSON.parse(rendered[names.seaweedfs]?.["s3.json"] ?? "");
  assert.deepEqual(seaweed.identities.map((identity) => identity.name), ["neon", "admin", "backups", "lake", "lakeReader", "workspaces"]);
});

test("on an external store, the setup refuses a main key that can write the snapshots bucket, and leaves nothing there it wrote", async () => {
  const credentials = { accessKeyId: "main-key", secretAccessKey: "main-secret" };
  const s3 = await startFakeS3({ bucket: "snapshots", region: "us-east-1", ...credentials });
  try {
    const bucket = makeS3Bucket({ endpoint: s3.endpoint, region: "us-east-1", bucket: "snapshots", ...credentials });
    await assert.rejects(refuseMainKeyOnSnapshots(bucket), {
      message: "the object store's main key (S3_ACCESS_KEY) can write the snapshots bucket, whose images the node agents restore as root: " +
        "give it no right to that bucket, which the snapshots' own key alone may write",
    });
    assert.equal(s3.objects.size, 0);
    // A key the store denies writing there is the one the setup goes on with.
    s3.refuse = (method) => (method === "PUT" ? 403 : null);
    await refuseMainKeyOnSnapshots(bucket);
    // A store that cannot be asked is no answer.
    s3.refuse = (method) => (method === "PUT" ? 503 : null);
    await assert.rejects(refuseMainKeyOnSnapshots(bucket), /^Error: whether the object store's main key can write the snapshots bucket could not be told: 503 /u);
  } finally {
    await s3.close();
  }
});
