/**
 * The log's access applier (src/log/access.ts): what it applies, against a log that
 * records what it is asked, as the log's admin would be.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { type Acl, AclOperations, AclPermissionTypes, ResourcePatternTypes, ResourceTypes } from "@platformatic/kafka";

import type { EnvironmentLogAccess, LogAccess } from "../cli/src/manifests/log.ts";
import { applyAccess, applyEnvironment, bindings, controllersWriter, CredentialRefused, type CredentialWriter, forgetEnvironment, type LogAdmin, NOT_CONTROLLER, restoreCredentials, type TopicConfig } from "../src/log/access.ts";

const ACCESS: LogAccess = {
  topics: [
    { name: "otel.spans", partitions: 2, retentionMs: 86_400_000, retentionBytes: 2 ** 28, configs: { "flush.ms": "1000" } },
    { name: "otel.logs", partitions: 2, retentionMs: 86_400_000, retentionBytes: 2 ** 28, configs: {} },
  ],
  principals: [
    { name: "gateway", acls: [{ resource: "topic", name: "otel.", pattern: "prefixed", operations: ["write", "describe"] }] },
    { name: "intake", acls: [{ resource: "group", name: "lake-intake", pattern: "literal", operations: ["read"] }] },
  ],
};

/** A topic's configurations as the log is asked them. */
const said = (configs: readonly TopicConfig[]) => configs.map(({ name, value }) => `${name}=${value}`).join(" ");

/** A log that has `topics` (by their partitions), recording what it is asked. */
function fakeLog(topics: Record<string, number> = {}): LogAdmin & { readonly asked: string[]; readonly acls: Acl[] } {
  const asked: string[] = [];
  const acls: Acl[] = [];
  return {
    asked,
    acls,
    async credential(name, password) {
      asked.push(`credential ${name} ${password}`);
    },
    async topics() {
      return new Map(Object.entries(topics));
    },
    async createTopic(name, partitions, configs) {
      asked.push(`create ${name} ${partitions} ${said(configs)}`);
    },
    async addPartitions(name, partitions) {
      asked.push(`partitions ${name} ${partitions}`);
    },
    async setConfigs(name, configs) {
      asked.push(`configs ${name} ${said(configs)}`);
    },
    async allow(given) {
      acls.push(...given);
    },
    async deleteTopic(name) {
      asked.push(`delete ${name}`);
    },
    async disallow(given) {
      asked.push(...given.map(({ principal, resourceName, operation }) => `disallow ${principal} ${resourceName} ${operation}`));
    },
    async close() {},
  };
}

test("makes each principal's credential, each topic of its retention and its other configurations, and every ACL, on a log that has none", async () => {
  const log = fakeLog();
  await applyAccess(log, ACCESS, { gateway: "g", intake: "i" });
  assert.deepEqual(log.asked, ["credential gateway g", "credential intake i", "create otel.spans 2 retention.ms=86400000 retention.bytes=268435456 flush.ms=1000", "create otel.logs 2 retention.ms=86400000 retention.bytes=268435456"]);
  assert.deepEqual(log.acls, [...bindings("gateway", { resource: "topic", name: "otel.", pattern: "prefixed", operations: ["write", "describe"] }), ...bindings("intake", { resource: "group", name: "lake-intake", pattern: "literal", operations: ["read"] })]);
  assert.deepEqual(log.acls[0], {
    resourceType: ResourceTypes.TOPIC,
    resourceName: "otel.",
    resourcePatternType: ResourcePatternTypes.PREFIXED,
    principal: "User:gateway",
    host: "*",
    operation: AclOperations.WRITE,
    permissionType: AclPermissionTypes.ALLOW,
  });
  assert.deepEqual([log.acls[2]?.resourceType, log.acls[2]?.resourcePatternType, log.acls[2]?.operation], [ResourceTypes.GROUP, ResourcePatternTypes.LITERAL, AclOperations.READ]);
});

test("gives a topic it has more partitions where it has fewer, never fewer, and sets its retention and its other configurations", async () => {
  const log = fakeLog({ "otel.spans": 1, "otel.logs": 4 });
  await applyAccess(log, ACCESS, { gateway: "g", intake: "i" });
  assert.deepEqual(log.asked.slice(2), [
    "partitions otel.spans 2",
    "configs otel.spans retention.ms=86400000 retention.bytes=268435456 flush.ms=1000",
    "configs otel.logs retention.ms=86400000 retention.bytes=268435456",
  ]);
});

test("stops where a principal has no password, before any topic or ACL", async () => {
  const log = fakeLog();
  await assert.rejects(applyAccess(log, ACCESS, { gateway: "g" }), /the principal intake has no password/u);
  assert.deepEqual(log.acls, []);
  assert.ok(!log.asked.some((asked) => asked.startsWith("create")));
});

/** A branch environment of two principals and two topics, as one may be given. */
const BRANCH: EnvironmentLogAccess = {
  principals: [
    { name: "alasio-try", password: "p", acls: [{ resource: "topic", name: "telegram.try", pattern: "literal", operations: ["read"] }] },
    { name: "loader-try", password: "q", acls: [{ resource: "topic", name: "lake.try", pattern: "literal", operations: ["read"] }] },
  ],
  topics: [
    { name: "telegram.try", partitions: 1, retentionMs: 604_800_000, retentionBytes: 2 ** 26, configs: { "flush.messages": "1" } },
    { name: "lake.try", partitions: 1, retentionMs: 86_400_000, retentionBytes: 2 ** 28, configs: {} },
  ],
};

test("a branch environment's access is made: each principal's credential, its topics, every principal's ACLs; and forgotten whole, but its credentials, each replaced by one of a random secret", async () => {
  const log = fakeLog();
  await applyEnvironment(log, BRANCH);
  assert.deepEqual(log.asked, ["credential alasio-try p", "credential loader-try q", "create telegram.try 1 retention.ms=604800000 retention.bytes=67108864 flush.messages=1", "create lake.try 1 retention.ms=86400000 retention.bytes=268435456"]);
  assert.deepEqual(log.acls, BRANCH.principals.flatMap(({ name, acls }) => acls.flatMap((acl) => bindings(name, acl))));
  const passwordless = { ...BRANCH, principals: BRANCH.principals.map((principal, index) => (index === 1 ? { ...principal, password: "" } : principal)) };
  const refused = fakeLog();
  await assert.rejects(applyEnvironment(refused, passwordless), /the principal loader-try has no password/u);
  assert.ok(!refused.asked.some((asked) => asked.startsWith("create")), "nothing is made for a principal without a password");
  const made = fakeLog({ "telegram.try": 1, "lake.try": 1 });
  await forgetEnvironment(made, BRANCH);
  // No credential removed, as a start of the log that replays a removal may take none (src/log/access.ts).
  assert.deepEqual(made.asked.slice(0, 4), [
    `disallow User:alasio-try telegram.try ${AclOperations.READ}`,
    `disallow User:loader-try lake.try ${AclOperations.READ}`,
    "delete telegram.try",
    "delete lake.try",
  ]);
  const replaced = made.asked.slice(4).map((asked) => /^credential (\S+) (\S+)$/u.exec(asked));
  assert.deepEqual(replaced.map((match) => match?.[1]), ["alasio-try", "loader-try"]);
  for (const match of replaced) {
    const secret = match?.[2] ?? "";
    assert.equal(Buffer.from(secret, "base64url").length, 32, secret);
    assert.ok(!["p", "q"].includes(secret));
  }
  assert.notEqual(replaced[0]?.[2], replaced[1]?.[2]);
  // A topic there is not is not deleted.
  const gone = fakeLog({ "telegram.try": 1 });
  await forgetEnvironment(gone, BRANCH);
  assert.ok(gone.asked.includes("delete telegram.try") && !gone.asked.includes("delete lake.try"));
});

test("every credential held is written again, the admin's among them, each of its password", async () => {
  const log = fakeLog();
  await restoreCredentials(log, { admin: "a", gateway: "g", intake: "i" });
  assert.deepEqual(log.asked, ["credential admin a", "credential gateway g", "credential intake i"]);
});

/** Controllers at each address, of which `active` alone writes: what each was asked, and what it wrote. */
function controllers(active: () => string, unreached: readonly string[] = []) {
  const asked: string[] = [];
  const written: string[] = [];
  const writerOf = (address: string): CredentialWriter => ({
    async credential(name, password) {
      asked.push(`${address} ${name}`);
      if (unreached.includes(address)) throw new Error(`connect ECONNREFUSED ${address}`);
      if (address !== active()) throw new CredentialRefused(name, NOT_CONTROLLER, "This is not the correct controller for this cluster.");
      written.push(`${address} ${name} ${password}`);
    },
  });
  return { asked, written, writerOf };
}

test("a credential is written by whichever voter is the active controller, the others' NOT_CONTROLLER and one not reached moved past, the one that wrote the last asked first", async () => {
  let active = "log-2:9093";
  const log = controllers(() => active, ["log-1:9093"]);
  const writer = controllersWriter(["log-0:9093", "log-1:9093", "log-2:9093"], log.writerOf);
  await writer.credential("admin", "a");
  await writer.credential("gateway", "g");
  assert.deepEqual(log.asked, ["log-0:9093 admin", "log-1:9093 admin", "log-2:9093 admin", "log-2:9093 gateway"]);
  assert.deepEqual(log.written, ["log-2:9093 admin a", "log-2:9093 gateway g"]);
  // Another elected: asked from the one before, around.
  active = "log-0:9093";
  await writer.credential("intake", "i");
  assert.deepEqual(log.asked.slice(4), ["log-2:9093 intake", "log-0:9093 intake"]);
  assert.deepEqual(log.written.slice(2), ["log-0:9093 intake i"]);
});

test("a credential no voter writes, as while they elect one, fails, and a refusal of its own fails it at once", async () => {
  const none = controllers(() => "");
  await assert.rejects(controllersWriter(["log-0:9093", "log-1:9093"], none.writerOf).credential("admin", "a"), /^Error: no controller of the log wrote the credential of admin: log-0:9093: the credential of admin was refused: This is not the correct controller for this cluster\.; log-1:9093: /u);
  const refusing = controllersWriter(["log-0:9093", "log-1:9093"], () => ({
    credential: async (name) => {
      throw new CredentialRefused(name, 68, "Iterations must be at least 4096");
    },
  }));
  await assert.rejects(refusing.credential("admin", "a"), (error) => error instanceof CredentialRefused && error.errorCode === 68);
});
