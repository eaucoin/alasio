/**
 * The log's access applier (src/log/access.ts): what it applies, against a log that
 * records what it is asked, as the log's admin would be.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { type Acl, AclOperations, AclPermissionTypes, ResourcePatternTypes, ResourceTypes } from "@platformatic/kafka";

import type { EnvironmentLogAccess, LogAccess } from "../cli/src/manifests/log.ts";
import { applyAccess, applyEnvironment, bindings, forgetEnvironment, type LogAdmin, type TopicConfig } from "../src/log/access.ts";

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
    async forgetCredential(name) {
      asked.push(`forget ${name}`);
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

test("a branch environment's access is made: each principal's credential, its topics, every principal's ACLs; and forgotten whole", async () => {
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
  assert.deepEqual(made.asked, [
    `disallow User:alasio-try telegram.try ${AclOperations.READ}`,
    `disallow User:loader-try lake.try ${AclOperations.READ}`,
    "delete telegram.try",
    "delete lake.try",
    "forget alasio-try",
    "forget loader-try",
  ]);
  // A topic there is not is not deleted.
  const gone = fakeLog({ "telegram.try": 1 });
  await forgetEnvironment(gone, BRANCH);
  assert.ok(gone.asked.includes("delete telegram.try") && !gone.asked.includes("delete lake.try"));
});
