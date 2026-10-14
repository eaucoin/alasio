/**
 * The log's access applier (src/log/access.ts): what it applies, against a log that
 * records what it is asked, as the log's admin would be.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { type Acl, AclOperations, AclPermissionTypes, ResourcePatternTypes, ResourceTypes } from "@platformatic/kafka";

import type { LogAccess } from "../cli/src/manifests/automq.ts";
import { applyAccess, bindings, type LogAdmin } from "../src/log/access.ts";

const ACCESS: LogAccess = {
  topics: [{ name: "otel.spans", partitions: 2, retentionMs: 86_400_000 }, { name: "otel.logs", partitions: 2, retentionMs: 86_400_000 }],
  principals: [
    { name: "gateway", acls: [{ resource: "topic", name: "otel.", pattern: "prefixed", operations: ["write", "describe"] }] },
    { name: "intake", acls: [{ resource: "group", name: "lake-intake", pattern: "literal", operations: ["read"] }] },
  ],
};

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
    async createTopic(name, partitions, retentionMs) {
      asked.push(`create ${name} ${partitions} ${retentionMs}`);
    },
    async addPartitions(name, partitions) {
      asked.push(`partitions ${name} ${partitions}`);
    },
    async setRetention(name, retentionMs) {
      asked.push(`retention ${name} ${retentionMs}`);
    },
    async allow(given) {
      acls.push(...given);
    },
    async close() {},
  };
}

test("makes each principal's credential, each topic, and every ACL, on a log that has none", async () => {
  const log = fakeLog();
  await applyAccess(log, ACCESS, { gateway: "g", intake: "i" });
  assert.deepEqual(log.asked, ["credential gateway g", "credential intake i", "create otel.spans 2 86400000", "create otel.logs 2 86400000"]);
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

test("gives a topic it has more partitions where it has fewer, never fewer, and sets its retention", async () => {
  const log = fakeLog({ "otel.spans": 1, "otel.logs": 4 });
  await applyAccess(log, ACCESS, { gateway: "g", intake: "i" });
  assert.deepEqual(log.asked.slice(2), ["partitions otel.spans 2", "retention otel.spans 86400000", "retention otel.logs 86400000"]);
});

test("stops where a principal has no password, before any topic or ACL", async () => {
  const log = fakeLog();
  await assert.rejects(applyAccess(log, ACCESS, { gateway: "g" }), /the principal intake has no password/u);
  assert.deepEqual(log.acls, []);
  assert.ok(!log.asked.some((asked) => asked.startsWith("create")));
});
