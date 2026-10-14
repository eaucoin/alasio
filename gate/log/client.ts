/**
 * @platformatic/kafka, the one Kafka client of alasio, the lake and the webhook intake,
 * against the log as the gate made it (gate/log/run.sh): each principal may do what 03 and
 * 04 give it and nothing else; the webhook's producer, idempotent, acked by all, keyed by
 * chat; alasio's consumer group, committing by hand, going on after a restart from what it
 * committed; the intake's, read from offsets it was given, as it reads them from the lake's
 * catalog; and its group's lag, as KEDA reads it with a principal that may read
 * nothing. Prints what it found; exits 1 on anything else.
 */
import assert from "node:assert/strict";
import { pbkdf2Sync, randomBytes } from "node:crypto";
import { inspect } from "node:util";

import {
  AclOperations,
  AclPermissionTypes,
  Admin,
  alterUserScramCredentialsV0,
  Connection,
  Consumer,
  MessagesStreamModes,
  ProduceAcks,
  Producer,
  ResourcePatternTypes,
  ResourceTypes,
  ScramMechanisms,
  stringDeserializers,
  stringSerializers,
} from "@platformatic/kafka";

const brokers = ["automq:9092"];
const sasl = (user: string, password = `${user}-secret`) => ({ mechanism: "SCRAM-SHA-512" as const, username: user, password });
const say = (line: string) => console.log(line);

/** Whether `run` is refused, as an authorization failure, rather than done. */
async function refused(run: () => Promise<unknown>): Promise<string> {
  try {
    await run();
  } catch (error) {
    const text = inspect(error, { depth: 8, breakLength: Infinity });
    assert.match(text, /authoriz|AUTHORIZATION/iu, text);
    return text.slice(0, 160);
  }
  return assert.fail("allowed");
}

const webhook = new Producer({ clientId: "webhook", bootstrapBrokers: brokers, sasl: sasl("webhook"), idempotent: true, acks: ProduceAcks.ALL, serializers: stringSerializers, retries: 3 });
const sent = await webhook.send({ messages: Array.from({ length: 6 }, (_, n) => ({ topic: "telegram.main", key: `chat-${n % 2}`, value: JSON.stringify({ update_id: n }) })) });
say(`webhook produced to telegram.main: ${JSON.stringify(sent.offsets?.map(({ partition, offset }) => [partition, String(offset)]))}`);
// A chat's updates on one partition, in order.
const partitions = new Set(sent.offsets?.map(({ partition }) => partition));
assert.ok(partitions.size <= 2);
say(`webhook refused on otel.spans: ${await refused(() => webhook.send({ messages: [{ topic: "otel.spans", value: "x" }] }))}`);
await webhook.close();

const consumer = (user: string, group: string) =>
  new Consumer({ clientId: user, groupId: group, bootstrapBrokers: brokers, sasl: sasl(user), deserializers: stringDeserializers, autocommit: false, maxWaitTime: 500 });

/** `count` records of `topic` read by `member`, each committed by hand once handled. */
async function readCommitting(member: Consumer<string, string, string, string>, topic: string, count: number): Promise<number[]> {
  const stream = await member.consume({ topics: [topic], mode: MessagesStreamModes.COMMITTED, fallbackMode: "earliest", autocommit: false });
  const seen: number[] = [];
  for await (const message of stream) {
    seen.push(JSON.parse(message.value).update_id);
    await message.commit();
    if (seen.length === count) break;
  }
  await stream.close();
  return seen;
}
const alasio = consumer("alasio-main", "alasio.main");
const first = await readCommitting(alasio, "telegram.main", 4);
await alasio.close();
say(`alasio.main read and committed ${JSON.stringify(first)}`);
const again = consumer("alasio-main", "alasio.main");
const rest = await readCommitting(again, "telegram.main", 2);
say(`alasio.main, restarted, went on with ${JSON.stringify(rest)}`);
assert.deepEqual([...first, ...rest].toSorted(), [0, 1, 2, 3, 4, 5]);
await again.close();
say(`alasio-main refused on telegram.branch: ${await refused(async () => {
  const other = consumer("alasio-main", "alasio.main");
  try {
    const stream = await other.consume({ topics: ["telegram.branch"], mode: MessagesStreamModes.EARLIEST });
    await stream.close();
  } finally {
    await other.close(true);
  }
})}`);

// A principal made after format, a branch's.
const branchProducer = new Producer({ clientId: "webhook", bootstrapBrokers: brokers, sasl: sasl("webhook"), idempotent: true, acks: ProduceAcks.ALL, serializers: stringSerializers });
await branchProducer.send({ messages: [{ topic: "telegram.branch", key: "chat-9", value: JSON.stringify({ update_id: 90 }) }] });
await branchProducer.close();
const branch = new Consumer({ clientId: "alasio-branch-two", groupId: "alasio.branch", bootstrapBrokers: brokers, sasl: sasl("alasio-branch-two", "branch-two-secret"), deserializers: stringDeserializers, autocommit: false, maxWaitTime: 500 });
assert.deepEqual(await readCommitting(branch, "telegram.branch", 1), [90]);
await branch.close();
say("a principal made after format reads its branch's topic");

// The intake: from offsets it was given (the catalog's), not the group's.
const gateway = new Producer({ clientId: "gateway", bootstrapBrokers: brokers, sasl: sasl("gateway"), idempotent: true, acks: ProduceAcks.ALL, serializers: stringSerializers });
for (let n = 0; n < 10; n += 1) await gateway.send({ messages: [{ topic: "otel.logs", partition: 0, value: `log-${n}` }] });
await gateway.close();
const intake = consumer("intake", "lake-intake");
const stream = await intake.consume({ topics: ["otel.logs"], mode: MessagesStreamModes.MANUAL, offsets: [{ topic: "otel.logs", partition: 0, offset: 7n }], autocommit: false });
const read: string[] = [];
for await (const message of stream) {
  read.push(`${message.partition}:${message.offset}:${message.value}`);
  if (message.offset === 9n) break;
}
await stream.close();
say(`lake-intake read from the offset it was given: ${JSON.stringify(read)}`);
assert.deepEqual(read, ["0:7:log-7", "0:8:log-8", "0:9:log-9"]);
await intake.commit({ offsets: [{ topic: "otel.logs", partition: 0, offset: 10n, leaderEpoch: 0 }] });
// The lag as KEDA reads it, with a principal of its own: the group's committed offsets, against the partitions' ends.
const scaler = new Admin({ clientId: "scaler", bootstrapBrokers: brokers, sasl: sasl("scaler") });
const [committed] = await scaler.listConsumerGroupOffsets({ groups: [{ groupId: "lake-intake", topics: [{ name: "otel.logs", partitionIndexes: [0, 1, 2, 3] }] }] });
const [ends] = await scaler.listOffsets({ topics: [{ name: "otel.logs", partitions: [0, 1, 2, 3].map((partitionIndex) => ({ partitionIndex, timestamp: -1n })) }] });
const lag = ends?.partitions.map(({ partitionIndex, offset }) => {
  const at = committed?.topics.find(({ name }) => name === "otel.logs")?.partitions.find((partition) => partition.partitionIndex === partitionIndex)?.committedOffset ?? -1n;
  return [partitionIndex, String(offset), String(at)];
});
say(`lake-intake's partitions of otel.logs, end and committed, as KEDA reads them: ${JSON.stringify(lag)}`);
assert.deepEqual(lag?.find(([partition]) => partition === 0), [0, "10", "10"]);
say(`the scaler refused reading: ${await refused(() => scaler.listTopics().then(async () => {
  const reader = consumer("scaler", "lake-intake");
  try {
    const records = await reader.consume({ topics: ["otel.logs"], mode: MessagesStreamModes.EARLIEST, autocommit: false });
    for await (const record of records) return record;
    return undefined;
  } finally {
    await reader.close(true);
  }
}))}`);
await scaler.close();
say(`intake refused writing: ${await refused(async () => {
  const writer = new Producer({ clientId: "intake", bootstrapBrokers: brokers, sasl: sasl("intake"), serializers: stringSerializers });
  try {
    await writer.send({ messages: [{ topic: "otel.logs", value: "x" }] });
  } finally {
    await writer.close(true);
  }
})}`);
await intake.close();

// A principal, its topic and its ACLs made through the client by the admin, as the log's
// access applier makes them: its SCRAM credential by AlterUserScramCredentials, which the
// client's Admin does not wrap, sent on a connection of its own.
{
  const salt = randomBytes(32);
  const iterations = 4096;
  const connection = new Connection("access", { sasl: sasl("admin") });
  await connection.connect("automq", 9092);
  const altered = await alterUserScramCredentialsV0.api.async(connection, [], [
    { name: "applied", mechanism: ScramMechanisms.SCRAM_SHA_512, iterations, salt, saltedPassword: pbkdf2Sync("applied-secret", salt, iterations, 64, "sha512") },
  ]);
  await connection.close();
  assert.deepEqual(altered.results.map(({ user, errorCode }) => [user, errorCode]), [["applied", 0]]);
  const admin = new Admin({ clientId: "access", bootstrapBrokers: brokers, sasl: sasl("admin") });
  await admin.createTopics({ topics: ["telegram.applied"], partitions: 2, configs: [{ name: "retention.ms", value: String(86_400_000) }] });
  const allow = (resourceType: (typeof ResourceTypes)[keyof typeof ResourceTypes], resourceName: string, operation: (typeof AclOperations)[keyof typeof AclOperations]) => ({
    resourceType, resourceName, resourcePatternType: ResourcePatternTypes.LITERAL, principal: "User:applied", host: "*", operation, permissionType: AclPermissionTypes.ALLOW,
  });
  await admin.createAcls({ creations: [allow(ResourceTypes.TOPIC, "telegram.applied", AclOperations.READ), allow(ResourceTypes.TOPIC, "telegram.applied", AclOperations.DESCRIBE), allow(ResourceTypes.GROUP, "alasio.applied", AclOperations.READ)] });
  // Made again, as the applier does at each start: nothing changes, nothing fails but the topic's being there.
  await admin.createAcls({ creations: [allow(ResourceTypes.TOPIC, "telegram.applied", AclOperations.READ)] });
  const remade = await admin.createTopics({ topics: ["telegram.applied"], partitions: 2 }).then(() => "made again", (error: unknown) => inspect(error, { depth: 6 }).match(/TOPIC_ALREADY_EXISTS|already exists/iu)?.[0] ?? inspect(error, { depth: 6 }));
  await admin.close();
  const producer = new Producer({ clientId: "webhook", bootstrapBrokers: brokers, sasl: sasl("webhook"), idempotent: true, acks: ProduceAcks.ALL, serializers: stringSerializers });
  await producer.send({ messages: [{ topic: "telegram.applied", key: "chat-1", value: JSON.stringify({ update_id: 7 }) }] });
  await producer.close();
  const applied = new Consumer({ clientId: "applied", groupId: "alasio.applied", bootstrapBrokers: brokers, sasl: sasl("applied"), deserializers: stringDeserializers, autocommit: false, maxWaitTime: 500 });
  assert.deepEqual(await readCommitting(applied, "telegram.applied", 1), [7]);
  await applied.close();
  say(`a principal made through the client reads its topic; its topic made again: ${remade}`);
}

// A wrong password is refused.
say(`a wrong password: ${await (async () => {
  const wrong = new Admin({ clientId: "wrong", bootstrapBrokers: brokers, sasl: sasl("webhook", "not-it"), retries: 0 });
  try {
    await wrong.listTopics();
    return assert.fail("a wrong password was taken");
  } catch (error) {
    return (error instanceof Error ? error.message : String(error)).slice(0, 120);
  } finally {
    await wrong.close().catch(() => {});
  }
})()}`);
say("client: passed");
