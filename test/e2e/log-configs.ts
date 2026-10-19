/**
 * The configuration a node of the log runs with and its telemetry topics have, as the node
 * answers for them, for the end-to-end run to check what it takes effect as: run by node in
 * the access applier's container beside a node of the log (`node --input-type=module-typescript -`
 * from /opt/alasio, where the client library is), as the log's admin, a super user. It prints,
 * as JSON, the configuration of the node it runs beside, its id its argument, and of each
 * topic, by the topic's name, each configuration by name, its value as the node takes it, its
 * own or its default.
 *
 * The admin's password is the container's LOG_ADMIN_PASSWORD.
 */
import { Admin, ConfigResourceTypes } from "@platformatic/kafka";

const [node = "0"] = process.argv.slice(2);
const TOPICS = ["otel.spans", "otel.logs", "otel.metrics"];
const admin = new Admin({
  clientId: "e2e-log-configs",
  bootstrapBrokers: ["127.0.0.1:9092"],
  sasl: { mechanism: "SCRAM-SHA-512", username: "admin", password: process.env["LOG_ADMIN_PASSWORD"] ?? "" },
});
try {
  const described = await admin.describeConfigs({
    resources: [{ resourceType: ConfigResourceTypes.BROKER, resourceName: node }, ...TOPICS.map((resourceName) => ({ resourceType: ConfigResourceTypes.TOPIC, resourceName }))],
  });
  const byName = (type: number, resource: string) =>
    Object.fromEntries((described.find(({ resourceType, resourceName }) => resourceType === type && resourceName === resource)?.configs ?? []).map(({ name, value }) => [name, value]));
  console.log(JSON.stringify({ node: byName(ConfigResourceTypes.BROKER, node), topics: Object.fromEntries(TOPICS.map((topic) => [topic, byName(ConfigResourceTypes.TOPIC, topic)])) }));
} finally {
  await admin.close().catch(() => {});
}
