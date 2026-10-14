/**
 * The log's access applier, beside each of its nodes (cli/src/manifests/automq.ts): logs
 * in as the log's admin, once its node serves, and applies what its access says,
 * idempotently, once a start of the node (what it applies changes with the installation,
 * which restarts the node, and the passwords never change):
 *
 * - each principal's SCRAM-SHA-512 credential, from its password, as the stack's setup
 *   made it (neon/control/kube-setup.ts), by AlterUserScramCredentials, which the client
 *   library's Admin does not wrap, on a connection of its own;
 * - each topic: made where it is not, of its partitions and retention; given more
 *   partitions where it has fewer, as the intake's replicas may be raised; its retention
 *   set where it is;
 * - each principal's ACLs, which the log keeps once of each.
 *
 * It answers /readyz once it has applied them all, which its node's readiness waits for,
 * and anything else while it runs. What it applies is the installation's alone: none of
 * it is ever removed here.
 *
 * Environment: LOG_BROKERS (its node's address), LOG_ADMIN_PASSWORD, LOG_ACCESS (the file
 * of what it applies), LOG_PASSWORDS (the directory of each principal's password, a file
 * of its name), LOG_ACCESS_PORT.
 *
 * alasio's command line runs it in the applier's container for each branch environment it
 * makes or deletes, with what it is given on its input (EnvironmentAccess):
 *
 *   node access.ts environment    makes the environment's principal, its topic and its ACLs
 *   node access.ts forget         deletes them
 */
import { pbkdf2Sync, randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import { join } from "node:path";

import {
  type Acl,
  AclOperations,
  AclPermissionTypes,
  Admin,
  alterUserScramCredentialsV0,
  ConfigResourceTypes,
  Connection,
  IncrementalAlterConfigOperationTypes,
  ResourcePatternTypes,
  ResourceTypes,
  ScramMechanisms,
} from "@platformatic/kafka";

import type { LogAccess, LogAcl } from "../../cli/src/manifests/automq.ts";

/** How soon an apply that failed is tried again, as while the node starts. */
const RETRY_MS = 5_000;
/** SCRAM-SHA-512's iterations, as Kafka's own tools use by default. */
const ITERATIONS = 4096;

/** A branch environment's access, as alasio's command line gives it: its principal, its password where it is made, its topic and its ACLs. */
export interface EnvironmentAccess {
  readonly principal: string;
  readonly password?: string;
  readonly topic: LogAccess["topics"][number];
  readonly acls: readonly LogAcl[];
}

/** What the applier does on the log, as its admin. */
export interface LogAdmin {
  /** Sets the principal `name`'s credential to one of `password`. */
  credential(name: string, password: string): Promise<void>;
  /** Deletes the principal `name`'s credential, where it has one. */
  forgetCredential(name: string): Promise<void>;
  deleteTopic(name: string): Promise<void>;
  disallow(acls: readonly Acl[]): Promise<void>;
  /** The topics there are, and how many partitions each has. */
  topics(): Promise<ReadonlyMap<string, number>>;
  createTopic(name: string, partitions: number, retentionMs: number): Promise<void>;
  addPartitions(name: string, partitions: number): Promise<void>;
  setRetention(name: string, retentionMs: number): Promise<void>;
  allow(acls: readonly Acl[]): Promise<void>;
  close(): Promise<void>;
}

const OPERATIONS = { read: AclOperations.READ, write: AclOperations.WRITE, describe: AclOperations.DESCRIBE } as const;

/** The ACL bindings of `principal`'s `acl`, one an operation. */
export function bindings(principal: string, { resource, name, pattern, operations }: LogAcl): Acl[] {
  return operations.map((operation) => ({
    resourceType: resource === "topic" ? ResourceTypes.TOPIC : ResourceTypes.GROUP,
    resourceName: name,
    resourcePatternType: pattern === "prefixed" ? ResourcePatternTypes.PREFIXED : ResourcePatternTypes.LITERAL,
    principal: `User:${principal}`,
    host: "*",
    operation: OPERATIONS[operation],
    permissionType: AclPermissionTypes.ALLOW,
  }));
}

/** Applies `access` on the log through `admin`, each principal's password of `passwords`. */
export async function applyAccess(admin: LogAdmin, access: LogAccess, passwords: Readonly<Record<string, string>>): Promise<void> {
  for (const { name } of access.principals) {
    const password = passwords[name];
    if (!password) throw new Error(`the principal ${name} has no password`);
    await admin.credential(name, password);
  }
  const topics = await admin.topics();
  for (const { name, partitions, retentionMs } of access.topics) {
    const has = topics.get(name);
    if (has === undefined) {
      await admin.createTopic(name, partitions, retentionMs);
      continue;
    }
    if (has < partitions) await admin.addPartitions(name, partitions);
    await admin.setRetention(name, retentionMs);
  }
  await admin.allow(access.principals.flatMap(({ name, acls }) => acls.flatMap((acl) => bindings(name, acl))));
}

/** Makes the branch environment's `access` on the log: its principal's credential, its topic and its ACLs. */
export async function applyEnvironment(admin: LogAdmin, { principal, password, topic, acls }: EnvironmentAccess): Promise<void> {
  if (!password) throw new Error(`the principal ${principal} has no password`);
  await applyAccess(admin, { topics: [topic], principals: [] }, {});
  await admin.credential(principal, password);
  await admin.allow(acls.flatMap((acl) => bindings(principal, acl)));
}

/** Deletes the branch environment's `access` from the log: its ACLs, its topic, and its principal's credential. */
export async function forgetEnvironment(admin: LogAdmin, { principal, topic, acls }: EnvironmentAccess): Promise<void> {
  await admin.disallow(acls.flatMap((acl) => bindings(principal, acl)));
  if ((await admin.topics()).has(topic.name)) await admin.deleteTopic(topic.name);
  await admin.forgetCredential(principal);
}

/** The log's admin at `brokers`, as @platformatic/kafka reaches it. */
export function logAdmin(brokers: readonly string[], password: string): LogAdmin {
  const sasl = { mechanism: "SCRAM-SHA-512" as const, username: "admin", password };
  const admin = new Admin({ clientId: "log-access", bootstrapBrokers: [...brokers], sasl });
  /** Alters credentials, on a connection of its own, as the Admin does not wrap it; a deletion of none there is is none. */
  const alterCredentials = async (...[deletions, upsertions]: Parameters<typeof alterUserScramCredentialsV0.api.async> extends [unknown, ...infer Rest] ? Rest : never) => {
    const [host = "", port = ""] = (brokers[0] ?? "").split(":");
    const connection = new Connection("log-access", { sasl });
    try {
      await connection.connect(host, Number(port));
      const { results } = await alterUserScramCredentialsV0.api.async(connection, deletions, upsertions);
      // 91: RESOURCE_NOT_FOUND, a credential to delete that there is not.
      const failed = results.find(({ errorCode }) => errorCode !== 0 && !(deletions.length > 0 && errorCode === 91));
      if (failed) throw new Error(`the credential of ${failed.user} was refused: ${failed.errorMessage ?? `error ${failed.errorCode}`}`);
    } finally {
      await connection.close();
    }
  };
  return {
    async credential(name, secret) {
      const salt = randomBytes(32);
      const saltedPassword = pbkdf2Sync(secret, salt, ITERATIONS, 64, "sha512");
      await alterCredentials([], [{ name, mechanism: ScramMechanisms.SCRAM_SHA_512, iterations: ITERATIONS, salt, saltedPassword }]);
    },
    async forgetCredential(name) {
      await alterCredentials([{ name, mechanism: ScramMechanisms.SCRAM_SHA_512 }], []);
    },
    async deleteTopic(name) {
      await admin.deleteTopics({ topics: [name] });
    },
    async disallow(acls) {
      await admin.deleteAcls({ filters: [...acls] });
    },
    async topics() {
      const names = await admin.listTopics();
      if (names.length === 0) return new Map();
      const { topics } = await admin.metadata({ topics: names, forceUpdate: true });
      return new Map([...topics].map(([name, { partitionsCount }]) => [name, partitionsCount]));
    },
    async createTopic(name, partitions, retentionMs) {
      await admin.createTopics({ topics: [name], partitions, configs: [{ name: "retention.ms", value: String(retentionMs) }] });
    },
    async addPartitions(name, partitions) {
      await admin.createPartitions({ topics: [{ name, count: partitions }] });
    },
    async setRetention(name, retentionMs) {
      await admin.incrementalAlterConfigs({
        resources: [{
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: name,
          configs: [{ name: "retention.ms", configOperation: IncrementalAlterConfigOperationTypes.SET, value: String(retentionMs) }],
        }],
      });
    },
    async allow(acls) {
      await admin.createAcls({ creations: [...acls] });
    },
    close: () => admin.close(),
  };
}

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

function log(message: string, fields: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({ time: new Date().toISOString(), message, ...fields }));
}

const required = (key: string) => {
  const value = process.env[key]?.trim();
  if (!value) throw new Error(`${key} must be set`);
  return value;
};

/** Makes, or forgets, the environment its input says, then exits: 0 once done, 1 saying why where it failed. */
async function changeEnvironment(change: "environment" | "forget"): Promise<void> {
  const access: EnvironmentAccess = JSON.parse(readFileSync(0, "utf8"));
  const admin = logAdmin(required("LOG_BROKERS").split(","), required("LOG_ADMIN_PASSWORD"));
  try {
    await (change === "environment" ? applyEnvironment : forgetEnvironment)(admin, access);
    log(change === "environment" ? "an environment's access made" : "an environment's access forgotten", { principal: access.principal, topic: access.topic.name });
  } catch (error) {
    console.error(errorText(error));
    process.exitCode = 1;
  } finally {
    await admin.close().catch(() => {});
  }
}

const [mode] = process.argv.slice(2);
if (import.meta.url === `file://${process.argv[1]}` && (mode === "environment" || mode === "forget")) {
  await changeEnvironment(mode);
} else if (import.meta.url === `file://${process.argv[1]}`) {
  const brokers = required("LOG_BROKERS").split(",");
  const password = required("LOG_ADMIN_PASSWORD");
  const access: LogAccess = JSON.parse(readFileSync(required("LOG_ACCESS"), "utf8"));
  const directory = required("LOG_PASSWORDS");
  let applied = false;
  const server = createServer((request, response) => {
    const ready = request.url !== "/readyz" || applied;
    response.writeHead(ready ? 200 : 503, { "content-type": "text/plain" }).end(applied ? "applied\n" : "applying\n");
  });
  server.listen(Number(required("LOG_ACCESS_PORT")), "0.0.0.0");
  let timer: NodeJS.Timeout | undefined;
  const apply = async (): Promise<void> => {
    const admin = logAdmin(brokers, password);
    try {
      const passwords = Object.fromEntries(access.principals.map(({ name }) => [name, readFileSync(join(directory, name), "utf8")]));
      await applyAccess(admin, access, passwords);
      log("the log's access applied", { principals: access.principals.map(({ name }) => name), topics: access.topics.map(({ name }) => name) });
      applied = true;
    } catch (error) {
      log("the log's access could not be applied yet", { error: errorText(error) });
      timer = setTimeout(apply, RETRY_MS);
    } finally {
      await admin.close().catch(() => {});
    }
  };
  void apply();
  process.on("SIGTERM", () => {
    clearTimeout(timer);
    server.close();
    process.exit(0);
  });
}
