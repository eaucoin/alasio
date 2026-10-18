/**
 * The log's access applier, beside each of its nodes (cli/src/manifests/log.ts): logs
 * in as the log's admin, once its node serves, and applies what its access says,
 * idempotently, once a start of the node (what it applies changes with the installation,
 * which restarts the node, and the passwords never change):
 *
 * - each principal's SCRAM-SHA-512 credential, from its password, as the stack's setup
 *   made it (neon/control/kube-setup.ts), by AlterUserScramCredentials, which the client
 *   library's Admin does not wrap, on a connection of its own;
 * - each topic: made where it is not, of its partitions, its retention, by time and by
 *   size, and its other configurations; given more partitions where it has fewer, as the
 *   intake's replicas may be raised; its retention and its other configurations set where
 *   it is;
 * - each principal's ACLs, which the log keeps once of each.
 *
 * Before it logs in as the admin, it writes every credential it holds again, the admin's
 * among them, through the controllers' listener, which admits what reaches it from the
 * log's own pods unauthenticated (restoreCredentials): of each voter in turn, as KRaft's
 * active controller alone writes one, the others answering NOT_CONTROLLER without
 * forwarding it (controllersWriter). A broker whose start replayed a
 * credential's removal may hold no credential of its mechanism until then: as it starts,
 * Kafka 4.2's metadata loader applies every record since the last snapshot as one delta,
 * and ScramDelta.apply drops a mechanism whole where a removal, in the order of its map of
 * users, comes before every addition, the additions with it. So no credential is ever
 * removed: one forgotten is replaced by one of a random secret. A branch environment's
 * principals are not written again as the log starts, as the applier holds none of their
 * passwords: safe, as the cut-over to a release refuses while a branch exists, and nothing
 * writes a removal any more.
 *
 * It answers /readyz once it has applied them all, which its node's readiness waits for,
 * and anything else while it runs. What it applies is the installation's alone: none of
 * it is ever removed here.
 *
 * Environment: LOG_BROKERS (its node's address), LOG_CONTROLLERS (every voter's
 * controllers' listener, comma-separated), LOG_ADMIN_PASSWORD, LOG_ACCESS (the file of what it applies), LOG_PASSWORDS
 * (the directory of each principal's password, a file of its name), LOG_ACCESS_PORT.
 *
 * alasio's command line runs it in the applier's container for each branch environment it
 * makes or deletes, with what it is given on its input (EnvironmentLogAccess):
 *
 *   node access.ts environment    makes the environment's principals, their ACLs and its topics
 *   node access.ts forget         deletes the ACLs and the topics, and replaces each
 *                                 principal's credential with one of a random secret
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

import type { EnvironmentLogAccess, LogAccess, LogAcl, LogTopic } from "../../cli/src/manifests/log.ts";

/** How soon an apply that failed is tried again, as while the node starts. */
const RETRY_MS = 5_000;
/** SCRAM-SHA-512's iterations, as Kafka's own tools use by default. */
const ITERATIONS = 4096;
/** Kafka's error of a controller that is not the active one, which writes nothing of what it is asked. */
export const NOT_CONTROLLER = 41;

/** A configuration of a topic's own, by Kafka's name. */
export interface TopicConfig {
  readonly name: string;
  readonly value: string;
}

/** What writes principals' SCRAM-SHA-512 credentials on the log. */
export interface CredentialWriter {
  /** Sets the principal `name`'s credential to one of `password`. */
  credential(name: string, password: string): Promise<void>;
}

/** A credential the log refused to write, with Kafka's error code. */
export class CredentialRefused extends Error {
  readonly user: string;
  readonly errorCode: number;
  constructor(user: string, errorCode: number, reason: string) {
    super(`the credential of ${user} was refused: ${reason}`);
    this.user = user;
    this.errorCode = errorCode;
  }
}

/** What the applier does on the log, as its admin. */
export interface LogAdmin extends CredentialWriter {
  deleteTopic(name: string): Promise<void>;
  disallow(acls: readonly Acl[]): Promise<void>;
  /** The topics there are, and how many partitions each has. */
  topics(): Promise<ReadonlyMap<string, number>>;
  createTopic(name: string, partitions: number, configs: readonly TopicConfig[]): Promise<void>;
  addPartitions(name: string, partitions: number): Promise<void>;
  setConfigs(name: string, configs: readonly TopicConfig[]): Promise<void>;
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
  for (const topic of access.topics) {
    const has = topics.get(topic.name);
    if (has === undefined) {
      await admin.createTopic(topic.name, topic.partitions, topicConfigs(topic));
      continue;
    }
    if (has < topic.partitions) await admin.addPartitions(topic.name, topic.partitions);
    await admin.setConfigs(topic.name, topicConfigs(topic));
  }
  await admin.allow(access.principals.flatMap(({ name, acls }) => acls.flatMap((acl) => bindings(name, acl))));
}

/** A topic's retention and its other configurations, as those of its own it is made or altered with. */
const topicConfigs = ({ retentionMs, retentionBytes, configs }: LogTopic): TopicConfig[] => [
  { name: "retention.ms", value: String(retentionMs) },
  { name: "retention.bytes", value: String(retentionBytes) },
  ...Object.entries(configs).map(([name, value]) => ({ name, value })),
];

/** Makes the branch environment's `access` on the log: its topics, and each principal's credential and ACLs. */
export async function applyEnvironment(admin: LogAdmin, { principals, topics }: EnvironmentLogAccess): Promise<void> {
  const passwords = Object.fromEntries(principals.flatMap(({ name, password }) => (password ? [[name, password]] : [])));
  await applyAccess(admin, { topics, principals }, passwords);
}

/**
 * Forgets the branch environment's `access` on the log: each principal's ACLs and its
 * topics deleted, and each principal's credential replaced by one of a random secret no
 * one holds, as no credential is ever removed (the module says why).
 */
export async function forgetEnvironment(admin: LogAdmin, { principals, topics }: EnvironmentLogAccess): Promise<void> {
  await admin.disallow(principals.flatMap(({ name, acls }) => acls.flatMap((acl) => bindings(name, acl))));
  const there = await admin.topics();
  for (const { name } of topics) if (there.has(name)) await admin.deleteTopic(name);
  for (const { name } of principals) await admin.credential(name, randomBytes(32).toString("base64url"));
}

/** Writes each credential of `passwords`, by its principal's name, through `writer`. */
export async function restoreCredentials(writer: CredentialWriter, passwords: Readonly<Record<string, string>>): Promise<void> {
  for (const [name, password] of Object.entries(passwords)) await writer.credential(name, password);
}

/** The credential writer on a connection of its own to `address` (host:port), signed in as `sasl` says, or not at all. */
export function credentialWriter(address: string, sasl?: ConnectionOptions["sasl"]): CredentialWriter {
  return {
    async credential(name, secret) {
      const salt = randomBytes(32);
      const saltedPassword = pbkdf2Sync(secret, salt, ITERATIONS, 64, "sha512");
      const [host = "", port = ""] = address.split(":");
      const connection = new Connection("log-access", sasl === undefined ? {} : { sasl });
      try {
        await connection.connect(host, Number(port));
        // The client library throws a response's refusal, of the response it holds.
        const { results } = await alterUserScramCredentialsV0.api.async(connection, [], [{ name, mechanism: ScramMechanisms.SCRAM_SHA_512, iterations: ITERATIONS, salt, saltedPassword }]).catch((error: unknown) => {
          const refused = (error as { response?: Awaited<ReturnType<typeof alterUserScramCredentialsV0.api.async>> }).response?.results.find(({ errorCode }) => errorCode !== 0);
          throw refused ? new CredentialRefused(refused.user, refused.errorCode, refused.errorMessage ?? `error ${refused.errorCode}`) : error;
        });
        const failed = results.find(({ errorCode }) => errorCode !== 0);
        if (failed) throw new CredentialRefused(failed.user, failed.errorCode, failed.errorMessage ?? `error ${failed.errorCode}`);
      } finally {
        await connection.close();
      }
    },
  };
}

type ConnectionOptions = NonNullable<ConstructorParameters<typeof Connection>[1]>;

/**
 * The credential writer of the log's controllers at `addresses` (host:port), each as
 * `writerOf` reaches it, unauthenticated: a credential is asked of each in turn, from the
 * one that wrote the last, until one writes it. KRaft's active controller alone does; the
 * others answer NOT_CONTROLLER, and one not reached yet, as its node starts, may be it. Any
 * other refusal is the credential's own, and fails it; none writing it, as while they elect
 * one, fails it too.
 */
export function controllersWriter(addresses: readonly string[], writerOf: (address: string) => CredentialWriter = credentialWriter): CredentialWriter {
  // Of its address alone: credentialWriter takes a second argument, which map's index is not.
  const writers = addresses.map((address) => writerOf(address));
  let active = 0;
  return {
    async credential(name, password) {
      const missed: string[] = [];
      for (let tried = 0; tried < writers.length; tried += 1) {
        const at = (active + tried) % writers.length;
        try {
          await writers[at]?.credential(name, password);
          active = at;
          return;
        } catch (error) {
          if (error instanceof CredentialRefused && error.errorCode !== NOT_CONTROLLER) throw error;
          missed.push(`${addresses[at]}: ${errorText(error)}`);
        }
      }
      throw new Error(`no controller of the log wrote the credential of ${name}: ${missed.join("; ")}`);
    },
  };
}

/** The log's admin at `brokers`, as @platformatic/kafka reaches it. */
export function logAdmin(brokers: readonly string[], password: string): LogAdmin {
  const sasl = { mechanism: "SCRAM-SHA-512" as const, username: "admin", password };
  const admin = new Admin({ clientId: "log-access", bootstrapBrokers: [...brokers], sasl });
  // On a connection of its own, as the Admin does not wrap AlterUserScramCredentials.
  const { credential } = credentialWriter(brokers[0] ?? "", sasl);
  return {
    credential,
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
    async createTopic(name, partitions, configs) {
      await admin.createTopics({ topics: [name], partitions, configs: [...configs] });
    },
    async addPartitions(name, partitions) {
      await admin.createPartitions({ topics: [{ name, count: partitions }] });
    },
    async setConfigs(name, configs) {
      await admin.incrementalAlterConfigs({
        resources: [{
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: name,
          configs: configs.map((config) => ({ ...config, configOperation: IncrementalAlterConfigOperationTypes.SET })),
        }],
      });
    },
    async allow(acls) {
      await admin.createAcls({ creations: [...acls] });
    },
    close: () => admin.close(),
  };
}

/** What `error` says, with what caused it, each in turn. */
const errorText = (error: unknown): string =>
  error instanceof Error ? (error.cause === undefined ? error.message : `${error.message} (${errorText(error.cause)})`) : String(error);

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
  const access: EnvironmentLogAccess = JSON.parse(readFileSync(0, "utf8"));
  const admin = logAdmin(required("LOG_BROKERS").split(","), required("LOG_ADMIN_PASSWORD"));
  try {
    await (change === "environment" ? applyEnvironment : forgetEnvironment)(admin, access);
    log(change === "environment" ? "an environment's access made" : "an environment's access forgotten", {
      principals: access.principals.map(({ name }) => name),
      topics: access.topics.map(({ name }) => name),
    });
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
  const controllers = controllersWriter(required("LOG_CONTROLLERS").split(","));
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
  let restored = false;
  const apply = async (): Promise<void> => {
    const admin = logAdmin(brokers, password);
    try {
      const passwords = Object.fromEntries(access.principals.map(({ name }) => [name, readFileSync(join(directory, name), "utf8")]));
      // Every credential it holds written again as it starts, before it logs in as the admin (the module says why).
      if (!restored) {
        await restoreCredentials(controllers, { admin: password, ...passwords });
        restored = true;
        log("the log's credentials written again through its controllers", { principals: ["admin", ...access.principals.map(({ name }) => name)] });
      }
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
