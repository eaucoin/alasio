/**
 * Branch environments (./manifests/branch.ts), as alasio's command line makes, lists and
 * deletes them (./commands/branch.ts).
 *
 * A branch is made at main's database's current WAL position, so it holds every commit
 * main has made: a branch of Neon's at that position, by neon-control, and the branch's
 * objects on it, with Secrets of its own: credentials of its own for what it runs (the
 * password of alasio's role on its compute, which neon-control gives the role there, the
 * lake's role's, which its alasio gives it, and its compute's token, which fetches its
 * spec alone), none of which reaches main's compute or another branch's; its bot's;
 * Claude Code's token when main has one; the pull Secrets of its images; and its token to
 * ask main to fork with. Its lake has main's object store identity, as it reads main's
 * files where they are (one data path: DuckLake keeps every file's path relative to it),
 * and SeaweedFS's Write, which writing needs, deletes too. neon-control is called
 * in its own pod, as the API server's service proxy drops the Authorization header it
 * authenticates its callers by, which neon-control would read a token from.
 *
 * A branch is deleted in the order nothing of it outlives what it runs on: its alasio and
 * lake stopped, its Sandboxes and their claims deleted (JuiceFS keeps every chunk main
 * still uses), its compute stopped, its branch of Neon's deleted, and its namespaces with
 * all that is left in them.
 */
import { randomBytes } from "node:crypto";

import type { KubernetesObject, V1Deployment, V1Namespace, V1Node, V1PersistentVolume, V1Pod, V1Secret } from "@kubernetes/client-node";
import { Console, Effect, Predicate, Redacted, Result, Schedule, Schema } from "effect";

import { type BranchView, nameProblem, tokenSha256 } from "../../neon/control/branches.ts";
import { scramVerifier } from "../../neon/control/scram.ts";
import { branchNamespace, branchSessionsNamespace, forkToken } from "../../src/branch/names.ts";
import { installConfigOf, type OperatorConfig, OperatorConfigError } from "./config.ts";
import { type Ran, runIn, runningPod, runningPodIn } from "./commands/common.ts";
import { hasStatus, kind, KubeApi, type KubeApiError, type ObjectRef, refOf } from "./kube/api.ts";
import { applyObjects } from "./kube/apply.ts";
import { awaitGone, awaitReclaimed, readiness, selectorOf, type WaitOptions } from "./kube/rollout.ts";
import { branchObjects } from "./manifests/branch.ts";
import { BRANCH_FORK_SECRET, BRANCH_LABEL, componentName, NAMESPACE, neonName, RELEASE, selectorLabels } from "./manifests/common.ts";
import type { InstallConfig } from "./manifests/config.ts";
import { CLAUDE_SECRET, readClaudeToken, readTelegramBot, TELEGRAM_SECRET, type TelegramBot } from "./secrets.ts";

/** A branch environment alasio will not make or delete now, and why, as the operator is told. */
export class BranchRefused extends Schema.TaggedError<BranchRefused>()("BranchRefused", {
  message: Schema.String,
}) {}

const SECRET = kind("Secret");
const NAMESPACE_KIND = kind("Namespace");
const DEPLOYMENT = kind("Deployment");
const POD = kind("Pod");

/** The labels of the Secrets alasio writes for the branch `name`. */
const branchSecretLabels = (name: string) => ({ "app.kubernetes.io/managed-by": "alasio", "app.kubernetes.io/part-of": "alasio", [BRANCH_LABEL]: name });

/** What neon-control answered: its status, and the JSON it answered with, if any. */
interface ControlAnswer<Body> {
  readonly status: number;
  readonly body: Body | null;
}

/** The value of `key` of the Secret `name` of main's, or null. */
const mainSecret = (name: string, key: string): Effect.Effect<string | null, KubeApiError, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) => kube.get<V1Secret>({ ...SECRET, namespace: NAMESPACE, name })).pipe(
    Effect.map((found) => {
      const encoded = found?.data?.[key];
      return encoded === undefined ? null : Buffer.from(encoded, "base64").toString("utf8");
    }),
  );

/** A Secret of main's, which the stack's setup makes; the branch is refused when there is none, as Neon has not run yet. */
const requiredSecret = (name: string, key: string): Effect.Effect<string, KubeApiError | BranchRefused, KubeApi> =>
  Effect.flatMap(mainSecret(name, key), (value) =>
    value === null ? Effect.fail(new BranchRefused({ message: `alasio's Secret ${name} has no ${key}: alasio up makes it` })) : Effect.succeed(value));

/** What a command run in a pod came to, refused unless it exited with 0. */
const succeeded = (what: string) => (ran: Ran): Effect.Effect<string, BranchRefused> =>
  ran.exitCode === 0 ? Effect.succeed(ran.stdout) : Effect.fail(new BranchRefused({ message: `${what} failed (exit ${ran.exitCode}): ${ran.stderr.trim() || ran.stdout.trim()}` }));

/**
 * Calls neon-control's API (neon/control/service.ts) in its pod, with the stack's admin
 * token (the `alasio-branches` Secret's), which goes on stdin, never on a command line.
 */
function neonControl<Body>(method: "GET" | "POST" | "DELETE", path: string, body?: object) {
  return Effect.gen(function*() {
    const token = yield* requiredSecret(componentName("branches"), "control-token");
    const pod = yield* runningPod("neon-control");
    const command = [
      "curl", "-sS", "-X", method, "-H", "@-",
      ...(body === undefined ? [] : ["-H", "content-type: application/json", "--data-binary", JSON.stringify(body)]),
      "-w", "\n%{http_code}", `http://127.0.0.1:8080${path}`,
    ];
    const answered = yield* runIn({ namespace: NAMESPACE, pod, container: "neon-control" }, command, `authorization: Bearer ${token}\n`).pipe(Effect.flatMap(succeeded("calling neon-control")));
    const lines = answered.trimEnd().split("\n");
    const status = Number(lines.pop());
    const text = lines.join("\n");
    const parsed: Body | null = text ? JSON.parse(text) : null;
    return { status, body: parsed } satisfies ControlAnswer<Body>;
  });
}

/** What neon-control says of a refusal: its error, or its status. */
const refusalOf = (answer: ControlAnswer<unknown>): string =>
  Predicate.hasProperty(answer.body, "error") ? String(answer.body.error) : `it answered ${answer.status}`;

/** Neon's branches but main, as neon-control lists them. */
const neonBranches = Effect.gen(function*() {
  const answer = yield* neonControl<{ readonly branches: readonly BranchView[] }>("GET", "/branches");
  if (answer.status !== 200 || !answer.body) return yield* new BranchRefused({ message: `neon-control did not list Neon's branches: ${refusalOf(answer)}` });
  return answer.body.branches.filter(({ parent }) => parent !== null);
});

/** Runs `sql` as the compute's own superuser, in its pod: what psql printed of its one value. */
const onCompute = (namespace: string, pod: string, sql: string) =>
  runIn({ namespace, pod, container: "compute" }, ["psql", "-h", "127.0.0.1", "-p", "55433", "-U", "cloud_admin", "-d", "alasio", "-Atc", sql]).pipe(
    Effect.flatMap(succeeded(`asking the compute in ${namespace}`)),
    Effect.map((printed) => printed.trim()),
  );

/** A memory quantity, in bytes. */
export function memoryBytes(quantity: string): number {
  const match = /^([0-9.]+)([KMGTPE]i?|k|m)?$/u.exec(quantity.trim());
  if (!match) return 0;
  const units: Record<string, number> = { k: 1e3, K: 1e3, M: 1e6, G: 1e9, T: 1e12, P: 1e15, E: 1e18, Ki: 2 ** 10, Mi: 2 ** 20, Gi: 2 ** 30, Ti: 2 ** 40, Pi: 2 ** 50, Ei: 2 ** 60, m: 1e-3 };
  return Number(match[1]) * (match[2] ? units[match[2]] ?? 1 : 1);
}

/** `bytes`, as people read an amount of memory. */
export function memoryText(bytes: number): string {
  return bytes >= 2 ** 30 ? `${(bytes / 2 ** 30).toFixed(1)} GiB` : `${Math.round(bytes / 2 ** 20)} MiB`;
}

/** A pod's or node's use of memory now, as the metrics server measures it. */
interface MemoryUsage extends KubernetesObject {
  readonly usage?: { readonly memory?: string };
  readonly containers?: readonly { readonly name?: string; readonly usage?: { readonly memory?: string } }[];
}

/** The containers of main's pods a branch environment runs none of: the lake's query endpoint, as a branch has no Grafana. */
const UNBRANCHED_CONTAINERS: readonly string[] = ["query"];

/** The memory `of` uses now; of a pod, that of the containers a branch runs its own of. */
const used = (of: MemoryUsage): number =>
  of.containers
    ? of.containers.filter(({ name }) => !UNBRANCHED_CONTAINERS.includes(name ?? "")).reduce((sum, container) => sum + memoryBytes(container.usage?.memory ?? "0"), 0)
    : memoryBytes(of.usage?.memory ?? "0");

/** The components a branch environment runs its own of. */
const BRANCHED_COMPONENTS = ["alasio", "neon-compute", "lake"] as const;

/** What a branch takes and what the cluster has, measured now: null where the cluster measures nothing. */
export interface MemoryMeasure {
  /** What main's alasio, compute and lake use, which a branch runs its own of. */
  readonly branch: number;
  /** The most memory a node of the cluster has free. */
  readonly free: number;
}

/** What `measure` lists, or null where the cluster has no metrics server to answer. */
const measured = <A>(measure: Effect.Effect<A, KubeApiError, KubeApi>): Effect.Effect<A | null, KubeApiError, KubeApi> =>
  measure.pipe(Effect.catchIf((error) => error.status === 404 || error.status === 503, () => Effect.succeed(null)));

/** What a branch takes, as main's alasio, compute and lake use memory now, and the most a node has free. */
export const measureMemory: Effect.Effect<MemoryMeasure | null, KubeApiError, KubeApi> = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const pods = yield* measured(Effect.forEach(BRANCHED_COMPONENTS, (component) =>
    kube.list<MemoryUsage>(kind("PodMetrics"), { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels(component)) })));
  const usage = yield* measured(kube.list<MemoryUsage>(kind("NodeMetrics")));
  if (!pods || !usage) return null;
  const nodes = yield* kube.list<V1Node>(kind("Node"));
  const free = nodes.map((node) => memoryBytes(node.status?.allocatable?.["memory"] ?? "0") - used(usage.find(({ metadata }) => metadata?.name === node.metadata?.name) ?? {}));
  return { branch: pods.flat().reduce((sum, pod) => sum + used(pod), 0), free: Math.max(0, ...free) };
});

/** `overrides` over `base`, objects merged key by key, anything else replaced. */
function merged(base: Readonly<Record<string, unknown>>, overrides: Readonly<Record<string, unknown>>): Record<string, unknown> {
  const result: Record<string, unknown> = { ...base };
  for (const [key, value] of Object.entries(overrides)) {
    const current = result[key];
    result[key] = Predicate.isObject(value) && !Array.isArray(value) && Predicate.isObject(current) && !Array.isArray(current)
      ? merged(current as Readonly<Record<string, unknown>>, value as Readonly<Record<string, unknown>>)
      : value;
  }
  return result;
}

/**
 * The install configuration of a branch environment: main's, with `overrides` (as the
 * config's `install` holds settings), and without the host profile, whose folders a
 * branch never works in.
 */
export const branchConfig = Effect.fnUntraced(function*(config: OperatorConfig, overrides: Readonly<Record<string, unknown>>): Effect.fn.Return<InstallConfig, KubeApiError | OperatorConfigError, KubeApi> {
  const claude = (yield* readClaudeToken) !== null;
  const install = merged(config.install, merged(overrides, { host: { enabled: false } }));
  const decoded = installConfigOf(install, { claude });
  if (Result.isFailure(decoded)) return yield* new OperatorConfigError({ path: config.path, reason: `with the branch's overrides, ${decoded.failure}` });
  return decoded.success;
});

/** A branch environment to make: its name, its bot, and the settings it has over main's. */
export interface BranchRequest {
  readonly name: string;
  readonly bot: TelegramBot;
  readonly overrides: Readonly<Record<string, unknown>>;
}

/** A Secret of the branch `name`'s, in its namespace. */
const branchSecret = (name: string, secret: string, data: Readonly<Record<string, string>>): V1Secret => ({
  apiVersion: "v1",
  kind: "Secret",
  metadata: { name: secret, namespace: branchNamespace(name), labels: branchSecretLabels(name) },
  type: "Opaque",
  data: Object.fromEntries(Object.entries(data).map(([key, value]) => [key, Buffer.from(value, "utf8").toString("base64")])),
});

/**
 * A Secret of main's, as the branch `name` is given it, in its namespace: the same, as its
 * copy of the data is; of its keys only `keys`, when given, what the branch uses.
 */
const copiedSecret = Effect.fnUntraced(function*(name: string, secret: string, keys?: readonly string[]): Effect.fn.Return<V1Secret, KubeApiError | BranchRefused, KubeApi> {
  const kube = yield* KubeApi;
  const found = yield* kube.get<V1Secret>({ ...SECRET, namespace: NAMESPACE, name: secret });
  if (!found?.data) return yield* new BranchRefused({ message: `alasio has no Secret ${secret}: alasio up makes it` });
  const data = keys ? Object.fromEntries(Object.entries(found.data).filter(([key]) => keys.includes(key))) : found.data;
  return { apiVersion: "v1", kind: "Secret", metadata: { name: secret, namespace: branchNamespace(name), labels: branchSecretLabels(name) }, type: found.type ?? "Opaque", data };
});

/**
 * Makes the branch environment `request` asks for, as the module says, and waits until
 * it runs.
 */
export const createBranch = Effect.fnUntraced(function*(config: OperatorConfig, { name, bot, overrides }: BranchRequest, options: WaitOptions) {
  const kube = yield* KubeApi;
  const problem = nameProblem(name);
  if (problem) return yield* new BranchRefused({ message: `${name} is no branch's name: ${problem}` });
  const settings = yield* branchConfig(config, overrides);
  if (!settings.neon.enabled) return yield* new BranchRefused({ message: "a branch environment is a branch of alasio's Neon, which this installation does not run" });
  if (!(yield* kube.get({ ...DEPLOYMENT, namespace: NAMESPACE, name: RELEASE }))) {
    return yield* new BranchRefused({ message: "alasio is not installed in this cluster: alasio up installs it" });
  }
  if (yield* kube.get({ ...NAMESPACE_KIND, name: branchNamespace(name) })) return yield* new BranchRefused({ message: `there is a branch ${name} already` });
  const main = yield* readTelegramBot;
  if (main && Redacted.value(main.token) === Redacted.value(bot.token)) {
    return yield* new BranchRefused({ message: "a branch's bot is its own: Telegram gives a bot's updates to one poller, and main's alasio polls main's bot" });
  }

  const memory = yield* measureMemory;
  if (memory === null) yield* Console.log("This cluster has no metrics server, so what a branch takes is not measured: about what main's alasio, compute and lake take.");
  else if (memory.branch > memory.free) {
    yield* Console.log(`Warning: a branch takes about ${memoryText(memory.branch)}, as main's alasio, compute and lake use now, and no node has more than ${memoryText(memory.free)} free.`);
  }

  // The branch's own, as the module says: none of main's passwords or tokens of its Neon.
  const own = () => randomBytes(24).toString("base64url");
  const credentials = { alasio: own(), lake: own(), compute: own() };
  const url = new URL(yield* requiredSecret(componentName("database"), "url"));
  url.password = credentials.alasio;
  const lake = settings.lake.enabled ? yield* copiedSecret(name, componentName("lake"), ["LAKE_S3_KEY", "LAKE_S3_SECRET"]) : null;
  const secrets = [
    branchSecret(name, componentName("database"), { url: url.toString(), "lake-password": credentials.lake }),
    branchSecret(name, neonName("compute"), { NEON_CONTROL_PLANE_TOKEN: credentials.compute }),
    // The lake service's: main's object store identity, the one thing of main's a branch is given (see the module), and its role's password.
    ...(lake ? [{ ...lake, data: { ...lake.data, LAKE_DATABASE_PASSWORD: Buffer.from(credentials.lake, "utf8").toString("base64") } }] : []),
    ...(settings.alasio.claude.existingSecret ? [yield* copiedSecret(name, CLAUDE_SECRET)] : []),
    ...(yield* Effect.forEach(settings.imagePullSecrets, (secret) => copiedSecret(name, secret))),
    branchSecret(name, TELEGRAM_SECRET, { token: Redacted.value(bot.token), allowedUserIds: bot.allowedUserIds.join(",") }),
    branchSecret(name, BRANCH_FORK_SECRET, { token: forkToken(yield* requiredSecret(componentName("branches"), "fork-key"), name) }),
  ];

  // At main's current WAL position, so the branch holds every commit main has made.
  const lsn = yield* onCompute(NAMESPACE, yield* runningPod("neon-compute"), "select pg_current_wal_lsn()");
  yield* Effect.logInfo(`branching Neon at ${lsn}`);
  const made = yield* neonControl<BranchView>("POST", "/branches", {
    name,
    lsn,
    compute: { passwordVerifier: scramVerifier(credentials.alasio), tokenSha256: tokenSha256(credentials.compute) },
  });
  if (made.status !== 201 && made.status !== 200) return yield* new BranchRefused({ message: `neon-control did not make the branch ${name}: ${refusalOf(made)}` });

  const objects = branchObjects(settings, name);
  yield* Effect.forEach(objects.filter((object) => object.kind === "Namespace"), (namespace) => kube.apply(namespace), { discard: true });
  yield* Effect.forEach(secrets, (secret) => kube.apply(secret), { discard: true });
  yield* applyObjects(objects, options);
});

/** A branch environment as `list` says it. */
export interface ListedBranch {
  readonly name: string;
  readonly parent: string | null;
  /** When it was made, as Neon or its namespace says; null when neither has it. */
  readonly createdAt: string | null;
  /** Its branch of Neon's state, or null when it has none. */
  readonly neon: string | null;
  /** Whether its alasio runs ready, or null when it has none. */
  readonly ready: boolean | null;
  /** The memory its workloads request, and what its pods use now, when that is measured. */
  readonly requested: number;
  readonly used: number | null;
}

/** The memory the workloads of `namespace` request. */
const requestedIn = (namespace: string): Effect.Effect<number, KubeApiError, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) => kube.list<V1Deployment>(DEPLOYMENT, { namespace })).pipe(
    Effect.map((deployments) =>
      deployments.flatMap(({ spec }) => spec?.template.spec?.containers ?? []).reduce((sum, container) => sum + memoryBytes(container.resources?.requests?.["memory"] ?? "0"), 0)
    ),
  );

/** The namespaces of branch environments, each its own. */
const namespacesOfBranches: Effect.Effect<V1Namespace[], KubeApiError, KubeApi> = Effect.flatMap(KubeApi, (kube) => kube.list<V1Namespace>(NAMESPACE_KIND)).pipe(
  Effect.map((namespaces) => namespaces.filter(({ metadata }) => metadata?.labels?.[BRANCH_LABEL] && metadata.name === branchNamespace(metadata.labels[BRANCH_LABEL]))),
);

/** The names of the branch environments made in the cluster, by their namespaces. */
export const branchNamespaces: Effect.Effect<string[], KubeApiError, KubeApi> = Effect.map(namespacesOfBranches, (namespaces) =>
  namespaces.map(({ metadata }) => metadata?.labels?.[BRANCH_LABEL] ?? ""));

/** Every branch environment, by Neon's branches and the namespaces made of them. */
export const listBranches = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const branches = yield* neonBranches;
  const namespaces = yield* namespacesOfBranches;
  const names = [...new Set([...branches.map((branch) => branch.name), ...namespaces.map(({ metadata }) => metadata?.labels?.[BRANCH_LABEL] ?? "")])].sort();
  return yield* Effect.forEach(names, (name) =>
    Effect.gen(function*() {
      const branch = branches.find((each) => each.name === name);
      const namespace = namespaces.find(({ metadata }) => metadata?.name === branchNamespace(name));
      const alasio = namespace ? yield* kube.get({ ...DEPLOYMENT, namespace: branchNamespace(name), name: RELEASE }) : null;
      const pods = namespace ? yield* measured(kube.list<MemoryUsage>(kind("PodMetrics"), { namespace: branchNamespace(name) })) : null;
      return {
        name,
        parent: branch?.parent ?? null,
        createdAt: branch?.createdAt ?? namespace?.metadata?.creationTimestamp?.toString() ?? null,
        neon: branch?.state ?? null,
        ready: alasio ? readiness(alasio)._tag === "Ready" : null,
        requested: namespace ? yield* requestedIn(branchNamespace(name)) : 0,
        used: pods ? pods.reduce((sum, pod) => sum + used(pod), 0) : null,
      } satisfies ListedBranch;
    }));
});

/** Deletes the objects `refs`, and waits until they are gone. */
const removed = (refs: readonly ObjectRef[], options: WaitOptions) =>
  Effect.flatMap(KubeApi, (kube) => Effect.forEach(refs, (ref) => kube.remove(ref), { concurrency: 8, discard: true })).pipe(Effect.andThen(awaitGone(refs, options)));

/** Stops the Deployments `names` of `namespace`: deletes them, and waits until their pods are gone. */
const stopped = Effect.fnUntraced(function*(namespace: string, names: readonly string[], options: WaitOptions) {
  const kube = yield* KubeApi;
  const workloads = (yield* Effect.forEach(names, (name) => kube.get<V1Deployment>({ ...DEPLOYMENT, namespace, name }))).filter((found) => found !== null);
  const pods = (yield* Effect.forEach(workloads, ({ spec }) => kube.list<V1Pod>(POD, { namespace, labelSelector: selectorOf(spec?.selector.matchLabels ?? {}) }))).flat();
  yield* removed([...workloads, ...pods].map(refOf), options);
});

/**
 * Deletes the branch environment `name`, as the module says. Unless `force`, refuses
 * while its alasio has a turn running, as its database says, or when that cannot be
 * read.
 */
export const deleteBranch = Effect.fnUntraced(function*(name: string, { force }: { readonly force: boolean }, options: WaitOptions) {
  const kube = yield* KubeApi;
  const namespace = branchNamespace(name);
  const sessions = branchSessionsNamespace(name);
  const branch = (yield* neonBranches).find((each) => each.name === name);
  const made = yield* kube.get({ ...NAMESPACE_KIND, name: namespace });
  if (!branch && !made) return yield* new BranchRefused({ message: `there is no branch ${name}` });

  if (made && !force) {
    const turns = yield* runningPodIn(namespace, "neon-compute").pipe(
      Effect.flatMap((pod) => onCompute(namespace, pod, "select count(*) from state.turns where state = 'active'")),
      Effect.catch((error) => Effect.fail(new BranchRefused({ message: `whether its alasio has a turn running cannot be read: ${error.message}; --force deletes it all the same` }))),
    );
    if (Number(turns) > 0) return yield* new BranchRefused({ message: `its alasio has ${turns} turn${turns === "1" ? "" : "s"} running; --force deletes it all the same` });
  }

  if (made) {
    yield* stopped(namespace, [RELEASE, componentName("lake")], options);
    const sandboxes = yield* kube.list(kind("Sandbox"), { namespace: sessions }).pipe(Effect.catchIf(hasStatus(404), () => Effect.succeed([])));
    yield* removed(sandboxes.map(refOf), options);
    const claimed = (yield* kube.list<V1PersistentVolume>(kind("PersistentVolume"))).filter(({ spec }) => spec?.claimRef?.namespace === sessions);
    yield* removed((yield* kube.list(kind("PersistentVolumeClaim"), { namespace: sessions })).map(refOf), options);
    yield* awaitReclaimed(claimed.map(refOf), options);
    yield* stopped(namespace, [neonName("compute")], options);
  }
  if (branch) {
    // neon-control answers 409 while the storage controller is still deleting it.
    yield* neonControl("DELETE", `/branches/${name}`).pipe(
      Effect.flatMap((answer) =>
        answer.status === 204 || answer.status === 404
          ? Effect.void
          : Effect.fail(new BranchRefused({ message: `neon-control did not delete the branch ${name}: ${refusalOf(answer)}` }))
      ),
      Effect.retry({ while: (error) => error._tag === "BranchRefused" && error.message.includes("still deleting"), schedule: Schedule.max([Schedule.spaced(options.poll), Schedule.during(options.timeout)]) }),
    );
  }
  yield* removed([namespace, sessions].map((each) => ({ ...NAMESPACE_KIND, name: each })), options);
});

