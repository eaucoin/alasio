/**
 * `alasio logs [component]`: what alasio, or one of its components, logs: its pods' logs, as
 * they keep them; or, of one that runs no pod, as KEDA lets alasio sleep at none, why, and what
 * the lake kept of its last run's (its telemetry's logs, otel.logs), where it keeps any.
 */
import { PassThrough, type Writable } from "node:stream";

import { Console, Duration, Effect, Option, Stdio, Stream } from "effect";
import { Argument, Command, Flag } from "effect/cli";

import { hasStatus, kind, KubeApi } from "../kube/api.ts";
import { NAMESPACE, RELEASE } from "../manifests/common.ts";
import type { ScaledObject } from "../manifests/scalers.ts";
import { describeAsleep, wakesOf } from "../wakes.ts";
import { type Component, component, onCluster, podsOf, runningPod, sinceFlag } from "./common.ts";

/** The lake's query, as its image has it (neon/lake). */
const QUERY = ["node", "/opt/lake/src/query.ts"] as const;

/** The most of a component's last run's lines the lake is asked for: its newest. */
const LAKE_LINES = 1000;

/**
 * How long after alasio's last run ended the lake may not have loaded its logs yet: its intake
 * writes a minute's at a time (neon/lake/src/intake.ts BATCH), and as long again goes to the
 * collector's sending them on and the intake's waking for them.
 */
export const LAKE_CATCH_UP = Duration.minutes(2);

/** How far back a component's logs are asked of the lake where neither its last start nor --since says. */
const LAKE_BACK = Duration.hours(1);

/** The service a component's telemetry names, as its pods' first container is told (OTEL_SERVICE_NAME), alasio's own its name. */
function serviceOf(found: Component): string | null {
  const container = (found.workload.spec?.template?.spec?.containers ?? [])[0] as { readonly env?: readonly { readonly name: string; readonly value?: string }[] } | undefined;
  return container?.env?.find(({ name }) => name === "OTEL_SERVICE_NAME")?.value ?? (found.name === RELEASE ? "alasio" : null);
}

/** `text` quoted as a literal of SQL's. */
const literal = (text: string): string => `'${text.split("'").join("''")}'`;

/**
 * What the lake kept of the logs `service` sent from `from` on, main's, its newest LAKE_LINES,
 * each a line `<when> <what>`, oldest first; or why it holds none, or was not read.
 */
const lakeLines = (service: string, from: Date): Effect.Effect<{ readonly lines: readonly string[] } | { readonly unread: string }, never, KubeApi> =>
  Effect.gen(function*() {
    const kube = yield* KubeApi;
    const pod = yield* runningPod("lake-query");
    const sql = `SELECT Timestamp, Body FROM otel.logs WHERE ServiceName = ${literal(service)} AND coalesce(ResourceAttributes['alasio.branch'], '') = '' ` +
      `AND Timestamp >= TIMESTAMP ${literal(from.toISOString().replace("T", " ").replace("Z", ""))} ORDER BY Timestamp DESC LIMIT ${LAKE_LINES}`;
    let printed = "";
    const stdout: Writable = new PassThrough().on("data", (chunk: Buffer) => void (printed += chunk.toString("utf8")));
    const stderr = new PassThrough();
    let failed = "";
    stderr.on("data", (chunk: Buffer) => void (failed += chunk.toString("utf8")));
    const exitCode = yield* kube.exec({ namespace: NAMESPACE, pod, container: "query" }, [...QUERY, "--format", "json", sql], { stdin: null, stdout, stderr, tty: false });
    if (exitCode !== 0) return { unread: `the lake's query failed: ${failed.trim() || `exit ${exitCode}`}` };
    const rows = printed.split("\n").filter(Boolean).map((line) => JSON.parse(line) as { readonly Timestamp?: unknown; readonly Body?: unknown });
    return { lines: rows.reverse().map(({ Timestamp, Body }) => `${String(Timestamp)} ${String(Body)}`) };
  }).pipe(Effect.catch((error) => Effect.succeed({ unread: error.message })));

/**
 * Of `found`, which runs no pod, why, as KEDA's ScaledObject of it says, and what the lake kept
 * of its last run's logs: from its last start, alasio's as it keeps it, or `since` back, or
 * LAKE_BACK.
 */
const noPod = (found: Component, since: Option.Option<Duration.Duration>): Effect.Effect<void, never, KubeApi> =>
  Effect.gen(function*() {
    const kube = yield* KubeApi;
    const replicas = (found.workload as { readonly spec?: { readonly replicas?: number } }).spec?.replicas;
    const scaler = yield* kube.get<ScaledObject>({ ...kind("ScaledObject"), namespace: NAMESPACE, name: found.workload.metadata?.name ?? "" }).pipe(
      Effect.catchIf((error) => hasStatus(403)(error) || hasStatus(404)(error), () => Effect.succeed(null)),
      Effect.orElseSucceed(() => null),
    );
    const wakes = scaler ? yield* wakesOf(scaler) : null;
    yield* Console.log(
      replicas === 0
        ? `${found.name} runs no pod: ${scaler && wakes ? describeAsleep(scaler, wakes) : "it is at no replica"}.`
        : `${found.name} runs no pod yet: its pods are being made.`,
    );
    const service = serviceOf(found);
    if (!service) return yield* Console.log(`Its pods' logs went with them, and its telemetry sends none the lake keeps.`);
    const started = found.name === RELEASE && wakes?.started ? new Date(wakes.started) : null;
    const from = Option.match(since, { onNone: () => started ?? new Date(Date.now() - Duration.toMillis(LAKE_BACK)), onSome: (back) => new Date(Date.now() - Duration.toMillis(back)) });
    const kept = yield* lakeLines(service, from);
    if ("unread" in kept) return yield* Console.log(`Its last run's logs, which the lake keeps, were not read: ${kept.unread}.`);
    const of = started && Option.isNone(since) ? `of its last run, since it started at ${started.toISOString()}` : `since ${from.toISOString()}`;
    if (kept.lines.length === 0) {
      // Its last run ended so lately that the lake may not have them yet.
      const ended = wakes?.slept ?? null;
      const loading = ended !== null && Date.now() - Date.parse(ended) < Duration.toMillis(LAKE_CATCH_UP);
      return yield* Console.log(
        loading
          ? `The lake keeps none yet of its logs ${of}; they may still be loading, as its last run ended at ${ended}, within the ${Duration.toMinutes(LAKE_CATCH_UP)} minutes the lake may take to load what is logged.`
          : `The lake keeps none of its logs ${of}.`,
      );
    }
    yield* Console.log(`Its logs ${of}, as the lake keeps them${kept.lines.length === LAKE_LINES ? `, the newest ${LAKE_LINES}` : ""}:`);
    for (const line of kept.lines) yield* Console.log(line);
  });

export const logs = Command.make(
  "logs",
  {
    component: Argument.String("component").pipe(
      Argument.withDefault("alasio"),
      Argument.withDescription("The component: alasio (the default), or another, such as lake or neon-compute; alasio status lists them"),
    ),
    follow: Flag.Boolean("follow").pipe(Flag.withAlias("f"), Flag.withDefault(false), Flag.withDescription("Keep following what it logs")),
    since: sinceFlag,
  },
  ({ component: name, follow, since }) =>
    onCluster(() =>
      Effect.gen(function*() {
        const kube = yield* KubeApi;
        const stdio = yield* Stdio.Stdio;
        const found = yield* component(name);
        const pods = yield* podsOf(found);
        if (pods.length === 0) return yield* noPod(found, since);
        const containers = (found.workload.spec?.template?.spec?.containers ?? [{ name }]).map((container) => container.name);
        const sinceSeconds = Option.map(since, (duration) => Math.ceil(Duration.toSeconds(duration)));
        // A component of several pods (the safekeepers, say) has each line said with its pod's
        // name, and one of several containers (the lake and its query endpoint) with its container's.
        const said = (podName: string, container: string) =>
          [...(pods.length > 1 ? [podName] : []), ...(containers.length > 1 ? [container] : [])].join("/");
        const streams = pods.flatMap((pod) => {
          const podName = pod.metadata?.name ?? "";
          return containers.map((container) => {
            const prefix = said(podName, container);
            return kube.logs(NAMESPACE, podName, { container, follow, sinceSeconds: Option.getOrUndefined(sinceSeconds) }).pipe(
              Stream.decodeText(),
              Stream.splitLines,
              Stream.map((line) => `${prefix ? `[${prefix}] ` : ""}${line}\n`),
            );
          });
        });
        yield* Stream.mergeAll(streams, { concurrency: "unbounded" }).pipe(Stream.run(stdio.stdout()));
      })
    ),
).pipe(
  Command.withShortDescription("Show what alasio, or one of its components, logs"),
  Command.withDescription(
    "Shows what alasio's pod logs, or a component's pods, each of their containers: each line of a component of several pods " +
      "begins with its pod's name, and of several containers, as the lake's pod has its query endpoint beside it, with its " +
      "container's. " +
      "With --follow it keeps following until interrupted; with --since, only what was logged since then. " +
      "Of one that runs no pod, as KEDA lets alasio sleep at none while nothing waits for it, it says so, since when and what " +
      "wakes it, and prints what the lake keeps of its logs: of its last run, since it last started, or since --since.",
  ),
);
