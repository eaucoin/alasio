/**
 * `alasio lake [--format table|csv|json] <sql>`: a read-only query of the analytics lake, in a
 * reader's container; `alasio lake reconcile [--branch <name>]`: an environment's lake's
 * catalog set right with the object store.
 */
import { PassThrough } from "node:stream";

import { NodeStream } from "@effect/platform-node";
import { Console, Effect, Option, Schema, Stdio, Stream } from "effect";
import { Argument, Command, Flag } from "effect/cli";

import { branchNamespaces } from "../branches.ts";
import { loadConfig } from "../config.ts";
import { installConfig } from "../install.ts";
import { KubeApi } from "../kube/api.ts";
import { reconcileLake } from "../lake.ts";
import { branchEnvironment } from "../manifests/branch.ts";
import { lakeRuns, MAIN_ENVIRONMENT, NAMESPACE } from "../manifests/common.ts";
import { CommandFailed, onCluster, runningPod, timeoutFlag, waitOptions } from "./common.ts";

/** The lake's query, as its image has it (neon/lake). */
const QUERY = ["node", "/opt/lake/src/query.ts"] as const;

/** What alasio lake reconcile cannot set right. */
export class ReconcileRefused extends Schema.TaggedError<ReconcileRefused>()("ReconcileRefused", {
  message: Schema.String,
}) {}

const reconcile = Command.make(
  "reconcile",
  {
    branch: Flag.String("branch").pipe(
      Flag.optional,
      Flag.withMetavar("name"),
      Flag.withDescription("The branch environment whose lake to set right, rather than main's"),
    ),
    timeout: timeoutFlag,
  },
  ({ branch, timeout }) =>
    onCluster(() =>
      Effect.gen(function*() {
        const settings = yield* installConfig(yield* loadConfig);
        if (!lakeRuns(settings)) return yield* new ReconcileRefused({ message: "alasio runs no lake" });
        if (Option.isSome(branch) && !(yield* branchNamespaces).includes(branch.value)) {
          return yield* new ReconcileRefused({ message: `there is no branch environment ${branch.value}` });
        }
        const lost = yield* reconcileLake(settings, Option.match(branch, { onNone: () => MAIN_ENVIRONMENT, onSome: branchEnvironment }), waitOptions(timeout));
        yield* Console.log(lost.length > 0 ? lost.join("\n") : "the lake lost no file");
      })
    ),
).pipe(
  Command.withShortDescription("Set the lake's catalog right with the object store"),
  Command.withDescription(
    "Sets the lake's catalog right with the object store where files it names are gone from the store, as a restore of the filer store loses " +
      "what was written since its backup (alasio restore-filer-store runs it then, for every environment): it forgets each, in one transaction of " +
      "the catalog, loads again from alasio's database what the transcripts' and rollouts' tables lost, has the intake read again from the log " +
      "what the telemetry lost, where the log still holds it, and says what each table lost, its rows, files and time, and what of the telemetry the log no " +
      "longer holds, or has yet to give back. Main's lake, or with --branch a branch environment's, each " +
      "of a catalog of its own. It runs on demand alone, once you know why the files are gone: the alert \"The lake names files the object store " +
      "does not hold\" says when maintenance fails on one. Past --timeout it gives up, saying what it still waits for.",
  ),
);

export const lake = Command.make(
  "lake",
  {
    sql: Argument.String("sql").pipe(Argument.withDescription("The query, in DuckDB's SQL, such as \"SELECT count(*) FROM claude.entries\"")),
    format: Flag.Literals("format", ["table", "csv", "json"]).pipe(
      Flag.withDefault("table"),
      Flag.withDescription("How its rows are printed: a table (the default), CSV, or a JSON object a line"),
    ),
  },
  ({ sql, format }) =>
    onCluster(() =>
      Effect.gen(function*() {
        const kube = yield* KubeApi;
        const stdio = yield* Stdio.Stdio;
        const pod = yield* runningPod("lake-query");
        const stdout = new PassThrough();
        const stderr = new PassThrough();
        const forward = (stream: PassThrough, sink: ReturnType<Stdio.Stdio["stdout"]>) =>
          NodeStream.fromReadable({ evaluate: () => stream, onError: (cause) => cause }).pipe(Stream.run(sink), Effect.orDie);
        const [exitCode] = yield* Effect.all([
          kube.exec({ namespace: NAMESPACE, pod, container: "query" }, [...QUERY, "--format", format, sql], { stdin: null, stdout, stderr, tty: false }).pipe(
            Effect.ensuring(Effect.sync(() => {
              stdout.end();
              stderr.end();
            })),
          ),
          forward(stdout, stdio.stdout()),
          forward(stderr, stdio.stderr()),
        ], { concurrency: "unbounded" });
        if (exitCode !== 0) return yield* new CommandFailed({ command: "the lake's query", exitCode });
      })
    ),
).pipe(
  Command.withShortDescription("Query the analytics lake"),
  Command.withDescription(
    "Runs a read-only SQL query of the analytics lake through its query endpoint, as Grafana does, and prints its rows, as a " +
      "table unless --format says CSV or JSON: a query is one statement, which runs for 30 s and returns 100,000 rows at most, " +
      "a map, list or struct as JSON. The lake holds every Claude Code transcript entry (claude.entries, and claude.messages, content_blocks and " +
      "tool_calls), every Codex rollout line (codex.lines, and codex.turns, token_usage and tool_calls), and alasio's telemetry, " +
      "kept telemetry.retentionDays days: its spans, logs and metric points in otel.traces, otel.logs and otel.metrics_gauge, " +
      "_sum, _histogram, _exponential_histogram and _summary, the tables of OpenTelemetry's ClickHouse exporter, their " +
      "attributes maps of text. Telemetry joins transcripts by these attributes: alasio.conversation.id, the conversation, on " +
      "alasio's alasio.turn span and on the resource of Claude Code's and folder workspaces' bayma's telemetry; " +
      "alasio.session.id, the harness's session (claude.entries.session_id, codex.lines.thread_id), on alasio.turn; " +
      "alasio.volume.id, a session workspace, on alasio.turn and on the resource of its bayma's telemetry; alasio.branch, a " +
      "branch environment's name (alasio branch), on the resource of everything it sends. Errors by harness: " +
      "\"SELECT SpanAttributes['alasio.harness'] AS harness, count(*) FILTER (StatusCode = 'Error') AS failed, count(*) AS turns " +
      "FROM otel.traces WHERE SpanName = 'alasio.turn' GROUP BY 1\". A conversation's Claude output tokens: " +
      "\"SELECT conversation, sum(output_tokens) FROM (SELECT DISTINCT " +
      "SpanAttributes['alasio.conversation.id'] AS conversation, SpanAttributes['alasio.session.id'] AS session_id FROM " +
      "otel.traces WHERE SpanName = 'alasio.turn') JOIN claude.messages USING (session_id) GROUP BY 1\". " +
      "alasio lake reconcile sets its catalog right with the object store where files it names are gone.",
  ),
  Command.withSubcommands([reconcile]),
);
