/** `alasio lake [--format table|csv|json] <sql>`: a read-only query of the analytics lake, in the lake's pod. */
import { PassThrough } from "node:stream";

import { NodeStream } from "@effect/platform-node";
import { Effect, Stdio, Stream } from "effect";
import { Argument, Command, Flag } from "effect/cli";

import { KubeApi } from "../kube/api.ts";
import { NAMESPACE } from "../manifests/common.ts";
import { CommandFailed, onCluster, runningPod } from "./common.ts";

/** The lake's query, as its image has it (neon/lake). */
const QUERY = ["node", "/opt/lake/src/query.ts"] as const;

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
        const pod = yield* runningPod("lake");
        const stdout = new PassThrough();
        const stderr = new PassThrough();
        const forward = (stream: PassThrough, sink: ReturnType<Stdio.Stdio["stdout"]>) =>
          NodeStream.fromReadable({ evaluate: () => stream, onError: (cause) => cause }).pipe(Stream.run(sink), Effect.orDie);
        const [exitCode] = yield* Effect.all([
          kube.exec({ namespace: NAMESPACE, pod, container: "lake" }, [...QUERY, "--format", format, sql], { stdin: null, stdout, stderr, tty: false }).pipe(
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
    "Runs a read-only SQL query of the analytics lake, where every Claude Code transcript entry and Codex rollout line is a row, " +
      "in the lake's pod, and prints its rows, as a table unless --format says CSV or JSON.",
  ),
);
