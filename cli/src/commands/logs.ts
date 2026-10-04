/** `alasio logs [component]`: what alasio, or one of its components, logs. */
import { Duration, Effect, Option, Stdio, Stream } from "effect";
import { Argument, Command, Flag } from "effect/cli";

import { KubeApi } from "../kube/api.ts";
import { NAMESPACE } from "../manifests/common.ts";
import { component, onCluster, podsOf, sinceFlag } from "./common.ts";

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
        const container = found.workload.spec?.template?.spec?.containers?.[0]?.name ?? name;
        const sinceSeconds = Option.map(since, (duration) => Math.ceil(Duration.toSeconds(duration)));
        // A component of several pods (the safekeepers, say) has each line said with its pod's name.
        const prefixed = pods.length > 1;
        const streams = pods.map((pod) => {
          const podName = pod.metadata?.name ?? "";
          return kube.logs(NAMESPACE, podName, { container, follow, sinceSeconds: Option.getOrUndefined(sinceSeconds) }).pipe(
            Stream.decodeText(),
            Stream.splitLines,
            Stream.map((line) => `${prefixed ? `[${podName}] ` : ""}${line}\n`),
          );
        });
        yield* Stream.mergeAll(streams, { concurrency: "unbounded" }).pipe(Stream.run(stdio.stdout()));
      })
    ),
).pipe(
  Command.withShortDescription("Show what alasio, or one of its components, logs"),
  Command.withDescription(
    "Shows what alasio's pod logs, or a component's pods: each line of a component of several pods begins with its pod's name. " +
      "With --follow it keeps following until interrupted; with --since, only what was logged since then.",
  ),
);
