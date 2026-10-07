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
      "With --follow it keeps following until interrupted; with --since, only what was logged since then.",
  ),
);
