/** `alasio grafana`: Grafana, reached from this machine through a port-forward, until interrupted. */
import { type AddressInfo, createServer, type Server, type Socket } from "node:net";

import type { V1Secret } from "@kubernetes/client-node";
import { Console, Effect, FiberSet, Schema } from "effect";
import { Command, Flag } from "effect/cli";

import { kind, KubeApi } from "../kube/api.ts";
import { NAMESPACE } from "../manifests/common.ts";
import { GRAFANA, GRAFANA_PORT } from "../manifests/grafana.ts";
import { onCluster, runningPod } from "./common.ts";

/** The local port is taken, or cannot be listened on. */
export class PortUnavailable extends Schema.TaggedError<PortUnavailable>()("PortUnavailable", {
  port: Schema.Number,
  reason: Schema.String,
}) {
  override get message(): string {
    return `Grafana cannot be forwarded to port ${this.port} here: ${this.reason}; --port names another`;
  }
}

/** Grafana's admin's password is not in its Secret, as the stack's setup writes it. */
export class NoAdminPassword extends Schema.TaggedError<NoAdminPassword>()("NoAdminPassword", {}) {
  override get message(): string {
    return `${GRAFANA} holds no admin password: alasio up writes it`;
  }
}

/** The key of Grafana's Secret that holds its admin's password. */
const ADMIN_PASSWORD = "GF_SECURITY_ADMIN_PASSWORD";

/** A server on the loopback's `port`, listening while the scope is open. */
const listening = (port: number, onConnection: (socket: Socket) => void) =>
  Effect.acquireRelease(
    Effect.callback<Server, PortUnavailable>((resume) => {
      const server = createServer(onConnection);
      server.once("error", (error) => resume(Effect.fail(new PortUnavailable({ port, reason: error.message }))));
      server.listen(port, "127.0.0.1", () => resume(Effect.succeed(server)));
    }),
    (server) => Effect.sync(() => server.close()),
  );

const forward = (port: number) =>
  Effect.gen(function*() {
    const kube = yield* KubeApi;
    const pod = yield* runningPod("grafana");
    const run = yield* FiberSet.makeRuntime<never>();
    const server = yield* listening(port, (socket) => {
      socket.on("error", () => socket.destroy());
      run(kube.portForward({ namespace: NAMESPACE, pod }, GRAFANA_PORT, socket).pipe(
        Effect.catch((error) => Effect.logWarning(`a connection to Grafana failed: ${error.message}`)),
      ));
    });
    // A server listening on a TCP port has an address of its own.
    const { port: listened } = server.address() as AddressInfo;
    yield* Console.log(`Grafana is at http://127.0.0.1:${listened}, as admin; alasio grafana --password prints its password. Interrupt this to end it.`);
    return yield* Effect.never;
  }).pipe(Effect.scoped);

const password = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const secret = yield* kube.get<V1Secret>({ ...kind("Secret"), namespace: NAMESPACE, name: GRAFANA });
  const value = secret?.data?.[ADMIN_PASSWORD];
  if (!value) return yield* new NoAdminPassword();
  yield* Console.log(Buffer.from(value, "base64").toString("utf8"));
});

export const grafana = Command.make(
  "grafana",
  {
    port: Flag.Int("port").pipe(Flag.withDefault(3000), Flag.withDescription("The port of this machine Grafana is reached on: 3000 unless given, any free one for 0")),
    password: Flag.Boolean("password").pipe(Flag.withDefault(false), Flag.withDescription("Print Grafana's admin's password, and do nothing else")),
  },
  ({ port, password: printPassword }) =>
    onCluster(() =>
      Effect.gen(function*() {
        if (printPassword) yield* password;
        else yield* forward(port);
      })
    ),
).pipe(
  Command.withShortDescription("Open Grafana, on this machine"),
  Command.withDescription(
    "Forwards a port of this machine to Grafana, through the cluster's API, and keeps it forwarded until interrupted; " +
      "alasio grafana --password prints the password of its admin, as whom one logs in. Grafana has no other way in: no " +
      "Ingress, and no pod may reach it. It shows three dashboards on the analytics lake (see alasio lake), in the folder " +
      "alasio: Stack health (whether the stack's services answer its collector's scrapes, telemetry arriving, what the stack " +
      "logs as a warning or worse, and what its storage holds), Agents (turns by outcome and duration, Claude Code's and " +
      "Codex's tokens, tool calls and their failures, and a turn's trace, span by span, with its spans' attributes) and " +
      "Conversations (each conversation's turns, the harness sessions and session workspaces they ran in, and what those " +
      "spent). Its alert rules (a service of the stack down for minutes, turns failing, telemetry absent from the lake) " +
      "alert through the Telegram bot, to each of its allowed users. Dashboards, rules and the contact point are " +
      "provisioned from alasio's repository (neon/grafana), and read-only in Grafana: a change is made there, and released " +
      "in alasio's Grafana image. Grafana keeps nothing but its database, in Neon.",
  ),
);
