/**
 * The workflow hook server: where an agent's session reports that it waits on a GitHub
 * Actions run (../policy/workflow-wait.ts notifyWorkflowWait), so the turn's status can
 * say so. It listens on localhost while alasio runs.
 */
import { createServer } from "node:http";

import { NodeHttpServer } from "@effect/platform-node";
import { Clock, Context, Effect, Layer, Schema, type Scope } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";

import { Store } from "../persistence/store.ts";
import type { WorkflowWaitType } from "../policy/workflow-wait.ts";
import { withLogScope } from "../shared/log.ts";

/** A wait on a workflow run an agent reported for its session, shown in the turn's status. */
export interface WorkflowWait {
  readonly runId: string;
  readonly waitType: WorkflowWaitType | "unknown";
  readonly command: string;
  readonly threadKey: string;
  readonly startedAt: number;
}

/** What notifyWorkflowWait posts, as the server decodes it. */
const WorkflowHookNotification = Schema.Struct({
  session_id: Schema.NonEmptyString,
  run_id: Schema.NonEmptyString,
  wait_type: Schema.optional(Schema.Literals(["watch", "poll", "check"])),
  command: Schema.optional(Schema.String),
});

const HOOK_PATH = "/hook/workflow";

/**
 * The workflow waits agents report, by session, as the hook server on localhost:`port`
 * records them while the service lasts; a turn's status shows its session's wait when it
 * next checks (codex/status-reporter.ts).
 */
export class WorkflowHooks extends Context.Service<WorkflowHooks, {
  /** The port the server listens on: the one it was given, or the one it was assigned for 0. */
  readonly port: number;
  readonly waits: ReadonlyMap<string, WorkflowWait>;
}>()("alasio/workflow/WorkflowHooks") {
  static readonly layer = (port: number): Layer.Layer<WorkflowHooks, never, Store> =>
    Layer.effect(WorkflowHooks, serveWorkflowHooks(port));
}

const serveWorkflowHooks = Effect.fnUntraced(function*(port: number): Effect.fn.Return<WorkflowHooks["Service"], never, Store | Scope.Scope> {
  const store = yield* Store;
  const waits = new Map<string, WorkflowWait>();

  const record = Effect.gen(function*() {
    const notification = yield* HttpServerRequest.schemaBodyJson(WorkflowHookNotification);
    waits.set(notification.session_id, {
      runId: notification.run_id,
      waitType: notification.wait_type ?? "unknown",
      command: notification.command ?? "",
      threadKey: (yield* store.getActiveTurns).find((turn) => turn.session_id === notification.session_id)?.thread_key ?? "",
      startedAt: (yield* Clock.currentTimeMillis) / 1000,
    });
    return HttpServerResponse.text("OK");
  }).pipe(
    Effect.catchTag("SchemaError", () => Effect.succeed(HttpServerResponse.text("Missing session_id or run_id", { status: 400 }))),
    Effect.catchTag("StoreError", (error) =>
      Effect.logError(`Error handling workflow hook: ${error.message}`).pipe(Effect.as(HttpServerResponse.text(error.message, { status: 503 })))),
    Effect.catchTag("HttpServerError", (error) =>
      Effect.logError(`Error handling workflow hook: ${error}`).pipe(Effect.as(HttpServerResponse.text(String(error), { status: 500 })))),
  );

  const handle = HttpServerRequest.HttpServerRequest.pipe(
    Effect.flatMap((request) =>
      request.method === "POST" && request.url === HOOK_PATH ? record : Effect.succeed(HttpServerResponse.text("Not found", { status: 404 }))
    ),
    withLogScope("telegram-app"),
  );

  // A port taken or refused is a deployment alasio cannot run in.
  const server = yield* Effect.orDie(NodeHttpServer.make(() => createServer(), { port, host: "localhost" }));
  yield* server.serve(handle);
  yield* Effect.logInfo(`Hook server started on localhost:${port}`).pipe(withLogScope("telegram-app"));
  return WorkflowHooks.of({ port: server.address._tag === "UnixPathAddress" ? port : server.address.port, waits });
});
