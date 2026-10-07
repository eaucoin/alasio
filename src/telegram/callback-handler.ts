import type { CallbackQuery } from "@grammyjs/types";
import { Effect, Option, Result } from "effect";

import { type ConcurrentPromptPayload, Turns } from "../codex/turns.ts";
import { ActiveTurns } from "../harness/active-turns.ts";
import { Harnesses, NO_SERVICE_MOUNTED, NO_WORKSPACE_MOUNTED, isHarnessName } from "../harness/index.ts";
import type { CommandError, OperatorServices } from "../operator/command-handler.ts";
import { handleGoalControlCallback, isGoalControlAction } from "../operator/goal-control.ts";
import { handleModelControlCallback, isModelControlAction } from "../operator/model-control.ts";
import { handleServiceControlCallback, isServiceControlAction } from "../operator/service-control.ts";
import { handleSessionControlCallback, isSessionControlAction } from "../operator/session-control.ts";
import { handleWorkspaceControlCallback, isWorkspaceControlAction } from "../operator/workspace-control.ts";
import type { CallbackAction } from "../persistence/callback-repository.ts";
import { Store } from "../persistence/store.ts";
import { Authorizer } from "./authorizer.ts";
import { TelegramClient } from "./client.ts";
import { ReceivedFiles } from "./files.ts";

/** A concurrent prompt's button payload: askHowToHandleConcurrentPrompt is the only writer of these kinds. */
function concurrentPromptPayload(action: CallbackAction): ConcurrentPromptPayload {
  return action.payload as ConcurrentPromptPayload;
}

/**
 * Runs `control` after the press has been answered "Working...": the control's own
 * answers then go nowhere, as a query is answered once.
 */
const afterAcknowledging = <A, E, R>(callbackQueryId: string, control: Effect.Effect<A, E, R>) =>
  Effect.flatMap(TelegramClient, (client) => client.answerCallbackQuery(callbackQueryId, "Working...")).pipe(
    Effect.andThen(control.pipe(
      Effect.updateService(TelegramClient, (client) => TelegramClient.of({ ...client, answerCallbackQuery: () => Effect.succeed(null) })),
    )),
  );

/** Handles a press of one of alasio's buttons. */
export const handleCallbackQuery = Effect.fnUntraced(function*(callbackQuery: CallbackQuery): Effect.fn.Return<
  void,
  CommandError,
  Authorizer | OperatorServices
> {
  const store = yield* Store;
  const client = yield* TelegramClient;
  const activeTurns = yield* ActiveTurns;
  const turns = yield* Turns;
  if (!(yield* Effect.flatMap(Authorizer, (authorizer) => authorizer.isAuthorizedCallbackQuery(callbackQuery)))) {
    yield* client.answerCallbackQuery(callbackQuery.id, "This action is not authorized for this Telegram user.");
    return;
  }
  // Every button alasio sends carries callback_data; a query without it is no action of alasio's.
  const action = callbackQuery.data === undefined ? null : yield* store.consumeCallbackAction(callbackQuery.data);
  if (!action) {
    yield* client.answerCallbackQuery(callbackQuery.id, "This action is no longer available.");
    return;
  }
  const { conversationId } = action;
  const closes = action.kind.endsWith(":close");
  const mount = yield* store.getMount(conversationId);
  if (!closes && mount.sessionId !== action.payload["expectedSessionId"]) {
    yield* client.answerCallbackQuery(callbackQuery.id, "This panel is stale. Open it again.");
    return;
  }
  if (!closes
    && !isServiceControlAction(action.kind)
    && isHarnessName(action.payload["expectedHarness"])
    && mount.harness !== action.payload["expectedHarness"]) {
    yield* client.answerCallbackQuery(callbackQuery.id, "This panel belongs to another service. Open it again.");
    return;
  }
  const harness = Option.getOrNull(yield* Effect.flatMap(Harnesses, (harnesses) => harnesses.forMount(mount)));
  const chatId = callbackQuery.message?.chat?.id;
  const messageId = callbackQuery.message?.message_id;
  if (!chatId || !messageId) {
    yield* client.answerCallbackQuery(callbackQuery.id, "Missing message context.");
    return;
  }
  const press = { action, callbackQueryId: callbackQuery.id, chatId, messageId };
  if (isModelControlAction(action.kind)) {
    return yield* handleModelControlCallback(press);
  }
  if (isServiceControlAction(action.kind)) {
    return yield* handleServiceControlCallback(press);
  }
  if (isWorkspaceControlAction(action.kind)) {
    return yield* handleWorkspaceControlCallback(press);
  }
  if (!harness) {
    const reason = mount.harness ? NO_WORKSPACE_MOUNTED : NO_SERVICE_MOUNTED;
    yield* client.answerCallbackQuery(callbackQuery.id, reason);
    return;
  }
  if (isSessionControlAction(action.kind)) {
    const control = handleSessionControlCallback({ ...press, harness });
    return yield* closes ? control : afterAcknowledging(callbackQuery.id, control);
  }
  if (action.kind === "steer") {
    const payload = concurrentPromptPayload(action);
    const promptJob = payload.jobId ? yield* store.getPromptJob(payload.jobId) : null;
    const prompt = promptJob?.prompt ?? payload.prompt;
    // The files the prompt names, written again if a restart came since.
    if (promptJob) {
      yield* Effect.flatMap(ReceivedFiles, (files) => files.materialize(promptJob.file_ids));
    }
    /** Keeps the message for after the running turn instead: its job made pending again, or a queued message. */
    const queueInstead = promptJob ? turns.setPromptDisposition(promptJob.id, "pending") : turns.enqueueMessage(conversationId, prompt);
    const steered = yield* activeTurns.steer(conversationId, prompt).pipe(Effect.result);
    if (Result.isFailure(steered)) {
      yield* queueInstead;
      yield* client.answerCallbackQuery(callbackQuery.id, "Queued.");
      yield* client.editMessageText(chatId, messageId, `Steer failed; queued instead.\n\n${steered.failure.message}`, { format: "plain" });
      return;
    }
    if (Option.getOrElse(steered.success, () => false)) {
      if (promptJob) {
        yield* turns.setPromptDisposition(promptJob.id, "completed");
      }
      yield* client.answerCallbackQuery(callbackQuery.id, "Steered.");
      yield* client.editMessageText(chatId, messageId, `Sent as guidance to the active ${harness.displayName} turn.`);
      return;
    }
    // No turn runs to take it, or the one running cannot yet (a Codex turn not yet started upstream).
    yield* queueInstead;
    yield* client.answerCallbackQuery(callbackQuery.id, "Queued.");
    yield* client.editMessageText(chatId, messageId, `${harness.displayName} is not ready to steer yet. Queued instead.`);
    return;
  }
  if (isGoalControlAction(action.kind)) {
    if (!harness.supportsGoals && !closes) {
      yield* client.answerCallbackQuery(callbackQuery.id, `Goals are a Codex feature; ${harness.displayName} is active.`);
      return;
    }
    const control = handleGoalControlCallback({ ...press, goals: harness.goals });
    return yield* closes ? control : afterAcknowledging(callbackQuery.id, control);
  }
  if (action.kind === "queue") {
    const payload = concurrentPromptPayload(action);
    yield* payload.jobId ? turns.setPromptDisposition(payload.jobId, "pending") : turns.enqueueMessage(conversationId, payload.prompt);
    yield* client.answerCallbackQuery(callbackQuery.id, "Queued.");
    yield* client.editMessageText(chatId, messageId, `Queued. ${harness.displayName} will process this after the current task.`);
    return;
  }
  if (action.kind === "discard") {
    const payload = concurrentPromptPayload(action);
    if (payload.jobId) {
      yield* turns.setPromptDisposition(payload.jobId, "cancelled");
    }
    yield* client.answerCallbackQuery(callbackQuery.id, "Discarded.");
    yield* client.editMessageText(chatId, messageId, "Discarded.");
    return;
  }
  if (action.kind === "swerve") {
    const payload = concurrentPromptPayload(action);
    if (payload.jobId) {
      // First in line, scheduled once the running turn has let go.
      yield* store.setPromptJobDisposition(payload.jobId, "pending", 1);
    } else {
      yield* turns.enqueueMessage(conversationId, payload.prompt, true);
    }
    yield* activeTurns.stop(conversationId, "swerve");
    if (payload.jobId) {
      yield* turns.schedule(conversationId);
    }
    yield* client.answerCallbackQuery(callbackQuery.id, "Swerving.");
    yield* client.editMessageText(chatId, messageId, `Swerving ${harness.displayName} to this message.`);
    return;
  }
  yield* client.answerCallbackQuery(callbackQuery.id, "Unknown action.");
});
