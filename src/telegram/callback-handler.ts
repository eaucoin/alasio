import type { CallbackQuery } from "@grammyjs/types";
import { Effect, Option } from "effect";

import type { ConcurrentPromptPayload } from "../codex/turns.ts";
import { discardHeld, isBusy, queueHeld, steerWithHeld, swerveTo } from "../harness/commands.ts";
import { Harnesses, harnessLabelOf, NO_SERVICE_MOUNTED, NO_WORKSPACE_MOUNTED, isHarnessName } from "../harness/index.ts";
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

/** The kinds of a held prompt's buttons, which settle its job. */
const HELD_PROMPT_ACTIONS: ReadonlySet<string> = new Set(["steer", "queue", "swerve", "discard"]);

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
  const heldPrompt = HELD_PROMPT_ACTIONS.has(action.kind);
  const mount = yield* store.getMount(conversationId);
  if (!closes && !heldPrompt && mount.sessionId !== action.payload["expectedSessionId"]) {
    yield* client.answerCallbackQuery(callbackQuery.id, "This panel is stale. Open it again.");
    return;
  }
  if (!closes
    && !heldPrompt
    && !isServiceControlAction(action.kind)
    && isHarnessName(action.payload["expectedHarness"])
    && mount.harness !== action.payload["expectedHarness"]) {
    yield* client.answerCallbackQuery(callbackQuery.id, "This panel belongs to another service. Open it again.");
    return;
  }
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
  // A prompt held while a turn ran: its job is all its buttons need, whatever the conversation mounts now.
  if (heldPrompt) {
    const jobId = concurrentPromptPayload(action).jobId;
    const label = harnessLabelOf(mount);
    const gone = Effect.asVoid(client.answerCallbackQuery(callbackQuery.id, "That message is no longer held."));
    if (action.kind === "steer") {
      const steered = yield* steerWithHeld(conversationId, jobId);
      if (steered.outcome === "gone") return yield* gone;
      if (steered.outcome === "steered") {
        yield* client.answerCallbackQuery(callbackQuery.id, "Steered.");
        yield* client.editMessageText(chatId, messageId, `Sent as guidance to the active ${label} turn.`);
        return;
      }
      yield* client.answerCallbackQuery(callbackQuery.id, "Queued.");
      // No turn runs to take it, or the one running cannot yet (a Codex turn not yet started upstream), or the steer failed.
      if (steered.reason === "idle") {
        yield* client.editMessageText(chatId, messageId, `${label} is not running a turn to steer. Queued to run next.`);
      } else if (steered.reason === "not ready") {
        yield* client.editMessageText(chatId, messageId, `${label} is not ready to steer yet. Queued instead.`);
      } else {
        yield* client.editMessageText(chatId, messageId, `Steer failed; queued instead.\n\n${steered.failure}`, { format: "plain" });
      }
      return;
    }
    if (action.kind === "queue") {
      // Asked again once its turn ended (Run it), it runs now, unless another turn has begun meanwhile.
      const busy = yield* isBusy(conversationId);
      if (!(yield* queueHeld(jobId))) return yield* gone;
      yield* client.answerCallbackQuery(callbackQuery.id, busy ? "Queued." : "Running.");
      yield* client.editMessageText(chatId, messageId, busy ? `Queued. ${label} will process this after the current task.` : `${label} is running this now.`);
      return;
    }
    if (action.kind === "discard") {
      if (!(yield* discardHeld(jobId))) return yield* gone;
      yield* client.answerCallbackQuery(callbackQuery.id, "Discarded.");
      yield* client.editMessageText(chatId, messageId, "Discarded.");
      return;
    }
    if (!(yield* swerveTo(conversationId, jobId))) return yield* gone;
    yield* client.answerCallbackQuery(callbackQuery.id, "Swerving.");
    yield* client.editMessageText(chatId, messageId, `Swerving ${label} to this message.`);
    return;
  }
  // Every remaining action is the mounted harness's, which the mount may have none of.
  const harness = Option.getOrNull(yield* Effect.flatMap(Harnesses, (harnesses) => harnesses.forMount(mount)));
  if (!harness) {
    const reason = mount.harness ? NO_WORKSPACE_MOUNTED : NO_SERVICE_MOUNTED;
    yield* client.answerCallbackQuery(callbackQuery.id, reason);
    return;
  }
  if (isSessionControlAction(action.kind)) {
    const control = handleSessionControlCallback({ ...press, harness });
    return yield* closes ? control : afterAcknowledging(callbackQuery.id, control);
  }
  if (isGoalControlAction(action.kind)) {
    if (!harness.supportsGoals && !closes) {
      yield* client.answerCallbackQuery(callbackQuery.id, `Goals are a Codex feature; ${harness.displayName} is active.`);
      return;
    }
    const control = handleGoalControlCallback({ ...press, goals: harness.goals });
    return yield* closes ? control : afterAcknowledging(callbackQuery.id, control);
  }
  yield* client.answerCallbackQuery(callbackQuery.id, "Unknown action.");
});
