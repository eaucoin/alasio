import type { CallbackQuery } from "@grammyjs/types";
import { Effect, Option } from "effect";

import type { ConcurrentPromptPayload } from "../codex/turns.ts";
import { discardHeld, queueHeld, shownBusy, steerWithHeld, swerveTo } from "../harness/commands.ts";
import { harnessLabelOf, NO_SERVICE_MOUNTED, NO_WORKSPACE_MOUNTED, isHarnessName } from "../harness/index.ts";
import { Panels } from "../harness/panels.ts";
import { FOLDER_RESTART, FOLDER_RETRY } from "../mcp/bayma.ts";
import type { CommandError, OperatorServices } from "../operator/command-handler.ts";
import { handleGoalControlCallback, isGoalControlAction } from "../operator/goal-control.ts";
import { handleModelControlCallback, isModelControlAction } from "../operator/model-control.ts";
import { handleServiceControlCallback, isServiceControlAction } from "../operator/service-control.ts";
import { handleSessionControlCallback, isSessionControlAction } from "../operator/session-control.ts";
import { handleWorkspaceControlCallback, isWorkspaceControlAction } from "../operator/workspace-control.ts";
import type { CallbackAction } from "../persistence/callback-repository.ts";
import type { Mount } from "../persistence/conversation-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
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

/** What a press does once taken (takeCallbackQuery): its answer, and the control it runs. */
export type PressHandling = Effect.Effect<void, CommandError, OperatorServices>;

/** Answers the press `callbackQueryId` with `text`, and does nothing more. */
const answering = (callbackQueryId: string, text: string): PressHandling =>
  Effect.asVoid(Effect.flatMap(TelegramClient, (client) => client.answerCallbackQuery(callbackQueryId, text)));

/** A press taken from the store (takePress): what it then does, and the action it took, which it was taken for; none where it took none. */
export interface TakenPress {
  readonly handling: PressHandling;
  readonly action: CallbackAction | null;
}

/**
 * Takes a press of one of alasio's buttons from the store: whether its user may press it,
 * its action, consumed, and the conversation's mount; and gives what it then does, its answer
 * and its control, to run apart (handlePress), with the action, which a press kept past its
 * update is run again from (replayPress). The store failing fails it, and the press is taken
 * again; its action is consumed only where the store answers.
 */
export const takePress = Effect.fnUntraced(function*(callbackQuery: CallbackQuery): Effect.fn.Return<
  TakenPress,
  StoreError,
  Authorizer | Store
> {
  const store = yield* Store;
  if (!(yield* Effect.flatMap(Authorizer, (authorizer) => authorizer.isAuthorizedCallbackQuery(callbackQuery)))) {
    return { handling: answering(callbackQuery.id, "This action is not authorized for this Telegram user."), action: null };
  }
  // Every button alasio sends carries callback_data; a query without it is no action of alasio's.
  const action = callbackQuery.data === undefined ? null : yield* store.consumeCallbackAction(callbackQuery.data);
  if (!action) return { handling: answering(callbackQuery.id, "This action is no longer available."), action: null };
  return { handling: pressing(callbackQuery, action, yield* store.getMount(action.conversationId)), action };
});

/** What a press does, as takePress gives it: its handling alone. */
export const takeCallbackQuery = (callbackQuery: CallbackQuery): Effect.Effect<PressHandling, StoreError, Authorizer | Store> =>
  Effect.map(takePress(callbackQuery), ({ handling }) => handling);

/**
 * A press taken before, and kept as its control ran on past its update (./presses.ts), run
 * again from its action, as the alasio that ran it stopped first: its user was let press it,
 * and its action consumed, as it was taken; the conversation's mount is read anew.
 */
export const replayPress = (callbackQuery: CallbackQuery, action: CallbackAction): PressHandling =>
  Effect.flatMap(Store, (store) => store.getMount(action.conversationId)).pipe(Effect.flatMap((mount) => pressing(callbackQuery, action, mount)));

/** The press of `action`, of the conversation `mount` is the mount of. */
const pressing = (callbackQuery: CallbackQuery, action: CallbackAction, mount: Mount): PressHandling =>
  handlePress(callbackQuery, action, mount).pipe(
    // A control its harness host is not reached for says so, as the press is answered.
    Effect.catchTag("HarnessHostError", (error) => answering(callbackQuery.id, error.message)),
  );

/** Handles a press taken from the store (takeCallbackQuery): its action, of the conversation `mount` is the mount of. */
const handlePress = Effect.fnUntraced(function*(callbackQuery: CallbackQuery, action: CallbackAction, mount: Mount): Effect.fn.Return<
  void,
  CommandError,
  OperatorServices
> {
  const client = yield* TelegramClient;
  const { conversationId } = action;
  const closes = action.kind.endsWith(":close");
  const heldPrompt = HELD_PROMPT_ACTIONS.has(action.kind);
  // A folder instance's buttons are of its harness's instance, whatever the conversation mounts now.
  const ofFolder = action.kind === FOLDER_RETRY || action.kind === FOLDER_RESTART;
  if (!closes && !heldPrompt && !ofFolder && mount.sessionId !== action.payload["expectedSessionId"]) {
    yield* client.answerCallbackQuery(callbackQuery.id, "This panel is stale. Open it again.");
    return;
  }
  if (!closes
    && !heldPrompt
    && !ofFolder
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
      const busy = yield* shownBusy(conversationId);
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
  const harness = Option.getOrNull(yield* Effect.flatMap(Panels, (panels) => panels.forMount(conversationId, mount)));
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
