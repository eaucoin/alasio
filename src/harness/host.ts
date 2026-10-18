/**
 * The harness host: where alasio's turns run, as the control plane reaches them, by
 * conversation. It starts a turn, steers it, stops it, says whether one runs, and starts a
 * conversation's fresh session; and it tells the control plane, as events, of what a
 * harness that runs on between prompts does on its own: a reply of its own to deliver, and
 * its letting go of the conversation.
 *
 * What it is given is data alone, which crosses a process as it is: no closure of the
 * control plane's reaches a harness. A turn's writes are its own, made under the epoch the
 * control plane claimed the turn under, as are the claims of the turns its harness starts
 * on its own, and the progress of the prompt job it runs.
 *
 * It runs in alasio's process for now, over Harnesses and ActiveTurns; a host elsewhere
 * (14.3's harness gateway) is another implementation of this service, which the control
 * plane reaches through it as it does this one.
 */
import { Context, Effect, Layer, Option, PubSub, type Scope } from "effect";

import type { ModelChoice, Mount } from "../persistence/conversation-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store, type TurnFenced } from "../persistence/store.ts";
import { ActiveTurns, type StopReason } from "./active-turns.ts";
import { type AttachedTurn, type Harness, type HarnessError, Harnesses, type HarnessHostError, type HarnessUnavailable, type OwnTurn, type OwnTurns, type TurnResult } from "./index.ts";
import type { HarnessName } from "./names.ts";

/** A turn for the host to run: where, on what, and as which claim of the conversation. */
export interface TurnStart {
  readonly conversationId: string;
  readonly chatId: string;
  readonly messageId: string;
  readonly harness: HarnessName;
  readonly workingDirectory: string;
  readonly prompt: string;
  /** The session it resumes, or none for one the harness makes. */
  readonly resumeSession: string | null;
  /** The model the conversation chose for the harness with /model; null for its default. */
  readonly modelChoice: ModelChoice | null;
  /** The epoch the control plane claimed the turn under, which every write of the turn's holds under. */
  readonly epoch: number;
  /** The prompt job the turn runs, whose progress the host records; none for a turn of no job. */
  readonly jobId: string | null;
  /** A turn already running upstream (a Codex goal's) that it attaches to instead of starting one. */
  readonly attachedTurn: AttachedTurn | null;
}

/** What a harness running on between prompts did on its own, for the control plane to act on. */
export type HostEvent =
  /** It has a reply of its own completed in the store, to deliver. */
  | { readonly kind: "background-response"; readonly conversationId: string }
  /** It let go of the conversation, which may run what waits on it. */
  | { readonly kind: "idle"; readonly conversationId: string };

/**
 * What the operator's panels reach of a mount's harness, through its host: what it is,
 * its sessions, its goals where it has them, and its models.
 */
export type HarnessPanels = Pick<Harness, "name" | "displayName" | "supportsGoals" | "sessions" | "goals" | "listModels" | "defaultModelChoice">;

/** What the host says of a conversation. */
export interface HarnessStatus {
  /** Whether no turn may start in it: one runs, or is about to, or a hold of the store's keeps it (../harness/holds.ts). */
  readonly busy: boolean;
  /** Whether a turn runs in it, or is about to: reserved for one. */
  readonly running: boolean;
  /** Whether the turn running is one its harness started on its own, as Claude Code reports a background task's end. */
  readonly cliInitiated: boolean;
  /** Whether a hold of the store's keeps it busy, as no turn (a fork). */
  readonly held: boolean;
  /** Whether its harness has work of its own going on between turns: Claude Code's background tasks. */
  readonly backgroundWork: boolean;
  /** Since when, in milliseconds since the epoch, no turn has run in it; null while one runs. */
  readonly idleSince: number | null;
}

export class HarnessHost extends Context.Service<HarnessHost, {
  /**
   * Holds the conversation for a turn the control plane is about to start, until the scope
   * closes, unless it is busy, a hold of the store's among what keeps it so: whether it
   * holds it. The host held first and the store read after, as a hold is made the other way
   * round (../harness/holds.ts), so a hold and a turn begun at once do not both go on.
   */
  readonly reserve: (conversationId: string) => Effect.Effect<boolean, StoreError | HarnessHostError, Scope.Scope>;
  /**
   * Runs a turn to its end, its conversation busy meanwhile. It fails only as alasio's
   * store does, with TurnFenced where another turn has claimed the conversation since, or
   * where no harness serves it there: what else goes wrong ends up in its response.
   */
  readonly startTurn: (start: TurnStart) => Effect.Effect<TurnResult, StoreError | TurnFenced | HarnessUnavailable>;
  /** Sends the conversation's running turn more guidance: whether it took it, or none when no turn runs. */
  readonly steer: (conversationId: string, prompt: string) => Effect.Effect<Option.Option<boolean>, HarnessError>;
  /** Stops the conversation's running turn for `reason`, once it has let go: whether one ran; failing where a host elsewhere is not reached. */
  readonly interrupt: (conversationId: string, reason: StopReason) => Effect.Effect<boolean, HarnessHostError>;
  /** What the host says of the conversation; failing where it cannot say, a host elsewhere not reached. */
  readonly status: (conversationId: string) => Effect.Effect<HarnessStatus, StoreError | HarnessHostError>;
  /** A new, empty session of `harness` in `workingDirectory` for the conversation: its id. */
  readonly startFreshSession: (params: { readonly conversationId: string; readonly harness: HarnessName; readonly workingDirectory: string }) => Effect.Effect<string, HarnessError | HarnessUnavailable>;
  /** What the panels reach of the mount's harness in its folder; none until both are mounted. */
  readonly panels: (mount: Mount) => Effect.Effect<Option.Option<HarnessPanels>, HarnessUnavailable>;
  /** Subscribes to the host's events, for as long as the scope lasts. */
  readonly events: Effect.Effect<PubSub.Subscription<HostEvent>, never, Scope.Scope>;
}>()("alasio/harness/HarnessHost") {
  /** The host in alasio's process, over its harnesses and its running turns. */
  static readonly layer: Layer.Layer<HarnessHost, never, Harnesses | ActiveTurns | Store> = Layer.effect(HarnessHost, Effect.gen(function*() {
    const harnesses = yield* Harnesses;
    const activeTurns = yield* ActiveTurns;
    const store = yield* Store;
    const events = yield* PubSub.unbounded<HostEvent>();
    const tell = (event: HostEvent): Effect.Effect<void> => Effect.asVoid(PubSub.publish(events, event));

    /** How the conversation's harness claims it for a turn it starts on its own: as alasio's turns are claimed, ending under the epoch it claimed. */
    const ownTurnsOf = (conversationId: string, chatId: string, harness: HarnessName): OwnTurns => ({
      claim: (messageId, sessionId) =>
        Effect.map(
          store.claimTurn({ conversationId, chatId, messageId, sessionId, harness, prompt: null }),
          (epoch): OwnTurn => ({
            persistence: store.turnPersistence(conversationId, epoch),
            end: (outcome) => Effect.asVoid(store.endTurn(conversationId, epoch, outcome)),
          }),
        ),
      held: store.isConversationHeld(conversationId),
    });

    return HarnessHost.of({
      reserve: (conversationId) =>
        Effect.gen(function*() {
          if (!(yield* activeTurns.reserve(conversationId))) return false;
          return !(yield* store.isConversationHeld(conversationId));
        }),
      startTurn: (start) =>
        Effect.gen(function*() {
          const harness = yield* harnesses.getFor(start.harness, start.workingDirectory);
          /**
           * What the job's turn records of its progress, for a restart to know what became of
           * its prompt. The turn dies on a record it cannot make: so a prompt whose dispatch is
           * not recorded is never sent, and a restart never misjudges the prompt of a turn
           * that ran.
           */
          const recordJob = (record: (jobId: string) => Effect.Effect<void, StoreError>): Effect.Effect<void> =>
            start.jobId ? Effect.orDie(record(start.jobId)) : Effect.void;
          return yield* harness.runTurn({
            prompt: start.prompt,
            resumeSession: start.resumeSession,
            threadKey: start.conversationId,
            chatId: start.chatId,
            messageId: start.messageId,
            workingDirectory: start.workingDirectory,
            modelChoice: start.modelChoice,
            persistence: store.turnPersistence(start.conversationId, start.epoch),
            ownTurns: ownTurnsOf(start.conversationId, start.chatId, harness.name),
            attachedTurn: start.attachedTurn,
            onPromptDispatched: recordJob(store.markPromptJobDispatched),
            onTransportStarted: ({ sessionId, turnId }) => recordJob((id) => store.markPromptJobUpstreamStarted(id, sessionId, turnId)),
            onTransportCompleted: ({ sessionId, turnId }) => recordJob((id) => store.markPromptJobUpstreamCompleted(id, sessionId, turnId)),
            // A harness that keeps running between prompts (Claude Code's background work)
            // produces replies of its own and frees the conversation when they finish.
            onBackgroundResponse: tell({ kind: "background-response", conversationId: start.conversationId }),
            onIdle: tell({ kind: "idle", conversationId: start.conversationId }),
          });
        }),
      steer: (conversationId, prompt) => activeTurns.steer(conversationId, prompt),
      interrupt: (conversationId, reason) => activeTurns.stop(conversationId, reason),
      status: (conversationId) =>
        Effect.gen(function*() {
          const turn = yield* activeTurns.get(conversationId);
          const held = yield* store.isConversationHeld(conversationId);
          const mount = yield* store.getMount(conversationId);
          const harness = Option.getOrNull(yield* harnesses.forMount(mount).pipe(Effect.orElseSucceed(() => Option.none<Harness>())));
          const backgroundWork = harness?.backgroundWork ? yield* harness.backgroundWork(conversationId) : false;
          return {
            busy: Option.isSome(turn) || held,
            running: Option.isSome(turn),
            cliInitiated: Option.exists(turn, ({ cliInitiated }) => cliInitiated),
            held,
            backgroundWork,
            idleSince: yield* activeTurns.idleSince(conversationId),
          };
        }),
      startFreshSession: ({ conversationId, harness, workingDirectory }) =>
        Effect.flatMap(harnesses.getFor(harness, workingDirectory), (found) => found.startFreshSession({ threadKey: conversationId, workingDirectory })),
      panels: (mount) => harnesses.forMount(mount),
      events: PubSub.subscribe(events),
    });
  }));
}
