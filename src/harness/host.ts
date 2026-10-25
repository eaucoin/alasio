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
 * HarnessHost.layer is the host over Harnesses and ActiveTurns, a harness pool's pod's, an
 * `alasio harness` (./program.ts), telling its events as HostEvents does (./host-events.ts); the
 * control plane reaches it over HTTP (./serve.ts, ./remote-host.ts), through the sandbox gateway's
 * front (./pooled-host.ts).
 */
import { Context, Effect, Fiber, Layer, Option, type Scope } from "effect";

import type { ModelChoice } from "../persistence/conversation-repository.ts";
import type { BaymaEndpoint } from "../sandbox/gateway-client.ts";
import type { StoreError } from "../persistence/sql.ts";
import { type ReceivedFileError, ReceivedFiles } from "../telegram/files.ts";
import { Store, type TurnFenced } from "../persistence/store.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import { ActiveTurns, type StopReason } from "./active-turns.ts";
import { type AttachedTurn, type Harness, type HarnessError, type HarnessGoals, Harnesses, HarnessHostError, type HarnessUnavailable, type OwnTurn, type OwnTurns, type TurnResult } from "./index.ts";
import { HostEvents } from "./host-events.ts";
import type { HarnessName } from "./names.ts";

/** A turn alasio asks a host to run: where, on what, and as which claim of the conversation. */
export interface TurnRequest {
  readonly conversationId: string;
  readonly chatId: string;
  readonly messageId: string;
  readonly harness: HarnessName;
  readonly workingDirectory: string;
  readonly prompt: string;
  /** The files the prompt names, which the host writes where its harness reads them before the turn runs. */
  readonly fileIds: readonly string[];
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

/**
 * A turn as a harness pool's pod's host is given it (./program.ts): alasio's request, with what
 * alasio brought up for it, which the pod has no key to reach of its own.
 */
export interface TurnStart extends TurnRequest {
  /** The bayma of the turn's workspace, or why it has none. */
  readonly bayma: TurnBayma;
  /** A Codex turn's login and configuration (TurnCodex); null for a turn of another harness. */
  readonly codex: TurnCodex | null;
}

/**
 * A Codex turn's login and configuration, as the control plane gives a pool's pod's host, which
 * holds no login: the relay of alasio's login it calls the model through, and a token of the
 * turn's for it, good for minutes, which alasio renews as the turn runs (renewCodexLogin);
 * and, of a folder conversation's, the operator's Codex
 * configuration (its config.toml, AGENTS.md, prompts and skills), by path under their Codex
 * home, which the host's own Codex home is given anew each turn, and never their login.
 */
export interface TurnCodex {
  readonly login: { readonly url: string; readonly token: string };
  readonly home: Readonly<Record<string, string>> | null;
}

/**
 * A turn's workspace's bayma, as the control plane gives it a pool's pod's host: its endpoint,
 * through the sandbox gateway, a caller's token of the workspace's key (a session's, or a
 * folder instance's) its bearer, held to the host's binding (../binder/tokens.ts HeldTo); or
 * none, its bind having failed, and why, which the harness puts before the turn's prompt.
 */
export type TurnBayma = { readonly endpoint: BaymaEndpoint; readonly notice: null } | { readonly endpoint: null; readonly notice: string };

/**
 * What the operator's panels reach of a mount's harness (./panels.ts): what it is, its
 * sessions, its goals where it has them, and its models.
 */
export type HarnessPanels = Pick<Harness, "name" | "displayName" | "supportsGoals" | "sessions" | "goals" | "listModels" | "defaultModelChoice">;

/** A turn a caller attaches to (HarnessHost.attach): its conversation, the epoch it was claimed under, and the message it answers. */
export interface AttachedTo {
  readonly conversationId: string;
  readonly epoch: number;
  readonly messageId: string;
}

/** A conversation, and the harness and workspace it mounts, which a call of the host's is of. */
export interface ConversationHarness {
  readonly conversationId: string;
  readonly harness: HarnessName;
  readonly workingDirectory: string;
}

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
  /** Since when, in milliseconds since the epoch, no turn has run in it; null while one runs. */
  readonly idleSince: number | null;
}

/** What a turn attached to again came to (HarnessHost.attach): its result; unknown to its conversation's host; or its conversation bound to no pod. */
export type Attached =
  | { readonly _tag: "Answered"; readonly result: TurnResult }
  | { readonly _tag: "Unknown" }
  | { readonly _tag: "Unbound" };

/**
 * The binding of the conversation's harness a host serves, as a pool's pod knows it from its
 * binding's token (./program.ts): the binding a turn its harness starts on its own runs on, which
 * a loss of another, as of a pod before it that lingers, never ends.
 */
export class HostBinding extends Context.Service<HostBinding, { readonly binding: string }>()("alasio/harness/HostBinding") {}

export class HarnessHost extends Context.Service<HarnessHost, {
  /**
   * Holds the conversation for a turn the control plane is about to start, until the scope
   * closes, unless it is busy, a hold of the store's among what keeps it so: whether it
   * holds it. The host held first and the store read after, as a hold is made the other way
   * round (../harness/holds.ts), so a hold and a turn begun at once do not both go on. What
   * it waits on before it can hold it, as a host elsewhere is bound to the conversation first,
   * its pod readied, it tells `waiting`, and null once it waits on it no more.
   */
  readonly reserve: (conversationId: string, waiting?: (what: string | null) => Effect.Effect<void>) => Effect.Effect<boolean, StoreError | HarnessHostError, Scope.Scope>;
  /**
   * Runs a turn to its end, its conversation busy meanwhile. It fails only as alasio's
   * store does, with TurnFenced where another turn has claimed the conversation since,
   * where no harness serves it there, or where the files its prompt names cannot be
   * written: what else goes wrong ends up in its response.
   */
  readonly startTurn: (request: TurnRequest) => Effect.Effect<TurnResult, StoreError | TurnFenced | HarnessUnavailable | ReceivedFileError>;
  /**
   * The conversation's turn claimed under `epoch` for the message `messageId`, as its host
   * started it: what it came to, once it ends, as startTurn would have answered it (Attached);
   * unknown where the host knows no such turn, as one that never ran there, or whose result it
   * let go of; or, of a host in a pool's pod, unbound where the conversation is bound to no pod,
   * its binding gone. It starts no turn, and binds nothing (../harness/pooled-host.ts): a caller
   * that lost a turn's answer, as alasio stopped while it ran, takes it so. A host keeps a turn's
   * result until the next turn of its conversation starts, or the host stops, however often it
   * is taken, so an attach whose answer was lost on its way, its connection broken, is asked again.
   */
  readonly attach: (turn: AttachedTo) => Effect.Effect<Attached, StoreError | TurnFenced | HarnessUnavailable | ReceivedFileError | HarnessHostError>;
  /**
   * Sends the conversation's running turn more guidance, the files it names written first
   * where its harness reads them: whether it took it, or none when no turn runs.
   */
  readonly steer: (conversationId: string, prompt: string, fileIds: readonly string[]) => Effect.Effect<Option.Option<boolean>, HarnessError | StoreError | ReceivedFileError>;
  /** Stops the conversation's running turn for `reason`, once it has let go: whether one ran; failing where a host elsewhere is not reached. */
  readonly interrupt: (conversationId: string, reason: StopReason) => Effect.Effect<boolean, HarnessHostError>;
  /** What the host says of the conversation; failing where it cannot say, a host elsewhere not reached. */
  readonly status: (conversationId: string) => Effect.Effect<HarnessStatus, StoreError | HarnessHostError>;
  /**
   * Whether the conversation's harness has work of its own going on between the turns
   * alasio runs (Harness.backgroundWork), which a harness pool's pod's release waits for: asked apart from
   * status, as its harness may have to be asked, so what asks whether it is busy alone asks
   * no harness.
   */
  readonly backgroundWork: (conversationId: string) => Effect.Effect<boolean, StoreError | HarnessHostError>;
  /**
   * Readies the host to be let go of, as a harness pool's pod's release does before it deletes
   * the pod, where nothing goes on in the conversation: no turn runs in it, or is about to
   * (reserved), none of its harness's own, no hold keeps it, and its harness works on nothing of
   * its own; whether it drained. Drained, no turn is started in it from then on (it reads as
   * busy, and is reserved for none), and what its harness keeps of its session beside it is
   * mirrored (Harness.mirror); not drained, as something goes on in it again, it is kept as it
   * was, and its release is the caller's to give up, as nothing it waits on waits on the
   * release. How long a mirror may take is the caller's to bound.
   */
  readonly drain: (conversationId: string) => Effect.Effect<boolean, StoreError | HarnessHostError>;
  /**
   * Gives the conversation's Codex `login`, a fresh token for alasio's relay of its login, as
   * a turn alasio drives runs on past the last token's time, which is minutes (TurnCodex): the
   * pod's host relays with it from then on (./program.ts); HarnessHost.layer itself, which holds no
   * login, takes none.
   */
  readonly renewCodexLogin: (conversationId: string, login: TurnCodex["login"]) => Effect.Effect<void, HarnessHostError>;
  /** A new, empty session of `harness` in `workingDirectory` for the conversation: its id. */
  readonly startFreshSession: (params: { readonly conversationId: string; readonly harness: HarnessName; readonly workingDirectory: string }) => Effect.Effect<string, HarnessError | HarnessUnavailable>;
  /**
   * A new session of `harness` in `workingDirectory` for the conversation, holding `sessionId`'s
   * history before its message `beforeUuid`, as a rewind makes one: its id, or none where it
   * could not be made (Harness.sessions.createForkedSession).
   */
  readonly forkSession: (params: ConversationHarness & { readonly sessionId: string; readonly beforeUuid: string }) => Effect.Effect<string | null, HarnessError | HarnessUnavailable>;
  /** The goals of `harness`'s threads in `workingDirectory`, for the conversation, where its harness has goals (Harness.goals). */
  readonly goals: (params: ConversationHarness) => HarnessGoals;
}>()("alasio/harness/HarnessHost") {
  /** A host over its harnesses, its running turns, and the files received, telling its events as HostEvents does. */
  static readonly layer: Layer.Layer<HarnessHost, never, Harnesses | ActiveTurns | Store | ReceivedFiles | HostEvents> = Layer.effect(HarnessHost, Effect.gen(function*() {
    const harnesses = yield* Harnesses;
    const activeTurns = yield* ActiveTurns;
    const store = yield* Store;
    const receivedFiles = yield* ReceivedFiles;
    const { tell } = yield* HostEvents;
    const hostBinding = Option.getOrNull(yield* Effect.serviceOption(HostBinding));
    const scope = yield* Effect.scope;
    /**
     * Each conversation's latest turn, by the epoch it was claimed under and the message it
     * answers: running in the host's scope, not its caller's, so a caller that lets go of it, as
     * alasio does as it stops, leaves it running; asked for again so, it is the one started then
     * that answers; and its result kept until the conversation's next turn starts, however often
     * one attaches to it.
     */
    const turns = new Map<string, { readonly key: string; readonly fiber: Fiber.Fiber<TurnResult, StoreError | TurnFenced | HarnessUnavailable | ReceivedFileError> }>();
    const keyOf = ({ epoch, messageId }: { readonly epoch: number; readonly messageId: string }): string => `${epoch}:${messageId}`;
    /** The conversations let go of (drain), which no turn starts in again. */
    const draining = new Set<string>();
    /** A turn of none, which holds a conversation as it is drained, while its harness is mirrored. */
    const drainTurn = { stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated: false };
    /** The conversation's mount's harness, if one serves it. */
    const harnessOf = (conversationId: string) =>
      Effect.flatMap(store.getMount(conversationId), (mount) =>
        Effect.map(harnesses.forMount(mount).pipe(Effect.orElseSucceed(() => Option.none<Harness>())), (harness) => ({ mount, harness: Option.getOrNull(harness) })));

    /** How the conversation's harness claims it for a turn it starts on its own: as alasio's turns are claimed, ending under the epoch it claimed. */
    const ownTurnsOf = (conversationId: string, chatId: string, harness: HarnessName): OwnTurns => ({
      claim: (messageId, sessionId) =>
        Effect.map(
          // On the host's own binding, as a pool's pod knows it, the one whose loss takes the turn with it.
          store.claimTurn({ conversationId, chatId, messageId, sessionId, harness, prompt: null, binding: hostBinding?.binding ?? null }),
          (epoch): OwnTurn => ({
            persistence: store.turnPersistence(conversationId, epoch),
            end: (outcome) => Effect.asVoid(store.endTurn(conversationId, epoch, outcome)),
          }),
        ),
      held: store.isConversationHeld(conversationId),
    });

    /** The turn `start` run on its harness, under its claim, its job's progress recorded. */
    const run = (start: TurnRequest) =>
      Effect.gen(function*() {
        const harness = yield* harnesses.getFor(start.harness, start.workingDirectory);
        yield* receivedFiles.materialize(start.fileIds, parseWorkspace(start.workingDirectory));
        /**
         * What the job's turn records of its progress, for a restart to know what became of
         * its prompt. The turn dies on a record it cannot make: so a prompt whose dispatch is
         * not recorded is never sent, as one whose job was run again, or whose turn ended,
         * as its alasio stopped and another took the conversation over, is not; and a restart
         * never misjudges the prompt of a turn that ran.
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
          onPromptDispatched: recordJob((id) =>
            Effect.flatMap(store.markPromptJobDispatched(id, start.epoch), (recorded) =>
              recorded ? Effect.void : Effect.die(new Error(`the job ${id} of ${start.conversationId} is run again, or its turn ended: its prompt is not sent`)))
          ),
          onTransportStarted: ({ sessionId, turnId }) => recordJob((id) => store.markPromptJobUpstreamStarted(id, sessionId, turnId)),
          onTransportCompleted: ({ sessionId, turnId }) => recordJob((id) => store.markPromptJobUpstreamCompleted(id, sessionId, turnId)),
          // A harness that keeps running between prompts (Claude Code's background work)
          // produces replies of its own and frees the conversation when they finish.
          onBackgroundResponse: tell({ kind: "background-response", conversationId: start.conversationId }),
          onIdle: tell({ kind: "idle", conversationId: start.conversationId }),
        });
      });

    return HarnessHost.of({
      reserve: (conversationId) =>
        Effect.gen(function*() {
          if (draining.has(conversationId) || !(yield* activeTurns.reserve(conversationId))) return false;
          return !(yield* store.isConversationHeld(conversationId));
        }),
      startTurn: (start) =>
        Effect.suspend(() => {
          const key = keyOf(start);
          const latest = turns.get(start.conversationId);
          if (latest?.key === key) return Fiber.join(latest.fiber);
          return Effect.forkIn(run(start), scope).pipe(
            Effect.tap((fiber) => Effect.sync(() => void turns.set(start.conversationId, { key, fiber }))),
            Effect.flatMap(Fiber.join),
          );
        }),
      attach: (turn) =>
        Effect.suspend((): Effect.Effect<Attached, StoreError | TurnFenced | HarnessUnavailable | ReceivedFileError> => {
          const kept = turns.get(turn.conversationId);
          if (kept?.key !== keyOf(turn)) return Effect.succeed({ _tag: "Unknown" });
          return Effect.map(Fiber.join(kept.fiber), (result) => ({ _tag: "Answered", result }));
        }),
      steer: (conversationId, prompt, fileIds) =>
        store.getMount(conversationId).pipe(
          Effect.flatMap(({ workingDirectory }) => receivedFiles.materialize(fileIds, parseWorkspace(workingDirectory))),
          Effect.andThen(activeTurns.steer(conversationId, prompt)),
        ),
      interrupt: (conversationId, reason) => activeTurns.stop(conversationId, reason),
      status: (conversationId) =>
        Effect.gen(function*() {
          const turn = yield* activeTurns.get(conversationId);
          const held = yield* store.isConversationHeld(conversationId);
          return {
            busy: Option.isSome(turn) || held || draining.has(conversationId),
            running: Option.isSome(turn),
            cliInitiated: Option.exists(turn, ({ cliInitiated }) => cliInitiated),
            held,
            idleSince: yield* activeTurns.idleSince(conversationId),
          };
        }),
      backgroundWork: (conversationId) =>
        Effect.gen(function*() {
          const { harness } = yield* harnessOf(conversationId);
          return harness?.backgroundWork ? yield* harness.backgroundWork(conversationId) : false;
        }),
      drain: (conversationId) =>
        Effect.gen(function*() {
          // Held from a turn while its harness is mirrored, where none runs or is about to; drained only where nothing else goes on in it either.
          const drained = yield* Effect.scoped(Effect.gen(function*() {
            if (!(yield* activeTurns.register(conversationId, drainTurn, { onlyIfIdle: true }))) return false;
            const { mount, harness } = yield* harnessOf(conversationId);
            if ((yield* store.isConversationHeld(conversationId)) || (harness?.backgroundWork && (yield* harness.backgroundWork(conversationId)))) return false;
            draining.add(conversationId);
            if (harness?.mirror && mount.sessionId) yield* harness.mirror(mount.sessionId);
            return true;
          }));
          yield* Effect.logInfo(drained ? `drained ${conversationId}: no turn is started in it again` : `${conversationId} was not drained, as something goes on in it`);
          return drained;
        }),
      renewCodexLogin: () => Effect.void,
      startFreshSession: ({ conversationId, harness, workingDirectory }) =>
        Effect.flatMap(harnesses.getFor(harness, workingDirectory), (found) => found.startFreshSession({ threadKey: conversationId, workingDirectory })),
      forkSession: ({ conversationId, harness, workingDirectory, sessionId, beforeUuid }) =>
        Effect.flatMap(harnesses.getFor(harness, workingDirectory), (found) => found.sessions.createForkedSession(sessionId, beforeUuid, { threadKey: conversationId })),
      goals: ({ harness, workingDirectory }) => {
        /** The harness's goals, failing as not reached where it has none, or none serves the workspace. */
        const goals = harnesses.getFor(harness, workingDirectory).pipe(
          Effect.mapError((error) => new HarnessHostError({ reason: error.message })),
          Effect.flatMap((found) => (found.goals ? Effect.succeed(found.goals) : Effect.fail(new HarnessHostError({ reason: `${found.displayName} has no goals` })))),
        );
        return {
          read: (params) => Effect.flatMap(goals, (each) => each.read(params)),
          set: (params) => Effect.flatMap(goals, (each) => each.set(params)),
          clear: (params) => Effect.flatMap(goals, (each) => each.clear(params)),
          waitForTurnId: (threadId, timeoutMs) => Effect.flatMap(goals, (each) => each.waitForTurnId(threadId, timeoutMs)),
        };
      },
    });
  }));
}
