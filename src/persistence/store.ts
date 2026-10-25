/**
 * alasio's state, in its Neon: the conversations and what they have mounted, the prompts
 * queued and the turns running, the responses and replies on their way to Telegram,
 * and what came from Telegram. The repositories keep
 * a table or a few each; the store is what alasio uses of them, and the few changes that
 * span them, each in one transaction.
 */
import { randomUUID } from "node:crypto";

import type { Update } from "@grammyjs/types";
import { Context, Duration, Effect, Layer, Option, Schema } from "effect";
import type { Pool } from "pg";

import { EVENTS_HANDLED_KEPT_MS } from "../events/names.ts";
import type { HarnessName } from "../harness/names.ts";
import type { MediaAttachment } from "../telegram/client.ts";
import { startBranch } from "./branch-start.ts";
import { type CallbackAction, NeonCallbackRepository, type NewCallbackAction } from "./callback-repository.ts";
import { type CodexLoginRecord, NeonCodexLoginRepository } from "./codex-login-repository.ts";
import { CurrentWorkerFence, NeonWorkerRepository, takenOver, type WorkerFence } from "./worker-repository.ts";
export { CurrentWorkerFence, WorkerTakenOver, type WorkerFence } from "./worker-repository.ts";
export type { CodexLoginRecord } from "./codex-login-repository.ts";
import {
  type Conversation,
  type ModelChoice,
  type Mount,
  NeonConversationRepository,
  type NewConversation,
  type NewModelChoice,
  type WorkspaceConversation,
} from "./conversation-repository.ts";
import { NeonEventRepository } from "./event-repository.ts";
import { type ForkAnswer, type ForkRequestAnswered, NeonForkRepository } from "./fork-repository.ts";
import { type DueOutboxEntry, NeonOutboxRepository, type NewOutboxStatus, type NewOutboxText } from "./outbox-repository.ts";
import { type KeptControl, NeonControlRepository, type NewKeptControl } from "./control-repository.ts";
import { NeonPromptJobRepository, type NewPromptJob, type PromptJob, type PromptJobState } from "./prompt-job-repository.ts";
import { type CompletedResponse, NeonResponseRepository, type ResponseBlock } from "./response-repository.ts";
import { NeonHarnessGrantRepository } from "./harness-grant-repository.ts";
import { grantCodexLogin } from "./codex-login-grants.ts";
import { grantHarness } from "./harness-grants.ts";
import { ensureSchema } from "./schema.ts";
import { type NewSessionWorkspace, NeonSessionWorkspaceRepository, type SessionWorkspace, type SessionWorkspaceNetMode } from "./session-workspace-repository.ts";
import { poolDatabase, type Sql, StoreError } from "./sql.ts";
import { NeonStateRepository } from "./state-repository.ts";
import {
  type FileContent,
  type MediaGroup,
  type MediaGroupArrival,
  NeonTelegramContentRepository,
  type NewFile,
  type NewMessage,
  type StoredFile,
  type StoredMessage,
} from "./telegram-content-repository.ts";
import { type ActiveTurn, NeonTurnRepository, type Turn, type TurnOutcome, type TurnStatus } from "./turn-repository.ts";
import { NeonUsageRepository, type SessionUsage } from "./usage-repository.ts";

/**
 * The schema alasio's state is kept in. Not alasio's role's name: a role's search path
 * starts with the schema of its name, where whatever the role makes unqualified would
 * then go.
 */
export const DEFAULT_SCHEMA = "state";

/** What a recovery prompt's message id begins with: a turn whose harness instance was lost, run again (Store.stageLostRecovery). */
export const LOST_PREFIX = "lost:";

/**
 * The conversation's active turn whose harness instance was lost under it, of `epoch` where
 * given, as a turn alasio attached to again knew it, or running on `binding` where given, as a
 * binding's loss told of it, which a turn begun since on another binding is not; and how it is
 * run again: as a prompt of `messageId`, which a lost turn's message id begins with (LOST_PREFIX),
 * and the text `prompt` gives it, unless `rerun` is false, as of a pod its own work ran out of
 * memory, which would again; and what its status message says. Its prompt, where its job's was
 * never sent to the agent, runs again as that job, not as a prompt of its own: whatever else
 * would settle the job, its worker or its recovery, settles one running alone.
 */
export interface LostRecovery {
  readonly conversationId: string;
  readonly epoch: number | null;
  readonly binding: string | null;
  readonly messageId: string;
  readonly prompt: (turn: Turn) => string;
  /** Whether the turn is run again; true unless given. */
  readonly rerun?: boolean;
  /** What the chat is told of what became of the turn: on its status message, or, where it has none, as a message of its own, but of a reply complete, which tells it. */
  readonly said: (outcome: "staged" | "requeued" | "completed" | "given-up" | "ended", turn: Turn) => string;
}

/**
 * What staging a lost turn's recovery came to, of its turn: `staged`, run again as a prompt
 * of its own; `requeued`, its job, whose prompt was never sent, run again as itself; `completed`, its response complete already, which is delivered, so it is not
 * run again; `given-up`, a recovery itself lost, let go of rather than run again; `ended`, one
 * not to be run again (`rerun` false), ended; or `none`, no such turn active, as one ended, or
 * recovered, already.
 */
export type LostRecoveryOutcome =
  | { readonly outcome: "staged" | "requeued" | "completed" | "given-up" | "ended"; readonly turn: Turn }
  | { readonly outcome: "none" };

/** What the store is made on: the pool it shares, and where in it the state is. */
export interface StoreOptions {
  readonly pool: Pool;
  /** `schema` is where the tables live: alasio's, or a test's own. */
  readonly schema?: string | undefined;
  /** The folder a new conversation is mounted on, if any. */
  readonly workingDirectory?: string | null | undefined;
  /** The branch environment alasio is, whose state is made its own as it first opens (./branch-start.ts); none for main. */
  readonly branch?: string | null | undefined;
  /** How long handling a chat's update waits for the chat's handling lock (withHandlingLock): HANDLING_LOCK_TIMEOUT unless a test says. */
  readonly handlingLockTimeout?: Duration.Input | undefined;
  /** How long a statement of a chat's handling or delivery may run (CHAT_STATEMENT_TIMEOUT unless a test says). */
  readonly chatStatementTimeout?: Duration.Input | undefined;
  /**
   * Whether it makes the schema: its tables where missing, a branch's state its own, and the
   * harness host's and the Codex login relay's grants. alasio's store does; the harness host's
   * and the relay's, of the schema alasio made, as roles that may make nothing, do none of it.
   */
  readonly makesSchema?: boolean | undefined;
  /** The harness host's role, granted what it writes once the tables are made (./harness-grants.ts); none where no harness host writes. */
  readonly harnessRole?: string | null | undefined;
  /** The Codex login relay's role, granted the login once the tables are made (./codex-login-grants.ts); none where no relay keeps it. */
  readonly codexLoginRole?: string | null | undefined;
}

/**
 * How long handling a chat's update waits for the chat's handling lock before it fails,
 * and the update is taken again: far longer than an update's handling, so only a holder
 * that is stuck makes it fail.
 */
export const HANDLING_LOCK_TIMEOUT = Duration.seconds(30);

/**
 * How long a statement of a chat's handling (under its handling lock) or of its delivery (a
 * claim of its next reply, and its marks) may run before Postgres ends it: far longer than
 * any of them takes, and less than the grace alasio's pod stops with (60 s), so one that
 * never returns, as of a compute frozen under it, holds no delivery, which alasio does not
 * cut short as it stops, past its stopping. Set on those statements' transactions alone:
 * alasio's other statements, its schema's setup and its pruning among them, are not
 * bounded by it.
 */
export const CHAT_STATEMENT_TIMEOUT = Duration.seconds(45);

/**
 * A delivery pass's claim on the outbox entry it sends (Store.claimOutboxOf): the entry, and
 * the token that pass alone holds, which its renewals and marks name.
 */
export interface OutboxClaim {
  readonly entry: DueOutboxEntry;
  readonly token: string;
}

/** What an entry delivered was: a status message with its message's id, one given up on with why. */
export interface OutboxDelivered {
  readonly telegramMessageId?: number | null;
  readonly refused?: string | null;
}

/** Each of alasio's repositories, on one connection or transaction. */
const repositories = (sql: Sql, schema: string, workingDirectory: string | null) => ({
  state: new NeonStateRepository(sql, schema),
  conversations: new NeonConversationRepository(sql, schema, workingDirectory),
  callbacks: new NeonCallbackRepository(sql, schema),
  telegramContent: new NeonTelegramContentRepository(sql, schema),
  turns: new NeonTurnRepository(sql, schema),
  responses: new NeonResponseRepository(sql, schema),
  outbox: new NeonOutboxRepository(sql, schema),
  promptJobs: new NeonPromptJobRepository(sql, schema),
  usage: new NeonUsageRepository(sql, schema),
  codexLogin: new NeonCodexLoginRepository(sql, schema),
  sessionWorkspaces: new NeonSessionWorkspaceRepository(sql, schema),
  controls: new NeonControlRepository(sql, schema),
  events: new NeonEventRepository(sql, schema),
  forks: new NeonForkRepository(sql, schema),
  harnessGrants: new NeonHarnessGrantRepository(sql, schema),
  workers: new NeonWorkerRepository(sql, schema),
});

/**
 * The lost turn's recovery, on the transaction's repositories `on`, as Store.stageLostRecovery
 * says: the turn ended first, so of what let go of it the first alone acts, and its response's
 * row locked, so no write of its pod's completes it meanwhile; a response complete already
 * delivered rather than run again; a job whose prompt was never sent run again as itself; and a
 * recovery itself lost let go of, and a turn not to be run again ended, the job of either
 * ended, so nothing runs it again; the chat told of each but a reply delivered, in the same
 * transaction, so once.
 */
const stageLostOn = (on: ReturnType<typeof repositories>, { conversationId, epoch, binding, messageId, prompt, rerun = true, said }: LostRecovery): Effect.Effect<LostRecoveryOutcome, StoreError> =>
  Effect.gen(function*() {
    const turn = yield* on.turns.getActiveTurn(conversationId);
    if (!turn || (epoch !== null && Number(turn.epoch) !== epoch) || (binding !== null && turn.binding !== binding)) return { outcome: "none" } as const;
    const completed = turn.pending_response_id === null ? false : yield* on.responses.lockCompleted(turn.pending_response_id);
    if (!(yield* on.turns.end(conversationId, Number(turn.epoch), completed ? "completed" : "stopped"))) return { outcome: "none" } as const;
    // Its job, never sent, run again as itself; a recovery's own, itself lost, given up rather than run again.
    const requeued = !completed && rerun && !turn.thread_ts.startsWith(LOST_PREFIX) && (yield* on.promptJobs.requeueUnsent(conversationId, turn.thread_ts));
    const outcome = completed ? "completed" : !rerun ? "ended" : requeued ? "requeued" : turn.thread_ts.startsWith(LOST_PREFIX) ? "given-up" : "staged";
    if (!completed && turn.pending_response_id !== null) yield* on.responses.markPendingAsPosted(turn.pending_response_id);
    // Not run again, its job is not either, as one whose prompt was never sent would be once its worker's are settled.
    if (outcome === "given-up" || outcome === "ended") yield* on.promptJobs.endRunning(conversationId, turn.thread_ts);
    if (outcome === "staged") yield* on.promptJobs.enqueue({ conversationId, chatId: turn.channel, messageId, prompt: prompt(turn), priority: 1, harness: turn.harness });
    // Said on its status message; where it has none, as a message of its own, but of a reply it completed, which says it.
    if (turn.status_outbox_id !== null) yield* on.outbox.enqueueEdit(turn.status_outbox_id, { chatId: turn.channel, text: said(outcome, turn) });
    else if (outcome !== "completed") yield* on.outbox.enqueueText({ chatId: turn.channel, text: said(outcome, turn) });
    return { outcome, turn } as const;
  });

/** The record of bot_state that says when the store was last pruned (Store pruneTransientIfDue). */
const PRUNED_KEY = "pruned";

/** What alasio does with its store; each of it fails as Neon does. */
type Stored<A> = Effect.Effect<A, StoreError>;

/**
 * What a chat's handling lock's holder marks of an update, in the lock's transaction. The
 * lock is the transaction's advisory lock, held by one at a time of all the alasios on the
 * store, and let go of however what holds it ends; so a lock lost, as its connection is,
 * rolls back the marks made under it: none outlives its lock.
 */
export interface UpdateMarks {
  /** Keeps an update, unless it is kept already: whether it is yet to be processed. */
  readonly recordTelegramUpdate: (update: Update) => Stored<boolean>;
  readonly markTelegramUpdateProcessed: (updateId: number) => Stored<void>;
  /** Queues a reply to the chat, committed with what else the update marks, so an update handled again tells the chat once. */
  readonly enqueueOutboxText: (text: NewOutboxText) => Stored<string>;
  /** Keeps a control whose work runs on past its update, held as it says, unless it is kept already (src/telegram/controls.ts). */
  readonly keepControl: (control: NewKeptControl) => Stored<void>;
  /**
   * Queues a prompt the update brings, committed with what else the update marks, so the
   * update is told handled only once its prompt is kept: the job queued, or the one already
   * queued for its message.
   */
  readonly enqueuePromptJob: (job: NewPromptJob) => Stored<PromptJob>;
}

/** What a chat's handling lock's holder marks, in the lock's transaction: an update's handling, and an album's. */
export interface HandlingMarks extends UpdateMarks {
  /**
   * Claims the chat's album to be handled, marking it handled, where it still waits and none
   * of its messages was kept within `quietMs`: whether it did. Committed with the lock's
   * transaction, so an alasio that stops as it handles the album leaves it waiting.
   */
  readonly claimMediaGroup: (mediaGroupId: string, quietMs: number) => Stored<boolean>;
}

/**
 * What an event's handling writes, in the transaction that records the event handled
 * (Store.withEventHandled), so neither is kept without the other. Other writes of its, not
 * among these, are its own to make once.
 */
export interface EventWrites {
  /** Queues a reply: its id, or that of the one already queued for its pending response. */
  readonly enqueueOutboxText: (text: NewOutboxText) => Stored<string>;
  /** Queues a notice of the conversation's turns, unless it was told it already (Store.enqueueOutboxNotice): whether it was queued now. */
  readonly enqueueOutboxNotice: (conversationId: string, notice: NewOutboxText) => Stored<boolean>;
  /** Queues a prompt: the job queued, or the one already queued for its message. */
  readonly enqueuePromptJob: (job: NewPromptJob) => Stored<PromptJob>;
  /** Records the answer this alasio's parent gave its request `request` to fork `volumeId` for it (src/branch/fork.ts), unless it was already. */
  readonly recordForkAnswer: (request: string, volumeId: string, answer: ForkAnswer) => Stored<void>;
  /** Deletes the grant of the conversation's harness pool pod where it is of `binding`, let go of (Store.issueHarnessGrant). */
  readonly revokeHarnessGrant: (conversationId: string, binding: string) => Stored<void>;
  /** Runs again the conversation's active turn whose harness instance was lost under it, once (Store.stageLostRecovery). */
  readonly stageLostRecovery: (recovery: LostRecovery) => Stored<LostRecoveryOutcome>;
}

/**
 * What names a chat's handling lock to Postgres, whose advisory locks are numbers: its text,
 * of the store's schema and the chat, which Postgres hashes to one (hashtextextended).
 */
export const handlingLockName = (schema: string, chatId: string | number): string => `${schema}:handling:${chatId}`;

/** What names the store's Codex login lock to Postgres, as handlingLockName names a chat's. */
export const codexLoginLockName = (schema: string): string => `${schema}:codex-login`;

/** Codex's login as the holder of the store's Codex login lock reads and writes it (Store.withCodexLoginLock). */
export interface CodexLoginWrites {
  /** Codex's login as last kept, with when it was kept and when its refresh was refused, or null when none is. */
  readonly get: Stored<CodexLoginRecord | null>;
  /** Keeps `auth` as Codex's login: when it was kept. */
  readonly save: (auth: string) => Stored<number>;
  /** Records the login kept at `savedAt` as refused as it was refreshed, its refresh token spent or revoked: none, where another was kept since. */
  readonly refuse: (savedAt: number) => Stored<void>;
}

/**
 * A turn's write refused, as another turn has claimed its conversation since
 * (./turn-repository.ts): a write of a turn superseded, not a failure of the store's.
 */
export class TurnFenced extends Schema.TaggedError<TurnFenced>()("TurnFenced", {
  threadKey: Schema.String,
  epoch: Schema.Number,
}) {
  override get message(): string {
    return `the turn of epoch ${this.epoch} no longer holds conversation ${this.threadKey}: another turn has claimed it since`;
  }
}

/** A hold of conversations that could not hold them all, as another holds one: its transaction rolled back, so it holds none. */
class HeldAlready extends Schema.TaggedError<HeldAlready>()("HeldAlready", {}) {}

/** A turn's write: failing as the store does, or with TurnFenced where the turn no longer holds its conversation. */
export type TurnWrite<A> = Effect.Effect<A, StoreError | TurnFenced>;

/**
 * What a turn, and its harness, write in alasio's store as it runs, all of the
 * conversation's turn it was made for: its response, its session and usage, what it
 * waits on. Each write holds only while no turn has claimed
 * the conversation since the turn did (Store.turnPersistence), and fails otherwise, with
 * TurnFenced; the turn's epoch is checked in the statement, or the transaction, that
 * writes.
 */
export interface TurnPersistence {
  /** The epoch of the turn it writes for, which TurnFenced names where it no longer holds. */
  readonly epoch: number;
  /** A response to the chat's message as the harness begins it: its id. */
  readonly createPendingResponse: (chatId: number | string, messageId: number | string, sessionId?: string | null) => TurnWrite<string>;
  /** Records the response the turn writes. */
  readonly updateActiveTurnPendingResponseId: (pendingResponseId: string | null) => TurnWrite<void>;
  readonly updatePendingSessionId: (pendingResponseId: string, sessionId: string | null) => TurnWrite<void>;
  /** Records the session the turn runs in, there and as its harness's session in the conversation. */
  readonly updateActiveTurnSessionId: (sessionId: string | null) => TurnWrite<void>;
  /** Adds blocks to a response, after those it has. */
  readonly appendBlocksToPending: (pendingResponseId: string, blocks: readonly ResponseBlock[]) => TurnWrite<void>;
  readonly markPendingResponseComplete: (pendingResponseId: string) => TurnWrite<void>;
  readonly markPendingAsPosted: (pendingResponseId: string) => TurnWrite<void>;
  readonly updateSessionUsage: (sessionId: string | null | undefined, usage: SessionUsage | null | undefined) => TurnWrite<void>;
  /** Says the turn waits on `waitingOn` (a workflow run), or works again, for null, while it works. */
  readonly setWaiting: (waitingOn: string | null) => TurnWrite<void>;
}

export class Store extends Context.Service<Store, {
  readonly getState: (key: string) => Stored<string | null>;

  /** Makes the chat's conversation, or updates who it is with: its id. */
  readonly upsertConversation: (conversation: NewConversation) => Stored<string>;
  readonly getConversation: (conversationId: string) => Stored<Conversation | null>;
  readonly getConversationByChatId: (chatId: number | string) => Stored<Conversation | null>;
  /** What the conversation has mounted; nothing, for one not yet made. */
  readonly getMount: (conversationId: string) => Stored<Mount>;
  /** The conversations that know the workspace: mounting it, or keeping a session of it parked; none for one alasio never made. */
  readonly listWorkspaceConversations: (workingDirectory: string) => Stored<WorkspaceConversation[]>;
  readonly setActiveHarness: (conversationId: string, harness: HarnessName) => Stored<void>;
  /** Mounts a folder: the sessions of the folder left are kept for it, and those kept for this one restored. */
  readonly setWorkingDirectory: (conversationId: string, workingDirectory: string) => Stored<void>;
  readonly getModelChoice: (conversationId: string, harness: HarnessName) => Stored<ModelChoice | null>;
  readonly setModelChoice: (conversationId: string, harness: HarnessName, choice: NewModelChoice) => Stored<void>;
  readonly clearModelChoice: (conversationId: string, harness: HarnessName) => Stored<void>;
  /**
   * Sets the session of the conversation's mounted harness: one it has, or one no conversation
   * has, which it has from then on, as of a session a pod made for it; another conversation's,
   * or one the stores kept before any row named it, refused, as a pod may say any session's id.
   */
  readonly setSessionId: (conversationId: string, sessionId: string | null) => Stored<void>;
  /**
   * Gives the conversation `sessionId`, whoever has it, with the Codex threads its history starts
   * in, as its operator chose it: a session of a folder's panel resumed, or a fork alasio made of
   * its own (setSessionId refuses what this alone gives).
   */
  readonly giveSession: (conversationId: string, sessionId: string) => Stored<void>;
  /** Gives the conversation `sessionId` (giveSession) and sets it as its mounted harness's session, at once. */
  readonly mountGivenSession: (conversationId: string, sessionId: string) => Stored<void>;

  /** Records a session filesystem before it is made, not yet made. */
  readonly recordSessionWorkspace: (workspace: NewSessionWorkspace) => Stored<void>;
  /** Marks a session filesystem made, with the internet it was made with. */
  readonly markSessionWorkspaceMade: (volumeId: string, netMode: SessionWorkspaceNetMode) => Stored<void>;
  /** Records, as made, a session filesystem a branch environment inherited, which its parent forked into it (../branch/fork.ts). */
  readonly recordInheritedSessionWorkspace: (volumeId: string, netMode: SessionWorkspaceNetMode) => Stored<void>;
  readonly forgetSessionWorkspace: (volumeId: string) => Stored<void>;
  /** Every session filesystem recorded, made or not, newest first. */
  readonly listSessionWorkspaces: Stored<SessionWorkspace[]>;

  /** Buttons' actions in the conversation, kept until pressed, which remember what it has mounted now: their ids, in order. */
  readonly createCallbackActions: (conversationId: string, actions: readonly NewCallbackAction[]) => Stored<string[]>;
  /** The action of a button pressed for the first time; null for one pressed before, or unknown. */
  readonly consumeCallbackAction: (id: string) => Stored<CallbackAction | null>;
  /** Whether buttons that settle the held prompt job `jobId` are kept, as asking about it keeps them until it is settled. */
  readonly hasCallbackActionsOfJob: (jobId: string) => Stored<boolean>;

  /** Keeps a control whose work runs on past its update, held as it says, unless it is kept already (src/telegram/controls.ts). */
  readonly keepControl: (control: NewKeptControl) => Stored<void>;
  /** Holds the kept control `id` for `holdMs` more, where `holder` holds it still: whether it does. */
  readonly renewControl: (id: string, holder: string, holdMs: number) => Stored<boolean>;
  /** Lets go of the kept control `id`, where `holder` holds it, for another to run again at once. */
  readonly releaseControl: (id: string, holder: string) => Stored<void>;
  /** Keeps `answer` with the kept control `id`, done, where `holder` holds it, and lets go of it, for the next look to queue its answer alone. */
  readonly pendControlAnswer: (id: string, holder: string, answer: string) => Stored<void>;
  /** Forgets the kept control `id`, done, where `holder` holds it. */
  readonly forgetControl: (id: string, holder: string) => Stored<void>;
  /** Takes the kept controls whose holds lapsed, or were let go of, held by `holder` for `holdMs`; none another takes at once. */
  readonly takeLapsedControls: (holder: string, holdMs: number) => Stored<KeptControl[]>;
  /** What a control made, kept under `key` (keepControlResult): null where nothing is. */
  readonly controlResult: (key: string) => Stored<unknown>;
  /** Keeps what a control made, `result`, under `key`, unless a result is kept there already: the one kept there now. */
  readonly keepControlResult: (key: string, result: unknown) => Stored<unknown>;

  /** Keeps an update, unless it is kept already: whether it is yet to be processed. */
  readonly recordTelegramUpdate: (update: Update) => Stored<boolean>;
  /**
   * Runs `work` holding the chat's handling lock (HandlingMarks), given the marks it makes
   * in the lock's transaction, waiting while another holds it, as an update waits its turn,
   * but for the store's handling lock timeout at most: past it, it fails as the store does,
   * without running `work`.
   */
  readonly withHandlingLock: <A, E, R>(chatId: string | number, work: (marks: HandlingMarks) => Effect.Effect<A, E, R>) => Effect.Effect<A, E | StoreError, R>;
  readonly markTelegramUpdateProcessed: (updateId: number) => Stored<void>;
  readonly insertMessage: (message: NewMessage) => Stored<string>;
  readonly insertFile: (file: NewFile) => Stored<string>;
  /** The files `ids` name, with their content. */
  readonly getFileContents: (ids: readonly string[]) => Stored<FileContent[]>;
  /** Keeps the album a message arrived in, unless it is kept already: whether it still waits, or was handled before the message came. */
  readonly upsertMediaGroup: (arrival: MediaGroupArrival) => Stored<boolean>;
  readonly getMediaGroupMessages: (mediaGroupId: string) => Stored<StoredMessage[]>;
  readonly getFilesForMessages: (messageIds: readonly string[]) => Stored<StoredFile[]>;
  /** The albums still waiting none of whose messages was kept within `quietMs`, oldest first. */
  readonly getPendingMediaGroupsDue: (quietMs: number) => Stored<MediaGroup[]>;

  /** Claims the conversation for a turn starting: the turn's epoch, which its writes hold under (turnPersistence). */
  readonly claimTurn: (turn: ActiveTurn) => Stored<number>;
  /** What the turn of `epoch`, and its harness, write as it runs, each write holding while no turn has claimed the conversation since. */
  readonly turnPersistence: (conversationId: string, epoch: number) => TurnPersistence;
  /** Says what the conversation's active turn of `epoch` is doing: whether it still holds the conversation. */
  readonly setTurnStatus: (conversationId: string, epoch: number, status: TurnStatus) => Stored<boolean>;
  /** Records the binding of the conversation's harness its turn of `epoch` runs on, as alasio drives it there: whether it still holds the conversation. */
  readonly setTurnBinding: (conversationId: string, epoch: number, binding: string) => Stored<boolean>;
  /** Records the status message the conversation's turn of `epoch` queued: whether it still holds the conversation. */
  readonly setTurnStatusMessage: (conversationId: string, epoch: number, outboxId: string) => Stored<boolean>;
  /** Ends the conversation's active turn of `epoch`, as `outcome` says it ended: whether it was that turn, and active. */
  readonly endTurn: (conversationId: string, epoch: number, outcome: TurnOutcome) => Stored<boolean>;
  readonly getActiveTurns: Stored<Turn[]>;
  readonly getActiveTurn: (conversationId: string) => Stored<Turn | null>;
  /** The conversation's latest turn, active or ended: what its harness said it did, and how it ended. */
  readonly getTurn: (conversationId: string) => Stored<Turn | null>;
  /**
   * Takes the conversation's worker's claim for `holder`, for `lifeMs` unless renewed, where no
   * other alasio's holds it or its holder's lapsed: the claim taken, which the writes of the work
   * run under it hold under (CurrentWorkerFence), or null where another's holds it.
   */
  readonly claimWorker: (conversationId: string, holder: string, lifeMs: number) => Stored<WorkerFence | null>;
  /** Keeps the claim for `lifeMs` more: whether it is still the conversation's, no other alasio having taken it since. */
  readonly renewWorker: (fence: WorkerFence, lifeMs: number) => Stored<boolean>;
  /** Lets go of the claim, for another alasio to take at once; none where another took it since. */
  readonly releaseWorker: (fence: WorkerFence) => Stored<void>;
  /** Lets go of the claims held by the processes of the pod `pod` (holders `pod/uuid`), gone as one starts in it: how many. */
  readonly lapseWorkersOf: (pod: string) => Stored<number>;
  /**
   * The conversations with work left that no worker's claim holds: an active turn alasio drives
   * (none its harness started on its own), or a prompt job to run or running.
   */
  readonly unheldConversations: Stored<string[]>;
  /**
   * Holds the conversations busy for `reason`, as no turn does (a fork), for `ttlMs` unless
   * renewed, all at once where none is held already, or none at all: the holder they are
   * held as, or null where it held none.
   */
  readonly holdConversations: (conversationIds: readonly string[], reason: string, ttlMs: number) => Stored<string | null>;
  /** Keeps the holds of `holder` (holdConversations) for `ttlMs` more: how many it still has. */
  readonly renewHolds: (holder: string, ttlMs: number) => Stored<number>;
  /** Whether the conversation is held busy, as no turn, by a hold not expired (holdConversations). */
  readonly isConversationHeld: (conversationId: string) => Stored<boolean>;
  /** Lets go of the holds of `holder`. */
  readonly releaseHolds: (holder: string) => Stored<void>;
  /**
   * Keeps `digest` as the grant of the conversation's harness pool pod (../harness/grant.ts),
   * of its binding `binding`, which the harness host's role reaches the conversation's rows by
   * from then on, until the binding is let go of (revokeHarnessGrant), whatever other binding of
   * the conversation's harness is made meanwhile.
   */
  readonly issueHarnessGrant: (conversationId: string, binding: string, digest: Buffer) => Stored<void>;

  /** Lets go of a response, as delivered or never to be. */
  readonly markPendingAsPosted: (pendingResponseId: string) => Stored<void>;
  /** Lets go of a response a turn's host answered with, of the conversation's alone, as a pod may answer with any response's id. */
  readonly markAnswerPosted: (conversationId: string, pendingResponseId: string) => Stored<void>;
  readonly getCompletedResponsesPendingDelivery: Stored<CompletedResponse[]>;

  /** Queues a reply: its id, or that of the one already queued for its pending response. */
  readonly enqueueOutboxText: (text: NewOutboxText) => Stored<string>;
  /**
   * Queues a reply with buttons, `compose` making it of their actions' ids, in order: the
   * actions kept and the reply queued in one transaction, so that neither is without the
   * other. Its id.
   */
  readonly enqueueOutboxPanel: (conversationId: string, actions: readonly NewCallbackAction[], compose: (ids: readonly string[]) => NewOutboxText) => Stored<string>;
  /**
   * Queues `notice`, a reply that tells the conversation of its turns besides their replies,
   * unless it was told it already, recording it as told in the same transaction, so that it
   * is told once, by any alasio: whether it was queued now.
   */
  readonly enqueueOutboxNotice: (conversationId: string, notice: NewOutboxText) => Stored<boolean>;
  /** Queues a turn's status message: its id, which an edit of it names. */
  readonly enqueueOutboxStatus: (status: NewOutboxStatus) => Stored<string>;
  /** Queues an edit of the status message `edits`. */
  readonly enqueueOutboxEdit: (edits: string, edit: NewOutboxStatus) => Stored<string>;
  /** What is due, each chat's next entry, claimed or not. */
  readonly getDueOutbox: (limit?: number) => Stored<DueOutboxEntry[]>;
  /** The chat's next entry, if it is due, claimed or not. */
  readonly getDueOutboxOf: (chatId: string) => Stored<DueOutboxEntry | null>;
  /**
   * Claims the chat's next entry for a delivery pass, `token` its own, if it is due and no
   * other pass's claim on it holds, for `life`: the claim, or null. A claim that lapsed, its
   * pass taken to be gone, is taken over. The claim is kept in the store, not by a
   * connection, so it holds whatever becomes of the connection it was made on, until its
   * pass marks the entry (markOutboxSent, rescheduleOutbox) or lets it lapse. Waiting for a
   * connection, and the claim, may be interrupted, nothing claimed; once claimed it is the
   * caller's.
   */
  readonly claimOutboxOf: (chatId: string, token: string, life: Duration.Input) => Stored<OutboxClaim | null>;
  /** Keeps the claim for `life` from now: whether it still held. */
  readonly renewOutboxClaim: (claim: OutboxClaim, life: Duration.Input) => Stored<boolean>;
  /** Lets go of the claim of `token`, where it holds one, leaving its entry due. */
  readonly releaseOutboxClaim: (token: string) => Stored<void>;
  /** The media a queued reply shows, kept with it until it is sent. */
  readonly getOutboxMedia: (id: string) => Stored<MediaAttachment[]>;
  /**
   * Marks the claim's entry delivered and lets go of the claim: whether it still held. One
   * that lapsed, and that another pass claimed since, marks nothing: the entry is that pass's.
   */
  readonly markOutboxSent: (claim: OutboxClaim, delivered?: OutboxDelivered) => Stored<boolean>;
  /** Defers the claim's entry by `delayMs`, saying why, and lets go of the claim: whether it still held, as markOutboxSent says. */
  readonly rescheduleOutbox: (claim: OutboxClaim, error: unknown, delayMs: number) => Stored<boolean>;
  readonly getPendingOutboxCount: Stored<number>;

  /** Queues a prompt: the job queued, or the one already queued for its message. */
  readonly enqueuePromptJob: (job: NewPromptJob) => Stored<PromptJob>;
  readonly hasOpenPromptJobs: (conversationId: string) => Stored<boolean>;
  /** Whether the conversation has prompt jobs to run. */
  readonly hasPendingPromptJobs: (conversationId: string) => Stored<boolean>;
  readonly getPromptJob: (id: string) => Stored<PromptJob | null>;
  /** Starts the conversation's next pending job: the job, or null when none waits. */
  readonly claimNextPromptJob: (conversationId: string) => Stored<PromptJob | null>;
  /** Settles what becomes of a job, its priority kept unless given. */
  readonly setPromptJobDisposition: (id: string, state: PromptJobState, priority?: number) => Stored<void>;
  /** Settles the job its worker runs, where it runs still (prompt-job-repository.ts settleRunning): whether it did. */
  readonly settleRunningPromptJob: (id: string, state: PromptJobState, priority?: number) => Stored<boolean>;
  /**
   * Settles a job held for the operator's choice, the buttons that would settle it
   * forgotten: the job, or null where it is held no longer, when nothing changes.
   */
  readonly settleHeldPromptJob: (id: string, state: PromptJobState, priority?: number) => Stored<PromptJob | null>;
  /** The conversation's jobs held for the operator's choice, in the order queued. */
  readonly listHeldPromptJobs: (conversationId: string) => Stored<PromptJob[]>;
  /** Records the job's prompt sent, by its turn of `epoch` (prompt-job-repository.ts markDispatched): whether it was, so it may be sent. */
  readonly markPromptJobDispatched: (id: string, epoch: number) => Stored<boolean>;
  readonly markPromptJobUpstreamStarted: (id: string, sessionId: string | null | undefined, turnId: string | null | undefined) => Stored<void>;
  readonly markPromptJobUpstreamCompleted: (id: string, sessionId: string | null | undefined, turnId: string | null | undefined) => Stored<void>;
  /** Fails the job its worker runs, where it runs still (prompt-job-repository.ts fail): whether it did. */
  readonly failPromptJob: (id: string, error: unknown) => Stored<boolean>;
  /** The conversations with prompt jobs waiting: to run, or for the operator's choice. */
  readonly listWaitingPromptConversations: Stored<string[]>;
  /** Settles the conversation's jobs its last worker left running (prompt-job-repository.ts recoverLeft): whether the agent finished one's turn. */
  readonly recoverLeftPromptJobs: (conversationId: string) => Stored<boolean>;
  /** Settles the job its last worker left running of the conversation's turn that started at `turnStarted`, attached to again since (prompt-job-repository.ts recoverLeftOf). */
  readonly recoverLeftPromptJobOf: (conversationId: string, turnStarted: Date) => Stored<void>;
  /** Holds again the conversation's jobs its last worker left handed to a turn to steer it (prompt-job-repository.ts holdUnsteeredLeft). */
  readonly holdUnsteeredLeftPromptJobs: (conversationId: string) => Stored<PromptJob[]>;

  /**
   * Runs again the conversation's active turn whose harness instance was lost under it, as
   * `recovery` says, once, in one transaction: the turn its row's state guards, ended, as
   * whatever else let go of it first, its reply delivered or its recovery staged, ended it.
   */
  readonly stageLostRecovery: (recovery: LostRecovery) => Stored<LostRecoveryOutcome>;

  readonly getSessionTokens: (sessionId: string) => Stored<number>;

  /** The text of Codex's auth.json as last kept, or null when none is. */
  readonly getCodexLogin: Stored<string | null>;
  /** Keeps `auth` as Codex's login; null, Codex logged out, keeps none. */
  readonly setCodexLogin: (auth: string | null) => Stored<void>;
  /** Codex's login as last kept, with when it was kept and when its refresh was refused, by the store's own clock, or null when none is. */
  readonly getCodexLoginRecord: Stored<CodexLoginRecord | null>;
  /**
   * Runs `work` holding the store's Codex login lock, given the login as kept and its writes,
   * in one transaction: of the alasios on the store, one at a time, each waiting for the one
   * before, so a login whose refresh token works once is refreshed by one alone, and the next
   * reads what it kept (../codex/login-refresh.ts).
   */
  readonly withCodexLoginLock: <A, E, R>(work: (login: CodexLoginWrites) => Effect.Effect<A, E, R>) => Effect.Effect<A, E | StoreError, R>;

  /**
   * Runs `work` as the handling of the environment's event `id`, of `kind`, given what it
   * writes, in one transaction with the event's record as handled: what `work` gave, or none,
   * without running it, where the event was handled already, by this alasio or another, as one
   * delivered again is. `work` failing rolls back its writes and the record together, so the
   * event is handled again when it is delivered again.
   */
  readonly withEventHandled: <A, E, R>(id: string, kind: string, work: (writes: EventWrites) => Effect.Effect<A, E, R>) => Effect.Effect<Option.Option<A>, E | StoreError, R>;

  /** The session its branch `branch`'s fork request `id` asked this alasio, as a parent, for, and how it answered it; null where it has yet to. */
  readonly forkRequestAnswered: (branch: string, id: string) => Stored<ForkRequestAnswered | null>;
  /** Records `branch`'s fork request `id` for `volumeId`, answered as `answer`, unless it was answered already: what it holds, which is told again. */
  readonly recordForkRequest: (branch: string, id: string, volumeId: string, answer: ForkAnswer) => Stored<ForkRequestAnswered>;
  /** The answer this alasio's parent gave its own fork request `request`, or null where none has come yet. */
  readonly forkAnswerTo: (request: string) => Stored<ForkAnswer | null>;

  /**
   * Deletes what is only kept while it is in flight, `age` after it landed: updates
   * processed, albums handled, replies delivered, and forks' requests and answers; and the
   * events handled once their topic can hold them no more, EVENTS_HANDLED_KEPT_MS after,
   * however young `age` is (../events/names.ts).
   */
  readonly pruneTransient: (age: Duration.Input) => Stored<void>;
  /**
   * Prunes as pruneTransient does where the store was last pruned more than `every` ago, by
   * any alasio on it: the day claimed first, in one statement, so of the alasios that look at
   * once one alone prunes; whether this one did. An alasio that starts often, as one that
   * sleeps does, so prunes no more often than `every`; a prune that fails, its day claimed,
   * waits for the next.
   */
  readonly pruneTransientIfDue: (age: Duration.Input, every: Duration.Input) => Stored<boolean>;
}>()("alasio/persistence/Store") {
  /** The store in `schema` of the pool's database, made first as `makesSchema` says. */
  static readonly layer = (options: StoreOptions): Layer.Layer<Store, StoreError> => Layer.effect(Store, makeStore(options));
}

const makeStore = Effect.fnUntraced(function*({
  pool,
  schema = DEFAULT_SCHEMA,
  workingDirectory = null,
  branch = null,
  handlingLockTimeout = HANDLING_LOCK_TIMEOUT,
  chatStatementTimeout = CHAT_STATEMENT_TIMEOUT,
  makesSchema = true,
  harnessRole = null,
  codexLoginRole = null,
}: StoreOptions) {
  const database = poolDatabase(pool);
  /** Bounds the statements of a chat's handling or delivery transaction `sql` (CHAT_STATEMENT_TIMEOUT), for that transaction alone. */
  const bounded = (sql: Sql): Effect.Effect<void, StoreError> =>
    Effect.asVoid(sql.query("select set_config('statement_timeout', $1, true)", [`${Duration.toMillis(chatStatementTimeout)}ms`]));
  /** `work`'s statements of a chat's delivery, in a transaction of their own, bounded. */
  const delivering = <A>(work: (outbox: NeonOutboxRepository) => Stored<A>): Stored<A> =>
    database.transaction((sql) => bounded(sql).pipe(Effect.andThen(work(repositoriesOn(sql).outbox))));
  if (makesSchema) {
    yield* ensureSchema(database, schema);
    if (harnessRole) yield* grantHarness(database, schema, harnessRole);
    if (codexLoginRole) yield* grantCodexLogin(database, schema, codexLoginRole);
    if (branch && (yield* startBranch(database, schema, branch))) {
      yield* Effect.logInfo(`made the state the branch ${branch}'s: what was in flight where it was branched from is gone`);
    }
  }
  /** The repositories on `sql`: a transaction's. */
  const repositoriesOn = (sql: Sql) => repositories(sql, schema, workingDirectory);
  /** `notice` queued to the conversation, recorded as told, on the transaction's repositories `on`, unless it was told already: whether it was queued now. */
  const noticeOn = (on: ReturnType<typeof repositoriesOn>, conversationId: string, notice: NewOutboxText): Stored<boolean> =>
    Effect.flatMap(on.outbox.recordNotice(conversationId, notice.text), (fresh) => (fresh ? Effect.as(on.outbox.enqueueText(notice), true) : Effect.succeed(false)));
  /**
   * `work`, on the transaction's repositories `on`, where the current worker's claim
   * (CurrentWorkerFence) still holds its conversation, its row locked for share so a takeover waits
   * for the transaction to end: failing with WorkerTakenOver where it does not. As it is, where no
   * worker's work runs.
   */
  const fencedOn = <A, E>(on: ReturnType<typeof repositoriesOn>, work: Effect.Effect<A, E>): Effect.Effect<A, E | StoreError> =>
    CurrentWorkerFence.use((fence): Effect.Effect<A, E | StoreError> =>
      fence === null ? work : Effect.flatMap(on.workers.holds(fence), (holds): Effect.Effect<A, E | StoreError> => (holds ? work : Effect.fail(takenOver(fence)))));
  /** `write`'s statements, on the repositories of a transaction of their own, under the current worker's claim (fencedOn). */
  const transacting = <A, E>(write: (on: ReturnType<typeof repositoriesOn>) => Effect.Effect<A, E>): Effect.Effect<A, E | StoreError> =>
    database.transaction((sql) => {
      const on = repositoriesOn(sql);
      return fencedOn(on, write(on));
    });
  /**
   * A write of what a worker drives, `write` on the repositories: in a transaction of its own
   * under the current worker's claim where one runs (transacting); as it is, on any connection,
   * where none does.
   */
  const fenced = <A, E>(write: (on: ReturnType<typeof repositoriesOn>) => Effect.Effect<A, E>): Effect.Effect<A, E | StoreError> =>
    CurrentWorkerFence.use((fence): Effect.Effect<A, E | StoreError> => (fence === null ? write(repositoriesOn(database)) : transacting(write)));
  const { state, conversations, callbacks, telegramContent, turns, responses, outbox, promptJobs, usage, codexLogin, sessionWorkspaces, controls, events, forks, workers } = repositoriesOn(database);

  const pruneTransient = (age: Duration.Input): Stored<void> => {
    const ageMs = Duration.toMillis(age);
    return Effect.all([telegramContent.pruneHandled(ageMs), outbox.pruneSent(ageMs), events.pruneHandled(EVENTS_HANDLED_KEPT_MS), forks.prune(ageMs), controls.pruneResults(ageMs)], { discard: true });
  };

  return Store.of({
    getState: (key) => state.getState(key),

    upsertConversation: (conversation) => conversations.upsertConversation(conversation),
    getConversation: (conversationId) => conversations.getConversation(conversationId),
    getConversationByChatId: (chatId) => conversations.getConversationByChatId(chatId),
    getMount: (conversationId) => conversations.getMount(conversationId),
    listWorkspaceConversations: (workingDirectory) => conversations.listWorkspaceConversations(workingDirectory),
    setActiveHarness: (conversationId, harness) => conversations.setActiveHarness(conversationId, harness),
    setWorkingDirectory: (conversationId, workingDirectory) => conversations.setWorkingDirectory(conversationId, workingDirectory),
    getModelChoice: (conversationId, harness) => conversations.getModelChoice(conversationId, harness),
    setModelChoice: (conversationId, harness, choice) => conversations.setModelChoice(conversationId, harness, choice),
    clearModelChoice: (conversationId, harness) => conversations.clearModelChoice(conversationId, harness),
    setSessionId: (conversationId, sessionId) => fenced((on) => on.conversations.setSessionId(conversationId, sessionId)),
    giveSession: (conversationId, sessionId) => fenced((on) => on.conversations.giveSession(conversationId, sessionId)),
    mountGivenSession: (conversationId, sessionId) =>
      transacting((on) => Effect.andThen(on.conversations.giveSession(conversationId, sessionId), on.conversations.setSessionId(conversationId, sessionId))),

    recordSessionWorkspace: (workspace) => sessionWorkspaces.record(workspace),
    markSessionWorkspaceMade: (volumeId, netMode) => sessionWorkspaces.markMade(volumeId, netMode),
    recordInheritedSessionWorkspace: (volumeId, netMode) => sessionWorkspaces.recordInherited(volumeId, netMode),
    forgetSessionWorkspace: (volumeId) => sessionWorkspaces.forget(volumeId),
    listSessionWorkspaces: sessionWorkspaces.list(),

    createCallbackActions: (conversationId, actions) => fenced((on) => on.callbacks.createCallbackActions(conversationId, actions)),
    consumeCallbackAction: (id) => callbacks.consumeCallbackAction(id),
    hasCallbackActionsOfJob: (jobId) => callbacks.hasActionsOfJob(jobId),

    keepControl: (control) => controls.keep(control),
    renewControl: (id, holder, holdMs) => controls.renew(id, holder, holdMs),
    releaseControl: (id, holder) => controls.release(id, holder),
    pendControlAnswer: (id, holder, answer) => controls.pendAnswer(id, holder, answer),
    forgetControl: (id, holder) => controls.forget(id, holder),
    takeLapsedControls: (holder, holdMs) => controls.takeLapsed(holder, holdMs),
    controlResult: (key) => controls.resultOf(key),
    keepControlResult: (key, result) => controls.keepResult(key, result),

    recordTelegramUpdate: (update) => telegramContent.recordTelegramUpdate(update),
    withHandlingLock: (chatId, work) =>
      database.transaction((sql) =>
        // Past the timeout, Postgres fails the wait (lock_not_available), and the transaction with it.
        bounded(sql).pipe(
          Effect.andThen(sql.query("select set_config('lock_timeout', $1, true)", [`${Duration.toMillis(handlingLockTimeout)}ms`])),
          Effect.andThen(sql.query("select pg_advisory_xact_lock(hashtextextended($1, 0))", [handlingLockName(schema, chatId)])),
          Effect.andThen(Effect.suspend(() => {
            const { telegramContent: locked, outbox: lockedOutbox, controls: lockedControls, promptJobs: lockedPromptJobs } = repositoriesOn(sql);
            return work({
              recordTelegramUpdate: (update) => locked.recordTelegramUpdate(update),
              markTelegramUpdateProcessed: (updateId) => locked.markTelegramUpdateProcessed(updateId),
              keepControl: (control) => lockedControls.keep(control),
              enqueuePromptJob: (job) => lockedPromptJobs.enqueue(job),
              claimMediaGroup: (mediaGroupId, quietMs) => locked.claimMediaGroup(mediaGroupId, quietMs),
              enqueueOutboxText: (text) => lockedOutbox.enqueueText(text),
            });
          })),
        )
      ),
    markTelegramUpdateProcessed: (updateId) => telegramContent.markTelegramUpdateProcessed(updateId),
    insertMessage: (message) => telegramContent.insertMessage(message),
    insertFile: (file) => telegramContent.insertFile(file),
    getFileContents: (ids) => telegramContent.getFileContents(ids),
    upsertMediaGroup: (arrival) => telegramContent.upsertMediaGroup(arrival),
    getMediaGroupMessages: (mediaGroupId) => telegramContent.getMediaGroupMessages(mediaGroupId),
    getFilesForMessages: (messageIds) => telegramContent.getFilesForMessages(messageIds),
    getPendingMediaGroupsDue: (quietMs) => telegramContent.getPendingMediaGroupsDue(quietMs),

    claimTurn: (turn) => fenced((on) => on.turns.claim(turn)),
    turnPersistence: (conversationId, epoch) => {
      const superseded = () => new TurnFenced({ threadKey: conversationId, epoch });
      /** `write`, failing with TurnFenced where it did not hold. */
      const held = (write: (on: ReturnType<typeof repositoriesOn>) => Stored<boolean>): TurnWrite<void> =>
        Effect.flatMap(fenced(write), (holds) => (holds ? Effect.void : Effect.fail(superseded())));
      /**
       * `write`, of a table that names no turn, in one transaction with the turn's row
       * locked for share once it is seen to hold the conversation.
       */
      const holding = (write: (repositories: ReturnType<typeof repositoriesOn>) => Stored<void>): TurnWrite<void> =>
        transacting((on) => Effect.flatMap(on.turns.lockAt(conversationId, epoch), (holds): TurnWrite<void> => (holds ? write(on) : Effect.fail(superseded()))));
      return {
        epoch,
        createPendingResponse: (chatId, messageId, sessionId) =>
          Effect.flatMap(fenced((on) => on.responses.createPendingResponse(conversationId, epoch, chatId, messageId, sessionId)), (id) => (id === null ? Effect.fail(superseded()) : Effect.succeed(id))),
        updateActiveTurnPendingResponseId: (pendingResponseId) => held((on) => on.turns.updatePendingResponseIdAt(conversationId, epoch, pendingResponseId)),
        updatePendingSessionId: (pendingResponseId, sessionId) => held((on) => on.responses.updatePendingSessionId(pendingResponseId, sessionId)),
        updateActiveTurnSessionId: (sessionId) => held((on) => on.turns.updateSessionIdAt(conversationId, epoch, sessionId)),
        appendBlocksToPending: (pendingResponseId, blocks) => held((on) => on.responses.appendBlocks(pendingResponseId, blocks)),
        markPendingResponseComplete: (pendingResponseId) => held((on) => on.responses.markPendingComplete(pendingResponseId)),
        markPendingAsPosted: (pendingResponseId) => held((on) => on.responses.markPendingAsPosted(pendingResponseId, true)),
        updateSessionUsage: (sessionId, sessionUsage) => holding((on) => on.usage.updateSessionUsage(sessionId, sessionUsage)),
        setWaiting: (waitingOn) => held((on) => on.turns.setWaitingAt(conversationId, epoch, waitingOn)),
      };
    },
    setTurnStatus: (conversationId, epoch, status) => fenced((on) => on.turns.setStatus(conversationId, epoch, status)),
    setTurnBinding: (conversationId, epoch, binding) => fenced((on) => on.turns.setBindingAt(conversationId, epoch, binding)),
    setTurnStatusMessage: (conversationId, epoch, outboxId) => fenced((on) => on.turns.setStatusMessageAt(conversationId, epoch, outboxId)),
    endTurn: (conversationId, epoch, outcome) => fenced((on) => on.turns.end(conversationId, epoch, outcome)),
    getActiveTurns: turns.getActiveTurns(),
    getActiveTurn: (conversationId) => turns.getActiveTurn(conversationId),
    getTurn: (conversationId) => turns.getTurn(conversationId),
    claimWorker: (conversationId, holder, lifeMs) =>
      Effect.map(workers.take(conversationId, holder, lifeMs), (generation) => (generation === null ? null : { conversationId, generation })),
    renewWorker: (fence, lifeMs) => workers.renew(fence, lifeMs),
    releaseWorker: (fence) => workers.release(fence),
    lapseWorkersOf: (pod) => workers.lapseOf(pod),
    unheldConversations: workers.unheld(),
    holdConversations: (conversationIds, reason, ttlMs) => {
      const holder = randomUUID();
      // One that made fewer than all, as another held one of them, or held it as this ran, keeps none.
      return database.transaction((sql) =>
        Effect.flatMap(repositoriesOn(sql).turns.holdEach(conversationIds, reason, holder, ttlMs), (made) =>
          made === new Set(conversationIds).size ? Effect.succeed(holder) : Effect.fail(new HeldAlready()))
      ).pipe(Effect.catchTag("HeldAlready", () => Effect.succeed(null)));
    },
    renewHolds: (holder, ttlMs) => turns.renew(holder, ttlMs),
    releaseHolds: (holder) => turns.release(holder),
    issueHarnessGrant: (conversationId, binding, digest) => fenced((on) => on.harnessGrants.issue(conversationId, binding, digest)),
    isConversationHeld: (conversationId) => turns.isHeld(conversationId),

    markPendingAsPosted: (pendingResponseId) => Effect.asVoid(fenced((on) => on.responses.markPendingAsPosted(pendingResponseId))),
    markAnswerPosted: (conversationId, pendingResponseId) => Effect.asVoid(fenced((on) => on.responses.markPendingAsPosted(pendingResponseId, false, conversationId))),
    getCompletedResponsesPendingDelivery: responses.getCompletedResponsesPendingDelivery(),

    enqueueOutboxText: (text) => fenced((on) => on.outbox.enqueueText(text)),
    enqueueOutboxPanel: (conversationId, actions, compose) =>
      transacting((on) => Effect.flatMap(on.callbacks.createCallbackActions(conversationId, actions), (ids) => on.outbox.enqueueText(compose(ids)))),
    enqueueOutboxNotice: (conversationId, notice) => transacting((on) => noticeOn(on, conversationId, notice)),
    enqueueOutboxStatus: (status) => fenced((on) => on.outbox.enqueueStatus(status)),
    enqueueOutboxEdit: (edits, edit) => fenced((on) => on.outbox.enqueueEdit(edits, edit)),
    getDueOutbox: (limit) => outbox.getDue(limit),
    getDueOutboxOf: (chatId) => outbox.getDueOf(chatId),
    claimOutboxOf: (chatId, token, life) =>
      database.interruptibleTransaction((sql) =>
        bounded(sql).pipe(
          Effect.andThen(repositoriesOn(sql).outbox.claimNextOf(chatId, token, Duration.toMillis(life))),
          Effect.map((entry) => (entry === null ? null : { entry, token })),
        )
      ),
    renewOutboxClaim: ({ entry, token }, life) => delivering((on) => on.renewClaim(entry.id, token, Duration.toMillis(life))),
    releaseOutboxClaim: (token) => delivering((on) => on.releaseClaim(token)),
    getOutboxMedia: (id) => outbox.getMedia(id),
    // The entry's row locked before its claim's, as a claim of it locks them.
    markOutboxSent: ({ entry, token }, delivered) => delivering((on) => Effect.andThen(on.lock(entry.id), on.markSent(entry.id, token, delivered))),
    rescheduleOutbox: ({ entry, token }, error, delayMs) => delivering((on) => Effect.andThen(on.lock(entry.id), on.reschedule(entry.id, token, error, delayMs))),
    getPendingOutboxCount: outbox.getPendingCount(),

    enqueuePromptJob: (job) => fenced((on) => on.promptJobs.enqueue(job)),
    hasOpenPromptJobs: (conversationId) => promptJobs.hasOpenJobs(conversationId),
    hasPendingPromptJobs: (conversationId) => promptJobs.hasPendingJobs(conversationId),
    getPromptJob: (id) => promptJobs.get(id),
    claimNextPromptJob: (conversationId) => fenced((on) => on.promptJobs.claimNext(conversationId)),
    setPromptJobDisposition: (id, state, priority) => fenced((on) => on.promptJobs.setDisposition(id, state, priority)),
    settleRunningPromptJob: (id, state, priority) => fenced((on) => on.promptJobs.settleRunning(id, state, priority)),
    listHeldPromptJobs: (conversationId) => promptJobs.listHeld(conversationId),
    settleHeldPromptJob: (id, state, priority) =>
      transacting((on) => Effect.tap(on.promptJobs.settleHeld(id, state, priority), (job) => (job ? on.callbacks.forgetActionsOfJob(id) : Effect.void))),
    markPromptJobDispatched: (id, epoch) => fenced((on) => on.promptJobs.markDispatched(id, epoch)),
    markPromptJobUpstreamStarted: (id, sessionId, turnId) => fenced((on) => on.promptJobs.markUpstreamStarted(id, sessionId, turnId)),
    markPromptJobUpstreamCompleted: (id, sessionId, turnId) => fenced((on) => on.promptJobs.markUpstreamCompleted(id, sessionId, turnId)),
    failPromptJob: (id, error) => fenced((on) => on.promptJobs.fail(id, error)),
    listWaitingPromptConversations: promptJobs.listWaitingConversations(),
    recoverLeftPromptJobs: (conversationId) => fenced((on) => on.promptJobs.recoverLeft(conversationId)),
    recoverLeftPromptJobOf: (conversationId, turnStarted) => fenced((on) => on.promptJobs.recoverLeftOf(conversationId, turnStarted)),
    holdUnsteeredLeftPromptJobs: (conversationId) => fenced((on) => on.promptJobs.holdUnsteeredLeft(conversationId)),

    stageLostRecovery: (recovery) => transacting((on) => stageLostOn(on, recovery)),

    getSessionTokens: (sessionId) => usage.getSessionTokens(sessionId),

    getCodexLogin: codexLogin.getCodexLogin(),
    setCodexLogin: (auth) => codexLogin.setCodexLogin(auth),
    getCodexLoginRecord: codexLogin.getCodexLoginRecord(),
    withCodexLoginLock: (work) =>
      database.transaction((sql) => {
        const login = repositoriesOn(sql).codexLogin;
        return sql.query("select pg_advisory_xact_lock(hashtextextended($1, 0))", [codexLoginLockName(schema)]).pipe(
          Effect.andThen(work({ get: login.getCodexLoginRecord(), save: (auth) => login.saveCodexLogin(auth), refuse: (savedAt) => login.refuseCodexLogin(savedAt) })),
        );
      }),

    withEventHandled: (id, kind, work) =>
      database.transaction((sql) => {
        const on = repositoriesOn(sql);
        return Effect.flatMap(on.events.recordHandled(id, kind), (fresh) =>
          fresh
            ? Effect.map(
              work({
                enqueueOutboxText: (text) => on.outbox.enqueueText(text),
                enqueueOutboxNotice: (conversationId, notice) => noticeOn(on, conversationId, notice),
                enqueuePromptJob: (job) => on.promptJobs.enqueue(job),
                recordForkAnswer: (request, volumeId, answer) => on.forks.recordAnswer(request, volumeId, answer),
                revokeHarnessGrant: (conversationId, binding) => on.harnessGrants.revoke(conversationId, binding),
                stageLostRecovery: (recovery) => stageLostOn(on, recovery),
              }),
              Option.some,
            )
            : Effect.succeed(Option.none()));
      }),
    forkRequestAnswered: (branch, id) => forks.requestAnswered(branch, id),
    recordForkRequest: (branch, id, volumeId, answer) => forks.recordRequest(branch, id, volumeId, answer),
    forkAnswerTo: (request) => forks.answerTo(request),
    pruneTransient,
    pruneTransientIfDue: (age, every) =>
      Effect.flatMap(state.claimUnlessWithin(PRUNED_KEY, "pruned", Duration.toMillis(every)), (claimed) => (claimed ? Effect.as(pruneTransient(age), true) : Effect.succeed(false))),
  });
});
