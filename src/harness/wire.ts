/**
 * The harness host's interface as it crosses a process (./host.ts HarnessHost): the
 * paths `alasio harness` serves it on (./serve.ts), and what each takes and answers, as
 * JSON. A call answers once, but for two that last, a line of JSON each, begun at once
 * and kept alive by a line that says nothing else (Alive) at least every KEEP_ALIVE_MS, so
 * that neither the caller's client nor a proxy between gives up on one that is quiet: a
 * turn, answered once it ends; and holding a conversation (reserve), answered once it is
 * held and held until its caller lets go of it (release), or of the connection, as a caller
 * that is gone does. The host's events do not cross here: it tells them as its HostEvents
 * says (./host-events.ts), on the log. What fails is answered as the error it is, by its tag and what it
 * carries, so that the caller fails with the same error.
 */
import { Effect, Schema } from "effect";

import { StoreError } from "../persistence/sql.ts";
import { TurnFenced } from "../persistence/store.ts";
import { SessionFilesystemsDisabled } from "../sandbox/index.ts";
import { ReceivedFileError } from "../telegram/files.ts";
import type { TurnStart } from "./host.ts";
import { FolderOnBranch, type HarnessUnavailable, NoWorkspaceMounted, type TurnResult, UnknownHarness } from "./index.ts";
import { HARNESS_NAMES } from "./names.ts";

/** Where each of the host's calls is served, all of them POSTs. */
export const HOST_PATHS = {
  reserve: "/reserve",
  release: "/release",
  turns: "/turns",
  steer: "/steer",
  interrupt: "/interrupt",
  status: "/status",
  backgroundWork: "/background-work",
  drain: "/drain",
  freshSession: "/fresh-session",
} as const;

/** What names the conversation a call is about, which a host serving another refuses. */
const Conversation = Schema.Struct({ conversationId: Schema.String });

export const ReserveRequest = Conversation;
/**
 * The first line of a reservation's answer: whether the host holds the conversation, and
 * the hold's id to release it by, which it holds until then, or until the connection ends.
 */
export const Reserved = Schema.Struct({ held: Schema.Boolean, id: Schema.NullOr(Schema.String) });
/** A hold to let go of, which the answer comes once it has. */
export const ReleaseRequest = Schema.Struct({ conversationId: Schema.String, id: Schema.String });
/** Whether the host held it, until now. */
export const ReleaseAnswer = Schema.Struct({ released: Schema.Boolean });

const Harness = Schema.Literals(HARNESS_NAMES);

export const TurnStartRequest = Schema.Struct({
  conversationId: Schema.String,
  chatId: Schema.String,
  messageId: Schema.String,
  harness: Harness,
  workingDirectory: Schema.String,
  prompt: Schema.String,
  fileIds: Schema.Array(Schema.String),
  resumeSession: Schema.NullOr(Schema.String),
  modelChoice: Schema.NullOr(Schema.Struct({ model: Schema.String, effort: Schema.NullOr(Schema.String) })),
  epoch: Schema.Number,
  jobId: Schema.NullOr(Schema.String),
  attachedTurn: Schema.NullOr(Schema.Struct({ sessionId: Schema.String, turnId: Schema.String })),
  bayma: Schema.NullOr(Schema.Union([
    Schema.Struct({ endpoint: Schema.Struct({ url: Schema.String, headers: Schema.Record(Schema.String, Schema.String) }), notice: Schema.Null }),
    Schema.Struct({ endpoint: Schema.Null, notice: Schema.String }),
  ])),
});
// What the request carries is what the host's start is, field for field.
export const turnStartOf = (request: typeof TurnStartRequest.Type): TurnStart => request;

/**
 * A block of a response, as the projections make them (../codex/event-projection.ts). A
 * question's fields are the tool's own, beyond its header, kept as they come.
 */
const Block = Schema.Union([
  Schema.Struct({ type: Schema.Literal("text"), content: Schema.String, phase: Schema.optionalKey(Schema.NullOr(Schema.Literals(["commentary", "final_answer"]))) }),
  Schema.Struct({ type: Schema.Literal("tool"), name: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("ask_user_question"),
    tool_use_id: Schema.String,
    questions: Schema.Array(Schema.StructWithRest(Schema.Struct({ header: Schema.optionalKey(Schema.String) }), [Schema.Record(Schema.String, Schema.Unknown)])),
    summary: Schema.String,
  }),
]);

export const TurnAnswer = Schema.Struct({
  blockSequence: Schema.Array(Block),
  sessionId: Schema.NullOr(Schema.String),
  pendingResponseId: Schema.String,
  interrupted: Schema.Boolean,
  responseCompleted: Schema.Boolean,
  notice: Schema.NullOr(Schema.String),
});

/** A turn's result as it crosses: what the harness did not say of its session, or had no notice of, is said to be none. */
export const encodeTurnAnswer = (result: TurnResult): typeof TurnAnswer.Encoded =>
  Schema.encodeSync(TurnAnswer)({ ...result, sessionId: result.sessionId ?? null, notice: result.notice ?? null });
/** The turn's result an answer carries. */
export const decodeTurnAnswer = (body: unknown) => Schema.decodeUnknownEffect(TurnAnswer)(body).pipe(Effect.map((answer): TurnResult => ({ ...answer, blockSequence: [...answer.blockSequence] })));

export const SteerRequest = Schema.Struct({ conversationId: Schema.String, prompt: Schema.String, fileIds: Schema.Array(Schema.String) });
/** Whether the running turn took the guidance; null where no turn runs. */
export const SteerAnswer = Schema.Struct({ taken: Schema.NullOr(Schema.Boolean) });

export const InterruptRequest = Schema.Struct({ conversationId: Schema.String, reason: Schema.Literals(["interrupt", "swerve"]) });
export const InterruptAnswer = Schema.Struct({ interrupted: Schema.Boolean });

export const StatusRequest = Conversation;
export const StatusAnswer = Schema.Struct({
  busy: Schema.Boolean,
  running: Schema.Boolean,
  cliInitiated: Schema.Boolean,
  held: Schema.Boolean,
  idleSince: Schema.NullOr(Schema.Number),
});

export const DrainRequest = Conversation;
export const DrainAnswer = Schema.Struct({ drained: Schema.Boolean });

export const BackgroundWorkRequest = Conversation;
export const BackgroundWorkAnswer = Schema.Struct({ working: Schema.Boolean });

export const FreshSessionRequest = Schema.Struct({ conversationId: Schema.String, harness: Harness, workingDirectory: Schema.String });
export const FreshSessionAnswer = Schema.Struct({ sessionId: Schema.String });

/**
 * How often, at most, a call that lasts says it does: well under the five minutes Node's
 * fetch waits for an answer's headers, or its next bytes, before it gives up on it.
 */
export const KEEP_ALIVE_MS = 30_000;

/** A line of an answer that lasts which says only that it does, which the caller skips. */
export const Alive = Schema.Struct({ alive: Schema.Literal(true) });

/**
 * What a call failed with, as the host says it: an error of its own by its tag, with what
 * it carries; or one the host knows only by its message (a harness's own, or the
 * request's), with the tag of what it was.
 */
export const Failure = Schema.Struct({
  error: Schema.Struct({
    _tag: Schema.String,
    message: Schema.String,
    threadKey: Schema.optionalKey(Schema.String),
    epoch: Schema.optionalKey(Schema.Number),
    name: Schema.optionalKey(Schema.String),
    branch: Schema.optionalKey(Schema.String),
  }),
});

/** The last line of a turn's answer but where it failed (Failure): its result, as TurnAnswer encodes it. */
export const TurnAnswered = Schema.Struct({ answer: Schema.Unknown });

/** The errors of the host's that cross whole, as their tags name them. */
export type CrossingError = StoreError | TurnFenced | HarnessUnavailable | ReceivedFileError;

/** `error` as an answer says it: by its tag and message, with what an error of the host's that crosses whole carries. */
export function failureOf(error: { readonly _tag: string; readonly message: string }): typeof Failure.Encoded {
  const said = { _tag: error._tag, message: error.message };
  if (error instanceof TurnFenced) return { error: { ...said, threadKey: error.threadKey, epoch: error.epoch } };
  if (error instanceof UnknownHarness) return { error: { ...said, name: error.name } };
  if (error instanceof FolderOnBranch) return { error: { ...said, branch: error.branch } };
  return { error: said };
}

/** The error of the host's an answer names, as the host failed with it; null for one that crosses only as its message. */
export function crossingErrorOf({ error }: typeof Failure.Type): CrossingError | null {
  switch (error._tag) {
    case "StoreError":
      return new StoreError({ cause: new Error(error.message) });
    case "ReceivedFileError":
      return new ReceivedFileError({ cause: new Error(error.message) });
    case "TurnFenced":
      return error.threadKey !== undefined && error.epoch !== undefined ? new TurnFenced({ threadKey: error.threadKey, epoch: error.epoch }) : null;
    case "UnknownHarness":
      return error.name !== undefined ? new UnknownHarness({ name: error.name }) : null;
    case "FolderOnBranch":
      return error.branch !== undefined ? new FolderOnBranch({ branch: error.branch }) : null;
    case "NoWorkspaceMounted":
      return new NoWorkspaceMounted();
    case "SessionFilesystemsDisabled":
      return new SessionFilesystemsDisabled();
    default:
      return null;
  }
}
