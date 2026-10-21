/**
 * Codex's threads and their turns as the operator's session panels list them (../sessions.ts),
 * read from the rollouts kept in Neon (./store.ts), so no app-server need run, nor any pod be
 * bound, to list them. It reads what Codex's own `thread/list` and `thread/turns/list` report of
 * the same rollouts, as the panels use it (test/codex-rollout-listing.test.ts compares the two on
 * one home): a thread by its first line, `session_meta` (its id, its directory, the thread it is
 * a fork of and before which line of that one's), listed where its own lines hold a prompt of the
 * operator's, its preview the first of them, updated as its file last changed, newest first, to
 * the millisecond, as Codex orders them, of two in one second too; and its turns,
 * newest first, each by the `task_started` that began it and the items Codex completed in it,
 * a fork's own after those of the thread it is a fork of, up to the line it was forked before.
 *
 * Archived threads, as Codex lists none of them, and threads of another directory are left out.
 * A thread's name is none: Codex keeps names apart from its rollouts, and alasio names no thread.
 * A listing reads each thread's first chunk (./store.ts head), and its file whole only where its
 * first prompt is not in it yet, as it is not where the store first kept the file before the
 * prompt was written; a thread's turns read its file whole. A file Codex compresses is kept as it
 * was before (./mirror.ts), and read so.
 */
import { Effect, Schema } from "effect";

import type { v2 } from "../../../.types/codex/index.js";

import type { KeptRolloutFile, NeonRolloutStore } from "./store.ts";

/** A thread, as the panels read it of a listing. */
export type ListedThread = Pick<v2.Thread, "id" | "name" | "preview" | "updatedAt">;
/** A turn, as the panels read it of a listing. */
export type ListedTurn = Pick<v2.Turn, "id" | "startedAt" | "items">;

/** One line of a rollout, as much of it as the listing reads. */
interface Line {
  readonly timestamp?: string;
  readonly ordinal?: number;
  readonly type?: string;
  readonly payload?: {
    readonly type?: string;
    readonly id?: string;
    readonly cwd?: string;
    readonly source?: unknown;
    readonly forked_from_id?: string | null;
    readonly forked_from_ordinal_exclusive?: number | null;
    readonly turn_id?: string;
    readonly started_at?: number;
    readonly item?: { readonly type?: string; readonly id?: string; readonly content?: readonly { readonly type?: string; readonly text?: string }[] };
  };
}

/** The sources of the sessions Codex lists, as alasio asks it for them (../app-server/thread-client.ts). */
const LISTED_SOURCES: ReadonlySet<string> = new Set(["cli", "vscode", "exec"]);

/** A rollout file's lines; one that is no JSON skipped, as one half written is. */
function linesOf(bytes: Buffer): Line[] {
  const text = bytes.toString("utf8");
  const lines: Line[] = [];
  for (const raw of text.split("\n")) {
    if (!raw.trim()) continue;
    try {
      lines.push(JSON.parse(raw) as Line);
    } catch {
      // A line cut short, as the file's last is while Codex writes it.
    }
  }
  return lines;
}

/** What the operator wrote of a line, where it is the completion of their message: its text parts, joined. */
function promptOfLine(line: Line): string | null {
  const item = line.type === "event_msg" && line.payload?.type === "item_completed" ? line.payload.item : undefined;
  if (item?.type !== "UserMessage") return null;
  return (item.content ?? []).filter((part) => part.type === "text" && typeof part.text === "string").map((part) => part.text ?? "").join("\n");
}

/** A thread's lines, and what its first says of it. */
interface Rollout {
  readonly id: string;
  readonly cwd: string | null;
  readonly listed: boolean;
  readonly forkedFrom: { readonly id: string; readonly beforeOrdinal: number } | null;
  readonly lines: readonly Line[];
}

function rolloutOf(bytes: Buffer): Rollout | null {
  const lines = linesOf(bytes);
  const meta = lines[0]?.type === "session_meta" ? lines[0].payload : undefined;
  if (!meta?.id) return null;
  const forkedFromId = meta.forked_from_id ?? null;
  return {
    id: meta.id,
    cwd: meta.cwd ?? null,
    listed: typeof meta.source === "string" && LISTED_SOURCES.has(meta.source),
    forkedFrom: forkedFromId ? { id: forkedFromId, beforeOrdinal: meta.forked_from_ordinal_exclusive ?? Number.POSITIVE_INFINITY } : null,
    lines,
  };
}

/** The turns of `lines`, in the order they began, each with the user and agent messages Codex completed in it. */
function turnsOf(lines: readonly Line[]): ListedTurn[] {
  const turns = new Map<string, { id: string; startedAt: number | null; items: v2.ThreadItem[] }>();
  for (const line of lines) {
    const payload = line.type === "event_msg" ? line.payload : undefined;
    if (payload?.type === "task_started" && payload.turn_id && !turns.has(payload.turn_id)) {
      turns.set(payload.turn_id, { id: payload.turn_id, startedAt: payload.started_at ?? null, items: [] });
      continue;
    }
    if (payload?.type !== "item_completed" || !payload.turn_id) continue;
    const turn = turns.get(payload.turn_id);
    const item = payload.item;
    if (!turn || !item?.id) continue;
    if (item.type === "UserMessage") {
      const content = (item.content ?? []).filter((part) => part.type === "text" && typeof part.text === "string").map((part) => ({ type: "text" as const, text: part.text ?? "", text_elements: [] }));
      turn.items.push({ type: "userMessage", id: item.id, clientId: null, content });
    } else if (item.type === "AgentMessage") {
      const text = (item.content ?? []).filter((part) => typeof part.text === "string").map((part) => part.text ?? "").join("");
      turn.items.push({ type: "agentMessage", id: item.id, text, phase: null, memoryCitation: null, delivery: null, questions: null });
    }
  }
  return [...turns.values()];
}
/** What reading the rollouts failed with: the store, as it said. */
export class RolloutListingError extends Schema.TaggedError<RolloutListingError>()("RolloutListingError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** The threads and turns the rollouts in `store` hold, as the module says. */
export interface RolloutListing {
  /** The threads of the directory `cwd`, last updated first. */
  readonly listThreads: (scope: { readonly cwd: string }) => Effect.Effect<ListedThread[], RolloutListingError>;
  /** The turns of the thread `threadId`, newest first; none where it is not kept. */
  readonly listTurns: (scope: { readonly threadId: string }) => Effect.Effect<ListedTurn[], RolloutListingError>;
}

const reading = <A>(work: () => Promise<A>): Effect.Effect<A, RolloutListingError> => Effect.tryPromise({ try: work, catch: (cause) => new RolloutListingError({ cause }) });

/** The listing of the rollouts in `store`, as the module says. */
export function rolloutListing(store: NeonRolloutStore): RolloutListing {
  /** The thread `threadId`'s rollout: its newest file not archived, as a reverted thread's newer file replaces the one before. */
  const rolloutOfThread = async (threadId: string): Promise<{ readonly file: KeptRolloutFile; readonly rollout: Rollout } | null> => {
    const [file] = (await store.files({ threadIds: [threadId] })).filter((each) => !each.archived).sort((a, b) => b.modifiedMs - a.modifiedMs);
    if (!file) return null;
    const rollout = rolloutOf(await store.read(file.name));
    return rollout ? { file, rollout } : null;
  };

  /** The turns of `rollout`, a fork's own after those of what it is a fork of, before the line it was forked before, oldest first. */
  const allTurns = async (rollout: Rollout, seen: ReadonlySet<string> = new Set()): Promise<ListedTurn[]> => {
    const own = turnsOf(rollout.lines);
    if (!rollout.forkedFrom || seen.has(rollout.forkedFrom.id)) return own;
    const parent = await rolloutOfThread(rollout.forkedFrom.id);
    if (!parent) return own;
    const before = rollout.forkedFrom.beforeOrdinal;
    const inherited = await allTurns({ ...parent.rollout, lines: parent.rollout.lines.filter((line) => (line.ordinal ?? 0) < before) }, new Set([...seen, rollout.id]));
    return [...inherited, ...own];
  };

  return {
    listThreads: ({ cwd }) => reading(async () => {
      const newest = new Map<string, KeptRolloutFile>();
      for (const file of await store.files({})) {
        if (file.archived) continue;
        const kept = newest.get(file.threadId);
        if (!kept || kept.modifiedMs < file.modifiedMs) newest.set(file.threadId, file);
      }
      const threads: (ListedThread & { readonly changedMs: number })[] = [];
      for (const file of newest.values()) {
        const head = rolloutOf(await store.head(file.name));
        if (head && (!head.listed || head.cwd !== cwd)) continue;
        // Its first prompt read from its first chunk where it is there, from the whole file otherwise.
        const rollout = head?.lines.some((line) => promptOfLine(line) !== null) ? head : rolloutOf(await store.read(file.name));
        if (!rollout || !rollout.listed || rollout.cwd !== cwd) continue;
        const preview = rollout.lines.map(promptOfLine).find((prompt) => prompt !== null);
        // Codex lists a thread once its own lines hold a prompt.
        if (preview === undefined) continue;
        threads.push({ id: rollout.id, name: null, preview, updatedAt: Math.floor(file.modifiedMs / 1000), changedMs: file.modifiedMs });
      }
      // As Codex orders them: by when each last changed, to the millisecond, newest first; by id where two changed in one, the same each time.
      return threads
        .sort((a, b) => b.changedMs - a.changedMs || (a.id < b.id ? 1 : a.id > b.id ? -1 : 0))
        .map(({ changedMs: _changedMs, ...thread }) => thread);
    }),
    listTurns: ({ threadId }) =>
      reading(async () => {
        const found = await rolloutOfThread(threadId);
        return found ? (await allTurns(found.rollout)).reverse() : [];
      }),
  };
}
