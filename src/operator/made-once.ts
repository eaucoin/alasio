/**
 * What an operator's command makes, a session, a fork of one, a folder or a goal, made once
 * for the message it came in: a command kept as it ran on past its update is run again from
 * its start where its alasio stopped first (../telegram/controls.ts), and finds what its run
 * before made, kept in the store under a key its message names, rather than make another.
 * What its run before made but had not kept yet, cut short between the two, is made again.
 */
import { randomUUID } from "node:crypto";

import { Effect } from "effect";

import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";

/** The message a command came in, which names what it makes. */
export interface CommandMessage {
  readonly conversationId: string;
  readonly messageId: number;
}

/**
 * What `make` makes, `what` of the command of `message`, as JSON, made once: what the
 * command's run before made and kept, or `make`'s, kept; one that fails keeps nothing, and a
 * run again makes it again.
 */
export const madeOnce = <A extends NonNullable<unknown>, E, R>(message: CommandMessage, what: string, make: Effect.Effect<A, E, R>): Effect.Effect<A, E | StoreError, R | Store> =>
  Effect.gen(function*() {
    const store = yield* Store;
    const key = `${message.conversationId}:${message.messageId}:${what}`;
    // Kept as JSON by this module alone, of what `make` made: the same A.
    const kept = (yield* store.controlResult(key)) as A | null;
    if (kept !== null) return kept;
    return (yield* store.keepControlResult(key, yield* make)) as A;
  });

/**
 * Whether this is the first time the command of `message` comes to `what`, as kept in the
 * store as it does: false for a run again of a command whose run before came to it, as one
 * that asks for something no store keeps, a goal's turn, asks for it once.
 */
export const firstTime = (message: CommandMessage, what: string): Effect.Effect<boolean, StoreError, Store> =>
  Effect.gen(function*() {
    const token = randomUUID();
    const kept = (yield* Effect.flatMap(Store, (store) => store.keepControlResult(`${message.conversationId}:${message.messageId}:${what}`, { token }))) as { readonly token?: string };
    return kept.token === token;
  });
