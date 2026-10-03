import { randomUUID, type UUID } from "node:crypto";

import type { SDKMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import { type Cause, Clock, Effect, Queue, Stream } from "effect";

/** The streaming prompt of a Claude Code query: messages pushed in until it is ended. */
export interface PromptChannel {
  /** What Claude Code reads: the messages pushed, then the end. A reader that stops reading ends the channel. */
  readonly prompts: AsyncIterable<SDKUserMessage>;
  /** Queues a message; false once the channel has ended. */
  readonly push: (message: SDKUserMessage) => Effect.Effect<boolean>;
  /** Ends the channel once what is queued is read; true if this ended it. */
  readonly end: Effect.Effect<boolean>;
}

/**
 * A queue of prompts read as the Claude Agent SDK's streaming prompt.
 *
 * Streaming input keeps the Claude Code process attached for the whole turn so
 * operator guidance can be pushed into the live session; ending the channel
 * after the result message lets the SDK generator complete normally.
 */
export const makePromptChannel: Effect.Effect<PromptChannel> = Effect.gen(function*() {
  const queue = yield* Queue.unbounded<SDKUserMessage, Cause.Done>();
  const end = Queue.end(queue);
  const prompts = yield* Stream.fromQueue(queue).pipe(Stream.ensuring(end), Stream.toAsyncIterableEffect);
  return { prompts, push: (message) => Queue.offer(queue, message), end };
});

export function buildClaudeUserMessage(text: string, uuid: UUID = randomUUID()): SDKUserMessage {
  return {
    type: "user",
    uuid,
    message: { role: "user", content: text },
    parent_tool_use_id: null,
    origin: { kind: "human" },
  };
}

/**
 * Whether a result message answers one of the prompts we pushed (the operator
 * prompt, plus anything steered into the same turn).
 *
 * A result carries the client uuid of the user message its turn consumed
 * (`user_message_uuids`, or `user_message_uuid` from older producers). In a
 * resumed session the first result need not be ours: the CLI re-runs a turn a
 * previous worker left interrupted, and stamps that turn's own prompt uuid.
 * A result that names no prompt at all comes from a turn the CLI started
 * itself, such as its report that a background task stopped; the pinned SDK
 * attributes every turn a typed prompt starts.
 *
 * Ending the prompt channel on a foreign result closes the input stream while
 * our turn is still being planned, and the first tool call the model then
 * makes is cancelled.
 */
export function promptUuidsAnsweredBy(message: SDKMessage, promptUuids: string | Set<string>): string[] {
  const ours = promptUuids instanceof Set ? promptUuids : new Set([promptUuids]);
  if (ours.size === 0 || !message || message.type !== "result") {
    return [];
  }
  if (Array.isArray(message.user_message_uuids)) {
    return message.user_message_uuids.filter((uuid) => ours.has(uuid));
  }
  if (typeof message.user_message_uuid === "string") {
    return ours.has(message.user_message_uuid) ? [message.user_message_uuid] : [];
  }
  return [];
}

/** Now, as a log line gives it. */
const at = Effect.map(Clock.currentTimeMillis, (millis) => new Date(millis).toISOString());

/**
 * Wrap a prompt channel so every push and end is logged with a millisecond
 * timestamp.
 *
 * New input arriving on the streaming prompt while tool calls are pending is
 * one of the two ways Claude Code cancels a pending call (the other is an
 * abort of the query). A cancelled call is answered with Claude Code's
 * "the user doesn't want to take this action" text, which reads like a
 * permission denial even under bypassPermissions. Without this log there is
 * no record on our side of whether the channel moved at all, so a cancelled
 * call cannot be attributed.
 */
export function instrumentPromptChannel(channel: PromptChannel, threadKey: string): PromptChannel {
  return {
    prompts: channel.prompts,
    push: (message) =>
      Effect.tap(channel.push(message), (accepted) =>
        Effect.flatMap(at, (now) => Effect.logInfo(`prompt-channel push thread=${threadKey} at=${now} accepted=${accepted}`))),
    end: Effect.tap(channel.end, (ended) =>
      ended ? Effect.flatMap(at, (now) => Effect.logInfo(`prompt-channel end thread=${threadKey} at=${now}`)) : Effect.void),
  };
}
