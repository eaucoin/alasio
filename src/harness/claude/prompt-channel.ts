// @ts-nocheck
import { randomUUID } from "node:crypto";

/**
 * Pushable async iterable used as the Claude Agent SDK streaming prompt.
 *
 * Streaming input keeps the Claude Code process attached for the whole turn so
 * operator guidance can be pushed into the live session; ending the channel
 * after the result message lets the SDK generator complete normally.
 */
export function createPromptChannel() {
  const queue = [];
  const waiters = [];
  let ended = false;

  function push(value) {
    if (ended) {
      return false;
    }
    if (waiters.length > 0) {
      waiters.shift()({ value, done: false });
    } else {
      queue.push(value);
    }
    return true;
  }

  function end() {
    if (ended) {
      return;
    }
    ended = true;
    while (waiters.length > 0) {
      waiters.shift()({ value: undefined, done: true });
    }
  }

  const iterable = {
    [Symbol.asyncIterator]() {
      return {
        next() {
          if (queue.length > 0) {
            return Promise.resolve({ value: queue.shift(), done: false });
          }
          if (ended) {
            return Promise.resolve({ value: undefined, done: true });
          }
          return new Promise((resolve) => {
            waiters.push(resolve);
          });
        },
        return() {
          end();
          return Promise.resolve({ value: undefined, done: true });
        },
      };
    },
  };

  return {
    iterable,
    push,
    end,
    get ended() {
      return ended;
    },
  };
}

export function buildClaudeUserMessage(text, uuid = randomUUID()) {
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
export function promptUuidsAnsweredBy(message, promptUuids) {
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

export function resultAnswersPrompt(message, promptUuids) {
  return promptUuidsAnsweredBy(message, promptUuids).length > 0;
}

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
export function instrumentPromptChannel(channel, { threadKey, log }) {
  const at = () => new Date().toISOString();
  return {
    ...channel,
    push(value) {
      const accepted = channel.push(value);
      log.info(`prompt-channel push thread=${threadKey} at=${at()} accepted=${accepted}`);
      return accepted;
    },
    end() {
      const alreadyEnded = channel.ended;
      channel.end();
      if (!alreadyEnded) {
        log.info(`prompt-channel end thread=${threadKey} at=${at()}`);
      }
    },
    get ended() {
      return channel.ended;
    },
  };
}
