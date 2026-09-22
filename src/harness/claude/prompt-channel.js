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

export function buildClaudeUserMessage(text) {
  return {
    type: "user",
    message: { role: "user", content: text },
    parent_tool_use_id: null,
    origin: { kind: "human" },
  };
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
