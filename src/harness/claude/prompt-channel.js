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
