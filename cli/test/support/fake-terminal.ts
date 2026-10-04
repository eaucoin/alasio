/**
 * A terminal that types what a test says, for alasio's prompts: each prompt reads the
 * keys of the next answer, then the end of input, so one that wants more than it is
 * given quits rather than waits. It keeps what the prompts displayed, without their
 * escape codes.
 */
import { Cause, Effect, Layer, Option, Queue, Terminal } from "effect";

const key = (name: string, input?: string, ctrl = false): Terminal.UserInput => ({
  input: Option.fromUndefinedOr(input),
  key: { name, ctrl, meta: false, shift: false },
});

export const ENTER = key("enter");
export const DOWN = key("down");

/** Typing `text`, then enter. */
export const typed = (text: string): Terminal.UserInput[] => [...[...text].map((char) => key(char, char)), ENTER];

/** Clearing what a prompt holds (its default), typing `text`, then enter. */
export const replaced = (text: string): Terminal.UserInput[] => [key("u", undefined, true), ...typed(text)];

/** Pressing `char` alone, as a yes-or-no prompt takes it. */
export const pressed = (char: string): Terminal.UserInput[] => [key(char, char)];

export interface FakeTerminal {
  readonly layer: Layer.Layer<Terminal.Terminal>;
  /** What the prompts displayed. */
  readonly displayed: () => string;
  /** The answers no prompt read. */
  readonly unread: () => number;
}

export function fakeTerminal(answers: ReadonlyArray<readonly Terminal.UserInput[]>): FakeTerminal {
  const pending = [...answers];
  let displayed = "";
  const terminal = Terminal.make({
    columns: Effect.succeed(120),
    rows: Effect.succeed(40),
    readInput: Effect.gen(function*() {
      const queue = yield* Queue.make<Terminal.UserInput, Cause.Done>();
      Queue.offerAllUnsafe(queue, pending.shift() ?? []);
      Queue.endUnsafe(queue);
      return queue;
    }),
    readLine: Effect.fail(new Terminal.QuitError({})),
    display: (text) =>
      Effect.sync(() => {
        // Without the escape codes that colour, move and erase.
        displayed += text.replace(/\u001b\[[0-9;?]*[A-Za-z]/gu, "");
      }),
  });
  return { layer: Layer.succeed(Terminal.Terminal, terminal), displayed: () => displayed, unread: () => pending.length };
}
