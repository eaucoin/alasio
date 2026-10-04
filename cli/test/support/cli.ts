/**
 * alasio's command line, run in a test as the operator runs it: its arguments parsed by
 * the `alasio` command, in an environment the test gives (its home, its XDG directories,
 * the Docker and Telegram it reaches), at a terminal that types the test's answers. What
 * it prints, logs and exits with is kept.
 */
import { NodeServices } from "@effect/platform-node";
import { ConfigProvider, Effect, type Exit, Layer, Logger, Sink, Stdio, type Terminal } from "effect";
import { Command } from "effect/cli";
import { TestConsole } from "effect/testing";

import { alasio } from "../../src/commands.ts";
import { TelegramBotApi } from "../../src/telegram.ts";
import { fakeTerminal } from "./fake-terminal.ts";

/** What a run came to. */
export interface CliRun {
  readonly exit: Exit.Exit<void, unknown>;
  /** What it printed with Console.log, a line each. */
  readonly printed: readonly string[];
  /** What it wrote to stdout and stderr as streams (logs, the lake's answers). */
  readonly stdout: string;
  readonly stderr: string;
  /** What it said it was doing. */
  readonly progress: readonly string[];
  /** What its prompts displayed. */
  readonly prompts: string;
  /** The answers no prompt read. */
  readonly unread: number;
}

/** Runs `alasio ...args` with the environment `env` and a terminal that answers `answers`. */
export async function runAlasio(
  args: readonly string[],
  { env, answers = [] }: { readonly env: Readonly<Record<string, string>>; readonly answers?: ReadonlyArray<readonly Terminal.UserInput[]> },
): Promise<CliRun> {
  const terminal = fakeTerminal(answers);
  const progress: string[] = [];
  let stdout = "";
  let stderr = "";
  const writer = (write: (text: string) => void) => () => Sink.forEach((chunk: string | Uint8Array) => Effect.sync(() => write(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"))));
  const config = ConfigProvider.layer(ConfigProvider.fromEnv({ env }));
  const { exit, printed } = await Effect.runPromise(
    Effect.gen(function*() {
      const exit = yield* Effect.exit(Command.runWith(alasio, { version: "9.9.9", renderErrors: false })(args));
      const printed = (yield* TestConsole.logLines).map(String);
      return { exit, printed };
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          TestConsole.layer,
          terminal.layer,
          Stdio.layerTest({ stdout: writer((text) => (stdout += text)), stderr: writer((text) => (stderr += text)) }),
          Logger.layer([Logger.make(({ message }) => void progress.push((Array.isArray(message) ? message : [message]).map(String).join(" ")))]),
          Layer.provideMerge(TelegramBotApi.layer, config),
        ),
      ),
      Effect.provide(NodeServices.layer),
    ),
  );
  return { exit, printed, stdout, stderr, progress, prompts: terminal.displayed(), unread: terminal.unread() };
}
