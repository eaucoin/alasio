/**
 * alasio's command line, run in a test as the operator runs it: its arguments parsed by
 * the `alasio` command, in an environment the test gives (its home, its XDG directories,
 * the Docker and Telegram it reaches, the machine's kind and kernel settings), at a terminal that
 * types the test's answers. What it prints, logs and exits with is kept.
 */
import { NodeServices } from "@effect/platform-node";
import { ConfigProvider, Effect, type Exit, Layer, Logger, Sink, Stdio, type Terminal } from "effect";
import { Command } from "effect/cli";
import { TestConsole } from "effect/testing";

import { INOTIFY_MINIMUMS, Machine, Sysctl } from "../../src/cluster/machine.ts";
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

/** The kernel's settings of a machine that meets what the local cluster needs, as /proc/sys has them. */
const FIT_HOST: Readonly<Record<string, string>> = Object.fromEntries(Object.entries(INOTIFY_MINIMUMS).map(([name, minimum]) => [name, `${minimum}\n`]));

/** A machine the local cluster runs on, as Node names it. */
const FIT_MACHINE = { platform: "linux", arch: "x64" } as const;

/** Runs `alasio ...args` with the environment `env`, on a machine of the kind `machine` and the kernel settings `sysctl`, and a terminal that answers `answers`. */
export async function runAlasio(
  args: readonly string[],
  { env, machine = FIT_MACHINE, sysctl = FIT_HOST, answers = [] }: {
    readonly env: Readonly<Record<string, string>>;
    readonly machine?: { readonly platform: string; readonly arch: string };
    readonly sysctl?: Readonly<Record<string, string>>;
    readonly answers?: ReadonlyArray<readonly Terminal.UserInput[]>;
  },
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
          Layer.succeed(Machine, machine),
          Layer.succeed(Sysctl, { read: (name) => Effect.succeed(sysctl[name] ?? "") }),
        ),
      ),
      Effect.provide(NodeServices.layer),
    ),
  );
  return { exit, printed, stdout, stderr, progress, prompts: terminal.displayed(), unread: terminal.unread() };
}
