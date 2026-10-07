/**
 * alasio's command line, run in a test as the operator runs it: its arguments parsed by
 * the `alasio` command, in an environment the test gives (its home, its XDG directories,
 * the Docker and Telegram it reaches, the machine's kind, files and systemd, what it does
 * to the machine as root, and the releases of k3s and gVisor it downloads), at a terminal
 * that types the test's answers. What it prints, logs and exits with is kept.
 */
import { NodeServices } from "@effect/platform-node";
import { ConfigProvider, Effect, type Exit, Layer, Logger, Sink, Stdio, Stream, type Terminal } from "effect";
import { Command } from "effect/cli";
import { TestConsole } from "effect/testing";

import { Machine } from "../../src/cluster/machine.ts";
import { Root } from "../../src/cluster/root.ts";
import { alasio } from "../../src/commands.ts";
import { TelegramBotApi } from "../../src/telegram.ts";
import type { FakeMachine } from "./fake-machine.ts";
import { fakeReleases, type FakeReleases } from "./fake-releases.ts";
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

/** A machine a cluster here runs on, as Node names it. */
const FIT_MACHINE = { platform: "linux", arch: "x64" } as const;

/**
 * Runs `alasio ...args` with the environment `env`, on `machine`, of the kind `kind`,
 * where k3s and gVisor are `releases`, and a terminal that answers `answers`, or stdin
 * that reads `stdin`; interrupted, as by Ctrl-C, once `until` resolves, if given.
 */
export async function runAlasio(
  args: readonly string[],
  { env, machine, kind = FIT_MACHINE, releases = fakeReleases(), answers = [], stdin, until }: {
    readonly env: Readonly<Record<string, string>>;
    readonly machine: FakeMachine;
    readonly kind?: { readonly platform: string; readonly arch: string };
    readonly releases?: FakeReleases;
    readonly answers?: ReadonlyArray<readonly Terminal.UserInput[]>;
    readonly stdin?: string;
    readonly until?: Promise<void>;
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
      const run = Command.runWith(alasio, { version: "9.9.9", renderErrors: false })(args);
      const exit = yield* Effect.exit(until === undefined ? run : Effect.raceFirst(run, Effect.andThen(Effect.promise(() => until), Effect.interrupt)));
      const printed = (yield* TestConsole.logLines).map(String);
      return { exit, printed };
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          TestConsole.layer,
          terminal.layer,
          Stdio.layerTest({
            stdout: writer((text) => (stdout += text)),
            stderr: writer((text) => (stderr += text)),
            ...(stdin === undefined ? {} : { stdin: Stream.make(new TextEncoder().encode(stdin)) }),
          }),
          Logger.layer([Logger.make(({ message }) => void progress.push((Array.isArray(message) ? message : [message]).map(String).join(" ")))]),
          Layer.provideMerge(TelegramBotApi.layer, config),
          Root.inProcess.pipe(Layer.provideMerge(Layer.mergeAll(Layer.succeed(Machine, { ...kind, root: machine.root }), machine.layer))),
          machine.systemd,
          releases.layer,
        ),
      ),
      Effect.provide(NodeServices.layer),
    ),
  );
  return { exit, printed, stdout, stderr, progress, prompts: terminal.displayed(), unread: terminal.unread() };
}
