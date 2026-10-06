/**
 * What alasio does to this machine as root: the steps a command asks for, each one's own
 * work and no more, all of a command's at once. A command run as root does them itself;
 * any other runs them through sudo, as `alasio as-root` (../commands/as-root.ts), which
 * reads them on its standard input, after saying what they are. Without a terminal, sudo
 * cannot ask for a password, so it must need none.
 */
import { spawn } from "node:child_process";

import { Context, Effect, FileSystem, Layer, type PlatformError, Schema, Stdio } from "effect";

import {
  describeForgetInotify,
  describeRaiseInotify,
  ForgetInotify,
  forgetInotify,
  Machine,
  RaiseInotify,
  raiseInotify,
  RootCommandFailed,
  RootSystem,
} from "./machine.ts";
import { describeNodeStep, NodeStep, type NodeStepError, performNodeStep } from "./node.ts";

/** A step alasio does as root. */
export const RootStep = Schema.Union([RaiseInotify, ForgetInotify, ...NodeStep.members]);
export type RootStep = typeof RootStep.Type;

/** Steps as `alasio as-root` reads them: JSON. */
export const RootSteps = Schema.fromJsonString(Schema.Array(RootStep));

/** What steps came to: the kubeconfig k3s wrote, when one of them read it. */
export const RootOutcome = Schema.Struct({ kubeconfig: Schema.optionalKey(Schema.String) });
export type RootOutcome = typeof RootOutcome.Type;

/** An outcome as `alasio as-root` says it: JSON. */
export const RootOutcomeJson = Schema.fromJsonString(RootOutcome);

/** What `step` does, as alasio says it will. */
export function describeStep(step: RootStep): string {
  switch (step._tag) {
    case "RaiseInotify":
      return describeRaiseInotify(step);
    case "ForgetInotify":
      return describeForgetInotify();
    default:
      return describeNodeStep(step);
  }
}

/** How a step fails. */
export type StepError = PlatformError.PlatformError | RootCommandFailed | NodeStepError;

/** Does `steps`, in order, as root: what they came to. */
export const performSteps = (steps: readonly RootStep[]): Effect.Effect<RootOutcome, StepError, FileSystem.FileSystem | Machine | RootSystem> =>
  Effect.reduce(steps, (): RootOutcome => ({}), (outcome, step) => {
    switch (step._tag) {
      case "RaiseInotify":
        return Effect.as(raiseInotify(step), outcome);
      case "ForgetInotify":
        return Effect.as(forgetInotify, outcome);
      default:
        return Effect.map(performNodeStep(step), (kubeconfig) => (kubeconfig ? { ...outcome, kubeconfig } : outcome));
    }
  });

/** alasio must do something as root, and sudo cannot ask for a password without a terminal. */
export class RootUnavailable extends Schema.TaggedError<RootUnavailable>()("RootUnavailable", {
  steps: Schema.Array(Schema.String),
}) {
  override get message(): string {
    return [
      "alasio must change this machine as root, and sudo cannot ask for a password without a terminal:",
      ...this.steps.map((step) => `  ${step}`),
      "Run it at a terminal, or as root, or where sudo asks for no password.",
    ].join("\n");
  }
}

/** What alasio did as root, through sudo, failed, or said what it came to as something else than its outcome. */
export class RootStepsFailed extends Schema.TaggedError<RootStepsFailed>()("RootStepsFailed", {
  reason: Schema.String,
}) {
  override get message(): string {
    return `what alasio did as root, through sudo, ${this.reason}`;
  }
}

/** How doing steps as root fails. */
export type RootError = RootUnavailable | RootStepsFailed | StepError;

/** What a command run by sudo came to: its exit code, and what it wrote to stdout. */
interface Ran {
  readonly exitCode: number;
  readonly stdout: Buffer;
}

/**
 * `sudo ...args`, given `stdin` on its standard input, its standard error this process's,
 * where it says what it does; without `stdin`, a question to sudo, which says nothing.
 */
const sudo = (args: readonly string[], stdin?: string): Effect.Effect<Ran, RootCommandFailed> =>
  Effect.callback<Ran, RootCommandFailed>((resume) => {
    const child = spawn("sudo", args, { stdio: stdin === undefined ? "ignore" : ["pipe", "pipe", "inherit"] });
    const stdout: Buffer[] = [];
    child.stdout?.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.on("error", (cause) => resume(Effect.fail(new RootCommandFailed({ command: "sudo", reason: `did not start: ${cause.message}` }))));
    child.on("close", (code) => resume(Effect.succeed({ exitCode: code ?? 1, stdout: Buffer.concat(stdout) })));
    child.stdin?.end(stdin);
    return Effect.sync(() => child.kill());
  });

/** Says what `steps` do, as `how` alasio does them. */
const announce = (steps: readonly RootStep[], how: string): Effect.Effect<void> =>
  Effect.logInfo([`alasio changes this machine ${how}:`, ...steps.map((step) => `  ${describeStep(step)}`)].join("\n"));

/** Does steps as root, saying first what they are. */
export class Root extends Context.Service<Root, {
  /** Does `steps`, in order: what they came to; nothing, asking for nothing, when there are none. */
  readonly run: (steps: readonly RootStep[]) => Effect.Effect<RootOutcome, RootError>;
}>()("alasio/cluster/Root") {
  /** In this process, which is root's, or stands for it in tests. */
  static readonly inProcess: Layer.Layer<Root, never, FileSystem.FileSystem | Machine | RootSystem> = Layer.effect(
    Root,
    Effect.map(Effect.context<FileSystem.FileSystem | Machine | RootSystem>(), (context) =>
      Root.of({
        run: (steps) => (steps.length === 0 ? Effect.succeed({}) : Effect.andThen(announce(steps, "as root"), Effect.provide(performSteps(steps), context))),
      })),
  );

  /**
   * In this process when it is root's; else through sudo, as `alasio as-root` with this
   * process's Node and script, which asks for no password (-n) when stdin is not a
   * terminal, so it is asked first whether it needs one.
   */
  static readonly layer: Layer.Layer<Root, never, FileSystem.FileSystem | Machine | RootSystem | Stdio.Stdio> = Layer.unwrap(
    Effect.gen(function*() {
      if (process.getuid?.() === 0) return Root.inProcess;
      const stdio = yield* Stdio.Stdio;
      return Layer.succeed(
        Root,
        Root.of({
          run: (steps) =>
            Effect.gen(function*() {
              if (steps.length === 0) return {};
              const interactive = yield* stdio.stdinIsTerminal;
              if (!interactive && (yield* sudo(["-n", "true"])).exitCode !== 0) return yield* new RootUnavailable({ steps: steps.map(describeStep) });
              yield* announce(steps, "as root, through sudo");
              const self = [process.execPath, ...process.execArgv, process.argv[1] ?? "", "as-root"];
              const { exitCode, stdout } = yield* sudo([...(interactive ? [] : ["-n"]), "--", ...self], Schema.encodeSync(RootSteps)(steps));
              if (exitCode !== 0) return yield* new RootStepsFailed({ reason: `exited with ${exitCode}; it said why above` });
              return yield* Schema.decodeUnknownEffect(RootOutcomeJson)(stdout.toString("utf8")).pipe(
                Effect.mapError(() => new RootStepsFailed({ reason: `said what it came to as ${JSON.stringify(stdout.toString("utf8").slice(0, 200))}` })),
              );
            }),
        }),
      );
    }),
  );
}
