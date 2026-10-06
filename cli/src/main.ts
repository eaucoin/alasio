#!/usr/bin/env node
/**
 * alasio's command line: `npx alasio <command>`, which installs alasio, runs it, and
 * looks after it, on a cluster it makes on this machine or one a kubeconfig reaches.
 *
 * What a command is doing is said on stderr as it goes; what it answers, on stdout. A
 * command that fails says why on stderr and exits with 1, or 130 when interrupted.
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Cause, Console, Effect, Layer, Logger, Result } from "effect";
import { CliError, Command } from "effect/cli";

import { Machine, Sysctl } from "./cluster/host.ts";
import { alasio } from "./commands.ts";
import { VERSION } from "./release.ts";
import { TelegramBotApi } from "./telegram.ts";

/** Progress, as a line of its message on stderr. */
const progress = Logger.make(({ message }) => {
  process.stderr.write(`${(Array.isArray(message) ? message : [message]).map(String).join(" ")}\n`);
});

/** What failed, on stderr: a typed failure by its message (the command line's own, which it showed, not again), a defect whole. */
const report = (cause: Cause.Cause<unknown>): Effect.Effect<void> => {
  if (Cause.hasInterruptsOnly(cause)) return Effect.void;
  const error = Cause.findError(cause);
  if (Result.isSuccess(error)) {
    return CliError.isCliError(error.success) ? Effect.void : Console.error(`alasio: ${error.success instanceof Error ? error.success.message : String(error.success)}`);
  }
  return Console.error(Cause.pretty(cause));
};

Command.run(alasio, { version: VERSION }).pipe(
  Effect.tapCause(report),
  Effect.provide(Layer.mergeAll(TelegramBotApi.layer, Machine.layer, Sysctl.layer, Logger.layer([progress]))),
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain({ disableErrorReporting: true }),
);
