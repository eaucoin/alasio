/**
 * `alasio as-root`: does, as root, the steps another of alasio's commands reads it on its
 * standard input (../cluster/root.ts), which runs it so through sudo. It is no command of
 * the operator's, and `alasio --help` does not list it.
 */
import { Effect, Schema, Stdio, Stream } from "effect";
import { Command } from "effect/cli";

import { performSteps, RootSteps } from "../cluster/root.ts";

export const asRoot = Command.make("as-root", {}, () =>
  Effect.gen(function*() {
    const stdio = yield* Stdio.Stdio;
    const steps = yield* Schema.decodeUnknownEffect(RootSteps)(yield* stdio.stdin.pipe(Stream.decodeText(), Stream.mkString));
    yield* performSteps(steps);
  })).pipe(
    Command.withDescription("Does, as root, the steps another of alasio's commands gives it on its standard input, as JSON."),
    Command.unlisted,
  );
