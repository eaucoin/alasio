/**
 * `alasio as-root`: does, as root, the steps another of alasio's commands gives it on its
 * standard input (../cluster/root.ts), which runs it so through sudo, and says what they
 * came to on its standard output. It is no command of the operator's, and `alasio --help`
 * does not list it.
 */
import { Console, Effect, Schema, Stdio, Stream } from "effect";
import { Command } from "effect/cli";

import { performSteps, RootOutcomeJson, RootSteps } from "../cluster/root.ts";

export const asRoot = Command.make("as-root", {}, () =>
  Effect.gen(function*() {
    const stdio = yield* Stdio.Stdio;
    const steps = yield* Schema.decodeUnknownEffect(RootSteps)(yield* stdio.stdin.pipe(Stream.decodeText(), Stream.mkString));
    yield* Console.log(Schema.encodeSync(RootOutcomeJson)(yield* performSteps(steps)));
  })).pipe(
    Command.withDescription("Does, as root, the steps another of alasio's commands gives it on its standard input, as JSON, and says what they came to."),
    Command.unlisted,
  );
