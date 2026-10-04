/** `alasio login codex`: Codex's device login, run in alasio's pod, at this terminal. */
import { PassThrough, type Readable } from "node:stream";

import { Effect } from "effect";
import { Command } from "effect/cli";

import { KubeApi } from "../kube/api.ts";
import { NAMESPACE, RELEASE } from "../manifests/common.ts";
import { CommandFailed, onCluster, runningPod } from "./common.ts";

/** Codex's login by a code entered on another device, as alasio's image installs Codex (Dockerfile). */
export const CODEX_LOGIN = ["/opt/alasio/node_modules/.bin/codex", "login", "--device-auth"] as const;

/**
 * What is typed at this process's terminal, while `use` runs with it: raw, so it reaches
 * the command in the pod as typed, and let go of after, as it was, so this process can
 * end.
 */
const terminalInput = <A, E, R>(use: (input: Readable) => Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      const input = new PassThrough();
      if (process.stdin.isTTY) process.stdin.setRawMode(true);
      process.stdin.pipe(input);
      return input;
    }),
    use,
    (input) =>
      Effect.sync(() => {
        process.stdin.unpipe(input);
        process.stdin.pause();
        if (process.stdin.isTTY) process.stdin.setRawMode(false);
        input.end();
      }),
  );

const codex = Command.make("codex", {}, () =>
  onCluster(() =>
    Effect.gen(function*() {
      const kube = yield* KubeApi;
      const pod = yield* runningPod(RELEASE);
      // The terminal itself, not alasio's output: what Codex asks and is answered is the operator's.
      const exitCode = yield* terminalInput((stdin) =>
        kube.exec({ namespace: NAMESPACE, pod, container: RELEASE }, CODEX_LOGIN, { stdin, stdout: process.stdout, stderr: process.stderr, tty: process.stdout.isTTY })
      );
      if (exitCode !== 0) return yield* new CommandFailed({ command: "codex login", exitCode });
    })
  )).pipe(
    Command.withShortDescription("Log Codex in"),
    Command.withDescription(
      "Runs Codex's device login in alasio's pod, at this terminal: Codex shows a code to enter on a page it names, " +
        "and keeps the login on alasio's volume, so it is done once.",
    ),
  );

export const login = Command.make("login").pipe(
  Command.withSubcommands([codex]),
  Command.withShortDescription("Log a harness in"),
  Command.withDescription("Logs a harness in, in alasio's pod. Claude Code needs none: alasio init gives it a token."),
);
