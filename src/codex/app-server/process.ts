import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import * as NodeStream from "@effect/platform-node/NodeStream";
import { Deferred, Effect, Exit, Schema, type Scope, Stream } from "effect";

import { getCodexBinaryOverride } from "../../config.ts";
import { withLogScope } from "../../shared/log.ts";
import type { CodexEnv } from "../env.ts";
import { codexTelemetryArgs, codexTelemetryEnv } from "./telemetry.ts";

/** Where an app-server process is started, and with what environment. */
export interface AppServerProcessOptions {
  readonly cwd: string;
  readonly env: CodexEnv;
}

/** The codex binary alasio runs is not where alasio looks for it. */
export class CodexBinaryMissing extends Schema.TaggedError<CodexBinaryMissing>()("CodexBinaryMissing", {
  path: Schema.String,
}) {
  override get message(): string {
    return `Codex binary not found at ${this.path}`;
  }
}

/** The app-server process could not be started, as Node reported it. */
export class AppServerSpawnFailed extends Schema.TaggedError<AppServerSpawnFailed>()("AppServerSpawnFailed", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** The app-server process exited: its exit code, or the signal that ended it. */
export class AppServerExited extends Schema.TaggedError<AppServerExited>()("AppServerExited", {
  code: Schema.NullOr(Schema.Number),
  signal: Schema.NullOr(Schema.String),
}) {
  override get message(): string {
    return `Codex app-server exited code=${this.code} signal=${this.signal}`;
  }
}

/** How a started app-server process ends. */
export type AppServerEnded = AppServerSpawnFailed | AppServerExited;

/** A running app-server process, for as long as the scope it was started in is open. */
export interface AppServerProcess {
  /** Writes a line to its input. */
  readonly write: (line: string) => Effect.Effect<void>;
  /** What it writes to its output, a line at a time, until it ends. */
  readonly lines: Stream.Stream<string>;
  /** Waits for it to end, and fails with how it did. */
  readonly ended: Effect.Effect<never, AppServerEnded>;
}

/** Starts an app-server process, stopped when the scope it is started in closes. */
export type SpawnAppServer = (options: AppServerProcessOptions) => Effect.Effect<AppServerProcess, CodexBinaryMissing | AppServerSpawnFailed, Scope.Scope>;

function codexBinPath() {
  const here = dirname(fileURLToPath(import.meta.url));
  return getCodexBinaryOverride() || join(here, "..", "..", "..", "node_modules", ".bin", "codex");
}

/**
 * The local codex binary's app-server, over stdio: its error output is logged as it
 * comes, and it is sent SIGTERM when its scope closes.
 */
export const spawnAppServer: SpawnAppServer = Effect.fnUntraced(function*({ cwd, env }: AppServerProcessOptions) {
  const binPath = codexBinPath();
  if (!existsSync(binPath)) {
    return yield* new CodexBinaryMissing({ path: binPath });
  }
  const ended = yield* Deferred.make<never, AppServerEnded>();
  const child = yield* Effect.acquireRelease(
    Effect.try({
      try: () => spawn(binPath, ["app-server", "--disable", "plugins", ...codexTelemetryArgs(), "--listen", "stdio://"], {
        cwd,
        env: { ...env, ...codexTelemetryEnv() },
        stdio: ["pipe", "pipe", "pipe"],
      }),
      catch: (cause) => new AppServerSpawnFailed({ cause }),
    }),
    (child) => Effect.sync(() => child.kill("SIGTERM")),
  );
  child.on("exit", (code, signal) => Deferred.doneUnsafe(ended, Exit.fail(new AppServerExited({ code, signal }))));
  child.on("error", (cause) => Deferred.doneUnsafe(ended, Exit.fail(new AppServerSpawnFailed({ cause }))));
  // Its output streams end with it; reading them is no reason to fail.
  const output = (evaluate: () => NodeJS.ReadableStream) => NodeStream.fromReadable({ evaluate, onError: () => undefined }).pipe(Stream.ignore);
  yield* output(() => child.stderr).pipe(
    Stream.decodeText(),
    Stream.runForEach((data) => {
      const text = data.trimEnd();
      return text ? Effect.logWarning(`stderr ${text}`) : Effect.void;
    }),
    withLogScope("codex-app-server"),
    Effect.forkScoped,
  );
  return {
    write: (line) => Effect.sync(() => {
      child.stdin.write(`${line}\n`);
    }),
    lines: output(() => child.stdout).pipe(Stream.decodeText(), Stream.splitLines),
    ended: Deferred.await(ended),
  };
});
