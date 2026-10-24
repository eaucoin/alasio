import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import * as NodeStream from "@effect/platform-node/NodeStream";
import { Deferred, Effect, Exit, Schema, type Scope, Stream } from "effect";

import { getCodexBinaryOverride } from "../../config.ts";
import { withLogScope } from "../../shared/log.ts";
import { CGROUP_DIR, type MemoryKill, ranOutOf, watchMemoryKills } from "../../shared/memory-kills.ts";
import type { CodexEnv } from "../env.ts";
import { CODEX_DISABLED_FEATURES } from "../features.ts";
import { codexTelemetryArgs, codexTelemetryEnv } from "./telemetry.ts";

/** Where an app-server process is started, with what environment, and with what of Codex's configuration given over its home's (`-c key=value`, the value TOML). */
export interface AppServerProcessOptions {
  readonly cwd: string;
  readonly env: CodexEnv;
  readonly config?: readonly string[] | undefined;
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

/** The app-server process exited: its exit code, or the signal that ended it, and, where the kernel killed it for its memory, the limit it ran out of. */
export class AppServerExited extends Schema.TaggedError<AppServerExited>()("AppServerExited", {
  code: Schema.NullOr(Schema.Number),
  signal: Schema.NullOr(Schema.String),
  outOfMemory: Schema.optional(Schema.Struct({ limit: Schema.NullOr(Schema.String) })),
}) {
  override get message(): string {
    if (this.outOfMemory) return `Codex app-server ${ranOutOf(this.outOfMemory)} and was killed by the kernel`;
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
 * The local codex binary's app-server, over stdio, without CODEX_DISABLED_FEATURES
 * (../features.ts): its error output is logged as it comes, and it is sent SIGTERM when its
 * scope closes. One killed (SIGKILL) as the cgroup at `cgroup` counts a kill for memory since it
 * started ends saying so (../../shared/memory-kills.ts).
 */
export const makeSpawnAppServer = (cgroup: string = CGROUP_DIR): SpawnAppServer => Effect.fnUntraced(function*({ cwd, env, config = [] }: AppServerProcessOptions) {
  const binPath = codexBinPath();
  if (!existsSync(binPath)) {
    return yield* new CodexBinaryMissing({ path: binPath });
  }
  const ended = yield* Deferred.make<never, AppServerEnded>();
  const killedForMemory = watchMemoryKills(cgroup);
  const child = yield* Effect.acquireRelease(
    Effect.try({
      try: () => spawn(binPath, ["app-server", ...CODEX_DISABLED_FEATURES.flatMap((feature) => ["--disable", feature]), ...codexTelemetryArgs(), ...config.flatMap((each) => ["-c", each]), "--listen", "stdio://"], {
        cwd,
        env: { ...env, ...codexTelemetryEnv() },
        stdio: ["pipe", "pipe", "pipe"],
      }),
      catch: (cause) => new AppServerSpawnFailed({ cause }),
    }),
    (child) => Effect.sync(() => child.kill("SIGTERM")),
  );
  child.on("exit", (code, signal) => {
    const outOfMemory: MemoryKill | null = signal === "SIGKILL" ? killedForMemory() : null;
    Deferred.doneUnsafe(ended, Exit.fail(new AppServerExited({ code, signal, ...(outOfMemory ? { outOfMemory } : {}) })));
  });
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

/** The local codex binary's app-server, as makeSpawnAppServer starts it, of the container's own cgroup. */
export const spawnAppServer: SpawnAppServer = makeSpawnAppServer();
