/**
 * The gVisor releases on the node, and runsc run as their shims run it. Each release is a
 * directory of its own under the releases' root (`<root>/<release>/runsc`), which
 * containerd runs as the runtime `runsc-<label>`, its label the release with "." as "-";
 * that runtime's options file (`<containerd>/runsc-<label>.toml`) gives runsc, in its
 * `[runsc_config]` table, the flags its shim passes it. The agent runs a release's runsc
 * exactly so, beside the shim: with the shim's root (`--root`) and those flags, so it
 * finds the sandboxes the shim made and tears them down as the shim would.
 */
import { execFile } from "node:child_process";
import { readdir, readFile, readlink } from "node:fs/promises";
import { join, relative, sep } from "node:path";

import { Effect, Schema } from "effect";

import { gvisorHandler } from "./names.ts";

/** Where the node's releases, their runtimes' options and their sandboxes' state are. */
export interface RunscOptions {
  /** The directory of the node's releases, one directory each. */
  readonly releasesDir: string;
  /** containerd's configuration directory, holding each runtime's options file. */
  readonly containerdDir: string;
  /** The root the shims run runsc with, holding their sandboxes' state. */
  readonly root: string;
  /** The node's processes, as the agent sees them in its PID namespace: /proc unless told. */
  readonly procRoot?: string;
}

/** What runsc was asked to do and could not, and what it said. */
export class RunscError extends Schema.TaggedError<RunscError>()("RunscError", {
  release: Schema.String,
  command: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `runsc ${this.command} of ${this.release} failed: ${this.reason}`;
  }
}

/** A release is not on this node. */
export class ReleaseNotInstalled extends Schema.TaggedError<ReleaseNotInstalled>()("ReleaseNotInstalled", {
  release: Schema.String,
}) {
  override get message(): string {
    return `gVisor ${this.release} is not installed on this node`;
  }
}

/** What `runsc image inspect --json` says of an image, as far as the agent reads it. */
const ImageInspection = Schema.Struct({
  digest: Schema.NonEmptyString,
  state: Schema.Struct({ metadata: Schema.Record(Schema.String, Schema.String) }),
});

/** What `runsc state` says of a container, as far as the agent reads it: its sandbox's process, and its spec's annotations. */
const ContainerState = Schema.Struct({
  pid: Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0)),
  annotations: Schema.optional(Schema.Record(Schema.String, Schema.String)),
});

/** A container as the node runs it: the release whose runsc runs its sandbox, and its spec's annotations. */
export interface RunningContainer {
  readonly release: string;
  readonly annotations: Readonly<Record<string, string>>;
}

/** An image, as runsc describes it: its identity, the version of the runsc that saved it, and the CPU features it was saved with. */
export interface InspectedImage {
  readonly digest: string;
  readonly version: string;
  readonly cpuFeatures: readonly string[];
}

/** How a checkpoint is taken: of which container, into which directory, running what first. */
export interface CheckpointOptions {
  /** The container, by its runtime's id, whose sandbox is saved whole, and in which the hook runs. */
  readonly containerId: string;
  readonly imagePath: string;
  /** The program runsc runs in the container before it saves, which it splits at spaces. */
  readonly hook: readonly string[];
  /** How long the hook may take. */
  readonly hookTimeout: string;
}

/** The node's releases and their runsc; see makeRunsc. */
export interface Runsc {
  /** The releases installed on this node, in order. */
  readonly releases: Effect.Effect<readonly string[]>;
  /**
   * Saves the sandbox of a container whole into `imagePath`, uncompressed, as a restore
   * reading it directly needs, which ends the sandbox.
   */
  readonly checkpoint: (release: string, options: CheckpointOptions) => Effect.Effect<void, RunscError | ReleaseNotInstalled>;
  /** Resumes a container, by its runtime's id, of a sandbox restored paused; one that runs already is left so. */
  readonly resume: (release: string, containerId: string) => Effect.Effect<void, RunscError | ReleaseNotInstalled>;
  readonly inspect: (release: string, imagePath: string) => Effect.Effect<InspectedImage, RunscError | ReleaseNotInstalled>;
  /** The CPU features of this node, by the names `release` gives them. */
  readonly cpuFeatures: (release: string) => Effect.Effect<readonly string[], RunscError | ReleaseNotInstalled>;
  /**
   * The container, by its runtime's id, as the node runs it: which release runs its
   * sandbox, from the program its sandbox's process runs (whatever a pod's RuntimeClass
   * names now, it ran what it named as it started), and its annotations.
   */
  readonly container: (containerId: string) => Effect.Effect<RunningContainer, RunscError>;
}

/** An entry of a TOML table whose value is a basic string, its key bare or quoted. */
const TOML_ENTRY = /^\s*(?:([A-Za-z0-9_-]+)|"((?:[^"\\]|\\.)*)")\s*=\s*"((?:[^"\\]|\\.)*)"\s*(?:#.*)?$/u;
/** A TOML table's header. */
const TOML_TABLE = /^\s*\[([^\]]*)\]\s*(?:#.*)?$/u;

/** A TOML basic string's text, its escapes undone. */
function unescapeToml(text: string): string {
  return text.replace(/\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|.)/gu, (_escape, code: string) => {
    if (code.length > 1) return String.fromCodePoint(Number.parseInt(code.slice(1), 16));
    return ({ b: "\b", t: "\t", n: "\n", f: "\f", r: "\r", '"': '"', "\\": "\\" } as Readonly<Record<string, string>>)[code] ?? code;
  });
}

/**
 * The `[runsc_config]` table of a runtime's options file, as the flags its shim passes
 * runsc, in the order they are written: `--key=value`. Its entries are strings, as the
 * shim reads them; anything else in the table is refused.
 */
export function runscFlags(toml: string): string[] {
  const flags: string[] = [];
  let inTable = false;
  for (const line of toml.split("\n")) {
    const table = TOML_TABLE.exec(line);
    if (table) {
      inTable = table[1]?.trim() === "runsc_config";
      continue;
    }
    if (!inTable || /^\s*(#.*)?$/u.test(line)) continue;
    const entry = TOML_ENTRY.exec(line);
    if (!entry) throw new Error(`[runsc_config] holds what is not a string flag: ${line.trim()}`);
    flags.push(`--${entry[1] ?? unescapeToml(entry[2] ?? "")}=${unescapeToml(entry[3] ?? "")}`);
  }
  return flags;
}

/** What a run of runsc printed. */
interface Ran {
  readonly stdout: string;
}

/** The most a run of runsc may print. */
const MAX_OUTPUT_BYTES = 16 * 1024 * 1024;

/** The node's releases and their runsc, at `options`' paths. */
export function makeRunsc({ releasesDir, containerdDir, root, procRoot = "/proc" }: RunscOptions): Runsc {
  const releases = Effect.promise(async () => {
    const entries = await readdir(releasesDir, { withFileTypes: true }).catch(() => []);
    const found = await Promise.all(
      entries.filter((entry) => entry.isDirectory()).map(async ({ name }) =>
        (await readdir(join(releasesDir, name)).catch(() => [] as string[])).includes("runsc") ? [name] : []
      ),
    );
    return found.flat().sort();
  });

  const installed = (release: string): Effect.Effect<void, ReleaseNotInstalled> =>
    releases.pipe(Effect.filterOrFail((names) => names.includes(release), () => new ReleaseNotInstalled({ release })), Effect.asVoid);

  /** Runs `release`'s runsc's `command` with its shim's flags and `args`. */
  const run = (release: string, command: readonly string[], args: readonly string[]): Effect.Effect<Ran, RunscError | ReleaseNotInstalled> =>
    installed(release).pipe(
      Effect.andThen(Effect.tryPromise({
        try: async (signal) => {
          const options = join(containerdDir, `${gvisorHandler(release)}.toml`);
          const flags = runscFlags(await readFile(options, "utf8").catch((cause: unknown) => {
            throw new Error(`its runtime's options file ${options} is not readable: ${cause instanceof Error ? cause.message : String(cause)}`);
          }));
          return new Promise<Ran>((resolve, reject) => {
            execFile(join(releasesDir, release, "runsc"), [`--root=${root}`, ...flags, ...command, ...args], { signal, maxBuffer: MAX_OUTPUT_BYTES }, (error, stdout, stderr) => {
              if (error) reject(new Error(stderr.trim() || error.message));
              else resolve({ stdout });
            });
          });
        },
        catch: (cause) => new RunscError({ release, command: command.join(" "), reason: cause instanceof Error ? cause.message : String(cause) }),
      })),
    );

  // A node's CPU does not change while it runs.
  const features = new Map<string, readonly string[]>();

  return {
    releases,

    checkpoint: (release, { containerId, imagePath, hook, hookTimeout }) =>
      run(release, ["checkpoint"], [
        `--image-path=${imagePath}`,
        "--compression=none",
        `--save-restore-exec-argv=${hook.join(" ")}`,
        `--save-restore-exec-timeout=${hookTimeout}`,
        containerId,
      ]).pipe(Effect.asVoid),

    resume: (release, containerId) => run(release, ["resume"], [containerId]).pipe(Effect.asVoid),

    inspect: (release, imagePath) =>
      run(release, ["image", "inspect"], ["--json", imagePath]).pipe(
        Effect.flatMap(({ stdout }) =>
          Effect.try({
            try: () => Schema.decodeUnknownSync(ImageInspection)(JSON.parse(stdout)),
            catch: (cause) => new RunscError({ release, command: "image inspect", reason: `its description is not one: ${cause instanceof Error ? cause.message : String(cause)}` }),
          })
        ),
        Effect.map(({ digest, state: { metadata } }) => ({
          digest,
          // The metadata keys runsc saves an image with (runsc/boot/restore.go).
          version: metadata["runsc_version"] ?? "",
          cpuFeatures: (metadata["cpu_features"] ?? "").split(",").filter(Boolean),
        })),
      ),

    container: (containerId) =>
      Effect.gen(function*() {
        const failed = (reason: string) => new RunscError({ release: "the node's", command: `state ${containerId}`, reason });
        // Any release reads the state every shim keeps under the one root.
        const [any] = yield* releases;
        if (any === undefined) return yield* failed("no gVisor release is installed here");
        const { stdout } = yield* run(any, ["state"], [containerId]).pipe(Effect.mapError((error) => (error._tag === "RunscError" ? error : failed(error.message))));
        const state = yield* Effect.try({ try: () => Schema.decodeUnknownSync(ContainerState)(JSON.parse(stdout)), catch: (cause) => failed(`its state is not one: ${String(cause)}`) });
        const program = yield* Effect.tryPromise({ try: () => readlink(join(procRoot, String(state.pid), "exe")), catch: (cause) => failed(`its sandbox's process ${state.pid} is not readable: ${String(cause)}`) });
        const [release = "", ...under] = relative(releasesDir, program).split(sep);
        if (under.length === 0 || release === ".." || !(yield* releases).includes(release)) return yield* failed(`its sandbox runs ${program}, of no release installed here`);
        return { release, annotations: state.annotations ?? {} };
      }),

    cpuFeatures: (release) => {
      const known = features.get(release);
      if (known) return Effect.succeed(known);
      return run(release, ["cpu-features"], []).pipe(
        Effect.map(({ stdout }) => stdout.trim().split(",").filter(Boolean)),
        Effect.tap((names) => Effect.sync(() => features.set(release, names))),
      );
    },
  };
}
