/**
 * What a cluster alasio makes on this machine needs of the machine: Linux on x86-64, which
 * its k3s and gVisor are built for, and inotify limits that hold its file watches. Every
 * process that runs as root on the machine, in a container or not, shares
 * fs.inotify.max_user_instances, as root, with the machine's own: k3s, kubelet,
 * containerd's shims, and each pod's, so the kernel's default of 128 runs out once a few
 * dozen pods run, and new pods then fail to start. kind and k3d ask the same of a machine.
 * alasio raises the limits that are too low itself, as root (./root.ts): now, and for
 * every boot, in a file of /etc/sysctl.d/ of its own.
 *
 * The machine is its kind and its files, under the directory that stands for its root,
 * which is / but in tests; what alasio does to it as root, it does to those files, and
 * through RootSystem, which runs commands and kills processes.
 */
import { spawn } from "node:child_process";
import { join } from "node:path";

import { Context, Effect, FileSystem, Layer, type PlatformError, Schema } from "effect";

/** The operating system and architecture a cluster on this machine runs on, as Node names them. */
const LOCAL_PLATFORM = { platform: "linux", arch: "x64" } as const;

/** This machine: its operating system and processor architecture, as Node names them, and the directory that stands for its root. */
export class Machine extends Context.Service<Machine, {
  readonly platform: string;
  readonly arch: string;
  readonly root: string;
}>()("alasio/cluster/Machine") {
  /** This process's. */
  static readonly layer: Layer.Layer<Machine> = Layer.succeed(Machine, Machine.of({ platform: process.platform, arch: process.arch, root: "/" }));
}

/** Where `path`, a path of this machine, is under `root`. */
export const under = (root: string, path: string): string => join(root, path);

/** This machine is not one a cluster alasio makes runs on: what it is. */
export class UnsupportedMachine extends Schema.TaggedError<UnsupportedMachine>()("UnsupportedMachine", {
  platform: Schema.String,
  arch: Schema.String,
}) {
  override get message(): string {
    return `the cluster alasio makes on this machine runs on Linux on x86-64, and this is ${this.platform} on ${this.arch}: ` +
      "run alasio on such a machine, or give it a cluster elsewhere with alasio init --kubeconfig";
  }
}

/** Fails, saying what this machine is, unless a cluster alasio makes runs on it. */
export const requireLocalMachine: Effect.Effect<void, UnsupportedMachine, Machine> = Effect.flatMap(
  Machine,
  ({ platform, arch }) =>
    platform === LOCAL_PLATFORM.platform && arch === LOCAL_PLATFORM.arch ? Effect.void : Effect.fail(new UnsupportedMachine({ platform, arch })),
);

/** The inotify limits a cluster on this machine needs, by their sysctl names: at least these. */
export const INOTIFY_MINIMUMS: Readonly<Record<string, number>> = {
  "fs.inotify.max_user_instances": 1024,
  "fs.inotify.max_user_watches": 524288,
};

/** Where alasio raises the inotify limits for every boot: a file of /etc/sysctl.d/ of its own. */
export const SYSCTL_FILE = "/etc/sysctl.d/60-alasio-inotify.conf";

/** The path of the sysctl `name` under /proc/sys, its dots the path's slashes. */
const procSys = (name: string): string => `/proc/sys/${name.replaceAll(".", "/")}`;

/** A limit lower than a cluster on this machine needs: its sysctl name, its value, and the least it needs. */
const Shortfall = Schema.Struct({ name: Schema.String, value: Schema.Number, minimum: Schema.Number });

/** How a shortfall reads. */
export const describeShortfall = ({ name, value, minimum }: typeof Shortfall.Type): string => `${name} is ${value}, and the cluster needs at least ${minimum}`;

/** This machine's inotify limits are lower than a cluster on it needs: which, and what raises them. */
export class InotifyLimitsTooLow extends Schema.TaggedError<InotifyLimitsTooLow>()("InotifyLimitsTooLow", {
  shortfalls: Schema.Array(Shortfall),
}) {
  override get message(): string {
    return [
      "this machine's inotify limits are too low for the cluster alasio makes on it:",
      ...this.shortfalls.map((shortfall) => `  ${describeShortfall(shortfall)}`),
      "alasio up raises them, as root.",
    ].join("\n");
  }
}

/** The inotify limits of this machine that are lower than a cluster on it needs. */
export const inotifyShortfalls: Effect.Effect<readonly (typeof Shortfall.Type)[], PlatformError.PlatformError, Machine | FileSystem.FileSystem> = Effect.gen(
  function*() {
    const fs = yield* FileSystem.FileSystem;
    const { root } = yield* Machine;
    const limits = yield* Effect.forEach(Object.entries(INOTIFY_MINIMUMS), ([name, minimum]) =>
      Effect.map(fs.readFileString(under(root, procSys(name))), (text) => ({ name, value: Number(text.trim()), minimum })));
    // One that reads as no number is short too.
    return limits.filter(({ value, minimum }) => !(value >= minimum));
  },
);

/** A command alasio ran as root exited with another code than 0, or did not start. */
export class RootCommandFailed extends Schema.TaggedError<RootCommandFailed>()("RootCommandFailed", {
  command: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `${this.command} ${this.reason}`;
  }
}

/**
 * What alasio does to this machine as root beyond its files: the commands it runs, whose
 * output goes to stderr, as what a command is doing does, and the processes it kills, by
 * their executables.
 */
export class RootSystem extends Context.Service<RootSystem, {
  /** Runs `command` with `args`, its environment this process's with `env`. */
  readonly run: (command: string, args: readonly string[], env?: Readonly<Record<string, string>>) => Effect.Effect<void, RootCommandFailed>;
  /** Kills every process that runs one of `executables`, at once: how many it killed. */
  readonly kill: (executables: readonly string[]) => Effect.Effect<number, PlatformError.PlatformError>;
}>()("alasio/cluster/RootSystem") {
  /** This machine itself. */
  static readonly layer: Layer.Layer<RootSystem, never, FileSystem.FileSystem> = Layer.effect(
    RootSystem,
    Effect.map(FileSystem.FileSystem, (fs) =>
      RootSystem.of({
        run: (command, args, env = {}) =>
          Effect.callback<void, RootCommandFailed>((resume) => {
            const failed = (reason: string) => resume(Effect.fail(new RootCommandFailed({ command: [command, ...args].join(" "), reason })));
            const child = spawn(command, args, { env: { ...process.env, ...env }, stdio: ["ignore", process.stderr, process.stderr] });
            child.on("error", (cause) => failed(`did not start: ${cause.message}`));
            child.on("close", (code, signal) => (code === 0 ? resume(Effect.void) : failed(signal ? `was killed by ${signal}` : `exited with ${code}`)));
          }),
        kill: (executables) =>
          Effect.gen(function*() {
            let killed = 0;
            for (const pid of (yield* fs.readDirectory("/proc")).filter((entry) => /^\d+$/u.test(entry))) {
              // A process gone since, or a kernel thread, has no executable to read.
              const executable = yield* fs.readLink(`/proc/${pid}/exe`).pipe(Effect.orElseSucceed(() => ""));
              if (!executables.includes(executable.replace(/ \(deleted\)$/u, ""))) continue;
              try {
                process.kill(Number(pid), "SIGKILL");
                killed += 1;
              } catch {
                // Gone already.
              }
            }
            return killed;
          }),
      })),
  );
}

/** The step that raises the limits that are too low, to the least a cluster here needs, now and for every boot. */
export const RaiseInotify = Schema.TaggedStruct("RaiseInotify", {
  limits: Schema.Array(Schema.Struct({ name: Schema.String, minimum: Schema.Number })),
});
export type RaiseInotify = typeof RaiseInotify.Type;

/** The step that raises this machine's inotify limits that are too low, or none when none is. */
export const inotifyStep: Effect.Effect<RaiseInotify | null, PlatformError.PlatformError, Machine | FileSystem.FileSystem> = Effect.map(
  inotifyShortfalls,
  (shortfalls) => (shortfalls.length > 0 ? RaiseInotify.make({ limits: shortfalls.map(({ name, minimum }) => ({ name, minimum })) }) : null),
);

/** What the step does, as alasio says it will. */
export const describeRaiseInotify = ({ limits }: RaiseInotify): string =>
  `raise ${limits.map(({ name, minimum }) => `${name} to ${minimum}`).join(" and ")}, now and for every boot, in ${SYSCTL_FILE}`;

/** The limits SYSCTL_FILE says, `name = value` a line, by name. */
function sysctlLimits(file: string): ReadonlyMap<string, string> {
  return new Map(file.split("\n").flatMap((line) => {
    const match = /^\s*([\w.]+)\s*=\s*(\S+)\s*$/u.exec(line);
    return match ? [[match[1] ?? "", match[2] ?? ""] as const] : [];
  }));
}

/** SYSCTL_FILE with `limits`, beside those it says already. */
function sysctlFile(existing: string, limits: RaiseInotify["limits"]): string {
  const said = new Map(sysctlLimits(existing));
  for (const { name, minimum } of limits) said.set(name, String(minimum));
  const header = "# The inotify limits the cluster alasio makes here needs, which alasio raised; alasio uninstall --purge removes this file.";
  return [header, ...[...said].map(([name, value]) => `${name} = ${value}`), ""].join("\n");
}

/** Raises the limits, as root: in SYSCTL_FILE, beside those raised before, and in the running kernel. */
export const raiseInotify = Effect.fnUntraced(function*({ limits }: RaiseInotify): Effect.fn.Return<void, PlatformError.PlatformError, FileSystem.FileSystem | Machine> {
  const fs = yield* FileSystem.FileSystem;
  const { root } = yield* Machine;
  const file = under(root, SYSCTL_FILE);
  const existing = (yield* fs.exists(file)) ? yield* fs.readFileString(file) : "";
  yield* fs.makeDirectory(under(root, "/etc/sysctl.d"), { recursive: true });
  yield* fs.writeFileString(file, sysctlFile(existing, limits));
  for (const { name, minimum } of limits) yield* fs.writeFileString(under(root, procSys(name)), `${minimum}\n`);
  yield* Effect.logInfo(`raised ${limits.map(({ name, minimum }) => `${name} to ${minimum}`).join(" and ")}`);
});

/** The step that removes SYSCTL_FILE, which raises the inotify limits at every boot; the running kernel keeps them until it stops. */
export const ForgetInotify = Schema.TaggedStruct("ForgetInotify", {});
export type ForgetInotify = typeof ForgetInotify.Type;

/** The step that removes SYSCTL_FILE, or none when there is none. */
export const forgetInotifyStep: Effect.Effect<ForgetInotify | null, PlatformError.PlatformError, Machine | FileSystem.FileSystem> = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const { root } = yield* Machine;
  return (yield* fs.exists(under(root, SYSCTL_FILE))) ? ForgetInotify.make({}) : null;
});

/** What the step does, as alasio says it will. */
export const describeForgetInotify = (): string => `remove ${SYSCTL_FILE}, which raises the inotify limits at every boot`;

/** Removes SYSCTL_FILE, as root. */
export const forgetInotify: Effect.Effect<void, PlatformError.PlatformError, FileSystem.FileSystem | Machine> = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const { root } = yield* Machine;
  yield* fs.remove(under(root, SYSCTL_FILE), { force: true });
  yield* Effect.logInfo(`removed ${SYSCTL_FILE}`);
});
