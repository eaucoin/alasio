/**
 * What the local cluster needs of the machine it runs on beyond Docker: Linux on x86-64,
 * which its node image and gVisor are built for, and inotify limits that hold its file
 * watches. Every process that runs as root in any container on the
 * machine shares fs.inotify.max_user_instances, as root, with the machine's own: k3s,
 * kubelet, containerd's shims, and each pod's, so the kernel's default of 128 runs out
 * once a few dozen pods run, and new pods then fail to start. kind and k3d ask the same
 * of a machine.
 */
import { Context, Effect, FileSystem, Layer, type PlatformError, Schema } from "effect";

/** The operating system and architecture the local cluster runs on, as Node names them. */
const LOCAL_PLATFORM = { platform: "linux", arch: "x64" } as const;

/** The operating system and processor architecture of this machine, as Node names them. */
export class Machine extends Context.Service<Machine, {
  readonly platform: string;
  readonly arch: string;
}>()("alasio/cluster/Machine") {
  /** This process's. */
  static readonly layer: Layer.Layer<Machine> = Layer.succeed(Machine, Machine.of({ platform: process.platform, arch: process.arch }));
}

/** This machine is not one the local cluster runs on: what it is. */
export class UnsupportedMachine extends Schema.TaggedError<UnsupportedMachine>()("UnsupportedMachine", {
  platform: Schema.String,
  arch: Schema.String,
}) {
  override get message(): string {
    return `the cluster alasio makes on this machine runs on Linux on x86-64, and this is ${this.platform} on ${this.arch}: ` +
      "run alasio on such a machine, or give it a cluster elsewhere with alasio init --kubeconfig";
  }
}

/** Fails, saying what this machine is, unless the local cluster runs on it. */
export const requireLocalMachine: Effect.Effect<void, UnsupportedMachine, Machine> = Effect.flatMap(
  Machine,
  ({ platform, arch }) =>
    platform === LOCAL_PLATFORM.platform && arch === LOCAL_PLATFORM.arch ? Effect.void : Effect.fail(new UnsupportedMachine({ platform, arch })),
);

/** The inotify limits the local cluster needs, by their sysctl names: at least these. */
export const INOTIFY_MINIMUMS: Readonly<Record<string, number>> = {
  "fs.inotify.max_user_instances": 1024,
  "fs.inotify.max_user_watches": 524288,
};

/** Where the inotify limits are raised for every boot: a file of /etc/sysctl.d/. */
const SYSCTL_FILE = "/etc/sysctl.d/60-inotify.conf";

/** The kernel's settings on this machine, read by their sysctl names. */
export class Sysctl extends Context.Service<Sysctl, {
  readonly read: (name: string) => Effect.Effect<string, PlatformError.PlatformError>;
}>()("alasio/cluster/Sysctl") {
  /** The settings as /proc/sys has them, a name's dots its path's slashes. */
  static readonly layer: Layer.Layer<Sysctl, never, FileSystem.FileSystem> = Layer.effect(
    Sysctl,
    Effect.map(FileSystem.FileSystem, (fs) => Sysctl.of({ read: (name) => fs.readFileString(`/proc/sys/${name.replaceAll(".", "/")}`) })),
  );
}

/** A limit lower than the local cluster needs: its sysctl name, its value, and the least it needs. */
const Shortfall = Schema.Struct({ name: Schema.String, value: Schema.Number, minimum: Schema.Number });

/** How a shortfall reads. */
export const describeShortfall = ({ name, value, minimum }: typeof Shortfall.Type): string => `${name} is ${value}, and the cluster needs at least ${minimum}`;

/** This machine's inotify limits are lower than the local cluster needs: which, and how to raise them. */
export class InotifyLimitsTooLow extends Schema.TaggedError<InotifyLimitsTooLow>()("InotifyLimitsTooLow", {
  shortfalls: Schema.Array(Shortfall),
}) {
  override get message(): string {
    return [
      "this machine's inotify limits are too low for the cluster alasio makes on it:",
      ...this.shortfalls.map((shortfall) => `  ${describeShortfall(shortfall)}`),
      "Raise them as root, now:",
      `  sysctl -w ${this.shortfalls.map(({ name, minimum }) => `${name}=${minimum}`).join(" ")}`,
      `and for every boot, in ${SYSCTL_FILE}:`,
      ...this.shortfalls.map(({ name, minimum }) => `  ${name} = ${minimum}`),
    ].join("\n");
  }
}

/** The inotify limits of this machine that are lower than the local cluster needs. */
export const inotifyShortfalls: Effect.Effect<readonly (typeof Shortfall.Type)[], PlatformError.PlatformError, Sysctl> = Effect.flatMap(Sysctl, (sysctl) =>
  Effect.forEach(Object.entries(INOTIFY_MINIMUMS), ([name, minimum]) =>
    Effect.map(sysctl.read(name), (text) => ({ name, value: Number(text.trim()), minimum }))).pipe(
      // One that reads as no number is short too.
      Effect.map((limits) => limits.filter(({ value, minimum }) => !(value >= minimum))),
    ));

/** Fails with what to raise, and how, when this machine's inotify limits are lower than the local cluster needs. */
export const requireInotifyLimits: Effect.Effect<void, InotifyLimitsTooLow | PlatformError.PlatformError, Sysctl> = Effect.flatMap(
  inotifyShortfalls,
  (shortfalls) => (shortfalls.length > 0 ? Effect.fail(new InotifyLimitsTooLow({ shortfalls })) : Effect.void),
);

/** Fails, saying why and what to do, unless this machine is one the local cluster runs on, its inotify limits high enough. */
export const requireLocalHost: Effect.Effect<void, UnsupportedMachine | InotifyLimitsTooLow | PlatformError.PlatformError, Machine | Sysctl> = Effect
  .andThen(requireLocalMachine, requireInotifyLimits);
