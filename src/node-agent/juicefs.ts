/**
 * Workspace storage as JuiceFS's own command, of the agent's image, works on it: its own
 * release's static build, of the version the nodes mount workspace storage with (../../Dockerfile).
 *
 * - A workspace's directory's quota, set, read, repaired and deleted on the metadata engine alone
 *   (`juicefs quota`), as the agent's Valkey user there, which runs no command Valkey calls
 *   dangerous. The engine's address is given whole, as the stack's setup makes it, its
 *   password in it; the command is given it without, the password in META_PASSWORD, which
 *   JuiceFS reads, so no other process of the node, whose PIDs the agent shares, reads it in
 *   its arguments.
 * - A workspace's directory cloned into another's (`juicefs clone`), through the node's mount
 *   of workspace storage, which the agent sees and mounts nothing of: a copy of its metadata
 *   alone, its files' data shared until either is written.
 *
 * It holds no key of the object store: neither needs one.
 */
import { execFile } from "node:child_process";

import { Effect, Schema } from "effect";

/** JuiceFS's command failed, or said what is not what was asked: which, and why. */
export class JuicefsError extends Schema.TaggedError<JuicefsError>()("JuicefsError", { reason: Schema.String }) {
  override get message(): string {
    return this.reason;
  }
}

/** A directory's quota, and what it holds of it, in bytes. */
export interface Quota {
  readonly capacityBytes: number;
  readonly usedBytes: number;
}

/** JuiceFS's command, as the module says; see makeJuicefs. */
export interface Juicefs {
  /** Sets the directory `path`'s quota, of the file system's root, to `bytes`, rounded up to a GiB, as JuiceFS counts quotas. */
  readonly setQuota: (path: string, bytes: number) => Effect.Effect<void, JuicefsError>;
  /** The directory `path`'s quota, and what it holds; null where it has none. */
  readonly quota: (path: string) => Effect.Effect<Quota | null, JuicefsError>;
  /** Deletes the directory `path`'s quota. */
  readonly deleteQuota: (path: string) => Effect.Effect<void, JuicefsError>;
  /** Counts what the directory `path` holds anew and repairs its quota's usage where it differs. */
  readonly repairQuota: (path: string) => Effect.Effect<void, JuicefsError>;
  /** Whether the directory `path`'s quota's usage is what it holds, as a check without repair says. */
  readonly quotaHolds: (path: string) => Effect.Effect<boolean, JuicefsError>;
  /** Clones the directory `source`, a path of the node's mount, into `destination`, which is not there yet, its parent being. */
  readonly clone: (source: string, destination: string) => Effect.Effect<void, JuicefsError>;
}

/** The binary units JuiceFS says sizes in. */
const UNITS: Readonly<Record<string, number>> = { B: 1, KiB: 1024, MiB: 1024 ** 2, GiB: 1024 ** 3, TiB: 1024 ** 4, PiB: 1024 ** 5 };

/** `said`, a size as JuiceFS says one (`1.0 GiB`, `964 MiB`, `0 B`), in bytes; null where it is none. */
const bytesOf = (said: string): number | null => {
  const [, number, unit] = /^(\d+(?:\.\d+)?) ?([KMGTP]iB|B)$/u.exec(said.trim()) ?? [];
  return number === undefined || unit === undefined ? null : Math.round(Number(number) * (UNITS[unit] ?? 0));
};

/**
 * The quota of `path` in what `juicefs quota` printed, its table's row of `path`: its size
 * and what of it is used, as JuiceFS says them, to the precision it says them; null where
 * the table has no such row.
 */
export function quotaOf(printed: string, path: string): Quota | null {
  for (const line of printed.split("\n")) {
    const cells = line.split("|").map((cell) => cell.trim());
    // `| <path> | <size> | <used> | <use%> | <inodes> | <iused> | <iuse%> |`
    if (cells[1] !== path) continue;
    const capacityBytes = bytesOf(cells[2] ?? "");
    const usedBytes = bytesOf(cells[3] ?? "");
    return capacityBytes === null || usedBytes === null ? null : { capacityBytes, usedBytes };
  }
  return null;
}

/** What makeJuicefs is given: the metadata engine's address, its password in it, and, but for tests, the binary. */
export interface JuicefsOptions {
  readonly metaUrl: string;
  readonly juicefs?: string;
}

/** The image's juicefs. */
export const JUICEFS = "/usr/local/bin/juicefs";

/** How long a command of JuiceFS's may take: the metadata engine may not answer, and a clone copies a tree's metadata. */
const COMMAND_TIMEOUT_MS = 120_000;

/** JuiceFS's command, as the module says. */
export function makeJuicefs({ metaUrl, juicefs = JUICEFS }: JuicefsOptions): Juicefs {
  const url = new URL(metaUrl);
  const password = decodeURIComponent(url.password);
  url.password = "";
  const address = url.toString();
  /** What `juicefs <args>` printed; its last line of errors where it failed. */
  const run = (what: string, args: readonly string[]): Effect.Effect<string, JuicefsError> =>
    Effect.tryPromise({
      try: (signal) =>
        new Promise<string>((resolve, reject) => {
          execFile(juicefs, args, { env: { META_PASSWORD: password, PATH: "/usr/bin:/bin", HOME: "/tmp" }, signal, timeout: COMMAND_TIMEOUT_MS }, (error, stdout, stderr) =>
            error ? reject(new Error(stderr.trim().split("\n").at(-1) || error.message)) : resolve(stdout));
        }),
      catch: (cause) => new JuicefsError({ reason: `juicefs ${what} failed: ${cause instanceof Error ? cause.message : String(cause)}` }),
    });
  return {
    setQuota: (path, bytes) => Effect.asVoid(run("quota set", ["quota", "set", address, "--path", path, "--capacity", String(Math.max(1, Math.ceil(bytes / 1024 ** 3)))])),
    quota: (path) => Effect.map(run("quota get", ["quota", "get", address, "--path", path]), (printed) => quotaOf(printed, path)),
    deleteQuota: (path) => Effect.asVoid(run("quota delete", ["quota", "delete", address, "--path", path])),
    repairQuota: (path) => Effect.asVoid(run("quota check", ["quota", "check", address, "--path", path, "--repair"])),
    // A check that finds the usage off fails, saying the quota is inconsistent.
    quotaHolds: (path) =>
      run("quota check", ["quota", "check", address, "--path", path]).pipe(
        Effect.as(true),
        Effect.catchIf((error) => error.reason.includes(`quota of ${path} is inconsistent`), () => Effect.succeed(false)),
      ),
    // Its files' owners and modes as they are, not the agent's, which runs it as root.
    clone: (source, destination) => Effect.asVoid(run("clone", ["clone", "--preserve", source, destination])),
  };
}
