/**
 * Workspace storage's quotas, a workspace's directory's each, as JuiceFS's own command sets
 * and reads them (`juicefs quota`): the agent image's, copied from the very mount image the
 * nodes mount workspace storage with (../../Dockerfile), against the metadata engine alone. It
 * mounts nothing, and holds no key of the object store: a quota is the metadata's.
 *
 * The metadata engine's address is given whole, as the stack's setup makes it, its password
 * in it; the command is given it without, the password in META_PASSWORD, which JuiceFS reads,
 * so no other process of the node, whose PIDs the agent shares, reads it in its arguments.
 */
import { execFile } from "node:child_process";

import { Effect, Schema } from "effect";

/** A quota's command failed, or said what is not one: what, and why. */
export class QuotaError extends Schema.TaggedError<QuotaError>()("QuotaError", { reason: Schema.String }) {
  override get message(): string {
    return this.reason;
  }
}

/** A directory's quota, and what it holds of it, in bytes. */
export interface Quota {
  readonly capacityBytes: number;
  readonly usedBytes: number;
}

/** The quotas; see makeQuotas. */
export interface Quotas {
  /** Sets the directory `path`'s quota, of the file system's root, to `bytes`, rounded up to a GiB, as JuiceFS counts quotas. */
  readonly set: (path: string, bytes: number) => Effect.Effect<void, QuotaError>;
  /** The directory `path`'s quota, and what it holds; null where it has none. */
  readonly get: (path: string) => Effect.Effect<Quota | null, QuotaError>;
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

/** What makeQuotas is given: the metadata engine's address, its password in it, and, but for tests, the binary. */
export interface QuotasOptions {
  readonly metaUrl: string;
  readonly juicefs?: string;
}

/** The image's juicefs. */
export const JUICEFS = "/usr/local/bin/juicefs";

/** How long a quota's command may take: the metadata engine may not answer. */
const QUOTA_TIMEOUT_MS = 30_000;

/** The quotas, as the module says. */
export function makeQuotas({ metaUrl, juicefs = JUICEFS }: QuotasOptions): Quotas {
  const url = new URL(metaUrl);
  const password = decodeURIComponent(url.password);
  url.password = "";
  const address = url.toString();
  /** What `juicefs quota <command> <address> <args>` printed; its last line of errors where it failed. */
  const quota = (command: "set" | "get", args: readonly string[]): Effect.Effect<string, QuotaError> =>
    Effect.tryPromise({
      try: (signal) =>
        new Promise<string>((resolve, reject) => {
          execFile(juicefs, ["quota", command, address, ...args], { env: { META_PASSWORD: password, PATH: "/usr/bin:/bin" }, signal, timeout: QUOTA_TIMEOUT_MS }, (error, stdout, stderr) =>
            error ? reject(new Error(stderr.trim().split("\n").at(-1) || error.message)) : resolve(stdout));
        }),
      catch: (cause) => new QuotaError({ reason: `juicefs quota ${command} failed: ${cause instanceof Error ? cause.message : String(cause)}` }),
    });
  return {
    set: (path, bytes) => Effect.asVoid(quota("set", ["--path", path, "--capacity", String(Math.max(1, Math.ceil(bytes / 1024 ** 3)))])),
    get: (path) => Effect.map(quota("get", ["--path", path]), (printed) => quotaOf(printed, path)),
  };
}
