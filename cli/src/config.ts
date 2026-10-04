/**
 * The operator's config: where alasio runs, and how it is installed. It is a JSON file,
 * $XDG_CONFIG_HOME/alasio/config.json (~/.config/alasio/config.json) unless --config says
 * another, readable by its owner alone, in a directory only its owner opens; `alasio
 * init` writes it, and an operator may edit it. It holds no secret: alasio's Secrets are
 * in the cluster alone (./secrets.ts).
 *
 *     {
 *       "target": { "local": { "name": "alasio", "apiPort": 41873 } },
 *       "install": { "telemetry": { "otlpEndpoint": "http://collector.example.com:4318" } }
 *     }
 *
 * The target is the cluster alasio makes on this machine (./cluster/local.ts), with its
 * API server on `apiPort` of the loopback and its volumes in `storagePath`
 * ($XDG_DATA_HOME/alasio/storage unless given), or one a kubeconfig reaches:
 * `{ "kubeconfig": { "path": "/home/me/.kube/config", "context": "prod" } }`, both keys
 * optional. The installation is the install configuration (./manifests/config.ts), every
 * key optional, but for the Secrets of the bot and of Claude Code, which alasio names.
 */
import { randomUUID } from "node:crypto";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";

import { Config, Effect, FileSystem, Option, type PlatformError, Predicate, Result, Schema } from "effect";
import { Flag, GlobalFlag } from "effect/cli";

import { AbsolutePath, decodeInstallConfig, defaulted, describeIssue, type InstallConfig, matching, NonEmpty } from "./manifests/config.ts";
import { CLAUDE_SECRET, TELEGRAM_SECRET } from "./secrets.ts";

/** `--config`, the operator config's path, given before or after the command. */
export const ConfigFlag = GlobalFlag.Setting("config")({
  flag: Flag.String("config").pipe(
    Flag.optional,
    Flag.withMetavar("path"),
    Flag.withDescription("The config file to use, instead of $XDG_CONFIG_HOME/alasio/config.json (~/.config/alasio/config.json)"),
  ),
});

const Port = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65535 }, { message: "must be a port from 1 to 65535" }));
const Ipv4 = matching(/^(\d{1,3}\.){3}\d{1,3}$/u, "must be an IPv4 address");

/** The cluster alasio makes on this machine, its LocalClusterOptions. */
const LocalTarget = Schema.Struct({
  name: defaulted(matching(/^[a-z0-9]([-a-z0-9]{0,40}[a-z0-9])?$/u, "must be a name of lowercase letters, digits and dashes"), "alasio"),
  apiPort: Port.annotateKey({ messageMissingKey: "is required: alasio init chooses one" }),
  subnet: Schema.optionalKey(matching(/^(\d{1,3}\.){3}\d{1,3}\/\d{1,2}$/u, "must be an IPv4 subnet, such as 172.30.0.0/24")),
  hostAliases: defaulted(Schema.Array(Schema.Struct({ ip: Ipv4, hostnames: Schema.Array(NonEmpty).check(Schema.isMinLength(1, { message: "must name a host" })) })), []),
  mounts: defaulted(Schema.Array(Schema.Struct({ source: AbsolutePath, target: AbsolutePath, readOnly: Schema.optionalKey(Schema.Boolean) })), []),
  storagePath: Schema.optionalKey(AbsolutePath),
  agents: defaulted(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 16 }, { message: "must be from 0 to 16" })), 0),
});

/** A cluster a kubeconfig reaches: the default kubeconfig and its current context unless they are given. */
const KubeconfigTarget = Schema.Struct({ path: Schema.optionalKey(AbsolutePath), context: Schema.optionalKey(NonEmpty) });

const Target = Schema.Union([Schema.Struct({ local: LocalTarget }), Schema.Struct({ kubeconfig: KubeconfigTarget })]);

const OperatorConfigSchema = Schema.Struct({
  target: Target.annotateKey({ messageMissingKey: "is required: alasio init writes it" }),
  install: defaulted(Schema.Record(Schema.String, Schema.Unknown), {}),
});

/** What the config file holds. */
export type OperatorConfigFile = typeof OperatorConfigSchema.Encoded;
export type Target = typeof Target.Type;

/** The operator's config, read: where it is, what it holds as written, and as checked with its defaults. */
export interface OperatorConfig {
  readonly path: string;
  readonly file: OperatorConfigFile;
  readonly target: Target;
  /** The install configuration as written, without the Secrets alasio names. */
  readonly install: Readonly<Record<string, unknown>>;
}

/** The config file is not what alasio runs from: where, and which key and why. */
export class OperatorConfigError extends Schema.TaggedError<OperatorConfigError>()("OperatorConfigError", {
  path: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `${this.path}: ${this.reason}`;
  }
}

/** There is no config file yet. */
export class NotInitialized extends Schema.TaggedError<NotInitialized>()("NotInitialized", {
  path: Schema.String,
}) {
  override get message(): string {
    return `there is no alasio config at ${this.path}: alasio init makes one`;
  }
}

/**
 * The install configuration of `install`, with the Secrets alasio names: the bot's,
 * and Claude Code's when `claude`, as alasio finds Claude Code's login in its home when
 * there is none.
 */
export function installConfigOf(install: Readonly<Record<string, unknown>>, { claude }: { readonly claude: boolean }): Result.Result<InstallConfig, string> {
  const alasio = Predicate.isObject(install["alasio"]) ? install["alasio"] : {};
  const own = ["telegram", "claude"].find((key) => Predicate.hasProperty(alasio, key));
  if (own) return Result.fail(`install.alasio.${own} is alasio's own to say: its Secret is the one alasio init writes`);
  return Result.mapBoth(
    decodeInstallConfig({ ...install, alasio: { ...alasio, telegram: { existingSecret: TELEGRAM_SECRET }, claude: { existingSecret: claude ? CLAUDE_SECRET : "" } } }),
    { onFailure: (error) => `install.${error.message}`, onSuccess: (config) => config },
  );
}

/** The config `input` (parsed JSON) is, checked: a key alasio does not know is refused. */
export function decodeOperatorConfig(path: string, input: unknown): Result.Result<OperatorConfig, OperatorConfigError> {
  const decoded = Schema.decodeUnknownResult(OperatorConfigSchema)(input, { onExcessProperty: "error" });
  if (Result.isFailure(decoded)) return Result.fail(new OperatorConfigError({ path, reason: describeIssue(decoded.failure.issue) }));
  const { target, install } = decoded.success;
  const checked = installConfigOf(install, { claude: true });
  if (Result.isFailure(checked)) return Result.fail(new OperatorConfigError({ path, reason: checked.failure }));
  return Result.succeed({ path, file: input as OperatorConfigFile, target, install });
}

/** `$name`, or `fallback` under the home directory when it is unset or empty, as XDG says. */
const xdgHome = (name: string, fallback: string): Effect.Effect<string, Config.ConfigError> =>
  Effect.gen(function*() {
    const given = yield* Config.option(Config.String(name));
    if (Option.isSome(given) && given.value.trim()) return given.value;
    const home = yield* Config.String("HOME").pipe(Config.withDefault(homedir()));
    return join(home, fallback);
  });

/** Where the config file is: --config's path, or $XDG_CONFIG_HOME/alasio/config.json. */
export const configPath: Effect.Effect<string, Config.ConfigError, GlobalFlag.Setting.Identifier<"config">> = Effect.gen(function*() {
  const flag = yield* ConfigFlag;
  if (Option.isSome(flag)) return resolve(flag.value);
  return join(yield* xdgHome("XDG_CONFIG_HOME", ".config"), "alasio", "config.json");
});

/** Where the local cluster's volumes are unless its target says: $XDG_DATA_HOME/alasio/storage. */
export const defaultStoragePath: Effect.Effect<string, Config.ConfigError> = Effect.map(xdgHome("XDG_DATA_HOME", ".local/share"), (data) => join(data, "alasio", "storage"));

/** The kubeconfig of the local cluster, beside the config file. */
export const localKubeconfigPath = (config: Pick<OperatorConfig, "path">): string => join(dirname(config.path), "kubeconfig");

/** The config at `path`, or null when there is none. */
export const readConfig = Effect.fnUntraced(function*(path: string): Effect.fn.Return<OperatorConfig | null, OperatorConfigError | PlatformError.PlatformError, FileSystem.FileSystem> {
  const fs = yield* FileSystem.FileSystem;
  if (!(yield* fs.exists(path))) return null;
  const text = yield* fs.readFileString(path);
  const parsed = yield* Effect.try({
    try: (): unknown => JSON.parse(text),
    catch: (cause) => new OperatorConfigError({ path, reason: `is not JSON: ${cause instanceof Error ? cause.message : String(cause)}` }),
  });
  return yield* Effect.fromResult(decodeOperatorConfig(path, parsed));
});

/** The config at configPath, which must exist. */
export const loadConfig = Effect.gen(function*() {
  const path = yield* configPath;
  const config = yield* readConfig(path);
  if (!config) return yield* new NotInitialized({ path });
  return config;
});

/**
 * Writes `file` to `path`, checked first, readable by its owner alone: whole, beside it,
 * then moved over it. A directory made for it only its owner opens.
 */
export const writeConfig = Effect.fnUntraced(function*(path: string, file: OperatorConfigFile): Effect.fn.Return<OperatorConfig, OperatorConfigError | PlatformError.PlatformError, FileSystem.FileSystem> {
  const config = yield* Effect.fromResult(decodeOperatorConfig(path, file));
  const fs = yield* FileSystem.FileSystem;
  if (!(yield* fs.exists(dirname(path)))) yield* fs.makeDirectory(dirname(path), { recursive: true, mode: 0o700 });
  const written = `${path}.${randomUUID()}`;
  yield* fs.writeFileString(written, `${JSON.stringify(file, null, 2)}\n`, { flag: "wx", mode: 0o600 });
  yield* fs.rename(written, path);
  return config;
});
