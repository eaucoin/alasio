/**
 * `alasio init`: asks for what alasio needs (its bot's token and who may use it, Claude
 * Code's token, whether agents work in this machine's folders, where telemetry goes),
 * writes the config, makes the cluster on this machine when that is where alasio runs,
 * writes the Secrets there, and offers to start alasio.
 *
 * Run again, it shows what is set, and keeps what is not changed. Every question has a
 * flag that answers it instead, and --non-interactive asks none, for scripts and CI.
 * Secrets are never flags, as a command's arguments are visible to every user of the
 * machine: they are read from a file, or from the environment.
 */
import { createServer } from "node:net";
import { homedir } from "node:os";

import { Config, Console, Effect, FileSystem, Option, Predicate, Redacted, Result, Schema } from "effect";
import { Command, Flag, Prompt } from "effect/cli";

import { DockerCluster } from "../cluster/docker.ts";
import { requireLocalMachine } from "../cluster/machine.ts";
import { configPath, decodeOperatorConfig, defaultStoragePath, type OperatorConfigFile, readConfig, writeConfig } from "../config.ts";
import { applyNamespace } from "../install.ts";
import { OptionalUrl } from "../manifests/config.ts";
import { readClaudeToken, readTelegramBot, type TelegramBot, writeClaudeToken, writeTelegramBot } from "../secrets.ts";
import { dockerCluster, kubeApi, type ResolvedTarget, resolveTarget } from "../target.ts";
import { TelegramBotApi } from "../telegram.ts";
import { timeoutFlag } from "./common.ts";
import { bringUp, ensureCluster } from "./up.ts";

/** Something init must be told, and was not, with no questions to ask. */
export class SettingMissing extends Schema.TaggedError<SettingMissing>()("SettingMissing", {
  setting: Schema.String,
  give: Schema.String,
}) {
  override get message(): string {
    return `alasio init needs ${this.setting}: give it with ${this.give}, or run alasio init without --non-interactive`;
  }
}

/** A setting given is not one alasio takes. */
export class SettingInvalid extends Schema.TaggedError<SettingInvalid>()("SettingInvalid", {
  setting: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `${this.setting} ${this.reason}`;
  }
}

/** A port of the loopback no one listens on now, which the API server of the cluster in Docker keeps from then on. */
const freePort = Effect.callback<number>((resume) => {
  const server = createServer();
  server.listen(0, "127.0.0.1", () => {
    const address = server.address();
    server.close(() => resume(Effect.succeed(typeof address === "object" && address !== null ? address.port : 0)));
  });
});

/** Telegram user ids, comma-separated, as the bot's Secret keeps them: null when they are not. */
function parseUserIds(text: string): string[] | null {
  const ids = text.split(",").map((id) => id.trim()).filter(Boolean);
  return ids.length > 0 && ids.every((id) => /^\d+$/u.test(id)) ? ids : null;
}

/** Absolute paths, comma-separated: null when one is not absolute. */
function parsePaths(text: string): string[] | null {
  const paths = text.split(",").map((path) => path.trim()).filter(Boolean);
  return paths.every((path) => path.startsWith("/")) ? paths : null;
}

/** A host mount's name for `path`: its segments, as a volume's name may have them, unlike any `taken`. */
function mountName(path: string, taken: ReadonlySet<string>): string {
  const base = path.toLowerCase().replace(/[^a-z0-9]+/gu, "-").replace(/^-+|-+$/gu, "").slice(0, 55) || "root";
  let name = base;
  for (let suffix = 2; taken.has(name); suffix += 1) name = `${base}-${suffix}`;
  return name;
}

/** The user agents run as in folder workspaces, their home, and the other folders they see. */
interface FolderAccess {
  readonly uid: number;
  readonly gid: number;
  readonly home: string;
  readonly folders: readonly string[];
}

/** A mount of the host profile as the config holds it: a machine path at a path in the pods, and its other keys. */
type HostMount = Readonly<Record<string, unknown>> & { readonly name: string; readonly hostPath: string; readonly mountPath: string };

/** Whether `value` is a mount of the host profile. */
const isHostMount = (value: unknown): value is HostMount =>
  Predicate.hasProperty(value, "name") && Predicate.isString(value.name) &&
  Predicate.hasProperty(value, "hostPath") && Predicate.isString(value.hostPath) &&
  Predicate.hasProperty(value, "mountPath") && Predicate.isString(value.mountPath);

/**
 * The host profile of the install configuration `host` is, with `access`, or without
 * folder workspaces when it is null; its other keys kept. A folder it mounts already
 * keeps its mount as written (where in the pods, its type, read-only), and a new one is
 * mounted at its own path. The home is always in the pods, as alasio keeps its state
 * there, and bayma's in it: mounted, unless a mount already puts it there.
 */
function hostSettings(host: Readonly<Record<string, unknown>>, access: FolderAccess | null): Record<string, unknown> {
  if (!access) return { ...host, enabled: false };
  const existing = (Array.isArray(host["mounts"]) ? host["mounts"] : []).filter(isHostMount);
  const chosen = [...new Set([access.home, ...access.folders])];
  const kept = existing.filter(({ hostPath }) => chosen.includes(hostPath));
  const placed = (path: string) => kept.some(({ mountPath }) => path === mountPath || path.startsWith(`${mountPath.replace(/\/$/u, "")}/`));
  const taken = new Set(kept.map(({ name }) => name));
  const added = chosen
    .filter((path) => !kept.some(({ hostPath }) => hostPath === path) && !(path === access.home && placed(path)))
    .map((path) => {
      const name = mountName(path, taken);
      taken.add(name);
      return { name, hostPath: path, mountPath: path };
    });
  const stateRoot = host["stateRoot"];
  return {
    ...host,
    enabled: true,
    uid: access.uid,
    gid: access.gid,
    home: access.home,
    stateRoot: Predicate.isString(stateRoot) && stateRoot.startsWith(`${access.home}/`) ? stateRoot : `${access.home}/.alasio/bayma`,
    mounts: [...kept, ...added],
  };
}

/** The record `key` of `parent`; empty when it is not one. */
function record(parent: Readonly<Record<string, unknown>>, key: string): Readonly<Record<string, unknown>> {
  const value = parent[key];
  return Predicate.isObject(value) && !Array.isArray(value) ? (value as Readonly<Record<string, unknown>>) : {};
}

/** `record` without `key`. */
function without(of: Readonly<Record<string, unknown>>, key: string): Record<string, unknown> {
  return Object.fromEntries(Object.entries(of).filter(([each]) => each !== key));
}

/** A prompt's validation by `check`, which says what is wrong with a value, or null. */
const validating = (check: (value: string) => string | null) => (value: string): Effect.Effect<string, string> => {
  const refusal = check(value);
  return refusal === null ? Effect.succeed(value) : Effect.fail(refusal);
};

const flags = {
  kubeconfig: Flag.String("kubeconfig").pipe(
    Flag.optional,
    Flag.withMetavar("path"),
    Flag.withDescription("Run alasio in the cluster this kubeconfig reaches, rather than one alasio makes on this machine"),
  ),
  context: Flag.String("context").pipe(Flag.optional, Flag.withMetavar("name"), Flag.withDescription("The kubeconfig's context to use, rather than its current one")),
  botTokenFile: Flag.String("bot-token-file").pipe(
    Flag.optional,
    Flag.withMetavar("path"),
    Flag.withDescription("A file holding the bot's token, from @BotFather; or set TELEGRAM_BOT_TOKEN"),
  ),
  allowedUserIds: Flag.String("allowed-user-ids").pipe(
    Flag.optional,
    Flag.withMetavar("ids"),
    Flag.withDescription("The Telegram user ids allowed to use the bot, comma-separated"),
  ),
  claudeTokenFile: Flag.String("claude-token-file").pipe(
    Flag.optional,
    Flag.withMetavar("path"),
    Flag.withDescription("A file holding Claude Code's token, from claude setup-token; or set CLAUDE_CODE_OAUTH_TOKEN"),
  ),
  folderWorkspaces: Flag.Boolean("folder-workspaces").pipe(
    Flag.optional,
    Flag.withDescription("Let agents work in this machine's folders, as you; --no-folder-workspaces does not"),
  ),
  user: Flag.String("user").pipe(Flag.optional, Flag.withMetavar("uid:gid"), Flag.withDescription("Whom agents in folders run as; you, unless given")),
  home: Flag.String("home").pipe(Flag.optional, Flag.withMetavar("path"), Flag.withDescription("Their home, always among the folders they see; yours, unless given")),
  folders: Flag.String("folders").pipe(
    Flag.optional,
    Flag.withMetavar("paths"),
    Flag.withDescription("The other folders they see, comma-separated absolute paths"),
  ),
  telemetryEndpoint: Flag.String("telemetry-endpoint").pipe(
    Flag.optional,
    Flag.withMetavar("url"),
    Flag.withDescription("Where alasio sends OpenTelemetry, an OTLP/HTTP endpoint; an empty one sends nothing"),
  ),
  nonInteractive: Flag.Boolean("non-interactive").pipe(
    Flag.withDefault(false),
    Flag.withDescription("Ask nothing: what is not given keeps its current value or default, and what has neither fails"),
  ),
  up: Flag.Boolean("up").pipe(Flag.optional, Flag.withDescription("Start alasio afterwards without asking; --no-up does not")),
  timeout: timeoutFlag,
};

type Flags = Command.Command.Config.Infer<typeof flags>;

/** The answer of `flag`, else of the question `asked` when questions are asked, else `otherwise`. */
const answer = <A, E1, R1, E2, R2>(
  flag: Option.Option<A>,
  ask: boolean,
  asked: () => Effect.Effect<A, E1, R1>,
  otherwise: () => Effect.Effect<A, E2, R2>,
): Effect.Effect<A, E1 | E2, R1 | R2> => (Option.isSome(flag) ? Effect.succeed(flag.value) : ask ? asked() : otherwise());

/** Where alasio runs: as the flags say, else as the config does, else as the operator answers; here unless they say otherwise. */
const chooseTarget = Effect.fnUntraced(function*(given: Flags, ask: boolean, existing: OperatorConfigFile | null) {
  if (Option.isSome(given.kubeconfig) || Option.isSome(given.context)) {
    return { kubeconfig: { ...Option.match(given.kubeconfig, { onNone: () => ({}), onSome: (path) => ({ path }) }), ...Option.match(given.context, { onNone: () => ({}), onSome: (context) => ({ context }) }) } };
  }
  if (existing) return existing.target;
  const where = ask
    ? yield* Prompt.Select({
      message: "Where should alasio run?",
      choices: [
        { title: "Here", value: "docker", description: "in a cluster alasio makes on this machine, in Docker" },
        { title: "In a cluster of mine", value: "kubeconfig", description: "one a kubeconfig reaches" },
      ],
    })
    : "docker";
  if (where === "kubeconfig") {
    const path = yield* Prompt.String({
      message: "Its kubeconfig (empty for $KUBECONFIG, or ~/.kube/config)",
      validate: validating((value) => (value === "" || value.startsWith("/") ? null : "must be an absolute path")),
    });
    const context = yield* Prompt.String({ message: "Its context (empty for the kubeconfig's current one)" });
    return { kubeconfig: { ...(path ? { path } : {}), ...(context ? { context } : {}) } };
  }
  return { docker: { name: "alasio", apiPort: yield* freePort, storagePath: yield* defaultStoragePath } };
});

/** The Secrets the cluster has now, when it can be reached without being started: none for a cluster in Docker not running. */
const currentSecrets = Effect.fnUntraced(function*(target: ResolvedTarget) {
  if (target._tag === "Docker") {
    const { nodes } = yield* Effect.provide(Effect.flatMap(DockerCluster, (cluster) => cluster.status), dockerCluster(target.cluster));
    if (!nodes.some(({ role, state }) => role === "server" && state === "running")) return { bot: null, claude: null };
  }
  return yield* Effect.provide(Effect.all({ bot: readTelegramBot, claude: readClaudeToken }), kubeApi(target));
});

/** The bot: its token from a file, the environment or the operator, checked with Telegram, or the current one; and who may use it. */
const chooseBot = Effect.fnUntraced(function*(given: Flags, ask: boolean, current: TelegramBot | null) {
  const fs = yield* FileSystem.FileSystem;
  const telegram = yield* TelegramBotApi;
  const fromFlagOrEnv = Option.isSome(given.botTokenFile)
    ? Option.some(Redacted.make((yield* fs.readFileString(given.botTokenFile.value)).trim()))
    : yield* Config.option(Config.Redacted("TELEGRAM_BOT_TOKEN"));
  let token: Redacted.Redacted;
  let username: string;
  if (Option.isSome(fromFlagOrEnv)) {
    token = fromFlagOrEnv.value;
    username = yield* telegram.getMe(token);
  } else if (ask) {
    // The prompt checks a token with Telegram before it takes it, learning its bot.
    let checked = current?.username ?? "";
    const entered = Redacted.value(
      yield* Prompt.Password({
        message: `The bot's token, from @BotFather${current ? ` (empty keeps @${current.username})` : ""}`,
        validate: (value) =>
          value.trim() === "" && current
            ? Effect.succeed(value)
            : telegram.getMe(Redacted.make(value.trim())).pipe(
              Effect.map((name) => {
                checked = name;
                return value;
              }),
              Effect.mapError((error) => error.message),
            ),
      }),
    ).trim();
    token = entered === "" && current ? current.token : Redacted.make(entered);
    username = checked;
  } else if (current) {
    ({ token, username } = current);
  } else {
    return yield* new SettingMissing({ setting: "the bot's token", give: "--bot-token-file or TELEGRAM_BOT_TOKEN" });
  }
  const idsText = yield* answer(
    given.allowedUserIds,
    ask,
    () =>
      Prompt.String({
        message: "The Telegram user ids allowed to use it, comma-separated (@userinfobot tells yours)",
        default: current?.allowedUserIds.join(",") ?? "",
        validate: validating((value) => (parseUserIds(value) ? null : "must be Telegram user ids, comma-separated")),
      }),
    () => (current ? Effect.succeed(current.allowedUserIds.join(",")) : Effect.fail(new SettingMissing({ setting: "the Telegram users allowed to use the bot", give: "--allowed-user-ids" }))),
  );
  const allowedUserIds = parseUserIds(idsText);
  if (!allowedUserIds) return yield* new SettingInvalid({ setting: "--allowed-user-ids", reason: "must be Telegram user ids, comma-separated" });
  return { token, username, allowedUserIds } satisfies TelegramBot;
});

/** Claude Code's token, from a file, the environment or the operator; none to keep the current one, or to leave it for later. */
const chooseClaudeToken = Effect.fnUntraced(function*(given: Flags, ask: boolean, current: Redacted.Redacted | null) {
  const fs = yield* FileSystem.FileSystem;
  if (Option.isSome(given.claudeTokenFile)) return Option.some(Redacted.make((yield* fs.readFileString(given.claudeTokenFile.value)).trim()));
  const fromEnv = yield* Config.option(Config.Redacted("CLAUDE_CODE_OAUTH_TOKEN"));
  if (Option.isSome(fromEnv) || !ask) return fromEnv;
  const entered = Redacted.value(
    yield* Prompt.Password({
      message: `Claude Code's token, from claude setup-token (empty ${current ? "keeps the current one" : "leaves it for later"})`,
      validate: validating((value) => (/\s/u.test(value.trim()) ? "must be one token, without spaces" : null)),
    }),
  ).trim();
  return entered ? Option.some(Redacted.make(entered)) : Option.none();
});

/** Whether agents work in this machine's folders, and if so as whom, in which. */
const chooseFolderAccess = Effect.fnUntraced(function*(given: Flags, ask: boolean, host: Readonly<Record<string, unknown>>) {
  const wanted = yield* answer(
    given.folderWorkspaces,
    ask,
    () => Prompt.Confirm({ message: "Should agents also be able to work in this machine's folders, as you?", initial: host["enabled"] === true }),
    () => Effect.succeed(host["enabled"] === true),
  );
  if (!wanted) return null;
  const uid = Predicate.isNumber(host["uid"]) ? host["uid"] : process.getuid?.() ?? 1000;
  const gid = Predicate.isNumber(host["gid"]) ? host["gid"] : process.getgid?.() ?? 1000;
  const userText = yield* answer(
    given.user,
    ask,
    () => Prompt.String({ message: "Whom they run as, uid:gid", default: `${uid}:${gid}`, validate: validating((value) => (/^\d+:\d+$/u.test(value) ? null : "must be uid:gid")) }),
    () => Effect.succeed(`${uid}:${gid}`),
  );
  const user = /^(\d+):(\d+)$/u.exec(userText);
  if (!user) return yield* new SettingInvalid({ setting: "--user", reason: "must be uid:gid" });
  const currentHome = Predicate.isString(host["home"]) ? host["home"] : homedir();
  const home = yield* answer(
    given.home,
    ask,
    () => Prompt.String({ message: "Their home, which alasio keeps its state in", default: currentHome, validate: validating((value) => (value.startsWith("/") ? null : "must be an absolute path")) }),
    () => Effect.succeed(currentHome),
  );
  if (!home.startsWith("/")) return yield* new SettingInvalid({ setting: "--home", reason: "must be an absolute path" });
  const currentFolders = (Array.isArray(host["mounts"]) ? host["mounts"] : [])
    .flatMap((mount: unknown) => (Predicate.hasProperty(mount, "hostPath") && Predicate.isString(mount.hostPath) && mount.hostPath !== home ? [mount.hostPath] : []));
  const foldersText = yield* answer(
    given.folders,
    ask,
    () =>
      Prompt.String({
        message: "Which other folders they see, comma-separated (empty for none)",
        default: currentFolders.join(","),
        validate: validating((value) => (parsePaths(value) ? null : "must be absolute paths")),
      }),
    () => Effect.succeed(currentFolders.join(",")),
  );
  const folders = parsePaths(foldersText);
  if (!folders) return yield* new SettingInvalid({ setting: "--folders", reason: "must be absolute paths" });
  return { uid: Number(user[1]), gid: Number(user[2]), home, folders } satisfies FolderAccess;
});

/** Where telemetry goes: an OTLP/HTTP endpoint, or empty for nowhere. */
const chooseTelemetryEndpoint = Effect.fnUntraced(function*(given: Flags, ask: boolean, current: string) {
  const check = (value: string) => (Result.isSuccess(Schema.decodeUnknownResult(OptionalUrl)(value)) ? null : "must be an http(s) URL, or empty");
  const endpoint = yield* answer(
    given.telemetryEndpoint,
    ask,
    () => Prompt.String({ message: "Where alasio sends OpenTelemetry, an OTLP/HTTP endpoint (empty for nowhere)", default: current, validate: validating(check) }),
    () => Effect.succeed(current),
  );
  const refused = check(endpoint);
  if (refused) return yield* new SettingInvalid({ setting: "--telemetry-endpoint", reason: refused });
  return endpoint;
});

export const init = Command.make("init", flags, (given) =>
  Effect.gen(function*() {
    const ask = !given.nonInteractive;
    const path = yield* configPath;
    const existing = yield* readConfig(path);
    if (existing) yield* Console.log(`alasio's config is ${path}; what you leave as it is stays.`);
    const install = existing?.install ?? {};
    const target = yield* chooseTarget(given, ask, existing?.file ?? null);
    const resolved = yield* resolveTarget(yield* Effect.fromResult(decodeOperatorConfig(path, { target, install })));
    // Before any question, as the cluster here cannot run on another machine.
    if (resolved._tag === "Docker") yield* requireLocalMachine;
    const current = yield* currentSecrets(resolved);

    const bot = yield* chooseBot(given, ask, current.bot);
    const claude = yield* chooseClaudeToken(given, ask, current.claude);
    const host = record(install, "host");
    const access = yield* chooseFolderAccess(given, ask, host);
    const telemetry = record(install, "telemetry");
    const endpoint = yield* chooseTelemetryEndpoint(given, ask, Predicate.isString(telemetry["otlpEndpoint"]) ? telemetry["otlpEndpoint"] : "");

    const telemetrySettings = endpoint ? { ...telemetry, otlpEndpoint: endpoint } : without(telemetry, "otlpEndpoint");
    const config = yield* writeConfig(path, {
      target,
      install: {
        ...without(without(install, "host"), "telemetry"),
        ...(access || Object.keys(host).length > 0 ? { host: hostSettings(host, access) } : {}),
        ...(Object.keys(telemetrySettings).length > 0 ? { telemetry: telemetrySettings } : {}),
      },
    });
    yield* Console.log(`wrote ${path}`);

    const cluster = yield* resolveTarget(config);
    yield* ensureCluster(cluster);
    yield* Effect.provide(
      Effect.gen(function*() {
        yield* applyNamespace;
        yield* writeTelegramBot(bot);
        if (Option.isSome(claude)) yield* writeClaudeToken(claude.value);
      }),
      kubeApi(cluster),
    );
    const hasClaude = Option.isSome(claude) || current.claude !== null;
    yield* Console.log(`alasio's Secrets are in its cluster: the bot @${bot.username}'s token${hasClaude ? ", and Claude Code's" : ""}.`);

    const start = yield* answer(given.up, ask, () => Prompt.Confirm({ message: "Start alasio now?", initial: true }), () => Effect.succeed(false));
    if (start) yield* bringUp(config, given.timeout);
    else yield* Console.log("alasio up starts it.");
  })).pipe(
    Command.withShortDescription("Set alasio up: its bot, its tokens, and where it runs"),
    Command.withDescription(
      "Asks for the bot's token (checked with Telegram) and who may use it, Claude Code's token, whether agents may work in this " +
        "machine's folders and as whom, and where telemetry goes; writes the config, which holds no secret; makes the cluster on " +
        "this machine, unless --kubeconfig names another, once this machine is one it runs on, Linux on x86-64, raising its inotify " +
        "limits, as root, where they are too low for it; writes the tokens there, as Secrets; and offers to start alasio. Run it " +
        "again to change something: it shows what is set and keeps what you leave. Tokens are never flags, which other users of " +
        "the machine can see: give them in files (--bot-token-file, --claude-token-file), or as TELEGRAM_BOT_TOKEN and " +
        "CLAUDE_CODE_OAUTH_TOKEN.",
    ),
  );
