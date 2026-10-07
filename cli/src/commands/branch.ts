/** `alasio branch create|list|delete`: branch environments, a different alasio on a copy-on-write copy of alasio's data. */
import { Config, Console, Effect, FileSystem, Option, Predicate, Redacted } from "effect";
import { Argument, Command, Flag } from "effect/cli";

import { createBranch, deleteBranch, listBranches, memoryText } from "../branches.ts";
import { loadConfig } from "../config.ts";
import { readTelegramBot, type TelegramBot } from "../secrets.ts";
import { TelegramBotApi } from "../telegram.ts";
import { onCluster, timeoutFlag, waitOptions } from "./common.ts";
import { SettingInvalid, SettingMissing } from "./init.ts";

const name = Argument.String("name").pipe(Argument.withDescription("The branch's name: lowercase letters, digits and inner hyphens, at most 30 characters"));

/** How long ago `since` was, as people say it: in minutes, hours, or days. */
function age(since: Date, now: number): string {
  const minutes = Math.max(0, Math.round((now - since.getTime()) / 60_000));
  return minutes < 120 ? `${minutes}m` : minutes < 2 * 24 * 60 ? `${Math.round(minutes / 60)}h` : `${Math.round(minutes / (24 * 60))}d`;
}

/** The settings a branch has over main's: a JSON object, as the config's `install` holds them. */
const readOverrides = Effect.fnUntraced(function*(path: Option.Option<string>) {
  if (Option.isNone(path)) return {};
  const fs = yield* FileSystem.FileSystem;
  const text = yield* fs.readFileString(path.value);
  const parsed = yield* Effect.try({
    try: (): unknown => JSON.parse(text),
    catch: (cause) => new SettingInvalid({ setting: "--overrides", reason: `is not JSON: ${cause instanceof Error ? cause.message : String(cause)}` }),
  });
  if (!Predicate.isObject(parsed) || Array.isArray(parsed)) return yield* new SettingInvalid({ setting: "--overrides", reason: "must be a JSON object of install settings" });
  return parsed as Readonly<Record<string, unknown>>;
});

/** An image named as Docker names one, `repository[:tag][@digest]`, as the install configuration holds it. */
function imageSetting(reference: string): { readonly repository: string; readonly tag: string; readonly digest: string } {
  const [named = "", digest = ""] = reference.split("@");
  const slash = named.lastIndexOf("/");
  const colon = named.lastIndexOf(":");
  return colon > slash ? { repository: named.slice(0, colon), tag: named.slice(colon + 1), digest } : { repository: named, tag: "", digest };
}

/** The branch's bot: its token from a file or the environment, checked with Telegram; and who may use it, main's users unless given. */
const chooseBot = Effect.fnUntraced(function*(tokenFile: Option.Option<string>, allowedUserIds: Option.Option<string>) {
  const fs = yield* FileSystem.FileSystem;
  const telegram = yield* TelegramBotApi;
  const token = Option.isSome(tokenFile)
    ? Option.some(Redacted.make((yield* fs.readFileString(tokenFile.value)).trim()))
    : yield* Config.option(Config.Redacted("TELEGRAM_BOT_TOKEN"));
  if (Option.isNone(token)) return yield* new SettingMissing({ setting: "the branch's bot's token", give: "--bot-token-file or TELEGRAM_BOT_TOKEN" });
  const username = yield* telegram.getMe(token.value);
  const ids = Option.isSome(allowedUserIds)
    ? allowedUserIds.value.split(",").map((id) => id.trim()).filter(Boolean)
    : (yield* readTelegramBot)?.allowedUserIds ?? [];
  if (ids.length === 0 || !ids.every((id) => /^\d+$/u.test(id))) return yield* new SettingInvalid({ setting: "--allowed-user-ids", reason: "must be Telegram user ids, comma-separated" });
  return { token: token.value, username, allowedUserIds: ids } satisfies TelegramBot;
});

const create = Command.make(
  "create",
  {
    name,
    botTokenFile: Flag.String("bot-token-file").pipe(
      Flag.optional,
      Flag.withMetavar("path"),
      Flag.withDescription("A file holding the branch's own bot's token, from @BotFather; or set TELEGRAM_BOT_TOKEN"),
    ),
    allowedUserIds: Flag.String("allowed-user-ids").pipe(
      Flag.optional,
      Flag.withMetavar("ids"),
      Flag.withDescription("The Telegram user ids allowed to use the branch's bot, comma-separated; main's unless given"),
    ),
    image: Flag.String("image").pipe(
      Flag.optional,
      Flag.withMetavar("reference"),
      Flag.withDescription("The image the branch's alasio runs, repository[:tag][@digest], rather than main's"),
    ),
    overrides: Flag.String("overrides").pipe(
      Flag.optional,
      Flag.withMetavar("path"),
      Flag.withDescription("A JSON file of install settings the branch has over main's, as the config's install holds them, such as {\"alasio\": {\"defaultHarness\": \"codex\"}}"),
    ),
    timeout: timeoutFlag,
  },
  ({ name, botTokenFile, allowedUserIds, image, overrides, timeout }) =>
    Effect.gen(function*() {
      const config = yield* loadConfig;
      const settings = yield* readOverrides(overrides);
      const withImage = Option.match(image, { onNone: () => settings, onSome: (reference) => ({ ...settings, images: { alasio: imageSetting(reference) } }) });
      yield* onCluster(() =>
        Effect.gen(function*() {
          const bot = yield* chooseBot(botTokenFile, allowedUserIds);
          yield* createBranch(config, { name, bot, overrides: withImage }, waitOptions(timeout));
          yield* Console.log(`The branch ${name} runs, on a copy of alasio's data as it is now. Message @${bot.username} on Telegram to talk to it; alasio branch delete ${name} deletes it.`);
        })
      );
    }),
).pipe(
  Command.withShortDescription("Make a branch environment"),
  Command.withDescription(
    "Makes the branch environment name: a branch of alasio's Neon at its database's latest commit, and on it an alasio, a compute " +
      "and a lake of the branch's own, in the namespaces alasio-branch-<name> and alasio-branch-<name>-sessions, serving its own " +
      "bot (--bot-token-file), and running another image (--image) or other settings (--overrides) when given; and waits until it " +
      "runs, past --timeout saying what still waits and why. It says when no node of the cluster has the memory a branch takes, " +
      "about what main's alasio, compute and lake use as it is measured then.",
  ),
);

const list = Command.make("list", {}, () =>
  onCluster(() =>
    Effect.gen(function*() {
      const branches = yield* listBranches;
      if (branches.length === 0) return yield* Console.log("alasio has no branch environments.");
      const now = Date.now();
      for (const branch of branches) {
        const old = branch.createdAt === null ? "of no age known" : `${age(new Date(branch.createdAt), now)} old`;
        const alasio = branch.ready === null ? "no alasio" : branch.ready ? "its alasio ready" : "its alasio not ready";
        const memory = `${memoryText(branch.requested)} requested${branch.used === null ? "" : `, ${memoryText(branch.used)} used`}`;
        yield* Console.log(`${branch.name}: a branch of ${branch.parent ?? "nothing Neon has"}, ${old}, ${alasio}, ${memory}${branch.neon && branch.neon !== "ready" ? `, Neon's branch ${branch.neon}` : ""}`);
      }
    })
  )).pipe(
    Command.withShortDescription("List the branch environments"),
    Command.withDescription("Says each branch environment's name, what it is a branch of, its age, whether its alasio runs, and the memory its alasio, compute and lake request and use."),
  );

const remove = Command.make(
  "delete",
  {
    name,
    force: Flag.Boolean("force").pipe(Flag.withDefault(false), Flag.withDescription("Delete it even while its alasio has a turn running, or when that cannot be read")),
    timeout: timeoutFlag,
  },
  ({ name, force, timeout }) =>
    onCluster(() =>
      Effect.gen(function*() {
        yield* deleteBranch(name, { force }, waitOptions(timeout));
        yield* Console.log(`The branch ${name} is deleted, with all it had; main's data is as it was.`);
      })
    ),
).pipe(
  Command.withShortDescription("Delete a branch environment"),
  Command.withDescription(
    "Deletes the branch environment name: stops its alasio and lake, deletes its sessions and their volumes (JuiceFS keeps every " +
      "chunk main still uses), stops its compute, deletes its branch of Neon's, and its namespaces with its Secrets. It refuses " +
      "while the branch's alasio has a turn running, as its database says, unless --force.",
  ),
);

export const branch = Command.make("branch").pipe(
  Command.withSubcommands([create, list, remove]),
  Command.withShortDescription("Run a different alasio on a copy of alasio's data"),
  Command.withDescription(
    "A branch environment is a different alasio (another image, other settings) on a copy-on-write copy of alasio's data, made " +
      "in seconds without copying any: alasio's state and transcripts are a branch of its Neon, and the lake's catalog with them. " +
      "It serves a bot of its own and changes nothing of main's. What it shares with main is what is never branched: Neon's " +
      "storage, the object store, the lake's files (which main's maintenance keeps while any branch exists, and deletes once " +
      "none does, with what branches wrote), the telemetry collector, and JuiceFS. Its telemetry goes to main's lake, its " +
      "resource's alasio.branch its name. A session workspace its conversations inherited is forked from main's, " +
      "copy-on-write, as the branch first uses it, unless main has a turn running in it then; main's sessions are never " +
      "touched by the branch. A folder workspace is refused, as the machine's own files cannot be copied. What was in " +
      "flight in main as it was branched (queued prompts, unsent replies, buttons) is dropped, and so is Codex's login, " +
      "whose refresh token works once: alasio login codex --branch logs the branch's Codex in. Its credentials are its own " +
      "(its database's password, its lake's, its compute's token), none of which reaches main's database, but for two that " +
      "cannot be narrowed: its lake has main's object store identity, as it reads main's files where they are and SeaweedFS's " +
      "write permission deletes too; and its compute has Neon's storage token, the tenant's, as Neon's storage has no " +
      "narrower one. A branch's pods reach main's storage services and nothing else of main's. A branch takes about the " +
      "memory main's alasio, compute and lake take. Branches live until deleted.",
  ),
);

