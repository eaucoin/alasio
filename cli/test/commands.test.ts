/**
 * alasio's commands (cli/src/commands/), run as an operator runs them, at a terminal
 * that answers their questions, against a fake Docker whose nodes run k3s, a fake
 * Kubernetes API, and a fake Telegram, on a machine whose kernel settings the cluster
 * here needs unless a test gives others.
 */
import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import { Cause, Exit } from "effect";

import { CODEX_LOGIN } from "../src/commands/login.ts";
import { type CliRun, runAlasio } from "./support/cli.ts";
import type { FakeDocker } from "./support/fake-docker.ts";
import { serveK3sInDocker } from "./support/fake-k3s.ts";
import { type FakeKube, serveFakeKube } from "./support/fake-kube.ts";
import { serveFakeTelegram } from "./support/fake-telegram.ts";
import { DOWN, ENTER, pressed, replaced, typed } from "./support/fake-terminal.ts";

const BOT_TOKEN = "123:valid";
const BOT = "alasio_test_bot";
const DEPLOYMENT = "/apis/apps/v1/namespaces/alasio/deployments/alasio";

interface Rig {
  readonly kube: FakeKube;
  readonly docker: FakeDocker;
  readonly home: string;
  /** A kubeconfig that reaches the fake API. */
  readonly kubeconfig: string;
  /** Where the config is, by XDG_CONFIG_HOME. */
  readonly configFile: string;
  /** The environment alasio runs in: its home and XDG directories, and the Docker and Telegram it reaches. */
  readonly env: { readonly HOME: string; readonly XDG_CONFIG_HOME: string; readonly XDG_DATA_HOME: string; readonly DOCKER_HOST: string; readonly TELEGRAM_API_ROOT: string };
  readonly alasio: (args: readonly string[], answers?: Parameters<typeof runAlasio>[1]["answers"]) => Promise<CliRun>;
  /** Writes the config, as an operator would. */
  readonly configure: (config: unknown) => void;
  /** The config as written. */
  readonly config: () => unknown;
}

async function rig(t: TestContext): Promise<Rig> {
  const kube = await serveFakeKube();
  const telegram = await serveFakeTelegram();
  telegram.bots.set(BOT_TOKEN, BOT);
  // The local cluster's server node writes a kubeconfig that reaches the fake API, at the port the config gives it.
  const { fake: docker } = await serveK3sInDocker(kube.kubeconfig);
  const home = mkdtempSync(join(tmpdir(), "alasio-commands-"));
  t.after(async () => {
    await Promise.all([kube.close(), telegram.close(), docker.close()]);
    rmSync(home, { recursive: true, force: true });
  });
  const kubeconfig = join(home, "kubeconfig");
  writeFileSync(kubeconfig, kube.kubeconfig);
  const env = { HOME: home, XDG_CONFIG_HOME: join(home, ".config"), XDG_DATA_HOME: join(home, ".local/share"), DOCKER_HOST: docker.host, TELEGRAM_API_ROOT: telegram.root };
  const configFile = join(env.XDG_CONFIG_HOME, "alasio", "config.json");
  return {
    kube,
    docker,
    home,
    kubeconfig,
    configFile,
    env,
    alasio: (args, answers = []) => runAlasio(args, { env, answers }),
    configure: (config) => {
      mkdirSync(join(configFile, ".."), { recursive: true });
      writeFileSync(configFile, JSON.stringify(config));
    },
    config: () => JSON.parse(readFileSync(configFile, "utf8")),
  };
}

/** The message `run` failed with. */
function failure(run: CliRun): string {
  assert.ok(Exit.isFailure(run.exit), "the command failed");
  const error = Cause.findErrorOption(run.exit.cause);
  assert.ok(error._tag === "Some", Cause.pretty(run.exit.cause));
  return (error.value as Error).message;
}

/** That `run` succeeded. */
function succeeded(run: CliRun): void {
  assert.ok(Exit.isSuccess(run.exit), Exit.isFailure(run.exit) ? Cause.pretty(run.exit.cause) : "");
}

/** The value of `key` of the Secret `name` in alasio's namespace. */
function secretValue(kube: FakeKube, name: string, key: string): string | undefined {
  const data = kube.get(`/api/v1/namespaces/alasio/secrets/${name}`)?.["data"] as Record<string, string> | undefined;
  const encoded = data?.[key];
  return encoded === undefined ? undefined : Buffer.from(encoded, "base64").toString("utf8");
}

/** Gives alasio its bot, and Claude Code a token, in the cluster the kubeconfig reaches, asking nothing. */
async function initialize({ alasio, home, kubeconfig }: Rig): Promise<void> {
  const tokenFile = join(home, "bot-token");
  writeFileSync(tokenFile, `${BOT_TOKEN}\n`);
  const claudeFile = join(home, "claude-token");
  writeFileSync(claudeFile, "sk-ant-oat01-claude\n");
  succeeded(await alasio(["init", "--non-interactive", "--kubeconfig", kubeconfig, "--bot-token-file", tokenFile, "--claude-token-file", claudeFile, "--allowed-user-ids", "42", "--no-up"]));
}

/** alasio initialized and up, in the cluster the kubeconfig reaches. */
async function installed(t: TestContext): Promise<Rig> {
  const setup = await rig(t);
  await initialize(setup);
  succeeded(await setup.alasio(["up"]));
  return setup;
}

test("alasio answers --version with this package's version", async (t) => {
  const { alasio } = await rig(t);
  const run = await alasio(["--version"]);
  succeeded(run);
  assert.match(run.printed.join("\n"), /9\.9\.9/u);
});

test("init asks, checks the bot's token with Telegram, writes the config and the Secrets, and keeps every answer left as it is when run again", async (t) => {
  const { alasio, config, configFile, kube, kubeconfig } = await rig(t);
  const first = await alasio(["init", "--kubeconfig", kubeconfig], [
    [...typed("123:wrong"), ...replaced(BOT_TOKEN)],
    typed("42, 43"),
    typed("sk-ant-oat01-claude"),
    pressed("y"),
    replaced("1234:1235"),
    replaced("/home/op"),
    typed("/srv/code"),
    typed("http://collector.example.com:4318"),
    pressed("n"),
  ]);
  succeeded(first);
  assert.equal(first.unread, 0);
  assert.match(first.prompts, /Telegram refused the bot token: Unauthorized/u);
  const written = {
    target: { kubeconfig: { path: kubeconfig } },
    install: {
      host: {
        enabled: true,
        uid: 1234,
        gid: 1235,
        home: "/home/op",
        stateRoot: "/home/op/.alasio/bayma",
        mounts: [{ name: "home-op", hostPath: "/home/op", mountPath: "/home/op" }, { name: "srv-code", hostPath: "/srv/code", mountPath: "/srv/code" }],
      },
      telemetry: { otlpEndpoint: "http://collector.example.com:4318" },
    },
  };
  assert.deepEqual(config(), written);
  assert.equal(statSync(configFile).mode & 0o777, 0o600);
  assert.doesNotMatch(readFileSync(configFile, "utf8"), /123:valid|sk-ant/u);
  assert.equal(secretValue(kube, "alasio-telegram", "token"), BOT_TOKEN);
  assert.equal(secretValue(kube, "alasio-telegram", "allowedUserIds"), "42,43");
  assert.equal(secretValue(kube, "alasio-claude", "token"), "sk-ant-oat01-claude");
  assert.equal(kube.get("/api/v1/namespaces/alasio")?.metadata.labels?.["alasio.dev/installation"], "alasio");
  assert.ok(first.printed.includes("alasio up starts it."));

  const again = await alasio(["init"], [[ENTER], [ENTER], [ENTER], [ENTER], [ENTER], [ENTER], [ENTER], [ENTER], pressed("n")]);
  succeeded(again);
  assert.equal(again.unread, 0);
  assert.match(again.prompts, new RegExp(`empty keeps @${BOT}`, "u"));
  assert.match(again.prompts, /empty keeps the current one/u);
  assert.ok(again.printed.includes(`alasio's config is ${configFile}; what you leave as it is stays.`));
  assert.deepEqual(config(), written);
  assert.equal(secretValue(kube, "alasio-telegram", "token"), BOT_TOKEN);
  assert.equal(secretValue(kube, "alasio-telegram", "allowedUserIds"), "42,43");
  assert.equal(secretValue(kube, "alasio-claude", "token"), "sk-ant-oat01-claude");
});

test("init keeps the host profile's mounts as written, mounting only folders new to it, and the home unless a mount puts it there", async (t) => {
  const setup = await rig(t);
  const mounts = [
    { name: "home", hostPath: "/home", mountPath: "/home" },
    { name: "docker-socket", hostPath: "/host/run/docker.sock", mountPath: "/var/run/docker.sock", type: "Socket" },
    { name: "docker", hostPath: "/host/bin/docker", mountPath: "/usr/bin/docker", readOnly: true, type: "File" },
  ];
  const host = { enabled: true, uid: 1000, gid: 1000, supplementalGroups: [110], home: "/home/op", stateRoot: "/home/op/.local/share/alasio/bayma", mounts };
  setup.configure({ target: { kubeconfig: { path: setup.kubeconfig } }, install: { host } });
  const tokenFile = join(setup.home, "bot-token");
  writeFileSync(tokenFile, BOT_TOKEN);
  const again = ["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--no-up"];
  // The config as init wrote it.
  const written = () => (setup.config() as { install: { host: typeof host } }).install.host;
  succeeded(await setup.alasio(again));
  assert.deepEqual(written(), host);
  succeeded(await setup.alasio([...again, "--folders", ["/home", "/host/run/docker.sock", "/host/bin/docker", "/srv/code"].join(",")]));
  assert.deepEqual(written().mounts, [...mounts, { name: "srv-code", hostPath: "/srv/code", mountPath: "/srv/code" }]);
  succeeded(await setup.alasio([...again, "--folders", "/home"]));
  assert.deepEqual(written().mounts, [mounts[0]]);
});

test("init asks where alasio runs on a first run, and runs it in the cluster a kubeconfig reaches when told", async (t) => {
  const { alasio, config, kube, kubeconfig } = await rig(t);
  const run = await alasio(["init"], [[DOWN, ENTER], typed(kubeconfig), typed("fake"), typed(BOT_TOKEN), typed("42"), [ENTER], pressed("n"), [ENTER], pressed("n")]);
  succeeded(run);
  assert.deepEqual(config(), { target: { kubeconfig: { path: kubeconfig, context: "fake" } }, install: {} });
  assert.equal(secretValue(kube, "alasio-telegram", "allowedUserIds"), "42");
  assert.equal(kube.get("/api/v1/namespaces/alasio/secrets/alasio-claude"), undefined);
});

test("init makes the cluster here unless told otherwise, its API on a port it keeps, its volumes under XDG_DATA_HOME", async (t) => {
  const { alasio, config, docker, env } = await rig(t);
  const run = await alasio(["init", "--no-up"], [[ENTER], typed(BOT_TOKEN), typed("42"), [ENTER], pressed("n"), [ENTER]]);
  // The node's kubeconfig names the port the config chose, where the fake API is not, so the Secrets are not written.
  assert.match(failure(run), /^Kubernetes could not be reached for PATCH \/api\/v1\/namespaces\/alasio/u);
  const { target } = config() as { target: { local: { name: string; apiPort: number; storagePath: string } } };
  assert.equal(target.local.name, "alasio");
  assert.ok(target.local.apiPort > 0);
  assert.equal(target.local.storagePath, join(env.XDG_DATA_HOME, "alasio", "storage"));
  assert.equal(docker.containers.get("alasio-server-0")?.state, "running");
});

test("init asks nothing with --non-interactive, takes tokens from files and the environment, and makes the configured cluster", async (t) => {
  const { config, configFile, configure, docker, env, home, kube } = await rig(t);
  const storagePath = join(home, "storage");
  configure({ target: { local: { name: "dev", apiPort: kube.port, storagePath } } });
  const tokenFile = join(home, "bot-token");
  writeFileSync(tokenFile, `${BOT_TOKEN}\n`);
  const run = await runAlasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42"], {
    env: { ...env, CLAUDE_CODE_OAUTH_TOKEN: "sk-ant-oat01-env" },
  });
  succeeded(run);
  assert.deepEqual(config(), { target: { local: { name: "dev", apiPort: kube.port, storagePath } }, install: {} });
  assert.equal(docker.containers.get("dev-server-0")?.state, "running");
  assert.equal(statSync(join(configFile, "..", "kubeconfig")).mode & 0o777, 0o600);
  assert.equal(secretValue(kube, "alasio-telegram", "token"), BOT_TOKEN);
  assert.equal(secretValue(kube, "alasio-claude", "token"), "sk-ant-oat01-env");
  assert.ok(run.printed.includes("alasio up starts it."));
});

test("init without what it needs, and nothing to ask, says how to give it; a token Telegram refuses is not kept", async (t) => {
  const { alasio, home, kube, kubeconfig } = await rig(t);
  assert.equal(
    failure(await alasio(["init", "--non-interactive", "--kubeconfig", kubeconfig])),
    "alasio init needs the bot's token: give it with --bot-token-file or TELEGRAM_BOT_TOKEN, or run alasio init without --non-interactive",
  );
  const tokenFile = join(home, "bot-token");
  writeFileSync(tokenFile, "123:wrong");
  assert.equal(failure(await alasio(["init", "--non-interactive", "--kubeconfig", kubeconfig, "--bot-token-file", tokenFile])), "Telegram refused the bot token: Unauthorized");
  assert.equal(kube.get("/api/v1/namespaces/alasio/secrets/alasio-telegram"), undefined);
});

test("up applies alasio, waits until it runs, and says what to do next", async (t) => {
  const setup = await rig(t);
  await initialize(setup);
  const run = await setup.alasio(["up"]);
  succeeded(run);
  assert.deepEqual(run.printed, [
    [
      `alasio 0.0.0-development runs in the cluster of the current context of ${setup.kubeconfig}.`,
      "",
      `Message @${BOT} on Telegram to talk to it.`,
      "Codex logs in once, in alasio: alasio login codex",
      "alasio status says how it is, and alasio logs --follow follows it.",
    ].join("\n"),
  ]);
  const env = (setup.kube.get(DEPLOYMENT)?.spec as { template: { spec: { containers: { env: { name: string }[] }[] } } }).template.spec.containers[0]?.env;
  assert.ok(env?.some(({ name }) => name === "CLAUDE_CODE_OAUTH_TOKEN"));
  assert.ok(run.progress.some((line) => line.startsWith("applying ")));
});

test("up without a bot says to run init; up past its timeout says what still waits, and why", async (t) => {
  const setup = await rig(t);
  setup.configure({ target: { kubeconfig: { path: setup.kubeconfig } } });
  assert.equal(failure(await setup.alasio(["up"])), "alasio has no Telegram bot in this cluster yet: alasio init gives it one");
  await initialize(setup);
  setup.kube.stuck.add("alasio");
  const message = failure(await setup.alasio(["up", "--timeout", "1s"]));
  assert.match(message, /^not ready within 1s, still waiting for:\n {2}Deployment alasio\/alasio: 0 of 1 available\n/u);
  assert.match(message, /container alasio of pod alasio-0 is waiting: ImagePullBackOff/u);
});

test("status says each workload's state, and fails when one is not ready", async (t) => {
  const { alasio, kube, kubeconfig } = await installed(t);
  const healthy = await alasio(["status"]);
  succeeded(healthy);
  assert.equal(healthy.printed[0], `alasio 0.0.0-development, in the cluster of the current context of ${kubeconfig} (${kube.server}):`);
  assert.ok(healthy.printed.includes("  Deployment alasio/alasio: ready"));
  const deployment = kube.get(DEPLOYMENT);
  assert.ok(deployment?.status);
  deployment.status["availableReplicas"] = 0;
  const unhealthy = await alasio(["status"]);
  assert.match(failure(unhealthy), /^1 of alasio's \d+ workloads are not ready$/u);
  assert.ok(unhealthy.printed.includes("  Deployment alasio/alasio: 0 of 1 available"));
});

test("init, up and status here refuse inotify limits too low for the cluster, saying how to raise them", async (t) => {
  const { configure, docker, env, home, kube } = await rig(t);
  configure({ target: { local: { name: "dev", apiPort: kube.port, storagePath: join(home, "storage") } } });
  const sysctl = { "fs.inotify.max_user_instances": "128\n", "fs.inotify.max_user_watches": "524288\n" };
  const shortfall = "fs.inotify.max_user_instances is 128, and the cluster needs at least 1024";
  const refusal = [
    "this machine's inotify limits are too low for the cluster alasio makes on it:",
    `  ${shortfall}`,
    "Raise them as root, now:",
    "  sysctl -w fs.inotify.max_user_instances=1024",
    "and for every boot, in /etc/sysctl.d/60-inotify.conf:",
    "  fs.inotify.max_user_instances = 1024",
  ].join("\n");
  // init refuses before it asks anything.
  assert.equal(failure(await runAlasio(["init"], { env, sysctl })), refusal);
  assert.equal(failure(await runAlasio(["up"], { env, sysctl })), refusal);
  assert.equal(docker.containers.size, 0);

  const tokenFile = join(home, "bot-token");
  writeFileSync(tokenFile, BOT_TOKEN);
  succeeded(await runAlasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--up"], { env }));
  const status = await runAlasio(["status"], { env, sysctl });
  assert.equal(failure(status), refusal);
  assert.deepEqual(status.printed.slice(0, 3), ["cluster dev, in Docker 29.0.0:", "  dev-server-0: running", `  this machine's ${shortfall}`]);
  assert.ok(status.printed.includes("  Deployment alasio/alasio: ready"));
});

test("status of a stopped cluster here says that up starts it", async (t) => {
  const { alasio, configure, docker, home, kube } = await rig(t);
  configure({ target: { local: { name: "dev", apiPort: kube.port, storagePath: join(home, "storage") } } });
  assert.equal(failure(await alasio(["status"])), "there is no cluster dev yet: alasio up makes it");
  docker.containers.set("dev-server-0", { body: { Labels: { "alasio.cluster": "dev", "alasio.role": "server" } }, state: "exited" });
  const run = await alasio(["status"]);
  assert.equal(failure(run), "the cluster dev is stopped: alasio up starts it");
  assert.deepEqual(run.printed, ["cluster dev, in Docker 29.0.0:", "  dev-server-0: exited"]);
});

test("logs prints a component's log, each line with its pod's name when it has several", async (t) => {
  const { alasio, kube } = await installed(t);
  kube.logs.set("alasio-0", "started\nlistening\n");
  const own = await alasio(["logs"]);
  succeeded(own);
  assert.equal(own.stdout, "started\nlistening\n");

  const safekeeper = kube.get("/api/v1/namespaces/alasio/pods/alasio-neon-safekeeper-0");
  assert.ok(safekeeper);
  kube.put("pods", { ...safekeeper, metadata: { ...safekeeper.metadata, name: "alasio-neon-safekeeper-1" } });
  kube.logs.set("alasio-neon-safekeeper-0", "zero\n");
  kube.logs.set("alasio-neon-safekeeper-1", "one\n");
  const several = await alasio(["logs", "neon-safekeeper", "--since", "10m"]);
  succeeded(several);
  assert.deepEqual(several.stdout.split("\n").filter(Boolean).sort(), ["[alasio-neon-safekeeper-0] zero", "[alasio-neon-safekeeper-1] one"]);
  assert.match(failure(await alasio(["logs", "nothing"])), /^alasio has no component nothing; it has agent-sandbox-controller, alasio, lake, /u);
});

test("restart rolls alasio's pod out again as a manager of its own, and waits until it runs", async (t) => {
  const { alasio, kube } = await installed(t);
  const run = await alasio(["restart"]);
  succeeded(run);
  const patch = kube.changes.findLast(({ path }) => path === DEPLOYMENT);
  assert.equal(patch?.contentType, "application/merge-patch+json");
  assert.equal(patch?.query.get("fieldManager"), "alasio-restart");
  const template = (kube.get(DEPLOYMENT)?.spec as { template: { metadata: { annotations: Record<string, string> } } }).template;
  assert.match(template.metadata.annotations["kubectl.kubernetes.io/restartedAt"] ?? "", /^\d{4}-\d\d-\d\dT/u);
  assert.deepEqual(run.printed, ["alasio restarted; a turn it was running continues."]);
});

test("upgrade says what it installs, and which images change", async (t) => {
  const setup = await rig(t);
  await initialize(setup);
  const fresh = await setup.alasio(["upgrade"]);
  succeeded(fresh);
  assert.equal(fresh.printed[0], "alasio is not installed yet: installing 0.0.0-development");
  const same = await setup.alasio(["upgrade"]);
  assert.equal(same.printed[0], "alasio 0.0.0-development already runs this version's images; applying it again");
  setup.configure({ target: { kubeconfig: { path: setup.kubeconfig } }, install: { images: { lake: { tag: "9.9.9" } } } });
  const changed = await setup.alasio(["upgrade"]);
  succeeded(changed);
  assert.equal(changed.printed[0], "alasio 0.0.0-development, with other images:\n  alasio-lake: ghcr.io/eaucoin/alasio-lake:0.0.0-development → ghcr.io/eaucoin/alasio-lake:9.9.9");
});

test("login codex runs Codex's device login in alasio's pod, and fails as it does", async (t) => {
  const { alasio, kube } = await installed(t);
  succeeded(await alasio(["login", "codex"]));
  assert.deepEqual(kube.execs.at(-1), { namespace: "alasio", pod: "alasio-0", container: "alasio", command: CODEX_LOGIN, tty: false });
  kube.onExec = () => ({ exitCode: 1 });
  assert.equal(failure(await alasio(["login", "codex"])), "codex login exited with 1");
});

test("lake runs the query in the lake's pod and prints its answer, in the format asked for", async (t) => {
  const { alasio, kube } = await installed(t);
  kube.onExec = () => ({ exitCode: 0, stdout: "count\n42\n" });
  const run = await alasio(["lake", "SELECT count(*) FROM claude.entries"]);
  succeeded(run);
  assert.equal(run.stdout, "count\n42\n");
  assert.deepEqual(kube.execs.at(-1)?.command, ["node", "/opt/lake/src/query.ts", "--format", "table", "SELECT count(*) FROM claude.entries"]);
  succeeded(await alasio(["lake", "--format", "json", "SELECT 1"]));
  assert.deepEqual(kube.execs.at(-1)?.command, ["node", "/opt/lake/src/query.ts", "--format", "json", "SELECT 1"]);
  kube.onExec = () => ({ exitCode: 2, stderr: "Parser Error: syntax error\n" });
  const refused = await alasio(["lake", "SELEC"]);
  assert.equal(failure(refused), "the lake's query exited with 2");
  assert.equal(refused.stderr, "Parser Error: syntax error\n");
});

test("down stops the cluster here, keeping everything, and is not for a cluster a kubeconfig reaches", async (t) => {
  const { alasio, configure, docker, home, kube, kubeconfig } = await rig(t);
  configure({ target: { local: { name: "dev", apiPort: kube.port, storagePath: join(home, "storage") } } });
  docker.containers.set("dev-server-0", { body: { Labels: { "alasio.cluster": "dev", "alasio.role": "server" } }, state: "running" });
  succeeded(await alasio(["down"]));
  assert.equal(docker.containers.get("dev-server-0")?.state, "exited");
  configure({ target: { kubeconfig: { path: kubeconfig } } });
  assert.equal(
    failure(await alasio(["down"])),
    `alasio down is for the cluster alasio makes on this machine, and alasio runs in the cluster of the current context of ${kubeconfig}, which alasio does not stop or start: alasio uninstall removes alasio from it`,
  );
});

test("uninstall asks first, and keeps alasio's data unless --purge", async (t) => {
  const { alasio, kube } = await installed(t);
  const declined = await alasio(["uninstall"], [pressed("n")]);
  assert.equal(failure(declined), "nothing was removed");
  assert.ok(kube.get(DEPLOYMENT));
  succeeded(await alasio(["uninstall"], [pressed("y")]));
  assert.equal(kube.get(DEPLOYMENT), undefined);
  assert.ok(kube.get("/api/v1/namespaces/alasio/persistentvolumeclaims/alasio"));
  assert.equal(secretValue(kube, "alasio-telegram", "token"), BOT_TOKEN);
  succeeded(await alasio(["uninstall", "--purge", "--yes"]));
  assert.equal(kube.get("/api/v1/namespaces/alasio"), undefined);
  assert.equal(kube.get("/api/v1/namespaces/alasio/secrets/alasio-telegram"), undefined);
});

test("uninstall --purge removes the cluster here whole: its nodes, volumes, storage and kubeconfig, but not the config", async (t) => {
  const { alasio, configFile, configure, docker, env, home, kube } = await rig(t);
  const storagePath = join(home, "storage");
  configure({ target: { local: { name: "dev", apiPort: kube.port, storagePath } } });
  const tokenFile = join(home, "bot-token");
  writeFileSync(tokenFile, BOT_TOKEN);
  succeeded(await runAlasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--up"], { env }));
  assert.ok(existsSync(storagePath));
  const run = await alasio(["uninstall", "--purge"], [pressed("y")]);
  succeeded(run);
  assert.equal(docker.containers.size, 0);
  assert.equal(docker.volumes.size, 0);
  assert.equal(docker.networks.size, 0);
  assert.ok(!existsSync(storagePath));
  assert.ok(!existsSync(join(configFile, "..", "kubeconfig")));
  assert.ok(existsSync(configFile));
  assert.match(run.prompts, /Remove alasio and all its data from the cluster dev on this machine, and the cluster itself\? This cannot be undone/u);
});
