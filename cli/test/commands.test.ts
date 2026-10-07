/**
 * alasio's commands (cli/src/commands/), run as an operator runs them, at a terminal
 * that answers their questions, against a fake Docker whose nodes run k3s, a fake
 * Kubernetes API, and a fake Telegram, on a machine of the test's, whose inotify limits the cluster
 * here needs unless a test gives others.
 */
import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import { Cause, Exit } from "effect";

import { CODEX_LOGIN } from "../src/commands/login.ts";
import { SYSCTL_FILE } from "../src/cluster/machine.ts";
import { downloads } from "../src/cluster/node.ts";
import { type CliRun, runAlasio } from "./support/cli.ts";
import type { FakeDocker } from "./support/fake-docker.ts";
import { serveK3sInDocker } from "./support/fake-k3s.ts";
import { type FakeKube, serveFakeKube } from "./support/fake-kube.ts";
import { FakeMachine } from "./support/fake-machine.ts";
import { fakeReleases } from "./support/fake-releases.ts";
import { serveFakeTelegram } from "./support/fake-telegram.ts";
import { DOWN, ENTER, pressed, replaced, typed } from "./support/fake-terminal.ts";

const BOT_TOKEN = "123:valid";
const BOT = "alasio_test_bot";
const DEPLOYMENT = "/apis/apps/v1/namespaces/alasio/deployments/alasio";

interface Rig {
  readonly kube: FakeKube;
  readonly docker: FakeDocker;
  readonly machine: FakeMachine;
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
  // The server node of the cluster in Docker writes a kubeconfig that reaches the fake API, at the port the config gives it.
  const { fake: docker } = await serveK3sInDocker(kube.kubeconfig);
  const home = mkdtempSync(join(tmpdir(), "alasio-commands-"));
  t.after(async () => {
    await Promise.all([kube.close(), telegram.close(), docker.close()]);
    rmSync(home, { recursive: true, force: true });
  });
  // k3s on the machine writes a kubeconfig that reaches the fake API, which has its node, ready once it starts, and CoreDNS's ConfigMap.
  const machine = new FakeMachine(join(home, "machine"));
  machine.kubeconfig = kube.kubeconfig;
  machine.onStart = () => kube.put("nodes", { apiVersion: "v1", kind: "Node", metadata: { name: "machine" }, status: { conditions: [{ type: "Ready", status: "True", lastHeartbeatTime: new Date().toISOString() }] } });
  kube.put("configmaps", { apiVersion: "v1", kind: "ConfigMap", metadata: { name: "coredns", namespace: "kube-system", resourceVersion: "1" }, data: { NodeHosts: "10.0.0.5 machine" } });
  const kubeconfig = join(home, "kubeconfig");
  writeFileSync(kubeconfig, kube.kubeconfig);
  const env = { HOME: home, XDG_CONFIG_HOME: join(home, ".config"), XDG_DATA_HOME: join(home, ".local/share"), DOCKER_HOST: docker.host, TELEGRAM_API_ROOT: telegram.root };
  const configFile = join(env.XDG_CONFIG_HOME, "alasio", "config.json");
  return {
    kube,
    docker,
    machine,
    home,
    kubeconfig,
    configFile,
    env,
    alasio: (args, answers = []) => runAlasio(args, { env, machine, answers }),
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
  const run = await alasio(["init"], [[DOWN, DOWN, ENTER], typed(kubeconfig), typed("fake"), typed(BOT_TOKEN), typed("42"), [ENTER], pressed("n"), [ENTER], pressed("n")]);
  succeeded(run);
  assert.deepEqual(config(), { target: { kubeconfig: { path: kubeconfig, context: "fake" } }, install: {} });
  assert.equal(secretValue(kube, "alasio-telegram", "allowedUserIds"), "42");
  assert.equal(kube.get("/api/v1/namespaces/alasio/secrets/alasio-claude"), undefined);
});

test("init installs k3s and gVisor here unless told otherwise, as root at once, after saying what, its volumes under XDG_DATA_HOME", async (t) => {
  const { alasio, config, configFile, env, kube, machine } = await rig(t);
  const run = await alasio(["init", "--no-up"], [[ENTER], typed(BOT_TOKEN), typed("42"), [ENTER], pressed("n"), [ENTER]]);
  succeeded(run);
  const storagePath = join(env.XDG_DATA_HOME, "alasio", "storage");
  assert.deepEqual(config(), { target: { host: { storagePath } }, install: {} });
  assert.ok(run.progress.includes([
    "alasio changes this machine as root:",
    "  write k3s's configuration in /etc/rancher/k3s, and its containerd's, with gVisor as the runtime runsc, in /var/lib/rancher/k3s/agent/etc/containerd",
    "  install gVisor 20990101.0 in /usr/local/bin: runsc, containerd-shim-runsc-v1, gvisor-bin",
    "  install k3s v1.99.0+k3s1 in /usr/local/bin, with its own install script, as the systemd service k3s, enabled and started anew",
    "  read its kubeconfig, /etc/rancher/k3s/k3s.yaml, for alasio to reach it",
  ].join("\n")), run.progress.join("\n"));
  assert.equal(machine.read("/usr/local/bin/k3s"), "k3s v1.99.0+k3s1\n");
  assert.equal(machine.read("/usr/local/bin/gvisor-bin/gvisor_sentry"), "gvisor_sentry 20990101.0\n");
  assert.equal(machine.ran.length, 1);
  assert.deepEqual(machine.ran[0]?.env, {
    INSTALL_K3S_SKIP_DOWNLOAD: "true",
    INSTALL_K3S_SKIP_SELINUX_RPM: "true",
    INSTALL_K3S_SELINUX_WARN: "true",
    INSTALL_K3S_FORCE_RESTART: "true",
    INSTALL_K3S_EXEC: "server",
  });
  assert.equal(JSON.parse(machine.read("/etc/rancher/k3s/config.yaml")?.split("\n")[1] ?? "")["default-local-storage-path"], storagePath);
  assert.ok(existsSync(storagePath));
  const written = join(configFile, "..", "kubeconfig");
  assert.equal(statSync(written).mode & 0o777, 0o600);
  assert.equal(JSON.parse(readFileSync(written, "utf8"))["current-context"], "alasio");
  assert.equal(secretValue(kube, "alasio-telegram", "token"), BOT_TOKEN);
  assert.equal(kube.get("/apis/node.k8s.io/v1/runtimeclasses/gvisor")?.["handler"], "runsc");
});

test("init --target docker makes the cluster in Docker, its API on a port it keeps, its volumes under XDG_DATA_HOME", async (t) => {
  const { alasio, config, docker, env } = await rig(t);
  const run = await alasio(["init", "--no-up"], [[DOWN, ENTER], typed(BOT_TOKEN), typed("42"), [ENTER], pressed("n"), [ENTER]]);
  // The node's kubeconfig names the port the config chose, where the fake API is not, so the Secrets are not written.
  assert.match(failure(run), /^Kubernetes could not be reached for PATCH \/api\/v1\/namespaces\/alasio/u);
  const { target } = config() as { target: { docker: { name: string; apiPort: number; storagePath: string } } };
  assert.equal(target.docker.name, "alasio");
  assert.ok(target.docker.apiPort > 0);
  assert.equal(target.docker.storagePath, join(env.XDG_DATA_HOME, "alasio", "storage"));
  assert.equal(docker.containers.get("alasio-server-0")?.state, "running");
});

test("init asks nothing with --non-interactive, takes tokens from files and the environment, and makes the configured cluster", async (t) => {
  const { config, configFile, configure, docker, env, home, kube, machine } = await rig(t);
  const storagePath = join(home, "storage");
  configure({ target: { docker: { name: "dev", apiPort: kube.port, storagePath } } });
  const tokenFile = join(home, "bot-token");
  writeFileSync(tokenFile, `${BOT_TOKEN}\n`);
  const run = await runAlasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42"], {
    env: { ...env, CLAUDE_CODE_OAUTH_TOKEN: "sk-ant-oat01-env" },
    machine,
  });
  succeeded(run);
  assert.deepEqual(config(), { target: { docker: { name: "dev", apiPort: kube.port, storagePath } }, install: {} });
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
  // Workspace storage's: JuiceFS's driver, in its own namespace, and Valkey.
  for (const line of ["  StatefulSet kube-system/juicefs-csi-controller: ready", "  DaemonSet kube-system/juicefs-csi-node: ready", "  StatefulSet alasio/alasio-valkey: ready"]) {
    assert.ok(healthy.printed.includes(line), line);
  }
  const deployment = kube.get(DEPLOYMENT);
  assert.ok(deployment?.status);
  deployment.status["availableReplicas"] = 0;
  const unhealthy = await alasio(["status"]);
  assert.match(failure(unhealthy), /^1 of alasio's \d+ workloads are not ready$/u);
  assert.ok(unhealthy.printed.includes("  Deployment alasio/alasio: 0 of 1 available"));
});

test("init and up raise inotify limits too low for the cluster here as root, now and for every boot, and status says them too low", async (t) => {
  const { alasio, configure, home, kube, machine } = await rig(t);
  configure({ target: { docker: { name: "dev", apiPort: kube.port, storagePath: join(home, "storage") } } });
  machine.sysctl("fs.inotify.max_user_instances", 128);
  const shortfall = "fs.inotify.max_user_instances is 128, and the cluster needs at least 1024";
  const tokenFile = join(home, "bot-token");
  writeFileSync(tokenFile, BOT_TOKEN);
  const init = await alasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--up"]);
  succeeded(init);
  assert.ok(init.progress.includes([
    "alasio changes this machine as root:",
    `  raise fs.inotify.max_user_instances to 1024, now and for every boot, in ${SYSCTL_FILE}`,
  ].join("\n")));
  assert.equal(machine.read("/proc/sys/fs/inotify/max_user_instances"), "1024\n");
  assert.match(machine.read(SYSCTL_FILE) ?? "", /^# .*\nfs\.inotify\.max_user_instances = 1024\n$/u);

  // Another raised later is written beside it.
  machine.sysctl("fs.inotify.max_user_watches", 8192);
  const up = await alasio(["up"]);
  succeeded(up);
  assert.match(machine.read(SYSCTL_FILE) ?? "", /\nfs\.inotify\.max_user_instances = 1024\nfs\.inotify\.max_user_watches = 524288\n$/u);
  succeeded(await alasio(["up"]));

  machine.sysctl("fs.inotify.max_user_instances", 128);
  const status = await alasio(["status"]);
  assert.equal(failure(status), ["this machine's inotify limits are too low for the cluster alasio makes on it:", `  ${shortfall}`, "alasio up raises them, as root."].join("\n"));
  assert.deepEqual(status.printed.slice(0, 3), ["cluster dev, in Docker 29.0.0:", "  dev-server-0: running", `  this machine's ${shortfall}`]);
  assert.ok(status.printed.includes("  Deployment alasio/alasio: ready"));
});

test("init, up and status refuse to make a cluster on a machine other than Linux on x86-64, but reach one elsewhere from it", async (t) => {
  const { configure, docker, env, home, kube, machine } = await rig(t);
  const mac = { platform: "darwin", arch: "arm64" };
  configure({ target: { docker: { name: "dev", apiPort: kube.port, storagePath: join(home, "storage") } } });
  const refusal = "the cluster alasio makes on this machine runs on Linux on x86-64, and this is darwin on arm64: " +
    "run alasio on such a machine, or give it a cluster elsewhere with alasio init --kubeconfig";
  for (const command of ["init", "up", "status"]) assert.equal(failure(await runAlasio([command], { env, machine, kind: mac })), refusal, command);
  assert.equal(docker.containers.size, 0);

  const remote = await installed(t);
  const status = await runAlasio(["status"], { env: remote.env, machine: remote.machine, kind: mac });
  succeeded(status);
  assert.ok(status.printed.includes("  Deployment alasio/alasio: ready"));
});

test("status of a stopped cluster here says that up starts it", async (t) => {
  const { alasio, configure, docker, home, kube } = await rig(t);
  configure({ target: { docker: { name: "dev", apiPort: kube.port, storagePath: join(home, "storage") } } });
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
  assert.match(failure(await alasio(["logs", "nothing"])), /^alasio has no component nothing; it has agent-sandbox-controller, alasio, collector, grafana, lake, /u);
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

test("lake runs the query in the lake's query container and prints its answer, in the format asked for", async (t) => {
  const { alasio, kube } = await installed(t);
  kube.onExec = () => ({ exitCode: 0, stdout: "count\n42\n" });
  const run = await alasio(["lake", "SELECT count(*) FROM claude.entries"]);
  succeeded(run);
  assert.equal(run.stdout, "count\n42\n");
  assert.equal(kube.execs.at(-1)?.container, "query");
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
  configure({ target: { docker: { name: "dev", apiPort: kube.port, storagePath: join(home, "storage") } } });
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
  assert.ok(kube.get("/api/v1/namespaces/alasio/persistentvolumeclaims/alasio-neon-control"));
  assert.equal(secretValue(kube, "alasio-telegram", "token"), BOT_TOKEN);
  succeeded(await alasio(["uninstall", "--purge", "--yes"]));
  assert.equal(kube.get("/api/v1/namespaces/alasio"), undefined);
  assert.equal(kube.get("/api/v1/namespaces/alasio/secrets/alasio-telegram"), undefined);
});

test("uninstall --purge removes the cluster here whole: its nodes, volumes, storage and kubeconfig, and the limits' file, but not the config", async (t) => {
  const { alasio, configFile, configure, docker, home, kube, machine } = await rig(t);
  const storagePath = join(home, "storage");
  configure({ target: { docker: { name: "dev", apiPort: kube.port, storagePath } } });
  machine.sysctl("fs.inotify.max_user_watches", 8192);
  const tokenFile = join(home, "bot-token");
  writeFileSync(tokenFile, BOT_TOKEN);
  succeeded(await alasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--up"]));
  assert.ok(existsSync(storagePath));
  assert.ok(machine.read(SYSCTL_FILE));
  const run = await alasio(["uninstall", "--purge"], [pressed("y")]);
  succeeded(run);
  assert.equal(docker.containers.size, 0);
  assert.equal(docker.volumes.size, 0);
  assert.equal(docker.networks.size, 0);
  assert.ok(!existsSync(storagePath));
  assert.ok(!existsSync(join(configFile, "..", "kubeconfig")));
  assert.ok(existsSync(configFile));
  assert.equal(machine.read(SYSCTL_FILE), null);
  assert.match(run.prompts, /Remove alasio and all its data from the cluster dev on this machine, and the cluster itself\? This cannot be undone/u);
});

/** alasio initialized and up in k3s on the rig's machine: what init said. */
async function upOnHost({ alasio, home }: Rig): Promise<CliRun> {
  const tokenFile = join(home, "bot-token");
  writeFileSync(tokenFile, BOT_TOKEN);
  const run = await alasio(["init", "--non-interactive", "--target", "host", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--up"]);
  succeeded(run);
  return run;
}

/** alasio initialized and up in k3s on this machine, k3s and gVisor installed from `releases`. */
async function onHost(t: TestContext): Promise<Rig> {
  const setup = await rig(t);
  await upOnHost(setup);
  return setup;
}

/** What `run` said alasio does as root, a line a step; none when it did nothing as root. */
const rootSteps = (run: CliRun): string[] =>
  run.progress.filter((line) => line.startsWith("alasio changes this machine")).flatMap((line) => line.split("\n").slice(1).map((step) => step.trim()));

test("up on k3s here asks root for nothing once all is as it should, gives CoreDNS the aliases itself, and restarts k3s for new registries", async (t) => {
  const { alasio, config, configFile, kube, machine } = await onHost(t);
  const ran = machine.ran.length;
  const again = await alasio(["up"]);
  succeeded(again);
  assert.deepEqual(rootSteps(again), []);
  assert.equal(machine.ran.length, ran);

  const written = config() as { target: { host: Record<string, unknown> }; install: Record<string, unknown> };
  const configure = (host: Record<string, unknown>) => writeFileSync(configFile, JSON.stringify({ ...written, target: { host: { ...written.target.host, ...host } } }));
  configure({ hostAliases: [{ ip: "10.0.0.9", hostnames: ["collector.lan"] }] });
  const aliased = await alasio(["up"]);
  succeeded(aliased);
  assert.deepEqual(rootSteps(aliased), []);
  assert.equal((kube.get("/api/v1/namespaces/kube-system/configmaps/coredns")?.["data"] as Record<string, string>)["NodeHosts"], "10.0.0.5 machine\n10.0.0.9 collector.lan");

  configure({ registries: { mirrors: { "docker.io": { endpoint: ["https://mirror.lan"] } } } });
  const mirrored = await alasio(["up"]);
  succeeded(mirrored);
  assert.deepEqual(rootSteps(mirrored), [
    "write k3s's configuration in /etc/rancher/k3s, and its containerd's, with gVisor as the runtime runsc, in /var/lib/rancher/k3s/agent/etc/containerd",
    "restart the service k3s, with its new configuration",
    "read its kubeconfig, /etc/rancher/k3s/k3s.yaml, for alasio to reach it",
  ]);
  assert.match(machine.read("/etc/rancher/k3s/registries.yaml") ?? "", /"mirrors":\{"docker\.io":\{"endpoint":\["https:\/\/mirror\.lan"\]\}\}/u);
  assert.deepEqual(machine.commands().slice(ran), ["systemctl restart k3s"]);
});

test("upgrade installs this version's k3s and gVisor in place of those installed, and up after it nothing", async (t) => {
  const setup = await onHost(t);
  const newer = fakeReleases({ k3s: "v1.100.0+k3s1", gvisor: "21000101.0" });
  const upgraded = await runAlasio(["upgrade"], { env: setup.env, machine: setup.machine, releases: newer });
  succeeded(upgraded);
  assert.deepEqual(rootSteps(upgraded), [
    "install gVisor 21000101.0, in place of 20990101.0, in /usr/local/bin: runsc, containerd-shim-runsc-v1, gvisor-bin",
    "install k3s v1.100.0+k3s1, in place of v1.99.0+k3s1, in /usr/local/bin, with its own install script, as the systemd service k3s, enabled and started anew",
    "read its kubeconfig, /etc/rancher/k3s/k3s.yaml, for alasio to reach it",
  ]);
  assert.equal(setup.machine.read("/usr/local/bin/runsc"), "runsc 21000101.0\n");
  assert.equal(setup.machine.read("/usr/local/bin/k3s"), "k3s v1.100.0+k3s1\n");
  assert.deepEqual(rootSteps(await runAlasio(["up"], { env: setup.env, machine: setup.machine, releases: newer })), []);
});

test("a download that is not what alasio pins is refused before anything is done as root", async (t) => {
  const { env, home, machine } = await rig(t);
  const releases = fakeReleases();
  releases.served.set(downloads(releases.pins).gvisor.url, Buffer.from("not gVisor"));
  const tokenFile = join(home, "bot-token");
  writeFileSync(tokenFile, BOT_TOKEN);
  const run = await runAlasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42"], { env, machine, releases });
  assert.match(
    failure(run),
    /^https:\/\/storage\.googleapis\.com\/gvisor\/releases\/release\/20990101\.0\/x86_64\/gvisor\.tar\.zstd is not what alasio pins: its sha512 is [0-9a-f]{128}, not [0-9a-f]{128}$/u,
  );
  assert.ok(!run.progress.some((line) => line.startsWith("alasio changes this machine")));
  assert.deepEqual(machine.ran, []);
  assert.ok(!machine.has("/etc/rancher"));
});

test("status of k3s here says its versions, its service and node, and that up starts it once down stopped it", async (t) => {
  const { alasio, machine } = await onHost(t);
  const healthy = await alasio(["status"]);
  succeeded(healthy);
  assert.deepEqual(healthy.printed.slice(0, 3), ["k3s v1.99.0+k3s1 on this machine, with gVisor 20990101.0:", "  service k3s: active, enabled", "  node machine: ready"]);
  assert.ok(healthy.printed.includes("  Deployment alasio/alasio: ready"));

  const down = await alasio(["down"]);
  succeeded(down);
  assert.deepEqual(rootSteps(down), ["stop and disable the service k3s, and stop the cluster's pods, gVisor's and those k3s-killall.sh stops"]);
  assert.deepEqual(machine.commands().slice(-2), ["systemctl disable --now k3s", "/usr/local/bin/k3s-killall.sh"]);
  assert.deepEqual(machine.killed.at(-1), ["/usr/local/bin/runsc", "/usr/local/bin/containerd-shim-runsc-v1", "/usr/local/bin/gvisor-bin/gvisor_sentry"]);
  const stopped = await alasio(["status"]);
  assert.equal(failure(stopped), "k3s on this machine is stopped: alasio up starts it");
  assert.deepEqual(stopped.printed, ["k3s v1.99.0+k3s1 on this machine, with gVisor 20990101.0:", "  service k3s: inactive, disabled"]);
  assert.equal(failure(await alasio(["logs"])), "k3s on this machine is stopped: alasio up starts it");

  const up = await alasio(["up"]);
  succeeded(up);
  assert.deepEqual(rootSteps(up), ["enable and start the service k3s", "read its kubeconfig, /etc/rancher/k3s/k3s.yaml, for alasio to reach it"]);
  succeeded(await alasio(["status"]));
});

test("init leaves a k3s alasio did not install as it is", async (t) => {
  const { alasio, home, machine } = await rig(t);
  machine.unit = { loaded: true, active: "active", enabled: true };
  const tokenFile = join(home, "bot-token");
  writeFileSync(tokenFile, BOT_TOKEN);
  assert.equal(
    failure(await alasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42"])),
    "k3s is installed on this machine, but not by alasio, which leaves it as it is: run alasio in it with alasio init --kubeconfig /etc/rancher/k3s/k3s.yaml, or uninstall it first (k3s-uninstall.sh)",
  );
  assert.deepEqual(machine.ran, []);
});

test("uninstall --purge removes k3s, gVisor, JuiceFS's directories, the storage, the limits' file and the kubeconfig as root at once, but not the config", async (t) => {
  const { alasio, configFile, env, machine } = await rig(t);
  machine.sysctl("fs.inotify.max_user_instances", 128);
  const tokenFile = join(env.HOME, "bot-token");
  writeFileSync(tokenFile, BOT_TOKEN);
  succeeded(await alasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--up"]));
  machine.write("/proc/self/mounts", "proc /proc proc rw 0 0\nJuiceFS:workspaces /var/lib/juicefs/volume/pvc\\040one fuse.juicefs rw 0 0\n");
  machine.write("/var/lib/juicefs/volume/pvc one/file", "data");
  const storagePath = join(env.XDG_DATA_HOME, "alasio", "storage");
  const run = await alasio(["uninstall", "--purge"], [pressed("y")]);
  succeeded(run);
  assert.match(run.prompts, /Remove alasio and all its data from k3s on this machine, and k3s and gVisor themselves\? This cannot be undone/u);
  assert.deepEqual(rootSteps(run), [
    "stop the cluster's gVisor pods, uninstall k3s with k3s-uninstall.sh, which removes its service, /etc/rancher/k3s and its data, and remove what it leaves, its node's password in /etc/rancher/node, gVisor from /usr/local/bin, and JuiceFS's mounts and directories, /var/lib/juicefs and /run/juicefs-csi",
    `remove ${storagePath}, the cluster's volumes`,
    `remove ${SYSCTL_FILE}, which raises the inotify limits at every boot`,
  ]);
  assert.deepEqual(machine.commands().slice(-3), ["systemctl stop k3s", "/usr/local/bin/k3s-uninstall.sh", "umount --lazy /var/lib/juicefs/volume/pvc one"]);
  for (const path of ["/usr/local/bin/k3s", "/usr/local/bin/runsc", "/usr/local/bin/containerd-shim-runsc-v1", "/usr/local/bin/gvisor-bin", "/etc/rancher", "/var/lib/rancher", "/var/lib/juicefs", SYSCTL_FILE]) {
    assert.ok(!machine.has(path), `${path} is left`);
  }
  assert.ok(!existsSync(storagePath));
  assert.ok(!existsSync(join(configFile, "..", "kubeconfig")));
  assert.ok(existsSync(configFile));
  assert.equal(run.printed.at(-1), `alasio and k3s on this machine are removed, with gVisor and all their data; the config at ${configFile} is kept.`);
  const status = await alasio(["status"]);
  assert.equal(failure(status), "there is no k3s on this machine yet: alasio up installs it");
  assert.deepEqual(status.printed, ["k3s on this machine:", "  not installed"]);
});

test("an active ufw lets the cluster's pods and Services in, by rules alasio adds only where it has none, and uninstall removes those alone", async (t) => {
  const setup = await rig(t);
  const { alasio, machine } = setup;
  machine.enableUfw(["192.168.1.0/24", "10.43.0.0/16"]);
  const installed = await upOnHost(setup);
  assert.equal(
    rootSteps(installed)[0],
    "let the cluster's pods and Services, 10.42.0.0/16 and 10.43.0.0/16, in to this machine through ufw, as k3s needs; nothing else is opened, k3s's API server's port 6443 staying closed to the rest",
  );
  assert.deepEqual(machine.commands().filter((command) => command.startsWith("ufw")), ["ufw show added", "ufw allow from 10.42.0.0/16 to any", "ufw show added"]);
  assert.deepEqual([...machine.ufwRules].sort(), ["10.42.0.0/16", "10.43.0.0/16", "192.168.1.0/24"]);
  assert.deepEqual(rootSteps(await alasio(["up"])), []);

  const removed = await alasio(["uninstall", "--purge", "--yes"]);
  succeeded(removed);
  assert.match(rootSteps(removed)[0] ?? "", /^stop the cluster's gVisor pods, remove the rules alasio added to ufw for 10\.42\.0\.0\/16, uninstall k3s /u);
  assert.deepEqual([...machine.ufwRules].sort(), ["10.43.0.0/16", "192.168.1.0/24"]);
});

test("an active firewalld trusts the cluster's pods and Services, permanently and now, and uninstall takes them out again", async (t) => {
  const setup = await rig(t);
  const { alasio, machine } = setup;
  machine.enableFirewalld(["172.16.0.0/12"]);
  const installed = await upOnHost(setup);
  assert.match(rootSteps(installed)[0] ?? "", /^let the cluster's pods and Services, 10\.42\.0\.0\/16 and 10\.43\.0\.0\/16, in to this machine through firewalld, /u);
  assert.deepEqual(machine.commands().filter((command) => command.includes("--add-source") || command.endsWith("--reload")), [
    "firewall-cmd --permanent --zone=trusted --add-source=10.42.0.0/16",
    "firewall-cmd --permanent --zone=trusted --add-source=10.43.0.0/16",
    "firewall-cmd --reload",
  ]);
  assert.deepEqual([...machine.trusted].sort(), ["10.42.0.0/16", "10.43.0.0/16", "172.16.0.0/12"]);
  succeeded(await alasio(["uninstall", "--purge", "--yes"]));
  assert.deepEqual([...machine.trusted], ["172.16.0.0/12"]);
  assert.equal(machine.reloads, 2);
});

test("a firewall that is not active, as ufw installed but not enabled, is left as it is", async (t) => {
  const setup = await rig(t);
  setup.machine.write("/etc/ufw/ufw.conf", "ENABLED=no\n");
  const installed = await upOnHost(setup);
  assert.ok(!rootSteps(installed).some((step) => step.includes("ufw")));
  assert.deepEqual(setup.machine.commands().filter((command) => command.startsWith("ufw") || command.startsWith("firewall-cmd")), []);
  succeeded(await setup.alasio(["uninstall", "--purge", "--yes"]));
  assert.deepEqual(setup.machine.commands().filter((command) => command.startsWith("ufw") || command.startsWith("firewall-cmd")), []);
});
