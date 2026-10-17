/**
 * alasio's commands (cli/src/commands/), run as an operator runs them, at a terminal
 * that answers their questions, against a fake Docker whose nodes run k3s, a fake
 * Kubernetes API, and a fake Telegram, on a machine of the test's, whose inotify limits the cluster
 * here needs unless a test gives others.
 */
import assert from "node:assert/strict";
import { createHash, createHmac } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { createServer as createHttpServer } from "node:http";
import { type AddressInfo, createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test, type TestContext } from "node:test";

import { Cause, Exit } from "effect";

import { tokenSha256 } from "../../neon/control/branches.ts";
import { scramVerifier } from "../../neon/control/scram.ts";
import { forkToken } from "../../src/branch/names.ts";
import type { TenantView } from "../src/branches.ts";
import { CODEX_LOGIN } from "../src/commands/login.ts";
import { SYSCTL_FILE } from "../src/cluster/machine.ts";
import { currentGvisorRelease, downloads } from "../src/cluster/node.ts";
import { ROUTES_SECRET, TLS_SECRET, WEBHOOK, WEBHOOK_HEALTH_PORT } from "../src/manifests/webhook.ts";
import { type CliRun, runAlasio } from "./support/cli.ts";
import type { FakeDocker } from "./support/fake-docker.ts";
import { serveK3sInDocker } from "./support/fake-k3s.ts";
import { type FakeKube, serveFakeKube } from "./support/fake-kube.ts";
import { FakeMachine } from "./support/fake-machine.ts";
import { fakeReleases } from "./support/fake-releases.ts";
import { type FakeTelegram, serveFakeTelegram } from "./support/fake-telegram.ts";
import { DOWN, ENTER, pressed, replaced, typed } from "./support/fake-terminal.ts";

const BOT_TOKEN = "123:valid";
const BOT = "alasio_test_bot";
const DEPLOYMENT = "/apis/apps/v1/namespaces/alasio/deployments/alasio";
const TEMPLATES = "/api/v1/namespaces/alasio/configmaps/alasio-sandbox-templates";

/** Where the node agent's pod says its node's CPU features, as the fake API keeps what it says. */
const CPU_FEATURES = "alasio-node-agent/cpu-features";

/** What a node agent's pod says of a node with `features`, under the current gVisor release. */
const cpuReport = (features: readonly string[], release = currentGvisorRelease()): string => JSON.stringify({ release, cpuFeatures: features });

/** Neon's tenant as neon-control has it after alasio's first start: one shard, on one pageserver. */
const ONE_SHARD: TenantView = { shards: 1, stripeSize: 2048, pageservers: [{ id: 1, address: "alasio-neon-pageserver-0.alasio.svc.cluster.local:6400", availability: "Active", shards: 1 }] };

/** Neon's storage as the storage controller holds it: the pageservers registered, and its tenant's shards, null before it is made; and whether it answers. */
interface FakeStorage {
  pageservers: number;
  shards: number | null;
  answering: boolean;
  /** Whether its database answers, in its pod. */
  databaseAnswering: boolean;
}

/**
 * The storage controller's answer, in its pod, to curl asking its API for the path
 * `command` ends with, as `storage` holds: its pageservers, or the tenant.
 */
function storageControllerAnswer(storage: FakeStorage, command: readonly string[]): { exitCode: number; stdout: string } {
  const path = command.at(-1) ?? "";
  const answer = (status: number, answered: unknown) => ({ exitCode: 0, stdout: `${JSON.stringify(answered)}\n${status}` });
  if (!storage.answering) return answer(503, { msg: "Not ready: startup in progress" });
  if (path === "/control/v1/node") {
    return answer(200, Array.from({ length: storage.pageservers }, (_, index) => ({ id: index + 1, availability: "Active", scheduling: "Active", listen_pg_addr: `alasio-neon-pageserver-${index}`, listen_pg_port: 6400 })));
  }
  if (path.startsWith("/control/v1/tenant/") && storage.shards !== null) {
    const count = storage.shards;
    return answer(200, { shards: Array.from({ length: count }, (_, index) => ({ tenant_shard_id: `t-${index.toString(16).padStart(2, "0")}${count.toString(16).padStart(2, "0")}`, node_attached: 1, is_reconciling: false, is_splitting: false })) });
  }
  return answer(404, { msg: "NotFound" });
}

/**
 * The storage controller's database's answer, in its pod, to psql asking it `command`'s SQL,
 * as `storage` holds: its table of nodes made, and a node in it while it has a pageserver.
 */
function controllerDatabaseAnswer(storage: FakeStorage, command: readonly string[]): { exitCode: number; stdout: string; stderr?: string } {
  const sql = command.at(-1) ?? "";
  if (!storage.databaseAnswering) return { exitCode: 2, stdout: "", stderr: "psql: error: connection to server on socket \"/var/run/postgresql/.s.PGSQL.5432\" failed: No such file or directory" };
  if (sql.includes("to_regclass('nodes')")) return { exitCode: 0, stdout: "t\n" };
  if (sql.includes("from nodes")) return { exitCode: 0, stdout: `${storage.pageservers > 0 ? "t" : "f"}\n` };
  return { exitCode: 0, stdout: "" };
}

/** The address Telegram delivers to, as init is told it. */
const PUBLIC_ADDRESS = "203.0.113.10";
const CERTIFICATE = "-----BEGIN CERTIFICATE-----\nintake\n-----END CERTIFICATE-----\n";

interface Rig {
  readonly kube: FakeKube;
  readonly telegram: FakeTelegram;
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
  // The storage controller, once installed, says what Neon's storage holds, which alasio up
  // checks the configuration against: one pageserver, before the tenant is made.
  const storage: FakeStorage = { pageservers: 1, shards: null, answering: true, databaseAnswering: true };
  kube.onExec = ({ container, command }) =>
    container === "storage-controller" ? storageControllerAnswer(storage, command) : container === "postgres" ? controllerDatabaseAnswer(storage, command) : { exitCode: 0 };
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
  // Its node agent says what its node's CPU has, as it starts.
  kube.said.set(CPU_FEATURES, cpuReport(["sse", "avx2", "avx"]));
  // The webhook intake's pod, which routes what the routes Secret holds, and its certificate, as the stack's setup makes it.
  const intake = createHttpServer((_request, response) => {
    const routes = kube.get(`/api/v1/namespaces/alasio/secrets/${ROUTES_SECRET}`)?.["data"] as Record<string, string> | undefined;
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end(JSON.stringify(Object.keys(routes ?? {}).map((environment) => ({ environment }))));
  });
  await new Promise<void>((resolve) => intake.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => intake.close(resolve)));
  kube.forwarded.set(`${WEBHOOK}-0:${WEBHOOK_HEALTH_PORT}`, (intake.address() as AddressInfo).port);
  kube.put("secrets", { apiVersion: "v1", kind: "Secret", metadata: { name: TLS_SECRET, namespace: "alasio" }, data: { "tls.crt": Buffer.from(CERTIFICATE).toString("base64"), "tls.key": Buffer.from("key").toString("base64") } });
  const kubeconfig = join(home, "kubeconfig");
  writeFileSync(kubeconfig, kube.kubeconfig);
  const env = { HOME: home, XDG_CONFIG_HOME: join(home, ".config"), XDG_DATA_HOME: join(home, ".local/share"), DOCKER_HOST: docker.host, TELEGRAM_API_ROOT: telegram.root };
  const configFile = join(env.XDG_CONFIG_HOME, "alasio", "config.json");
  return {
    kube,
    telegram,
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

/** A port of the loopback no one listens on now. */
async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  // A server listening on a TCP port has an address of its own.
  const { port } = server.address() as AddressInfo;
  await new Promise((resolve) => server.close(resolve));
  return port;
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
  succeeded(await alasio(["init", "--non-interactive", "--kubeconfig", kubeconfig, "--bot-token-file", tokenFile, "--claude-token-file", claudeFile, "--allowed-user-ids", "42", "--public-address", PUBLIC_ADDRESS, "--no-up"]));
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
    typed(PUBLIC_ADDRESS),
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
      webhook: { publicAddress: PUBLIC_ADDRESS },
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

  const again = await alasio(["init"], [[ENTER], [ENTER], [ENTER], [ENTER], [ENTER], [ENTER], [ENTER], [ENTER], [ENTER], pressed("n")]);
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
  const again = ["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--public-address", PUBLIC_ADDRESS, "--no-up"];
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
  const run = await alasio(["init"], [[DOWN, DOWN, ENTER], typed(kubeconfig), typed("fake"), typed(BOT_TOKEN), typed("42"), [ENTER], pressed("n"), [ENTER], typed(PUBLIC_ADDRESS), pressed("n")]);
  succeeded(run);
  assert.deepEqual(config(), { target: { kubeconfig: { path: kubeconfig, context: "fake" } }, install: { webhook: { publicAddress: PUBLIC_ADDRESS } } });
  assert.equal(secretValue(kube, "alasio-telegram", "allowedUserIds"), "42");
  assert.equal(kube.get("/api/v1/namespaces/alasio/secrets/alasio-claude"), undefined);
});

test("init installs k3s and gVisor here unless told otherwise, as root at once, after saying what, its volumes under XDG_DATA_HOME", async (t) => {
  const { alasio, config, configFile, env, kube, machine } = await rig(t);
  const run = await alasio(["init", "--no-up"], [[ENTER], typed(BOT_TOKEN), typed("42"), [ENTER], pressed("n"), [ENTER], replaced(PUBLIC_ADDRESS)]);
  succeeded(run);
  const storagePath = join(env.XDG_DATA_HOME, "alasio", "storage");
  assert.deepEqual(config(), { target: { host: { storagePath } }, install: { webhook: { publicAddress: PUBLIC_ADDRESS } } });
  assert.ok(run.progress.includes([
    "alasio changes this machine as root:",
    "  write k3s's configuration in /etc/rancher/k3s, and its containerd's, with gVisor as the runtime runsc-20990101-0, in /var/lib/rancher/k3s/agent/etc/containerd",
    "  install gVisor 20990101.0 in /usr/local/lib/alasio/gvisor/20990101.0: runsc, containerd-shim-runsc-v1, gvisor-bin",
    "  install k3s v1.99.0+k3s1 in /usr/local/bin, with its own install script, as the systemd service k3s, enabled and started anew",
    "  read its kubeconfig, /etc/rancher/k3s/k3s.yaml, for alasio to reach it",
  ].join("\n")), run.progress.join("\n"));
  assert.equal(machine.read("/usr/local/bin/k3s"), "k3s v1.99.0+k3s1\n");
  assert.equal(machine.read("/usr/local/lib/alasio/gvisor/20990101.0/gvisor-bin/gvisor_sentry"), "gvisor_sentry 20990101.0\n");
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
  assert.equal(kube.get("/apis/node.k8s.io/v1/runtimeclasses/gvisor")?.["handler"], "runsc-20990101-0");
  assert.equal(kube.get("/apis/node.k8s.io/v1/runtimeclasses/gvisor-20990101-0")?.["handler"], "runsc-20990101-0");
});

test("init --target docker makes the cluster in Docker, its API on a port it keeps, its volumes under XDG_DATA_HOME", async (t) => {
  const { alasio, config, docker, env } = await rig(t);
  const run = await alasio(["init", "--no-up"], [[DOWN, ENTER], typed(BOT_TOKEN), typed("42"), [ENTER], pressed("n"), [ENTER], typed(PUBLIC_ADDRESS)]);
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
  const run = await runAlasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--public-address", PUBLIC_ADDRESS], {
    env: { ...env, CLAUDE_CODE_OAUTH_TOKEN: "sk-ant-oat01-env" },
    machine,
  });
  succeeded(run);
  assert.deepEqual(config(), { target: { docker: { name: "dev", apiPort: kube.port, storagePath } }, install: { webhook: { publicAddress: PUBLIC_ADDRESS } } });
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
  // main's bot is delivered to its route, on the public address, with the intake's certificate.
  const route = JSON.parse(secretValue(setup.kube, ROUTES_SECRET, "main") ?? "") as { path: string; secretToken: string; allowedUserIds: string[] };
  assert.match(route.path, /^\/telegram\/main\/[\w-]{22}$/u);
  assert.deepEqual(route.allowedUserIds, ["42"]);
  assert.deepEqual(setup.telegram.webhooks.get(BOT_TOKEN), {
    url: `https://${PUBLIC_ADDRESS}${route.path}`,
    secretToken: route.secretToken,
    certificate: CERTIFICATE,
    allowedUpdates: ["message", "callback_query"],
    ipAddress: PUBLIC_ADDRESS,
  });
  assert.ok(run.progress.includes(`Telegram delivers @${BOT}'s updates to the webhook intake, for main`));

  // Run again, it keeps the route, and registers the bot again.
  setup.telegram.webhooks.clear();
  succeeded(await setup.alasio(["up"]));
  assert.equal(secretValue(setup.kube, ROUTES_SECRET, "main"), JSON.stringify(route));
  assert.equal(setup.telegram.webhooks.get(BOT_TOKEN)?.url, `https://${PUBLIC_ADDRESS}${route.path}`);
});

test("up replaces alasio's pod when the users its bot allows change, which alasio reads as it starts, and not when they stay", async (t) => {
  const setup = await installed(t);
  const annotations = () => (setup.kube.get(DEPLOYMENT)?.spec as { template: { metadata: { annotations: Record<string, string> } } }).template.metadata.annotations;
  const first = annotations();
  assert.ok(first["checksum/telegram"]);
  succeeded(await setup.alasio(["up"]));
  assert.deepEqual(annotations(), first);

  const tokenFile = join(setup.home, "bot-token");
  succeeded(await setup.alasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42,43", "--public-address", PUBLIC_ADDRESS, "--no-up"]));
  succeeded(await setup.alasio(["up"]));
  const widened = annotations();
  assert.notEqual(widened["checksum/telegram"], first["checksum/telegram"]);
  assert.equal(widened["checksum/sandbox-templates"], first["checksum/sandbox-templates"]);
  // An HMAC with the bot's Secret's own key, kept as init writes the Secret again: no hash of what it sums alone.
  const key = secretValue(setup.kube, "alasio-telegram", "checksumKey") ?? "";
  assert.match(key, /^[\w-]{43}$/u);
  assert.equal(widened["checksum/telegram"], createHmac("sha256", key).update(JSON.stringify([BOT_TOKEN, "42,43"])).digest("hex"));
  assert.notEqual(widened["checksum/telegram"], createHash("sha256").update(JSON.stringify([BOT_TOKEN, "42,43"])).digest("hex"));

  // A Secret of a release before, of no key: up gives it one, and alasio's pod the checksum it makes.
  const path = "/api/v1/namespaces/alasio/secrets/alasio-telegram";
  const before = setup.kube.objects.get(path);
  assert.ok(before);
  const { checksumKey: _dropped, ...data } = before["data"] as Record<string, string>;
  setup.kube.objects.set(path, { ...before, data });
  succeeded(await setup.alasio(["up"]));
  const given = secretValue(setup.kube, "alasio-telegram", "checksumKey") ?? "";
  assert.match(given, /^[\w-]{43}$/u);
  assert.notEqual(given, key);
  assert.equal(annotations()["checksum/telegram"], createHmac("sha256", given).update(JSON.stringify([BOT_TOKEN, "42,43"])).digest("hex"));
});

test("up holds sessions to the CPU features every node agent says its node has, recorded in their template, and to none, unsnapshotted, until each has", async (t) => {
  const setup = await rig(t);
  await initialize(setup);
  const sessions = () => JSON.parse(String((setup.kube.get(TEMPLATES)?.["data"] as Record<string, string>)["templates.json"])) as {
    readonly sessions: { readonly podTemplate: { readonly metadata: { readonly annotations: Record<string, string> } }; readonly snapshots?: unknown };
    readonly previousSessions?: { readonly podTemplate: { readonly metadata: { readonly annotations: Record<string, string> } } };
  };
  const applied = () => setup.kube.changes.filter(({ path }) => path === TEMPLATES).length;
  const cpuFeatures = () => sessions().sessions.podTemplate.metadata.annotations["dev.gvisor.internal.cpufeatures"];
  // Installed first, sessions are held to none until the agents have said: then up records what they said, applying it again.
  succeeded(await setup.alasio(["up"]));
  assert.equal(cpuFeatures(), "avx,avx2,sse");
  assert.ok(sessions().sessions.snapshots);
  assert.equal(applied(), 2);
  // The same again changes nothing.
  succeeded(await setup.alasio(["up"]));
  assert.equal(applied(), 3);
  const status = await setup.alasio(["status"]);
  assert.ok(status.printed.includes("  sessions are held to 3 CPU features, those every node had as alasio up last surveyed them"), status.printed.join("\n"));
  // A node whose agent has not said keeps the baseline as it was, and up says so.
  setup.kube.said.delete(CPU_FEATURES);
  const unsaid = await setup.alasio(["up"]);
  succeeded(unsaid);
  assert.equal(cpuFeatures(), "avx,avx2,sse");
  assert.ok(unsaid.printed[0]?.includes("\nnode machine: its node agent has not said its CPU features yet\n"), unsaid.printed[0]);
  // A narrower node narrows it, and the template before is carried, so a snapshot taken on it still restores.
  setup.kube.said.set(CPU_FEATURES, cpuReport(["sse", "avx"]));
  succeeded(await setup.alasio(["up"]));
  assert.equal(cpuFeatures(), "avx,sse");
  assert.equal(sessions().previousSessions?.podTemplate.metadata.annotations["dev.gvisor.internal.cpufeatures"], "avx,avx2,sse");
  // Features said under another release are not the current one's: the baseline stays.
  setup.kube.said.set(CPU_FEATURES, cpuReport(["sse"], "20000101.0"));
  const stale = await setup.alasio(["up"]);
  succeeded(stale);
  assert.ok(stale.printed[0]?.includes(`\nnode machine: its node agent said its CPU features under gVisor 20000101.0, not ${currentGvisorRelease()}\n`), stale.printed[0]);
  assert.equal(cpuFeatures(), "avx,sse");
});

test("up records no CPU baseline the nodes do not share: sessions are not snapshotted, and it says why", async (t) => {
  const setup = await rig(t);
  await initialize(setup);
  setup.kube.said.set(CPU_FEATURES, cpuReport([]));
  const run = await setup.alasio(["up"]);
  succeeded(run);
  assert.ok(run.printed[0]?.includes("\nsessions are not snapshotted: they are held to no CPU baseline, as the nodes share no CPU feature under gVisor\n"), run.printed[0]);
  const { sessions } = JSON.parse(String((setup.kube.get(TEMPLATES)?.["data"] as Record<string, string>)["templates.json"])) as {
    readonly sessions: { readonly podTemplate: { readonly metadata: { readonly annotations: Record<string, string> } }; readonly snapshots?: unknown };
  };
  assert.equal(sessions.podTemplate.metadata.annotations["dev.gvisor.internal.cpufeatures"], undefined);
  assert.equal(sessions.snapshots, undefined);
});

test("up without a bot says to run init, and without the address Telegram delivers to that init asks it; up past its timeout says what still waits, and why", async (t) => {
  const setup = await rig(t);
  setup.configure({ target: { kubeconfig: { path: setup.kubeconfig } } });
  assert.equal(failure(await setup.alasio(["up"])), "alasio has no Telegram bot in this cluster yet: alasio init gives it one");
  await initialize(setup);
  const initialized = setup.config();
  setup.configure({ target: { kubeconfig: { path: setup.kubeconfig } } });
  assert.equal(
    failure(await setup.alasio(["up"])),
    "install.webhook.publicAddress is not set: the address Telegram delivers the bot's updates to, which the webhook intake's certificate is made for; alasio init asks for it",
  );
  assert.equal(setup.kube.get(DEPLOYMENT), undefined);
  setup.configure(initialized);
  // A bot that allows no user, as one alasio once let in its first user for, before anything changes.
  const bot = setup.kube.get("/api/v1/namespaces/alasio/secrets/alasio-telegram");
  assert.ok(bot);
  setup.kube.put("secrets", { ...bot, data: { ...(bot["data"] as Record<string, string>), allowedUserIds: "" } });
  const refused = "the bot allows no Telegram user (alasio-telegram's allowedUserIds is empty), and alasio admits no one but those it allows: alasio init --allowed-user-ids gives it them";
  assert.equal(failure(await setup.alasio(["up"])), refused);
  assert.equal(failure(await setup.alasio(["upgrade", "--check"])), refused);
  assert.equal(setup.kube.get(DEPLOYMENT), undefined);
  setup.kube.put("secrets", bot);
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
  // A compute neon-control has put to sleep sleeps, and is not unready.
  const compute = kube.get("/apis/apps/v1/namespaces/alasio/deployments/alasio-neon-compute");
  assert.ok(compute?.spec);
  compute.spec["replicas"] = 0;
  const sleeping = await alasio(["status"]);
  succeeded(sleeping);
  assert.ok(sleeping.printed.includes("  Deployment alasio/alasio-neon-compute: asleep"), sleeping.printed.join("\n"));
  const deployment = kube.get(DEPLOYMENT);
  assert.ok(deployment?.status);
  deployment.status["availableReplicas"] = 0;
  const unhealthy = await alasio(["status"]);
  assert.match(failure(unhealthy), /^1 of alasio's \d+ workloads are not ready$/u);
  assert.ok(unhealthy.printed.includes("  Deployment alasio/alasio: 0 of 1 available"));
});

test("status says how Telegram delivers each bot's updates, and fails when it does not, or its deliveries fail, with the fix Telegram's error points to", async (t) => {
  const { alasio, telegram } = await installed(t);
  const delivered = await alasio(["status"]);
  succeeded(delivered);
  const line = (run: CliRun) => run.printed[run.printed.indexOf("Telegram's deliveries:") + 1];
  assert.equal(line(delivered), `  main's bot @${BOT}: delivered, 0 updates waiting`);
  const lately = Math.floor(Date.now() / 1000) - 60;
  const at = new Date(lately * 1000).toISOString().replace(/\.\d{3}Z$/u, "Z");
  telegram.pending.set(BOT_TOKEN, 3);
  // Telegram cannot connect, as where its networks are not among those the intake admits.
  telegram.lastErrors.set(BOT_TOKEN, { date: lately, message: "Connection timed out" });
  const unreachable = await alasio(["status"]);
  assert.equal(
    failure(unreachable),
    "Telegram does not deliver to the webhook intake as it should: Telegram cannot connect to it, as where its networks have changed from those install.webhook.sources lists: " +
      "list Telegram's (core.telegram.org/bots/webhooks) there, and alasio up admits them",
  );
  assert.equal(line(unreachable), `  main's bot @${BOT}: 3 updates waiting; Telegram's last delivery failed at ${at}: Connection timed out`);
  telegram.lastErrors.set(BOT_TOKEN, { date: lately, message: "Failed to resolve host: Name or service not known" });
  assert.equal(
    failure(await alasio(["status"])),
    "Telegram does not deliver to the webhook intake as it should: Telegram cannot resolve install.webhook.publicAddress: give the name an address, or set the address itself, and alasio up registers each bot with it",
  );
  telegram.lastErrors.set(BOT_TOKEN, { date: lately, message: "Wrong response from the webhook: 401 Unauthorized" });
  assert.equal(failure(await alasio(["status"])), "Telegram does not deliver to the webhook intake as it should: alasio up registers each bot again");
  // A failure long past is not one now, nor one every update was delivered after.
  telegram.lastErrors.set(BOT_TOKEN, { date: Math.floor(Date.now() / 1000) - 3600, message: "Connection timed out" });
  succeeded(await alasio(["status"]));
  telegram.lastErrors.set(BOT_TOKEN, { date: lately, message: "Connection timed out" });
  telegram.pending.delete(BOT_TOKEN);
  succeeded(await alasio(["status"]));
  telegram.webhooks.delete(BOT_TOKEN);
  const undelivered = await alasio(["status"]);
  assert.equal(failure(undelivered), "Telegram does not deliver to the webhook intake as it should: alasio up registers each bot again");
  assert.equal(line(undelivered), `  main's bot @${BOT}: not delivered: alasio up registers it`);
});

test("status says each workload's replicas and scale, its requests beside VPA's recommendation, and the cluster's memory, and where no metrics server measures them", async (t) => {
  const { alasio, kube } = await installed(t);
  const lines = async () => {
    const run = await alasio(["status"]);
    succeeded(run);
    return run.printed;
  };
  const after = (printed: readonly string[], line: string) => printed[printed.indexOf(line) + 1];
  let printed = await lines();
  assert.equal(after(printed, "  Deployment alasio/alasio: ready"), "    1 replica; requests 250m CPU and 768 MiB");
  assert.equal(after(printed, "  DaemonSet kube-system/juicefs-csi-node: ready"), "    requests 30m CPU and 96 MiB");
  assert.equal(printed.at(-1), "the cluster has no metrics server: VPA recommends nothing");

  const vpa = kube.get("/apis/autoscaling.k8s.io/v1/namespaces/alasio/verticalpodautoscalers/alasio");
  assert.ok(vpa);
  vpa.status = { recommendation: { containerRecommendations: [{ containerName: "alasio", target: { cpu: "120m", memory: "600Mi" } }] } };
  kube.put("scaledobjects", {
    apiVersion: "keda.sh/v1alpha1",
    kind: "ScaledObject",
    metadata: { name: "alasio-grafana", namespace: "alasio", labels: { "alasio.dev/installation": "alasio" } },
    spec: { scaleTargetRef: { name: "alasio-grafana" }, minReplicaCount: 1, maxReplicaCount: 3, triggers: [{ type: "cpu", metricType: "Utilization", metadata: { value: "80" } }] },
  });
  kube.put("nodes", { apiVersion: "v1", kind: "Node", metadata: { name: "machine" }, status: { allocatable: { memory: "14Gi" } } });
  // What the pods that run request; a pod done requests nothing.
  const pod = (name: string, phase: string) => ({
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, namespace: "elsewhere" },
    spec: { containers: [{ name: "main", resources: { requests: { memory: "1Gi" } } }] },
    status: { phase },
  });
  kube.put("pods", pod("running", "Running"));
  kube.put("pods", pod("done", "Succeeded"));
  printed = await lines();
  assert.equal(after(printed, "  Deployment alasio/alasio: ready"), "    1 replica; requests 250m CPU and 768 MiB, VPA recommends 120m CPU and 600 MiB");
  assert.equal(after(printed, "  Deployment alasio/alasio-grafana: ready"), "    1 replica of 1 to 3, scaled by KEDA; requests 50m CPU and 256 MiB");
  assert.equal(printed.at(-2), "memory: 1.0 GiB requested, of 14.0 GiB the nodes have for pods");
  assert.equal(printed.at(-1), "the cluster has no metrics server: VPA recommends nothing, and what scales on its CPU (alasio-grafana) does not scale");

  kube.put("apiservices", { apiVersion: "apiregistration.k8s.io/v1", kind: "APIService", metadata: { name: "v1beta1.metrics.k8s.io" }, status: { conditions: [{ type: "Available", status: "True" }] } });
  kube.put("nodes", { apiVersion: "metrics.k8s.io/v1beta1", kind: "NodeMetrics", metadata: { name: "machine" }, usage: { memory: "3584Mi" } });
  assert.equal((await lines()).at(-1), "memory: 1.0 GiB requested, 3.5 GiB used, of 14.0 GiB the nodes have for pods");
});

test("status says the copies the object store keeps of each object, and of each volume server how near its disk is to its taking no more writes", async (t) => {
  const { alasio, kube } = await installed(t);
  const volumeServer = (name: string, phase: string) => ({
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, namespace: "alasio", labels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "seaweedfs-volume" } },
    status: { phase },
  });
  kube.put("pods", volumeServer("alasio-seaweedfs-volume-0", "Running"));
  kube.put("pods", volumeServer("alasio-seaweedfs-volume-1", "Running"));
  kube.put("pods", volumeServer("alasio-seaweedfs-volume-2", "Pending"));
  const GiB = 2 ** 30;
  // A volume server's metrics, as SeaweedFS serves them: its disk of 100 GiB, 20 free, 5 of them kept free.
  const metrics = [
    "# TYPE SeaweedFS_volumeServer_resource gauge",
    `SeaweedFS_volumeServer_resource{name="/data",type="all"} ${100 * GiB}`,
    `SeaweedFS_volumeServer_resource{name="/data",type="avail"} ${15 * GiB}`,
    `SeaweedFS_volumeServer_resource{name="/data",type="free"} ${20 * GiB}`,
    `SeaweedFS_volumeServer_resource{name="/data",type="used"} ${80 * GiB}`,
    "SeaweedFS_volumeServer_max_volumes 100",
    'SeaweedFS_volumeServer_volumes{collection="neon",type="volume"} 40',
    'SeaweedFS_volumeServer_volumes{collection="lake",type="volume"} 4',
    `SeaweedFS_volumeServer_total_disk_size{collection="neon",type="normal"} ${20 * GiB}`,
    `SeaweedFS_volumeServer_total_disk_size{collection="neon",type="deleted_bytes"} ${GiB}`,
    `SeaweedFS_volumeServer_total_disk_size{collection="lake",type="normal"} ${GiB}`,
    'SeaweedFS_volumeServer_total_disk_size{collection="lake",type="deleted_bytes"} 0',
    "",
  ].join("\n");
  // The second answers with no metrics of its disk.
  kube.onExec = ({ pod, container, command }) =>
    pod === "alasio-seaweedfs-volume-0" && container === "volume" && command.join(" ") === "wget -q -O - http://127.0.0.1:9327/metrics" ? { exitCode: 0, stdout: metrics } : { exitCode: 0, stdout: "" };
  const run = await alasio(["status"]);
  succeeded(run);
  const at = run.printed.indexOf("the object store: each object kept once (replication 000)");
  assert.ok(at > 0, run.printed.join("\n"));
  assert.deepEqual(run.printed.slice(at + 1, at + 4), [
    "  alasio-seaweedfs-volume-0: 15.0 GiB to fill before it takes no more writes, of 20.0 GiB free on its disk of 100.0 GiB, 5.0 GiB kept free (minFreeSpace); 44 of 100 volumes, taking 21.0 GiB, 1.0 GiB of it deleted",
    "  alasio-seaweedfs-volume-1: not measured, as its metrics do not say its disk",
    "  alasio-seaweedfs-volume-2: not measured, as it does not run",
  ]);
});

test("status with rights in alasio's namespace alone says what it sees there, and what it does not see", async (t) => {
  const { alasio, kube } = await installed(t);
  for (const path of ["/apis/apps/v1/deployments", "/apis/apps/v1/statefulsets", "/apis/apps/v1/daemonsets", "/apis/keda.sh/v1alpha1/scaledobjects", "/apis/autoscaling.k8s.io/v1/verticalpodautoscalers", "/api/v1/pods", "/api/v1/nodes", "/apis/apiregistration.k8s.io/v1/apiservices/v1beta1.metrics.k8s.io"]) {
    kube.forbidden.add(path);
  }
  const run = await alasio(["status"]);
  succeeded(run);
  assert.ok(run.printed.includes("  Deployment alasio/alasio: ready"));
  assert.ok(!run.printed.some((line) => line.includes("kube-system")), "nothing of kube-system's is listed");
  // The object store's, in alasio's namespace, it sees; its volume server's metrics, here, say nothing; Telegram it asks.
  assert.deepEqual(run.printed.slice(-7), [
    "  alasio's workloads outside its namespace, alasio: not seen, as this kubeconfig may list them there alone",
    "the object store: each object kept once (replication 000)",
    "  alasio-seaweedfs-volume-0: not measured, as its metrics do not say its disk",
    "Telegram's deliveries:",
    `  main's bot @${BOT}: delivered, 0 updates waiting`,
    "memory: not seen, as this kubeconfig may not list the cluster's nodes and pods",
    "whether the cluster has a metrics server: not seen, as this kubeconfig may not read its APIServices",
  ]);
});

test("up and upgrade refuse to start the object store's parts while its data is still on the volume of the one pod it ran as", async (t) => {
  const { alasio, kube } = await installed(t);
  kube.put("persistentvolumeclaims", { apiVersion: "v1", kind: "PersistentVolumeClaim", metadata: { name: "data-alasio-seaweedfs-0", namespace: "alasio" }, spec: {} });
  const refusal = "the object store still keeps its data on the volume of its one pod, the claim data-alasio-seaweedfs-0, which this release's parts do not use: " +
    "they would start the stack on an empty store, so alasio does not apply them; move the data onto them first with the release's cut-over, which deletes data-alasio-seaweedfs-0 once it has";
  assert.equal(failure(await alasio(["up"])), refusal);
  assert.equal(failure(await alasio(["upgrade"])), refusal);
  kube.objects.delete("/api/v1/namespaces/alasio/persistentvolumeclaims/data-alasio-seaweedfs-0");
  succeeded(await alasio(["up"]));
});

test("up changes no count of the object store's masters, nor lowers its volume servers' while one it would remove holds volumes", async (t) => {
  const { alasio, config, configure, kube } = await installed(t);
  const statefulSet = (name: string) => kube.get(`/apis/apps/v1/namespaces/alasio/statefulsets/${name}`);
  const scale = (name: string, replicas: number) => {
    const found = statefulSet(name);
    assert.ok(found?.spec);
    found.spec["replicas"] = replicas;
  };
  // Three masters run, which the configuration's one would lower.
  scale("alasio-seaweedfs-master", 3);
  assert.equal(
    failure(await alasio(["up"])),
    "the object store runs 3 masters, whose Raft keeps where every volume is: fewer would lose its quorum, and that with it, so alasio does not lower their count; objectStore.bundled.masters.scaling stays 3",
  );
  scale("alasio-seaweedfs-master", 1);
  // One master runs, which the configuration's three would raise.
  const before = config() as { install: object };
  configure({ ...before, install: { ...before.install, objectStore: { bundled: { masters: { scaling: { min: 3, max: 3 } } } } } });
  assert.equal(
    failure(await alasio(["up"])),
    "the object store runs 1 master, whose state, made without peers, is no Raft's that more masters could join, so alasio does not raise their count; " +
      "objectStore.bundled.masters.scaling stays 1, as the count of masters is set as the store is made",
  );
  configure(before);
  // Three volume servers run, of which the third holds volumes, as a running master says.
  scale("alasio-seaweedfs-volume", 3);
  kube.put("pods", {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name: "alasio-seaweedfs-master-0", namespace: "alasio", labels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "seaweedfs-master" } },
    status: { phase: "Running" },
  });
  const server = (ordinal: number, volumes: number) => ({ Url: `alasio-seaweedfs-volume-${ordinal}.alasio-seaweedfs-volume.alasio.svc.cluster.local:8080`, Volumes: volumes, EcShards: 0 });
  let held = 4;
  // The storage controller still says what Neon's storage holds, as the rig has it.
  const others = kube.onExec;
  kube.onExec = (exec) =>
    exec.pod === "alasio-seaweedfs-master-0" && exec.container === "master" && exec.command.at(-1) === "http://127.0.0.1:9333/dir/status"
      ? { exitCode: 0, stdout: JSON.stringify({ Topology: { DataCenters: [{ Racks: [{ DataNodes: [server(0, 7), server(1, 0), server(2, held)] }] }] } }) }
      : others(exec);
  assert.equal(
    failure(await alasio(["up"])),
    "objectStore.bundled.volumeServers.scaling would remove volume servers that still hold volumes, alasio-seaweedfs-volume-2.alasio-seaweedfs-volume.alasio.svc.cluster.local:8080: " +
      "move them to the others first, in weed shell in a gateway's buckets container, with lock, then volume.server.evacuate -node alasio-seaweedfs-volume-2.alasio-seaweedfs-volume.alasio.svc.cluster.local:8080 -apply, then unlock; then lower their count",
  );
  // Once they hold none, their count is lowered.
  held = 0;
  succeeded(await alasio(["up"]));
  assert.equal(statefulSet("alasio-seaweedfs-volume")?.spec?.["replicas"], 1);
});

test("restore-filer-store lists the filer store's backups, and puts one back with what writes to the store stopped, alasio and Neon first, the log left running, and started again as it was, then the lake's catalog set right with it, saying what the lake lost", async (t) => {
  const { alasio, kube } = await installed(t);
  const at = (path: string) => kube.get(`/apis/${path}`);
  const listing = "filer-store-20261013T082300Z.rdb\nfiler-store-20261013T092300Z.rdb\n";
  kube.logs.set("alasio-seaweedfs-filer-store-backups-listing-0/list", listing);
  const listed = await alasio(["restore-filer-store"]);
  succeeded(listed);
  assert.deepEqual(listed.printed.join("\n").split("\n"), [
    "The filer store's backups, the newest first:",
    "  20261013T092300Z  2026-10-13 09:23:00 UTC",
    "  20261013T082300Z  2026-10-13 08:23:00 UTC",
    "",
    "alasio restore-filer-store <backup> puts one back.",
  ]);
  // One it does not have is refused before anything is stopped.
  kube.logs.set("alasio-seaweedfs-filer-store-backups-listing-0/list", listing);
  const before = kube.changes.length;
  assert.equal(
    failure(await alasio(["restore-filer-store", "20261012T092300Z", "--yes"])),
    "the filer store has no backup 20261012T092300Z; it has 20261013T092300Z, 20261013T082300Z",
  );
  assert.deepEqual(kube.changes.slice(before).filter(({ path }) => !path.includes("filer-store-backups-listing")), []);

  // A branch environment's alasio; the gateways scaled by KEDA, and the lake loader's ScaledObject paused before; the lifecycle pass suspended before; a backup of Neon's running.
  const labels = (component: string) => ({ "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": component });
  kube.put("namespaces", { apiVersion: "v1", kind: "Namespace", metadata: { name: "alasio-branch-feature", labels: { "alasio.dev/branch": "feature" } } });
  kube.put("deployments", { apiVersion: "apps/v1", kind: "Deployment", metadata: { name: "alasio", namespace: "alasio-branch-feature", labels: labels("alasio") }, spec: { replicas: 1, selector: { matchLabels: labels("alasio") } } });
  kube.put("pods", { apiVersion: "v1", kind: "Pod", metadata: { name: "alasio-0", namespace: "alasio-branch-feature", labels: labels("alasio") }, status: { phase: "Running" } });
  const scaler = (name: string, annotations: Record<string, string>) =>
    kube.put("scaledobjects", { apiVersion: "keda.sh/v1alpha1", kind: "ScaledObject", metadata: { name, namespace: "alasio", labels: labels(name), annotations }, spec: { scaleTargetRef: { name } } });
  scaler("alasio-seaweedfs", {});
  scaler("alasio-lake-loader", { "autoscaling.keda.sh/paused-replicas": "1" });
  const lifecycle = at("batch/v1/namespaces/alasio/cronjobs/alasio-seaweedfs-lifecycle");
  assert.ok(lifecycle?.spec);
  lifecycle.spec["suspend"] = true;
  kube.put("jobs", { apiVersion: "batch/v1", kind: "Job", metadata: { name: "alasio-neon-backup-1", namespace: "alasio", labels: labels("neon-backup") }, spec: {}, status: { active: 1 } });
  setTimeout(() => {
    const job = at("batch/v1/namespaces/alasio/jobs/alasio-neon-backup-1");
    if (job) job.status = { conditions: [{ type: "Complete", status: "True" }] };
  }, 500);
  kube.logs.set("alasio-seaweedfs-filer-store-restore-0/restore", "restored 1234 keys\n");
  kube.onExec = ({ pod, container }) =>
    pod === "alasio-seaweedfs-filer-store-0" && container === "valkey"
      ? { exitCode: 0, stdout: "# Keyspace\ndb0:keys=1234,expires=0,avg_ttl=0\n# Persistence\nloading:0\naof_enabled:1\naof_last_bgrewrite_status:ok\n" }
      : { exitCode: 0 };
  // Each lake's reconciliation, main's and the branch environment's, once they run again, which says what each table lost (neon/lake/src/maintain.ts).
  const lost = (table: string, files: number, rows: number, recovered: "database" | "log") =>
    JSON.stringify({ message: "lost from the object store", table, files, rows, from: "2026-10-13 09:30:00+00", to: "2026-10-13 10:05:00+00", recovered });
  // The logs' first minute gone from the log too, and the metrics not read again yet.
  const lostForGood = JSON.stringify({ message: "lost from the object store, and from the log, which no longer holds it", signal: "logs", from: "2026-10-13T09:30:00.000Z", to: "2026-10-13T09:31:00.000Z" });
  const stillToRead = JSON.stringify({ message: "lost from the object store, still to be read again from the log", signals: ["metrics"] });
  kube.logs.set("alasio-lake-reconcile-0/maintenance", [lost("claude.entries", 1, 40, "database"), lost("otel.logs", 3, 1, "log"), lostForGood, stillToRead, '{"message":"reconciled","lost":2}', ""].join("\n"));
  const start = kube.changes.length;
  const restored = await alasio(["restore-filer-store", "20261013T092300Z", "--yes"]);
  succeeded(restored);
  const losses = (lake: string) => [
    `  ${lake}: the lake lost 40 rows of claude.entries, in 1 file, from 2026-10-13 09:30:00+00 to 2026-10-13 10:05:00+00: loaded again from alasio's database`,
    `  ${lake}: the lake lost 1 row of otel.logs, in 3 files, from 2026-10-13 09:30:00+00 to 2026-10-13 10:05:00+00: read again from the log, where it still holds it`,
    `  ${lake}: the lake's logs telemetry from 2026-10-13T09:30:00.000Z to 2026-10-13T09:31:00.000Z cannot be refilled: the log no longer holds it`,
    `  ${lake}: the lake's metrics telemetry is still to be read again from the log: its intake had not taken it up as the reconciliation ended`,
  ];
  assert.deepEqual(restored.printed.join("\n").split("\n"), [
    "The filer store is as it was at 2026-10-13 09:23:00 UTC, with its 1234 keys, and alasio runs on it again.",
    ...losses("main's lake"),
    ...losses("the lake of branch environment feature"),
  ]);
  // Stopped in order: the CronJobs, alasio, Neon and the lake, then the gateways, then the filer store, before the backup is put back.
  const changed = kube.changes.slice(start);
  const first = (path: string) => changed.findIndex((change) => change.path.endsWith(path));
  const order = [
    "/cronjobs/alasio-neon-backup",
    "/apps/v1/namespaces/alasio-branch-feature/deployments/alasio",
    "/statefulsets/alasio-neon-pageserver",
    "/deployments/alasio-seaweedfs",
    "/statefulsets/alasio-seaweedfs-filer-store",
    "/jobs/alasio-seaweedfs-filer-store-restore",
  ].map(first);
  assert.ok(order.every((index, position) => index >= 0 && (position === 0 || index > (order[position - 1] ?? 0))), JSON.stringify(order));
  // The log, whose segments are on its own volumes, goes on throughout, keeping what reaches it, Telegram's updates from the webhook intake among it.
  for (const path of ["apps/v1/namespaces/alasio/statefulsets/alasio-log", "apps/v1/namespaces/alasio/deployments/alasio-webhook"]) {
    assert.ok(at(path), path);
    assert.equal(first(path), -1, path);
  }
  // Started again, what its volumes hold to a node first: Neon's storage before what runs anywhere.
  const last = (path: string) => changed.findLastIndex((change) => change.path.endsWith(path));
  for (const [held, free] of [["/statefulsets/alasio-neon-pageserver", "/deployments/alasio-neon-compute"], ["/statefulsets/alasio-neon-safekeeper", "/deployments/alasio"]] as const) {
    assert.ok(last(held) >= 0 && last(held) < last(free), `${held} before ${free}`);
  }
  // Started again as each was: the replicas, the gateways' ScaledObject unpaused, the lake loader's paused as before, the lifecycle pass still suspended.
  for (const path of ["apps/v1/namespaces/alasio/deployments/alasio-seaweedfs", "apps/v1/namespaces/alasio-branch-feature/deployments/alasio", "apps/v1/namespaces/alasio/statefulsets/alasio-seaweedfs-filer-store"]) {
    const workload = at(path);
    assert.equal(workload?.spec?.["replicas"], 1, path);
    assert.equal((workload?.metadata["annotations"] as Record<string, string> | undefined)?.["alasio.dev/restore-filer-store"], undefined, path);
  }
  const annotations = (name: string) => at(`keda.sh/v1alpha1/namespaces/alasio/scaledobjects/${name}`)?.metadata["annotations"];
  assert.deepEqual([annotations("alasio-seaweedfs"), annotations("alasio-lake-loader")], [{}, { "autoscaling.keda.sh/paused-replicas": "1" }]);
  assert.deepEqual(
    ["alasio-seaweedfs-lifecycle", "alasio-seaweedfs-filer-store-backup"].map((name) => at(`batch/v1/namespaces/alasio/cronjobs/${name}`)?.spec?.["suspend"]),
    [true, false],
  );
  // Each reconciliation ran in its environment's namespace once the lakes ran again, before the CronJobs resumed; and nothing of the restore is left but its NetworkPolicy, of the installation.
  for (const namespace of ["alasio", "alasio-branch-feature"]) {
    const reconciled = first(`/namespaces/${namespace}/jobs/alasio-lake-reconcile`);
    assert.ok(reconciled > last("/statefulsets/alasio-neon-safekeeper") && reconciled < last("/cronjobs/alasio-seaweedfs-filer-store-backup"), namespace);
  }
  assert.deepEqual([...kube.objects.keys()].filter((path) => (path.includes("filer-store-restore") && !path.includes("/networkpolicies/")) || path.includes("lake-reconcile")), []);

  // A filer store that holds other than what was restored is said, and what is stopped stays so, until the next run.
  kube.logs.set("alasio-seaweedfs-filer-store-backups-listing-0/list", listing);
  kube.logs.set("alasio-seaweedfs-filer-store-restore-0/restore", "restored 1235 keys\n");
  assert.equal(
    failure(await alasio(["restore-filer-store", "20261013T092300Z", "--yes"])),
    "the filer store holds 1234 keys of the 1235 restored: everything else stays stopped; alasio restore-filer-store 20261013T092300Z again puts the backup back anew",
  );
  assert.equal(at("apps/v1/namespaces/alasio/deployments/alasio-seaweedfs")?.spec?.["replicas"], 0);
  assert.deepEqual(annotations("alasio-lake-loader"), {
    "autoscaling.keda.sh/paused-replicas": "0",
    "alasio.dev/restore-filer-store": '{"autoscaling.keda.sh/paused-replicas":"1","autoscaling.keda.sh/paused":null}',
  });
  kube.logs.set("alasio-seaweedfs-filer-store-backups-listing-0/list", listing);
  kube.logs.set("alasio-seaweedfs-filer-store-restore-0/restore", "restored 1234 keys\n");
  succeeded(await alasio(["restore-filer-store", "20261013T092300Z", "--yes"]));
  assert.equal(at("apps/v1/namespaces/alasio/deployments/alasio-seaweedfs")?.spec?.["replicas"], 1);
  assert.deepEqual(annotations("alasio-lake-loader"), { "autoscaling.keda.sh/paused-replicas": "1" });
});

test("init and up raise inotify limits too low for the cluster here as root, now and for every boot, and status says them too low", async (t) => {
  const { alasio, configure, home, kube, machine } = await rig(t);
  configure({ target: { docker: { name: "dev", apiPort: kube.port, storagePath: join(home, "storage") } } });
  machine.sysctl("fs.inotify.max_user_instances", 128);
  const shortfall = "fs.inotify.max_user_instances is 128, and the cluster needs at least 1024";
  const tokenFile = join(home, "bot-token");
  writeFileSync(tokenFile, BOT_TOKEN);
  const init = await alasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--public-address", PUBLIC_ADDRESS, "--up"]);
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

test("logs prints a component's log, each line with its pod's name when it has several pods, and its container's when several containers", async (t) => {
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
  // The log's pod's containers, each line said with its container's name.
  kube.logs.set("alasio-log-0/log", "serving\n");
  kube.logs.set("alasio-log-0/access", "applied\n");
  const log = await alasio(["logs", "log"]);
  succeeded(log);
  assert.deepEqual(log.stdout.split("\n").filter(Boolean).sort(), ["[access] applied", "[log] serving"]);
  assert.match(failure(await alasio(["logs", "nothing"])), /^alasio has no component nothing; it has agent-sandbox-controller, alasio, collector, collector-scraper, grafana, keda-admission-webhooks, keda-metrics-apiserver, keda-operator, lake-intake, lake-loader, lake-query, log, /u);
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
  setup.configure({ target: { kubeconfig: { path: setup.kubeconfig } }, install: { webhook: { publicAddress: PUBLIC_ADDRESS }, images: { lake: { tag: "9.9.9" } } } });
  const changed = await setup.alasio(["upgrade"]);
  succeeded(changed);
  assert.equal(changed.printed[0], [
    "alasio 0.0.0-development, with other images:",
    ...["loader", "maintenance", "merge", "query", "intake"].map((part) => `  alasio-lake-${part}: ghcr.io/eaucoin/alasio-lake:0.0.0-development → ghcr.io/eaucoin/alasio-lake:9.9.9`),
  ].join("\n"));
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

test("lake reconcile runs an environment's reconciliation as its Job, in its namespace, says what its lake lost, and fails as the Job does", async (t) => {
  const { alasio, kube } = await installed(t);
  const job = (namespace: string) => kube.changes.findIndex(({ method, path }) => method !== "DELETE" && path.endsWith(`/namespaces/${namespace}/jobs/alasio-lake-reconcile`));
  kube.logs.set("alasio-lake-reconcile-0/maintenance", '{"message":"reconciled","lost":0}\n');
  const none = await alasio(["lake", "reconcile"]);
  succeeded(none);
  assert.deepEqual(none.printed, ["the lake lost no file"]);
  assert.ok(job("alasio") >= 0);
  assert.equal(kube.get("/apis/batch/v1/namespaces/alasio/jobs/alasio-lake-reconcile"), undefined);
  // A branch environment's, against its own catalog; one there is not, refused before anything runs.
  assert.equal(failure(await alasio(["lake", "reconcile", "--branch", "try"])), "there is no branch environment try");
  assert.equal(job("alasio-branch-try"), -1);
  kube.put("namespaces", { apiVersion: "v1", kind: "Namespace", metadata: { name: "alasio-branch-try", labels: { "alasio.dev/branch": "try" } } });
  kube.logs.set("alasio-lake-reconcile-0/maintenance", [
    JSON.stringify({ message: "lost from the object store, loaded again from alasio's database", table: "codex.lines", files: 2, rows: 7, from: null, to: null, recovered: "database" }),
    JSON.stringify({ message: "lost from the object store, main's telemetry as of the branch point, which main's lake reads again from the log", table: "otel.logs", files: 1, rows: 3, from: null, to: null, recovered: "main" }),
    "",
  ].join("\n"));
  const branch = await alasio(["lake", "reconcile", "--branch", "try"]);
  succeeded(branch);
  assert.deepEqual(branch.printed.join("\n").split("\n"), [
    "the lake lost 7 rows of codex.lines, in 2 files: loaded again from alasio's database",
    "the lake lost 3 rows of otel.logs, in 1 file: main's telemetry as of the branch point, which main's lake reads again from the log",
  ]);
  assert.ok(job("alasio-branch-try") >= 0);
  kube.failing.add("alasio-lake-reconcile");
  assert.match(failure(await alasio(["lake", "reconcile"])), /alasio-lake-reconcile/u);
  // Failed, saying why: why.
  kube.logs.set("alasio-lake-reconcile-0/maintenance", `${JSON.stringify({ message: "reconciling the catalog with the object store failed", error: "HTTP Error: 403" })}\n`);
  assert.equal(failure(await alasio(["lake", "reconcile"])), "the lake's reconciliation in alasio failed: HTTP Error: 403");
  // One that runs still is left to end, and refused; one that ended is removed, and run afresh.
  kube.failing.delete("alasio-lake-reconcile");
  kube.logs.set("alasio-lake-reconcile-0/maintenance", '{"message":"reconciled","lost":0}\n');
  const running = { apiVersion: "batch/v1", kind: "Job", metadata: { name: "alasio-lake-reconcile", namespace: "alasio" }, spec: {}, status: { active: 1 } };
  kube.put("jobs", running);
  kube.changes.length = 0;
  assert.equal(
    failure(await alasio(["lake", "reconcile"])),
    "a reconciliation of the lake in alasio runs already, as its Job alasio-lake-reconcile does: once it ends (kubectl -n alasio get job alasio-lake-reconcile), run this again",
  );
  assert.deepEqual(kube.changes.filter(({ method }) => method !== "GET"), []);
  kube.put("jobs", { ...running, status: { conditions: [{ type: "Complete", status: "True" }] } });
  succeeded(await alasio(["lake", "reconcile"]));
});

test("grafana forwards a port here to Grafana's pod until interrupted, and prints its admin's password when asked", async (t) => {
  const { alasio, env, kube, machine } = await installed(t);
  // Grafana, as its pod serves it.
  const grafana = createHttpServer((request, response) => response.end(`grafana answers ${request.url}`));
  await new Promise<void>((resolve) => grafana.listen(0, "127.0.0.1", resolve));
  t.after(() => grafana.close());
  // A server listening on a TCP port has an address of its own.
  kube.forwarded.set("alasio-grafana-0:3000", (grafana.address() as AddressInfo).port);
  const port = await freePort();
  let interrupt = () => {};
  const running = runAlasio(["grafana", "--port", String(port)], { env, machine, until: new Promise((resolve) => (interrupt = resolve)) });
  let answered: string | null = null;
  for (let attempt = 0; attempt < 100 && answered === null; attempt++) {
    answered = await fetch(`http://127.0.0.1:${port}/api/health`).then((response) => response.text(), () => null);
    if (answered === null) await new Promise((resolve) => setTimeout(resolve, 100));
  }
  interrupt();
  const run = await running;
  assert.equal(answered, "grafana answers /api/health");
  assert.deepEqual(kube.forwards.at(-1), { namespace: "alasio", pod: "alasio-grafana-0", port: 3000 });
  assert.deepEqual(run.printed, [`Grafana is at http://127.0.0.1:${port}, as admin; alasio grafana --password prints its password. Interrupt this to end it.`]);
  await assert.rejects(fetch(`http://127.0.0.1:${port}/api/health`), "the port is closed once interrupted");

  assert.equal(failure(await alasio(["grafana", "--password"])), "alasio-grafana holds no admin password: alasio up writes it");
  kube.put("secrets", { apiVersion: "v1", kind: "Secret", metadata: { name: "alasio-grafana", namespace: "alasio" }, data: { GF_SECURITY_ADMIN_PASSWORD: Buffer.from("admin-password").toString("base64") } });
  const password = await alasio(["grafana", "--password"]);
  succeeded(password);
  assert.deepEqual(password.printed, ["admin-password"]);
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
  assert.ok(kube.get("/api/v1/namespaces/alasio/persistentvolumeclaims/alasio-seaweedfs-filer-store-backups"));
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
  succeeded(await alasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--public-address", PUBLIC_ADDRESS, "--up"]));
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
  const run = await alasio(["init", "--non-interactive", "--target", "host", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--public-address", PUBLIC_ADDRESS, "--up"]);
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
    "write k3s's configuration in /etc/rancher/k3s, and its containerd's, with gVisor as the runtime runsc-20990101-0, in /var/lib/rancher/k3s/agent/etc/containerd",
    "restart the service k3s, with its new configuration",
    "read its kubeconfig, /etc/rancher/k3s/k3s.yaml, for alasio to reach it",
  ]);
  assert.match(machine.read("/etc/rancher/k3s/registries.yaml") ?? "", /"mirrors":\{"docker\.io":\{"endpoint":\["https:\/\/mirror\.lan"\]\}\}/u);
  assert.deepEqual(machine.commands().slice(ran), ["systemctl restart k3s"]);
});

test("upgrade installs this version's k3s and gVisor release beside the one before, then removes that once it is no longer pinned, and up after each nothing", async (t) => {
  const setup = await onHost(t);
  const { env, kube, machine } = setup;
  const handlers = () =>
    [...kube.objects].filter(([path]) => path.startsWith("/apis/node.k8s.io/v1/runtimeclasses/")).map(([, { metadata, handler }]) => `${metadata.name} ${String(handler)}`).sort();
  const newer = fakeReleases({ k3s: "v1.100.0+k3s1", gvisor: ["21000101.0", "20990101.0"] });
  const upgraded = await runAlasio(["upgrade"], { env, machine, releases: newer });
  succeeded(upgraded);
  assert.deepEqual(rootSteps(upgraded), [
    "write k3s's configuration in /etc/rancher/k3s, and its containerd's, with gVisor as the runtimes runsc-21000101-0 and runsc-20990101-0, in /var/lib/rancher/k3s/agent/etc/containerd",
    "install gVisor 21000101.0 in /usr/local/lib/alasio/gvisor/21000101.0: runsc, containerd-shim-runsc-v1, gvisor-bin",
    "install k3s v1.100.0+k3s1, in place of v1.99.0+k3s1, in /usr/local/bin, with its own install script, as the systemd service k3s, enabled and started anew",
    "read its kubeconfig, /etc/rancher/k3s/k3s.yaml, for alasio to reach it",
  ]);
  assert.equal(machine.read("/usr/local/lib/alasio/gvisor/21000101.0/runsc"), "runsc 21000101.0\n");
  assert.equal(machine.read("/usr/local/lib/alasio/gvisor/20990101.0/runsc"), "runsc 20990101.0\n");
  assert.equal(machine.read("/usr/local/bin/k3s"), "k3s v1.100.0+k3s1\n");
  assert.deepEqual(handlers(), ["gvisor runsc-21000101-0", "gvisor-20990101-0 runsc-20990101-0", "gvisor-21000101-0 runsc-21000101-0"]);
  assert.deepEqual(rootSteps(await runAlasio(["up"], { env, machine, releases: newer })), []);
  assert.equal((await runAlasio(["status"], { env, machine, releases: newer })).printed[0], "k3s v1.100.0+k3s1 on this machine, with gVisor 20990101.0 and 21000101.0:");

  const latest = fakeReleases({ k3s: "v1.100.0+k3s1", gvisor: ["21000101.0"] });
  const pruned = await runAlasio(["upgrade"], { env, machine, releases: latest });
  succeeded(pruned);
  assert.deepEqual(rootSteps(pruned), [
    "write k3s's configuration in /etc/rancher/k3s, and its containerd's, with gVisor as the runtime runsc-21000101-0, in /var/lib/rancher/k3s/agent/etc/containerd",
    "stop the sandboxes of gVisor 20990101.0, which alasio no longer keeps, and remove /usr/local/lib/alasio/gvisor/20990101.0",
    "restart the service k3s, with its new configuration",
    "read its kubeconfig, /etc/rancher/k3s/k3s.yaml, for alasio to reach it",
  ]);
  assert.equal(machine.has("/usr/local/lib/alasio/gvisor/20990101.0"), false);
  assert.deepEqual(handlers(), ["gvisor runsc-21000101-0", "gvisor-21000101-0 runsc-21000101-0"]);
  assert.deepEqual(rootSteps(await runAlasio(["up"], { env, machine, releases: latest })), []);
});

test("an up or upgrade alasio refuses changes nothing of the machine first: no k3s or gVisor installed anew, nothing run as root", async (t) => {
  const setup = await onHost(t);
  const newer = fakeReleases({ k3s: "v1.100.0+k3s1", gvisor: ["21000101.0"] });
  const run = (command: string) => runAlasio(command.split(" "), { env: setup.env, machine: setup.machine, releases: newer });
  const ran = setup.machine.ran.length;
  setup.kube.put("persistentvolumeclaims", { apiVersion: "v1", kind: "PersistentVolumeClaim", metadata: { name: "data-alasio-seaweedfs-0", namespace: "alasio" }, spec: {} });
  for (const command of ["up", "upgrade", "upgrade --check"]) {
    const refused = await run(command);
    assert.match(failure(refused), /^the object store still keeps its data on the volume of its one pod, the claim data-alasio-seaweedfs-0, /u);
    assert.deepEqual(rootSteps(refused), []);
  }
  setup.kube.objects.delete("/api/v1/namespaces/alasio/persistentvolumeclaims/data-alasio-seaweedfs-0");
  // A branch environment another release made.
  setup.kube.put("namespaces", { apiVersion: "v1", kind: "Namespace", metadata: { name: "alasio-branch-try", labels: { "alasio.dev/branch": "try" } } });
  setup.kube.put("deployments", { apiVersion: "apps/v1", kind: "Deployment", metadata: { name: "alasio", namespace: "alasio-branch-try", labels: { "app.kubernetes.io/version": "6.0.0" } }, spec: {} });
  for (const command of ["upgrade", "upgrade --check"]) {
    const refused = await run(command);
    assert.match(failure(refused), /^alasio upgrades main alone, and a branch environment another release made would not run on what upgrading changes: try, which alasio 6\.0\.0 made\. /u);
    assert.deepEqual(rootSteps(refused), []);
  }
  setup.kube.objects.delete("/api/v1/namespaces/alasio-branch-try");
  const checked = await run("upgrade --check");
  succeeded(checked);
  assert.deepEqual(checked.printed, ["alasio would upgrade k3s on this machine to 0.0.0-development: it refuses nothing there now; nothing was changed."]);
  assert.deepEqual(rootSteps(checked), []);
  assert.equal(setup.machine.ran.length, ran);
  assert.equal(setup.machine.read("/usr/local/bin/k3s"), "k3s v1.99.0+k3s1\n");
  assert.equal(setup.machine.read("/usr/local/lib/alasio/gvisor/20990101.0/runsc"), "runsc 20990101.0\n");
  assert.equal(setup.machine.has("/usr/local/lib/alasio/gvisor/21000101.0"), false);
});

test("upgrade refuses to change the machine for a cluster it made that does not answer, where up starts it", async (t) => {
  const setup = await onHost(t);
  const newer = fakeReleases({ k3s: "v1.100.0+k3s1", gvisor: ["21000101.0"] });
  const run = (args: readonly string[]) => runAlasio(args, { env: setup.env, machine: setup.machine, releases: newer });
  const kubeconfig = join(dirname(setup.configFile), "kubeconfig");
  const written: { clusters: { cluster: { server: string } }[] } = JSON.parse(readFileSync(kubeconfig, "utf8"));
  // The API as a stopped k3s leaves it: nothing listens where the kubeconfig points.
  for (const { cluster } of written.clusters) cluster.server = "https://127.0.0.1:1";
  writeFileSync(kubeconfig, JSON.stringify(written));
  const ran = setup.machine.ran.length;
  const refused = await run(["upgrade"]);
  assert.equal(
    failure(refused),
    "k3s on this machine does not answer, so alasio cannot check whether it would refuse this upgrade before it changes this machine for it: " +
      "start it as it is, with the alasio that made it (its alasio up), then upgrade",
  );
  assert.deepEqual(rootSteps(refused), []);
  assert.equal(setup.machine.ran.length, ran);
  assert.match(failure(await run(["upgrade", "--check"])), /^Kubernetes could not be reached for GET /u);
  succeeded(await run(["up"]));
});

test("a download that is not what alasio pins is refused before anything is done as root", async (t) => {
  const { env, home, machine } = await rig(t);
  const releases = fakeReleases();
  releases.served.set(downloads(releases.pins).gvisor.get("20990101.0")?.url ?? "", Buffer.from("not gVisor"));
  const tokenFile = join(home, "bot-token");
  writeFileSync(tokenFile, BOT_TOKEN);
  const run = await runAlasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--public-address", PUBLIC_ADDRESS], { env, machine, releases });
  assert.match(
    failure(run),
    /^https:\/\/github\.com\/eaucoin\/gvisor\/releases\/download\/release-20990101\.0\/gvisor-x86_64\.tar\.zstd is not what alasio pins: its sha512 is [0-9a-f]{128}, not [0-9a-f]{128}$/u,
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
  const installed = "/usr/local/lib/alasio/gvisor/20990101.0";
  assert.deepEqual(machine.killed.at(-1), [`${installed}/runsc`, `${installed}/containerd-shim-runsc-v1`, `${installed}/gvisor-bin/gvisor_sentry`]);
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
    failure(await alasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--public-address", PUBLIC_ADDRESS])),
    "k3s is installed on this machine, but not by alasio, which leaves it as it is: run alasio in it with alasio init --kubeconfig /etc/rancher/k3s/k3s.yaml, or uninstall it first (k3s-uninstall.sh)",
  );
  assert.deepEqual(machine.ran, []);
});

test("uninstall --purge removes k3s, gVisor, JuiceFS's directories, the storage, the limits' file and the kubeconfig as root at once, but not the config", async (t) => {
  const { alasio, configFile, env, machine } = await rig(t);
  machine.sysctl("fs.inotify.max_user_instances", 128);
  const tokenFile = join(env.HOME, "bot-token");
  writeFileSync(tokenFile, BOT_TOKEN);
  succeeded(await alasio(["init", "--non-interactive", "--bot-token-file", tokenFile, "--allowed-user-ids", "42", "--public-address", PUBLIC_ADDRESS, "--up"]));
  machine.write("/proc/self/mounts", "proc /proc proc rw 0 0\nJuiceFS:workspaces /var/lib/juicefs/volume/pvc\\040one fuse.juicefs rw 0 0\n");
  machine.write("/var/lib/juicefs/volume/pvc one/file", "data");
  const storagePath = join(env.XDG_DATA_HOME, "alasio", "storage");
  const run = await alasio(["uninstall", "--purge"], [pressed("y")]);
  succeeded(run);
  assert.match(run.prompts, /Remove alasio and all its data from k3s on this machine, and k3s and gVisor themselves\? This cannot be undone/u);
  assert.deepEqual(rootSteps(run), [
    "stop the cluster's gVisor pods, uninstall k3s with k3s-uninstall.sh, which removes its service, /etc/rancher/k3s and its data, and remove what it leaves, its node's password in /etc/rancher/node, gVisor's releases in /usr/local/lib/alasio/gvisor, and JuiceFS's mounts and directories, /var/lib/juicefs and /run/juicefs-csi",
    `remove ${storagePath}, the cluster's volumes`,
    `remove ${SYSCTL_FILE}, which raises the inotify limits at every boot`,
  ]);
  assert.deepEqual(machine.commands().slice(-3), ["systemctl stop k3s", "/usr/local/bin/k3s-uninstall.sh", "umount --lazy /var/lib/juicefs/volume/pvc one"]);
  for (const path of ["/usr/local/bin/k3s", "/usr/local/lib/alasio", "/etc/rancher", "/var/lib/rancher", "/var/lib/juicefs", SYSCTL_FILE]) {
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

/** Neon's branches as the fake neon-control keeps them, and what the computes' databases answer. */
interface FakeNeon {
  readonly branches: { name: string; parent: string | null; state: string; createdAt: string }[];
  readonly asked: { readonly method: string; readonly path: string; readonly body?: unknown }[];
  /** The active turns a branch's database has. */
  turns: string;
  /** Each compute as neon-control lists it. */
  readonly computes: { id: string; namespace: string; name: string; wokenAt: string | null; suspendedAt: string | null; lagBytes?: number | null }[];
  /** Neon's tenant as neon-control has it; null while it cannot say (503). */
  tenant: TenantView | null;
  /** Neon's storage as the storage controller holds it. */
  readonly storage: FakeStorage;
}

/** A Secret of main's, as the stack's setup makes it. */
function stackSecret(kube: FakeKube, name: string, data: Record<string, string>): void {
  kube.put("secrets", { apiVersion: "v1", kind: "Secret", metadata: { name, namespace: "alasio" }, data: Object.fromEntries(Object.entries(data).map(([key, value]) => [key, Buffer.from(value).toString("base64")])) });
}

/** Whether `verifier` is a SCRAM verifier of `password`, made with its own salt. */
function verifies(verifier: string | undefined, password: string): boolean {
  const salt = /^SCRAM-SHA-256\$4096:([^$]+)\$/u.exec(verifier ?? "")?.[1];
  return salt !== undefined && scramVerifier(password, Buffer.from(salt, "base64")) === verifier;
}

/**
 * alasio installed, with the Secrets its stack's setup makes, and neon-control and the
 * computes answering in their pods as they do: neon-control's API through curl, the
 * databases through psql.
 */
async function branchable(t: TestContext): Promise<Rig & { readonly neon: FakeNeon }> {
  const setup = await installed(t);
  stackSecret(setup.kube, "alasio-database", {
    url: "postgresql://alasio:p@alasio-neon-proxy.alasio.svc:5432/alasio?options=endpoint%3Dalasio",
    password: "p",
    "lake-verifier": "SCRAM-SHA-256$l",
    "lake-reader-verifier": "SCRAM-SHA-256$r",
    "grafana-verifier": "SCRAM-SHA-256$g",
  });
  stackSecret(setup.kube, "alasio-lake", {
    LAKE_DATABASE_PASSWORD: "l",
    LAKE_S3_KEY: "k",
    LAKE_S3_SECRET: "s",
    LAKE_READER_PASSWORD: "r",
    LAKE_READER_S3_KEY: "rk",
    LAKE_READER_S3_SECRET: "rs",
    LAKE_QUERY_TOKEN: "q",
    LAKE_BRANCHES_TOKEN: "b",
  });
  stackSecret(setup.kube, "alasio-neon-compute", { NEON_CONTROL_PLANE_TOKEN: "c" });
  stackSecret(setup.kube, "alasio-log-scaler", { sasl: "scram_sha512", username: "scaler", password: "s" });
  stackSecret(setup.kube, "alasio-branches", { "control-token": "admin", "fork-key": "key" });
  const neon: FakeNeon = { branches: [{ name: "main", parent: null, state: "ready", createdAt: "2026-10-07T00:00:00Z" }], asked: [], turns: "0", computes: [],
    tenant: ONE_SHARD,
    storage: { pageservers: 1, shards: 1, answering: true, databaseAnswering: true },
  };
  // The storage controller's database's claim, bound as its first start bound it.
  setup.kube.put("persistentvolumeclaims", { apiVersion: "v1", kind: "PersistentVolumeClaim", metadata: { name: "data-alasio-neon-controller-db-0", namespace: "alasio" }, spec: {}, status: { phase: "Bound" } });
  // The tenant, as the stack's setup names it.
  stackSecret(setup.kube, "alasio-neon-root", { "secrets.json": JSON.stringify({ tenantId: "t" }) });
  setup.kube.onExec = ({ container, command }) => {
    if (container === "neon-control") {
      const method = command[command.indexOf("-X") + 1] ?? "";
      const path = new URL(command.at(-1) ?? "").pathname;
      const data = command.indexOf("--data-binary");
      const body: unknown = data === -1 ? undefined : JSON.parse(command[data + 1] ?? "");
      neon.asked.push({ method, path, ...(body === undefined ? {} : { body }) });
      const answer = (status: number, answered?: unknown) => ({ exitCode: 0, stdout: `${answered === undefined ? "" : JSON.stringify(answered)}\n${status}` });
      if (method === "GET" && path === "/computes") return answer(200, { computes: neon.computes });
      if (method === "GET" && path === "/tenant") return neon.tenant ? answer(200, neon.tenant) : answer(503, { error: "neon-control is bootstrapping" });
      if (method === "GET") return answer(200, { branches: neon.branches });
      if (method === "POST" && path.startsWith("/computes/")) return answer(200, { address: "woken" });
      if (method === "POST") {
        const { name } = body as { name: string };
        neon.branches.push({ name, parent: "main", state: "ready", createdAt: new Date().toISOString() });
        return answer(201, { name, parent: "main", state: "ready" });
      }
      const name = path.split("/").at(-1);
      neon.branches.splice(neon.branches.findIndex((branch) => branch.name === name), 1);
      return answer(204);
    }
    if (container === "storage-controller") return storageControllerAnswer(neon.storage, command);
    if (container === "postgres") return controllerDatabaseAnswer(neon.storage, command);
    if (container === "compute") {
      const sql = command.at(-1) ?? "";
      return { exitCode: 0, stdout: sql.includes("pg_current_wal_lsn") ? "0/1A2B3C8\n" : `${neon.turns}\n` };
    }
    return { exitCode: 0 };
  };
  return { ...setup, neon };
}

/** The value of `key` of the Secret `name` in `namespace`. */
function secretIn(kube: FakeKube, namespace: string, name: string, key: string): string | undefined {
  const encoded = (kube.get(`/api/v1/namespaces/${namespace}/secrets/${name}`)?.["data"] as Record<string, string> | undefined)?.[key];
  return encoded === undefined ? undefined : Buffer.from(encoded, "base64").toString("utf8");
}

const BRANCH_DEPLOYMENT = "/apis/apps/v1/namespaces/alasio-branch-try/deployments/alasio";

test("status says of each compute whether it is awake or asleep, and when neon-control last woke it, and of nothing else that it sleeps", async (t) => {
  const { alasio, kube, neon } = await branchable(t);
  const compute = kube.get("/apis/apps/v1/namespaces/alasio/deployments/alasio-neon-compute");
  assert.ok(compute?.spec);
  let run = await alasio(["status"]);
  succeeded(run);
  assert.ok(run.printed.includes("  Deployment alasio/alasio-neon-compute: awake"), run.printed.join("\n"));
  // Its read replica, a compute too.
  assert.ok(run.printed.includes("  Deployment alasio/alasio-neon-compute-ro: awake"), run.printed.join("\n"));
  neon.computes.push({ id: "alasio", namespace: "alasio", name: "alasio-neon-compute", wokenAt: "2026-10-12T08:00:00.000Z", suspendedAt: null });
  compute.spec["replicas"] = 0;
  run = await alasio(["status"]);
  succeeded(run);
  assert.ok(run.printed.includes("  Deployment alasio/alasio-neon-compute: asleep, last woken 2026-10-12T08:00:00.000Z"), run.printed.join("\n"));
  // Another workload at no replica is ready, as KEDA leaves one, not asleep.
  const collector = kube.get("/apis/apps/v1/namespaces/alasio/deployments/alasio-collector");
  assert.ok(collector?.spec);
  collector.spec["replicas"] = 0;
  run = await alasio(["status"]);
  assert.ok(run.printed.includes("  Deployment alasio/alasio-collector: ready"), run.printed.join("\n"));
});

test("status says how far each read replica is behind its primary", async (t) => {
  const { alasio, neon } = await branchable(t);
  neon.computes.push({ id: "alasio-ro", namespace: "alasio", name: "alasio-neon-compute-ro", wokenAt: null, suspendedAt: null, lagBytes: 3 * 1024 * 1024 });
  const run = await alasio(["status"]);
  succeeded(run);
  assert.ok(run.printed.includes("  Deployment alasio/alasio-neon-compute-ro: awake, 3 MiB of WAL behind its primary"), run.printed.join("\n"));
});

test("status says Neon's shards, and each pageserver, with how many of them it holds", async (t) => {
  const { alasio, neon } = await branchable(t);
  neon.tenant = {
    shards: 2,
    stripeSize: 2048,
    pageservers: [
      { id: 1, address: "alasio-neon-pageserver-0.alasio.svc.cluster.local:6400", availability: "Active", shards: 1 },
      { id: 2, address: "alasio-neon-pageserver-1.alasio.svc.cluster.local:6400", availability: "Offline", shards: 1 },
    ],
  };
  const run = await alasio(["status"]);
  succeeded(run);
  const at = run.printed.indexOf("Neon: 2 shards on 2 pageservers");
  assert.deepEqual(run.printed.slice(at, at + 3), [
    "Neon: 2 shards on 2 pageservers",
    "  pageserver 1 at alasio-neon-pageserver-0.alasio.svc.cluster.local:6400: Active, 1 shard",
    "  pageserver 2 at alasio-neon-pageserver-1.alasio.svc.cluster.local:6400: Offline, 1 shard",
  ]);
});

test("up refuses fewer of Neon's shards than its tenant has, as Neon merges none, before it applies anything", async (t) => {
  const { alasio, config, configure, kube, neon } = await branchable(t);
  neon.storage.shards = 4;
  const written = config() as { install?: Record<string, unknown> };
  configure({ ...written, install: { ...written.install, neon: { shards: 2 } } });
  kube.changes.length = 0;
  assert.equal(
    failure(await alasio(["up"])),
    "neon.shards is 2, but alasio's Neon has 4 shards already, and Neon merges no shards: set it to 4 or more",
  );
  assert.deepEqual(kube.changes.filter(({ method }) => method !== "GET"), []);
  configure({ ...written, install: { ...written.install, neon: { shards: 4 } } });
  succeeded(await alasio(["up"]));
});

test("up refuses to apply anything while the storage controller, which has registered a pageserver, cannot say what Neon's storage holds", async (t) => {
  const { alasio, kube, neon } = await branchable(t);
  neon.storage.answering = false;
  kube.changes.length = 0;
  assert.equal(
    failure(await alasio(["up"])),
    "alasio applies nothing it cannot check against Neon's storage, and its storage controller did not say what it holds (it answered its pageservers with 503): " +
      "once it runs, as alasio status says it does not, run alasio up again",
  );
  assert.deepEqual(kube.changes.filter(({ method }) => method !== "GET"), []);
  // One that has registered no pageserver, as its database says, as where alasio's first up stopped before it did, holds nothing to check against.
  neon.storage.pageservers = 0;
  succeeded(await alasio(["up"]));
});

test("up refuses to apply anything while the storage controller's database, its claim bound, does not answer, and checks nothing only where its claim never bound", async (t) => {
  const { alasio, kube, neon } = await branchable(t);
  neon.storage.databaseAnswering = false;
  kube.changes.length = 0;
  assert.equal(
    failure(await alasio(["up"])),
    "alasio applies nothing it cannot check against Neon's storage, and the storage controller's database, which holds what it registered, did not answer " +
      "(psql exited with 2: psql: error: connection to server on socket \"/var/run/postgresql/.s.PGSQL.5432\" failed: No such file or directory): " +
      "once its pod runs (kubectl -n alasio describe pod alasio-neon-controller-db-0 says why it does not), run alasio up again",
  );
  assert.deepEqual(kube.changes.filter(({ method }) => method !== "GET"), []);
  // Its claim never bound, as where alasio's first up stopped before the database first started: it holds nothing.
  const claim = kube.get("/api/v1/namespaces/alasio/persistentvolumeclaims/data-alasio-neon-controller-db-0");
  assert.ok(claim);
  claim["status"] = { phase: "Pending" };
  succeeded(await alasio(["up"]));
});

test("up refuses to apply anything while the Secret that names Neon's tenant cannot be read", async (t) => {
  const { alasio, kube } = await branchable(t);
  stackSecret(kube, "alasio-neon-root", { "secrets.json": "{" });
  kube.changes.length = 0;
  assert.match(failure(await alasio(["up"])), /^alasio applies nothing it cannot check against Neon's storage, and its storage controller did not say what it holds \(the Secret alasio-neon-root's secrets\.json, which names Neon's tenant, is no JSON\)/u);
  assert.deepEqual(kube.changes.filter(({ method }) => method !== "GET"), []);
});

test("up applies nothing where an object the cluster has cannot take this release's of it, as a dry run of each says, naming what makes it able", async (t) => {
  const { alasio, kube } = await branchable(t);
  // The pageservers' StatefulSet as 7.0.0 made it, its pods started in order.
  const pageservers = kube.get("/apis/apps/v1/namespaces/alasio/statefulsets/alasio-neon-pageserver");
  assert.ok(pageservers?.spec);
  pageservers.spec["podManagementPolicy"] = "OrderedReady";
  kube.changes.length = 0;
  for (const command of [["up"], ["upgrade", "--check"]]) {
    assert.equal(
      failure(await alasio(command)),
      "the cluster's StatefulSet alasio/alasio-neon-pageserver cannot take this release's of it (StatefulSet.apps \"alasio-neon-pageserver\" is invalid: spec: Forbidden: " +
        "updates to statefulset spec for fields other than 'replicas', 'ordinals', 'template', 'updateStrategy', 'persistentVolumeClaimRetentionPolicy' and 'minReadySeconds' are forbidden), " +
        "so alasio applies nothing: an installation of 7.0.0 is upgraded by upgrades/from-7.0.0.sh, which makes it anew, keeping its pod and claim",
    );
  }
  assert.deepEqual(kube.changes.filter(({ method }) => method !== "GET"), []);
});

test("up checks Neon's storage by the storage controller, which every release runs, and asks neon-control nothing, whose release it may be upgrading", async (t) => {
  const { alasio, neon } = await branchable(t);
  // neon-control of a release before this one, which has no /tenant.
  neon.tenant = null;
  neon.asked.length = 0;
  succeeded(await alasio(["up"]));
  assert.deepEqual(neon.asked.filter(({ path }) => path === "/tenant"), []);
});

test("up refuses fewer of Neon's pageservers than the storage controller has registered, as alasio removes none, before it applies anything", async (t) => {
  const { alasio, config, configure, kube, neon } = await branchable(t);
  neon.storage.pageservers = 2;
  const written = config() as { install?: Record<string, unknown> };
  kube.changes.length = 0;
  assert.equal(
    failure(await alasio(["up"])),
    "neon.pageserver.scaling is 1, but alasio's Neon has 2 pageservers registered, each holding shards or their secondaries, and alasio removes none: set it to 2 or more",
  );
  assert.deepEqual(kube.changes.filter(({ method }) => method !== "GET"), []);
  configure({ ...written, install: { ...written.install, neon: { pageserver: { scaling: { min: 2, max: 2 } } } } });
  succeeded(await alasio(["up"]));
});

test("status says a workload KEDA scales is waking while a pod it scaled to starts, as the lake's intake woken from none, and not ready where nothing scales it", async (t) => {
  const { alasio, kube } = await branchable(t);
  const intake = kube.get("/apis/apps/v1/namespaces/alasio/deployments/alasio-lake-intake");
  const pod = kube.get("/api/v1/namespaces/alasio/pods/alasio-lake-intake-0");
  assert.ok(intake?.spec && intake.status && pod?.metadata && pod.status);
  assert.ok(kube.get("/apis/keda.sh/v1alpha1/namespaces/alasio/scaledobjects/alasio-lake-intake"));
  // KEDA's taking it from none to one, its pod just made, not yet ready.
  intake.spec["replicas"] = 1;
  intake.status["availableReplicas"] = 0;
  pod.status["conditions"] = [{ type: "Ready", status: "False" }];
  pod.status["containerStatuses"] = [{ name: "intake", ready: false, restartCount: 0, image: "lake", imageID: "", state: { running: {} } }];
  pod.metadata["creationTimestamp"] = new Date().toISOString();
  let run = await alasio(["status"]);
  succeeded(run);
  assert.ok(run.printed.includes("  Deployment alasio/alasio-lake-intake: waking, 0 of 1 available"), run.printed.join("\n"));
  // Nothing scales it: a pod not ready is not ready.
  kube.objects.delete("/apis/keda.sh/v1alpha1/namespaces/alasio/scaledobjects/alasio-lake-intake");
  run = await alasio(["status"]);
  assert.match(failure(run), /^1 of alasio's \d+ workloads are not ready$/u);
});

test("status says of the lake's loader whether it is awake, waking, or asleep with nothing left to load", async (t) => {
  const { alasio, kube } = await branchable(t);
  const loader = kube.get("/apis/apps/v1/namespaces/alasio/deployments/alasio-lake-loader");
  assert.ok(loader?.spec && loader.status);
  loader.spec["replicas"] = 1;
  let run = await alasio(["status"]);
  succeeded(run);
  assert.ok(run.printed.includes("  Deployment alasio/alasio-lake-loader: awake"), run.printed.join("\n"));
  // Woken, its pod not yet ready: waking while the pod starts, within the Deployment's progress
  // deadline; not ready once a container of it restarted, or past the deadline.
  loader.status["availableReplicas"] = 0;
  const pod = kube.get("/api/v1/namespaces/alasio/pods/alasio-lake-loader-0");
  assert.ok(pod?.metadata && pod.status);
  pod.status["conditions"] = [{ type: "Ready", status: "False" }];
  const container = (restartCount: number) => [{ name: "loader", ready: false, restartCount, image: "lake", imageID: "", state: { running: {} } }];
  for (const [restarts, made, said] of [
    [0, new Date(), "waking, 0 of 1 available"],
    [1, new Date(), null],
    [0, new Date(Date.now() - 11 * 60_000), null],
  ] as const) {
    pod.status["containerStatuses"] = container(restarts);
    pod.metadata["creationTimestamp"] = made.toISOString();
    run = await alasio(["status"]);
    if (said) succeeded(run);
    else assert.match(failure(run), /^1 of alasio's \d+ workloads are not ready$/u);
    assert.ok(run.printed.includes(`  Deployment alasio/alasio-lake-loader: ${said ?? "0 of 1 available"}`), run.printed.join("\n"));
  }
  // Its pod ready that the Deployment does not count yet, as one that came up after status
  // read the Deployment: waking while the pod came up within the deadline, not ready once
  // the Deployment has not counted it for longer.
  pod.status["containerStatuses"] = [{ name: "loader", ready: true, restartCount: 0, image: "lake", imageID: "", state: { running: {} } }];
  pod.metadata["creationTimestamp"] = new Date().toISOString();
  for (const [readyAt, said] of [
    [new Date(), "waking, 0 of 1 available"],
    [new Date(Date.now() - 11 * 60_000), null],
  ] as const) {
    pod.status["conditions"] = [{ type: "Ready", status: "True", lastTransitionTime: readyAt.toISOString() }];
    run = await alasio(["status"]);
    if (said) succeeded(run);
    else assert.match(failure(run), /^1 of alasio's \d+ workloads are not ready$/u);
    assert.ok(run.printed.includes(`  Deployment alasio/alasio-lake-loader: ${said ?? "0 of 1 available"}`), run.printed.join("\n"));
  }
  // Woken, its pod not made yet, as just after KEDA scales it: waking while KEDA last found
  // it work within the progress deadline; not ready past it, where its pods are refused, or
  // once the Deployment says it is past its deadline.
  kube.objects.delete("/api/v1/namespaces/alasio/pods/alasio-lake-loader-0");
  const scaler = kube.get("/apis/keda.sh/v1alpha1/namespaces/alasio/scaledobjects/alasio-lake-loader");
  assert.ok(scaler);
  const unready = async () => {
    run = await alasio(["status"]);
    assert.match(failure(run), /^1 of alasio's \d+ workloads are not ready$/u);
  };
  for (const [woken, conditions, said] of [
    [new Date(), [], "waking, 0 of 1 available"],
    [new Date(Date.now() - 11 * 60_000), [], null],
    [new Date(), [{ type: "ReplicaFailure", status: "True", reason: "FailedCreate" }], null],
    [new Date(), [{ type: "Progressing", status: "False", reason: "ProgressDeadlineExceeded" }], null],
  ] as const) {
    scaler["status"] = { lastActiveTime: woken.toISOString() };
    loader.status["conditions"] = conditions;
    if (said) {
      run = await alasio(["status"]);
      succeeded(run);
      assert.ok(run.printed.includes(`  Deployment alasio/alasio-lake-loader: ${said}`), run.printed.join("\n"));
    } else {
      await unready();
    }
  }
  // Asleep, with nothing left to load, only while its ScaledObject would wake it.
  loader.spec["replicas"] = 0;
  loader.status["conditions"] = [];
  run = await alasio(["status"]);
  succeeded(run);
  assert.ok(run.printed.includes("  Deployment alasio/alasio-lake-loader: asleep, nothing left to load"), run.printed.join("\n"));
  for (const [annotations, conditions, said] of [
    [{ "autoscaling.keda.sh/paused": "true" }, [], "at no replica, its ScaledObject paused, which wakes it no more"],
    [{ "autoscaling.keda.sh/paused-replicas": "0" }, [], "at no replica, its ScaledObject paused, which wakes it no more"],
    [{}, [{ type: "Ready", status: "False", reason: "ScaledObjectCheckFailed", message: "the topic lake.main has no partitions" }], "at no replica, its ScaledObject not ready (the topic lake.main has no partitions), which wakes it no more"],
  ] as const) {
    (scaler["metadata"] as Record<string, unknown>)["annotations"] = annotations;
    scaler["status"] = { conditions };
    await unready();
    assert.ok(run.printed.includes(`  Deployment alasio/alasio-lake-loader: ${said}`), run.printed.join("\n"));
  }
});

test("branch create's help says how far back Neon keeps its history, and so a branch point", async (t) => {
  const { alasio } = await rig(t);
  const run = await alasio(["branch", "create", "--help"]);
  succeeded(run);
  assert.match(run.printed.join(" ").replaceAll(/\s+/gu, " "), /Neon keeps a day of its history, so a branch point can be at most a day back\./u);
});

test("branch create branches Neon at main's latest commit, runs the branch's alasio, compute and lake on it with Secrets of its own, and refuses main's bot", async (t) => {
  const { alasio, home, kube, neon, telegram } = await branchable(t);
  telegram.bots.set("456:branch", "branch_bot");
  const token = (name: string, value: string) => {
    writeFileSync(join(home, name), `${value}\n`);
    return join(home, name);
  };
  assert.match(failure(await alasio(["branch", "create", "Try", "--bot-token-file", token("branch", "456:branch")])), /^Try is no branch's name: /u);
  assert.equal(
    failure(await alasio(["branch", "create", "try", "--bot-token-file", token("main", BOT_TOKEN)])),
    "a branch's bot is its own: Telegram delivers a bot's updates to one webhook, and main's bot's to main",
  );
  assert.deepEqual(neon.asked, []);

  const run = await alasio(["branch", "create", "try", "--bot-token-file", token("branch", "456:branch"), "--image", "registry.example:5000/alasio:next"]);
  succeeded(run);
  // Asked nothing before, as asserted: what it was asked since.
  const posted = (neon.asked as FakeNeon["asked"]).filter(({ method }) => method === "POST");
  // Main's compute woken first, as it may sleep, and its WAL position read in it.
  assert.deepEqual(posted.map(({ path }) => path), ["/computes/alasio/wake", "/branches"]);
  const { compute, ...branched } = (posted[1]?.body ?? {}) as { readonly compute?: { readonly verifiers: Record<string, string>; readonly tokenSha256: string } };
  assert.deepEqual(branched, { name: "try", lsn: "0/1A2B3C8" });
  const deployment = kube.get(BRANCH_DEPLOYMENT) as { metadata: { labels: Record<string, string> }; spec: { template: { spec: { containers: { image: string }[] } } } } | undefined;
  assert.equal(deployment?.spec.template.spec.containers[0]?.image, "registry.example:5000/alasio:next");
  assert.equal(deployment?.metadata.labels["alasio.dev/branch"], "try");
  assert.ok(kube.get("/apis/apps/v1/namespaces/alasio-branch-try/deployments/alasio-neon-compute"));
  assert.ok(kube.get("/apis/apps/v1/namespaces/alasio-branch-try/deployments/alasio-lake-loader"));
  // Its own bot, for main's users; its own token to ask main to fork with.
  assert.equal(secretIn(kube, "alasio-branch-try", "alasio-telegram", "token"), "456:branch");
  assert.equal(secretIn(kube, "alasio-branch-try", "alasio-telegram", "allowedUserIds"), "42");
  // Its own password of alasio's role, whose verifier alone neon-control is given, for its compute's spec and Neon's proxy; its compute named to the proxy.
  const url = new URL(secretIn(kube, "alasio-branch-try", "alasio-database", "url") ?? "");
  assert.deepEqual([url.username, url.host, url.pathname, url.searchParams.get("options")], ["alasio", "alasio-neon-proxy.alasio.svc:5432", "/alasio", "endpoint=branch-try"]);
  assert.notEqual(url.password, "p");
  assert.ok(verifies(compute?.verifiers["alasio"], decodeURIComponent(url.password)));
  // Its compute's own token, whose hash alone neon-control is given; its lake's role's own password.
  const computeToken = secretIn(kube, "alasio-branch-try", "alasio-neon-compute", "NEON_CONTROL_PLANE_TOKEN") ?? "";
  assert.notEqual(computeToken, "c");
  assert.equal(compute?.tokenSha256, tokenSha256(computeToken));
  const lakePassword = secretIn(kube, "alasio-branch-try", "alasio-lake", "LAKE_DATABASE_PASSWORD");
  assert.notEqual(lakePassword, "l");
  // Its lake's role's verifier, which its alasio sets the role to, the one neon-control is given.
  assert.ok(verifies(compute?.verifiers["lake"], lakePassword ?? ""));
  assert.equal(secretIn(kube, "alasio-branch-try", "alasio-database", "lake-verifier"), compute?.verifiers["lake"]);
  assert.deepEqual(Object.keys(compute?.verifiers ?? {}), ["alasio", "lake"]);
  // Of main's, only the lake's object store identity, which reads main's files where they are.
  assert.equal(secretIn(kube, "alasio-branch-try", "alasio-lake", "LAKE_S3_KEY"), "k");
  const keys = (name: string) => Object.keys((kube.get(`/api/v1/namespaces/alasio-branch-try/secrets/${name}`)?.["data"] ?? {}) as object).sort();
  assert.deepEqual(keys("alasio-database"), ["lake-verifier", "url"]);
  assert.deepEqual(keys("alasio-lake"), ["LAKE_DATABASE_PASSWORD", "LAKE_S3_KEY", "LAKE_S3_SECRET"]);
  assert.equal(secretIn(kube, "alasio-branch-try", "alasio-claude", "token"), "sk-ant-oat01-claude");
  assert.equal(secretIn(kube, "alasio-branch-try", "alasio-branch-fork", "token"), forkToken("key", "try"));
  assert.equal(secretIn(kube, "alasio-branch-try", "alasio-branches", "control-token"), undefined);
  // KEDA's principal on the log, which its loader's scaler reads as, and which may only describe how far behind it is.
  assert.equal(secretIn(kube, "alasio-branch-try", "alasio-log-scaler", "username"), "scaler");
  // Its own principal and topic on the log, by the log's access applier; its own route, which Telegram delivers its bot's updates to.
  assert.ok(kube.execs.some(({ pod, container, command }) => pod === "alasio-log-0" && container === "access" && command.join(" ") === "node /opt/alasio/src/log/access.ts environment"));
  assert.equal(secretIn(kube, "alasio-branch-try", "alasio-log-alasio", "username"), "alasio-try");
  // Its lake loader's own, which reads its wake topic, of a password of its own.
  assert.equal(secretIn(kube, "alasio-branch-try", "alasio-log-loader", "username"), "loader-try");
  assert.notEqual(secretIn(kube, "alasio-branch-try", "alasio-log-loader", "password"), secretIn(kube, "alasio-branch-try", "alasio-log-alasio", "password"));
  const route = JSON.parse(secretValue(kube, ROUTES_SECRET, "try") ?? "") as { path: string; secretToken: string; allowedUserIds: string[] };
  assert.deepEqual(route.allowedUserIds, ["42"]);
  assert.notEqual(route.path, (JSON.parse(secretValue(kube, ROUTES_SECRET, "main") ?? "") as { path: string }).path);
  assert.deepEqual([telegram.webhooks.get("456:branch")?.url, telegram.webhooks.get("456:branch")?.secretToken], [`https://${PUBLIC_ADDRESS}${route.path}`, route.secretToken]);
  assert.equal(run.printed.at(-1), "The branch try runs, on a copy of alasio's data as it is now. Message @branch_bot on Telegram to talk to it; alasio branch delete try deletes it.");
  assert.equal(failure(await alasio(["branch", "create", "try", "--bot-token-file", token("branch", "456:branch")])), "there is a branch try already");
});

test("branch create warns when no node has the memory main's alasio, compute and lake use now, as the metrics server measures them", async (t) => {
  const { alasio, home, kube, telegram } = await branchable(t);
  telegram.bots.set("456:branch", "branch_bot");
  writeFileSync(join(home, "branch"), "456:branch");
  kube.put("nodes", { apiVersion: "v1", kind: "Node", metadata: { name: "machine" }, status: { allocatable: { memory: "4Gi" } } });
  kube.put("nodes", { apiVersion: "metrics.k8s.io/v1beta1", kind: "NodeMetrics", metadata: { name: "machine" }, usage: { memory: "3584Mi" } });
  // Not the lake's readers, which a branch has none of.
  for (const [component, containers] of [
    ["alasio", { alasio: "400Mi" }],
    ["neon-compute", { compute: "300Mi" }],
    ["lake-loader", { loader: "200Mi" }],
    ["lake-query", { query: "150Mi" }],
    ["neon-pageserver", { pageserver: "1Gi" }],
  ] as const) {
    const labels = { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": component };
    const usage = Object.entries(containers).map(([name, memory]) => ({ name, usage: { memory } }));
    kube.put("pods", { apiVersion: "metrics.k8s.io/v1beta1", kind: "PodMetrics", metadata: { name: `${component}-0`, namespace: "alasio", labels }, containers: usage });
  }
  const run = await alasio(["branch", "create", "try", "--bot-token-file", join(home, "branch")]);
  succeeded(run);
  assert.ok(run.printed.includes("Warning: a branch takes about 900 MiB, as main's alasio, compute and lake use now, and no node has more than 512 MiB free."), run.printed.join("\n"));
});

test("upgrade refuses while a branch environment another release made exists, naming it and how to go on, and upgrades once it is gone", async (t) => {
  const { alasio, home, kube, telegram } = await branchable(t);
  telegram.bots.set("456:branch", "branch_bot");
  writeFileSync(join(home, "branch"), "456:branch");
  succeeded(await alasio(["branch", "create", "try", "--bot-token-file", join(home, "branch")]));
  succeeded(await alasio(["upgrade"]));
  const made = kube.get(BRANCH_DEPLOYMENT);
  assert.ok(made?.metadata?.labels);
  made.metadata.labels["app.kubernetes.io/version"] = "6.0.0";
  kube.changes.length = 0;
  assert.equal(
    failure(await alasio(["upgrade"])),
    "alasio upgrades main alone, and a branch environment another release made would not run on what upgrading changes: try, which alasio 6.0.0 made. " +
      "Delete it first (alasio branch delete try), and make it again once alasio is upgraded",
  );
  assert.deepEqual(kube.changes.filter(({ method }) => method !== "GET"), []);
  succeeded(await alasio(["branch", "delete", "try", "--force"]));
  succeeded(await alasio(["upgrade"]));
});

test("branch list says each branch, what it is a branch of, its age, the release that made it, its alasio, and the memory it requests", async (t) => {
  const { alasio, home, telegram } = await branchable(t);
  assert.deepEqual((await alasio(["branch", "list"])).printed, ["alasio has no branch environments."]);
  telegram.bots.set("456:branch", "branch_bot");
  writeFileSync(join(home, "branch"), "456:branch");
  succeeded(await alasio(["branch", "create", "try", "--bot-token-file", join(home, "branch")]));
  const listed = await alasio(["branch", "list"]);
  succeeded(listed);
  assert.deepEqual(listed.printed, ["try: a branch of main, 0m old, made by alasio 0.0.0-development, its alasio ready, 1.8 GiB requested, 0 MiB used"]);
});

test("branch delete refuses while the branch's alasio has a turn running unless --force, and deletes it whole: its alasio and lake, its sessions, its compute, its branch of Neon, its namespaces, its Sandboxes' policy", async (t) => {
  const { alasio, home, kube, neon, telegram } = await branchable(t);
  telegram.bots.set("456:branch", "branch_bot");
  writeFileSync(join(home, "branch"), "456:branch");
  succeeded(await alasio(["branch", "create", "try", "--bot-token-file", join(home, "branch")]));
  const policy = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/alasio-branch-try-sandboxes";
  const binding = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/alasio-branch-try-sandboxes";
  assert.ok(kube.get(policy) && kube.get(binding));
  neon.turns = "1";
  neon.asked.length = 0;
  assert.equal(failure(await alasio(["branch", "delete", "try"])), "its alasio has 1 turn running; --force deletes it all the same");
  // Its compute, which may sleep, woken to be asked.
  assert.deepEqual(neon.asked.filter(({ method }) => method === "POST").map(({ path }) => path), ["/computes/branch-try/wake"]);
  assert.ok(kube.get(BRANCH_DEPLOYMENT));
  kube.changes.length = 0;
  const run = await alasio(["branch", "delete", "try", "--force"]);
  succeeded(run);
  assert.equal(run.printed.at(-1), "The branch try is deleted, with all it had; main's data is as it was.");
  const deleted = kube.changes.filter(({ method }) => method === "DELETE").map(({ path }) => path);
  const at = (path: string) => deleted.indexOf(path);
  assert.ok(at(BRANCH_DEPLOYMENT) >= 0 && at(BRANCH_DEPLOYMENT) < at("/apis/apps/v1/namespaces/alasio-branch-try/deployments/alasio-neon-compute"), deleted.join("\n"));
  // Its read replica too, before its branch of Neon is deleted.
  assert.ok(at("/apis/apps/v1/namespaces/alasio-branch-try/deployments/alasio-neon-compute-ro") >= 0, deleted.join("\n"));
  const loader = at("/apis/apps/v1/namespaces/alasio-branch-try/deployments/alasio-lake-loader");
  assert.ok(loader >= 0 && loader < at("/api/v1/namespaces/alasio-branch-try"), deleted.join("\n"));
  assert.deepEqual(neon.branches.map(({ name }) => name), ["main"]);
  assert.equal(kube.get("/api/v1/namespaces/alasio-branch-try"), undefined);
  assert.equal(kube.get("/api/v1/namespaces/alasio-branch-try-sessions"), undefined);
  assert.deepEqual([kube.get(policy), kube.get(binding)], [undefined, undefined]);
  // Its bot delivered nowhere, its route gone, its principal and topic forgotten.
  assert.equal(telegram.webhooks.get("456:branch"), undefined);
  assert.equal(secretValue(kube, ROUTES_SECRET, "try"), undefined);
  assert.ok(secretValue(kube, ROUTES_SECRET, "main"));
  assert.ok(kube.execs.some(({ pod, container, command }) => pod === "alasio-log-0" && container === "access" && command.join(" ") === "node /opt/alasio/src/log/access.ts forget"));
  assert.equal(failure(await alasio(["branch", "delete", "try"])), "there is no branch try");
});

test("login codex --branch logs the branch's own Codex in, in the branch's alasio's pod", async (t) => {
  const { alasio, home, kube, telegram } = await branchable(t);
  telegram.bots.set("456:branch", "branch_bot");
  writeFileSync(join(home, "branch"), "456:branch");
  succeeded(await alasio(["branch", "create", "try", "--bot-token-file", join(home, "branch")]));
  succeeded(await alasio(["login", "codex", "--branch", "try"]));
  assert.deepEqual(kube.execs.at(-1), { namespace: "alasio-branch-try", pod: "alasio-0", container: "alasio", command: CODEX_LOGIN, tty: false });
});

test("uninstall refuses while alasio has branch environments, which run on what it removes", async (t) => {
  const { alasio, home, kube, telegram } = await branchable(t);
  telegram.bots.set("456:branch", "branch_bot");
  writeFileSync(join(home, "branch"), "456:branch");
  succeeded(await alasio(["branch", "create", "try", "--bot-token-file", join(home, "branch")]));
  assert.equal(failure(await alasio(["uninstall", "--yes"])), "alasio has the branch environments try, which run on its storage: alasio branch delete deletes each first");
  assert.ok(kube.get(DEPLOYMENT));
});

test("a branch's configuration is main's with its overrides, and never the host profile, whose folders a branch cannot copy", async (t) => {
  const { alasio, configure, config, home, kube, telegram } = await branchable(t);
  configure({ ...(config() as object), install: { host: { enabled: true }, alasio: { defaultHarness: "claude" } } });
  telegram.bots.set("456:branch", "branch_bot");
  writeFileSync(join(home, "branch"), "456:branch");
  writeFileSync(join(home, "overrides.json"), JSON.stringify({ alasio: { defaultHarness: "codex", resources: { requests: { memory: "512Mi" } } }, imagePullSecrets: ["registry"] }));
  kube.put("secrets", { apiVersion: "v1", kind: "Secret", metadata: { name: "registry", namespace: "alasio" }, type: "kubernetes.io/dockerconfigjson", data: { ".dockerconfigjson": "e30=" } });
  succeeded(await alasio(["branch", "create", "try", "--bot-token-file", join(home, "branch"), "--overrides", join(home, "overrides.json")]));
  const container = (kube.get(BRANCH_DEPLOYMENT)?.spec as { template: { spec: { containers: { env: { name: string; value?: string }[]; resources: { requests: Record<string, string> } }[]; volumes: { name: string }[] } } }).template.spec;
  assert.equal(container.containers[0]?.env.find(({ name }) => name === "ALASIO_DEFAULT_HARNESS")?.value, "codex");
  assert.deepEqual(container.containers[0]?.resources.requests, { cpu: "250m", memory: "512Mi" });
  const templates = JSON.parse((kube.get("/api/v1/namespaces/alasio-branch-try/configmaps/alasio-sandbox-templates")?.["data"] as Record<string, string>)["templates.json"] ?? "");
  assert.equal(templates.host, undefined);
  // The pull Secrets of its images, in its own namespace.
  assert.equal(kube.get("/api/v1/namespaces/alasio-branch-try/secrets/registry")?.["type"], "kubernetes.io/dockerconfigjson");
  writeFileSync(join(home, "overrides.json"), JSON.stringify({ alasio: { telegram: { existingSecret: "mine" } } }));
  assert.match(failure(await alasio(["branch", "create", "other", "--bot-token-file", join(home, "branch"), "--overrides", join(home, "overrides.json")])), /install\.alasio\.telegram is alasio's own to say/u);
});
