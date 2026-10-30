/**
 * alasio's end-to-end run, set up as an operator sets alasio up: its npm package packed
 * and installed (tooling/cli-package.ts), its images built here, a cluster on this
 * machine made by `alasio init` from the node image built here, alasio's own images
 * pushed to a registry of the run's own beside the cluster, which the cluster's
 * `registries` have its nodes pull from, the stand-ins it talks to instead of Telegram
 * and a telemetry backend applied (./stand-ins.ts), and alasio started by `alasio up`,
 * running stand-ins for Codex and Claude Code in their place (./codex-stand-in.ts,
 * ./claude-stand-in.ts), as the run has no login of either, its Grafana then made to take the Telegram stand-in for Telegram
 * (trustTelegramStandIn). The tests then drive it through its command line (`alasio`), the stand-ins, and the
 * cluster's API (`kube`); the run is torn down by `alasio uninstall --purge`, after what
 * the cluster was doing is said, when something of the run failed.
 *
 * ALASIO_E2E_AGENTS is the number of agent nodes beside the server (0 unless set), and
 * ALASIO_E2E_KEEP=1 keeps the cluster, its registry and the run's directory afterwards.
 * ALASIO_E2E_REGISTRY names a registry the images were pushed to already, each as
 * `<registry>/<name>:e2e`, as CI's images job pushes them once for all its end-to-end
 * runs: `alasio init` pulls the node image, and the nodes alasio's, from there, without a
 * login, and the run builds none and starts no registry. ALASIO_E2E_SHARD runs a shard of
 * the suites (SHARD). Needs Docker and npm. Once pushed, the images built are removed,
 * and Docker's build cache with them, as a CI runner's disk holds the registry's copies,
 * and each node's of the images its pods run, and little more: a node pulls only those,
 * and again any its kubelet collected. alasio's command line raises the inotify limits
 * through sudo, which must ask for no password where they are too low.
 *
 * ALASIO_E2E_TARGET=host makes the cluster k3s on this machine itself, the host target,
 * rather than in Docker: of one node, from images pushed already, it needs npm and sudo
 * that asks for no password, through which alasio's command line does what it does as
 * root, and the tests what they do as root on the node, which is this machine.
 *
 * The `upgrade` shard sets alasio up as the release before set it up, on the host target,
 * as the box is: by the published package of the release ALASIO_E2E_PREVIOUS names, with
 * that release's images, but for alasio's, which is the stand-ins' built on that
 * release's alasio image, pushed as alasio-harness-stand-ins-previous. Its suite upgrades it to
 * this checkout by the cut-over the box runs (cutOver).
 */
import { type ChildProcess, execFile, spawn } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { type AddressInfo, createServer, type Server, type Socket } from "node:net";
import { hostname, tmpdir, userInfo } from "node:os";
import { join } from "node:path";
import { Readable, Writable } from "node:stream";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import {
  type CoreV1Event,
  CoreV1Api,
  KubeConfig,
  type KubernetesObject,
  PortForward,
  type V1ConfigMap,
  type V1CronJob,
  type V1Deployment,
  type V1Job,
  type V1NetworkPolicy,
  type V1Node,
  type V1Pod,
  type V1Secret,
  type V1Service,
} from "@kubernetes/client-node";
import { type Duration, Effect, Stream } from "effect";

import { cpuMillis, cpuText } from "../../cli/src/capacity.ts";
import { subnetAddress } from "../../cli/src/cluster/docker.ts";
import { kind, type KindName, KubeApi, type ListOptions, type ObjectRef } from "../../cli/src/kube/api.ts";
import { awaitReady, podProblems, selectorOf } from "../../cli/src/kube/rollout.ts";
import { GATEWAY_COMPONENT as COLLECTOR_COMPONENT } from "../../cli/src/manifests/collector.ts";
import { componentName, NAMESPACE, RELEASE, selectorLabels } from "../../cli/src/manifests/common.ts";
import { LOG_COMPONENT } from "../../cli/src/manifests/log.ts";
import { COMPONENTS as OBJECT_STORE_COMPONENTS } from "../../cli/src/manifests/object-store.ts";
import { GATEWAY, GATEWAY_COMPONENT, GATEWAY_PORT } from "../../cli/src/manifests/sandbox-gateway.ts";
import { WORKSPACES_ROOT } from "../../src/node-agent/names.ts";
import { BINDING_LABEL, environmentKey, keyHash, POOL_LABEL } from "../../src/binder/names.ts";
import { NO_BIND_HEADER } from "../../src/binder/names.ts";
import { mintToken } from "../../src/binder/tokens.ts";
import { FOLDER_POOL } from "../../src/gateway/names.ts";
import { BAYMA_CONTAINER } from "../../src/kube/sandbox-shape.ts";
import { sessionKey } from "../../src/sandbox/names.ts";
import { installCli, packCli } from "../../tooling/cli-package.ts";
import { NODE_BUILD_ARGS } from "../../tooling/node-image.ts";
import { OTLP, STAND_INS, standInObjects, TELEGRAM, TELEGRAM_TLS_PORT, urlOf } from "./stand-ins.ts";
import { createTelegramStub } from "./telegram-stub.ts";
import { CA } from "./telegram-tls.ts";

const run = promisify(execFile);
const root = fileURLToPath(new URL("../..", import.meta.url));

/** Where the run makes its cluster: k3s on this machine itself, or in Docker. */
export const TARGET = targetOf(process.env["ALASIO_E2E_TARGET"]);
/** Agent nodes beside the server. */
export const AGENTS = Number(process.env["ALASIO_E2E_AGENTS"] ?? 0);
/** Whether the cluster, and the run's directory, are kept afterwards. */
export const KEEP = process.env["ALASIO_E2E_KEEP"] === "1";
/** The cluster in Docker: one of its own for each number of agents, so runs of each can be kept side by side. */
export const CLUSTER = `alasio-e2e-${AGENTS}`;

/** The subnet of the cluster in Docker, its server node's address the second: where the Telegram stand-in delivers to the webhook intake. */
const DOCKER_SUBNET = `172.31.${200 + AGENTS}.0/24`;
/** The cluster, as alasio names it in what it says. */
const THE_CLUSTER = TARGET === "host" ? "k3s on this machine" : `the cluster ${CLUSTER}`;
/**
 * Whether folder workspaces are on. The host profile is for a single machine's single
 * node, so it is off on several, where alasio's images are placed once each: Neon and its
 * object store on the server, sessions on one agent and alasio on another, which still
 * crosses nodes everywhere alasio reaches.
 */
export const HOST_PROFILE = AGENTS < 2;
/** The Telegram user the stand-in's operator is, whom the bot is for. */
const OPERATOR = "1001";
/** Another user the bot is for, whose own chat a suite may talk in beside the operator's. */
export const OTHER_OPERATOR = 1002;
/** The user folder workspaces run as. */
export const HOST_USER = 1000;

/** The target `value` names: the cluster in Docker unless it names the host. */
export function targetOf(value: string | undefined): "host" | "docker" {
  if (!value || value === "docker") return "docker";
  if (value === "host") return "host";
  throw new Error(`ALASIO_E2E_TARGET is ${value}, not host or docker`);
}

/** The shards of the suites, each run on a cluster of its own. */
export const SHARDS = ["sessions", "workspaces", "neon", "telemetry", "branches", "scaling", "snapshots", "upgrade", "agents", "wake"] as const;
export type Shard = (typeof SHARDS)[number];

/** The shard `value` names, or none, when it is unset or empty. */
export function shardOf(value: string | undefined): Shard | undefined {
  if (!value) return undefined;
  const shard = SHARDS.find((name) => name === value);
  if (!shard) throw new Error(`ALASIO_E2E_SHARD is ${value}, not one of ${SHARDS.join(", ")}`);
  return shard;
}

/**
 * The shard of the suites the run runs, ALASIO_E2E_SHARD's: `sessions`, alasio on
 * Kubernetes; `workspaces`, workspaces on JuiceFS, on sessions of its own; `neon`,
 * alasio's Neon; `telemetry`, where alasio's telemetry goes, on a session of its own;
 * `branches`, branch environments; `scaling`, how alasio is scaled, sized and drained,
 * with Grafana highly available; `agents`, conversations' harnesses in pools of their own,
 * on an installation that releases an idle one within HARNESS_RELEASE_AFTER_SECONDS;
 * `snapshots`, sessions released into whole-sandbox snapshots and restored from them, on an
 * installation that releases them within RELEASE_AFTER_SECONDS; `upgrade`, what alasio keeps upgraded from the release before;
 * or `wake`, a press waking alasio from none, its gate's own at the default scale, sleeping within the run; every suite
 * but those four shards' unless set, as each needs an installation of its own.
 * Each shard runs the command line's suite, and uninstall's, as well.
 */
export const SHARD = shardOf(process.env["ALASIO_E2E_SHARD"]);

/** The shards whose suites need an installation of their own, run only when selected. */
const OWN_INSTALLATION: readonly Shard[] = ["snapshots", "upgrade", "agents", "wake"];

/** Whether a run of the shard `selected`, or of none, runs the suites of `shard`: the snapshots, upgrade, agents and wake shards' only when selected. */
export const runsShard = (selected: Shard | undefined, shard: Shard): boolean => selected === shard || (selected === undefined && !OWN_INSTALLATION.includes(shard));

/** Whether the run runs the suites of `shard`. */
export const inShard = (shard: Shard): boolean => runsShard(SHARD, shard);

/** The release the upgrade shard upgrades from, as `<major>.<minor>.<patch>`. */
export const PREVIOUS = process.env["ALASIO_E2E_PREVIOUS"] || undefined;

/** How long a conversation's harness of the agents shard does nothing before the gateway releases it. */
export const HARNESS_RELEASE_AFTER_SECONDS = 45;

/** How long a session of the snapshots and upgrade shards runs no exec, and no turn, before alasio releases it. */
export const RELEASE_AFTER_SECONDS = 60;
/** How often a session of the snapshots shard whose execs run is saved live as they run. */
export const LIVE_EVERY_SECONDS = 30;
/** How much a REPL of the snapshots shard holds live, whose snapshots it times. */
export const BIG_REPL_MEBIBYTES = 1024;

/**
 * The images built here: their names, and what they are built from. alasio's runs in
 * the run as alasio-harness-stand-ins, alasio's with the stand-ins for its harnesses in
 * it (./harness-stand-ins.Dockerfile), built on it.
 */
const IMAGES = { "alasio": ".", "alasio-agent": "sandbox/agent", "alasio-lake": "neon/lake", "alasio-grafana": "neon/grafana", "alasio-log": "neon/log", "alasio-collector": "neon/collector", "alasio-webhook": "webhook", "alasio-node": "cluster/node", "alasio-harness-stand-ins": "test" } as const;

/** Where alasio runs the stand-ins for Codex and Claude Code from, in alasio-harness-stand-ins. */
const CODEX_STAND_IN = "/opt/alasio/test/e2e/codex-stand-in.ts";
const CLAUDE_STAND_IN = "/opt/alasio/test/e2e/claude-stand-in.ts";

/** The registry the images were pushed to already, when the run builds none. */
const PUSHED = process.env["ALASIO_E2E_REGISTRY"] || undefined;

/**
 * Whether alasio's own images are eStargz, which the nodes pull lazily: as CI pushes them
 * (tooling/push-estargz.sh), not as the run builds them itself, whole; alasio's own but, which
 * CI pushes whole (tooling/push-whole.sh), as the release does.
 */
export const ESTARGZ = PUSHED !== undefined;

/** The run's registry: a container on the cluster's network, with a volume of its own, both of this name. */
const REGISTRY = `${CLUSTER}-registry`;
/** Distribution's registry. */
const REGISTRY_IMAGE = "registry:2.8.3@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373";
/** The registry as the nodes reach it, by its name on the cluster's network, over plain HTTP; the pods' images name it. */
const REGISTRY_HOST = `${REGISTRY}:5000`;

/** The image `name` as it is built here, and pushed to the run's registry. */
const imageOf = (name: keyof typeof IMAGES): string => `alasio-e2e/${name}:e2e`;
/** Where the image `name` is pulled from: where it was pushed already, or the run's registry. */
const pulledOf = (name: keyof typeof IMAGES) => ({ repository: `${PUSHED ?? `${REGISTRY_HOST}/alasio-e2e`}/${name}`, tag: "e2e" });
/** The node image, which alasio init makes the cluster from: pulled by it from where it was pushed already, or built here. */
const NODE_IMAGE = PUSHED ? `${PUSHED}/alasio-node:e2e` : imageOf("alasio-node");

/** What a command, run here or in a container, came to. */
export interface Ran {
  readonly code: number;
  readonly stdout: string;
  readonly stderr: string;
}

/** What the run set up. */
interface Setup {
  /** The run's directory: the config, the cluster's storage, the folder workspaces' home, and alasio installed. */
  readonly work: string;
  /** The `alasio` the run runs: `release`, or, until the upgrade shard's cut-over, the previous release's. */
  bin: string;
  /** This checkout's `alasio`, installed from its package. */
  readonly release: string;
  /** The environment alasio runs in: its config, and the cluster's storage, in the run's directory. */
  readonly env: NodeJS.ProcessEnv;
  readonly configFile: string;
  readonly kubeconfig: string;
  readonly storage: string;
  /** The home of the operator folder workspaces work as. */
  readonly home: string;
}

let setup: Setup | undefined;
/** The forward the command line reaches the Telegram stand-in through, for the whole run: closed as it ends, or it holds the run open. */
let botApi: Forward | undefined;

const current = (): Setup => {
  if (!setup) throw new Error("the end-to-end run is not set up");
  return setup;
};

/** Where the run keeps what alasio does: its config, its kubeconfig, the cluster's storage, and the folder workspaces' home. */
export const paths = (): Pick<Setup, "configFile" | "kubeconfig" | "storage" | "home"> => {
  const { configFile, kubeconfig, storage, home } = current();
  return { configFile, kubeconfig, storage, home };
};

/** Runs `alasio ...args` from its package, saying what it does as it goes. */
export function alasio(...args: string[]): Promise<Ran> {
  const { bin, env } = current();
  return capture(spawn(bin, args, { env, stdio: ["ignore", "pipe", "pipe"] }));
}

/** Runs `alasio ...args`, which must succeed: what it printed. */
export async function alasioOk(...args: string[]): Promise<string> {
  return alasioOkWith({}, ...args);
}

/** Runs `alasio ...args` with `env` over the run's environment, which must succeed: what it printed. */
export async function alasioOkWith(env: NodeJS.ProcessEnv, ...args: string[]): Promise<string> {
  const { bin } = current();
  const ran = await capture(spawn(bin, args, { env: { ...current().env, ...env }, stdio: ["ignore", "pipe", "pipe"] }));
  // What it printed too, as status's list of what is not ready.
  if (ran.code !== 0) throw new Error(`alasio ${args.join(" ")} exited with ${ran.code}:\n${ran.stderr}${ran.stdout ? `printed:\n${ran.stdout}` : ""}`);
  return ran.stdout;
}

/** `alasio ...args` from its package, which runs until it is killed, as `alasio grafana` does: the process, and its first line of output, once it says it. */
export async function alasioRunning(...args: string[]): Promise<{ readonly child: ChildProcess; readonly said: string }> {
  const { bin, env } = current();
  const child = spawn(bin, args, { env, stdio: ["ignore", "pipe", "inherit"] });
  const said = await new Promise<string>((resolve, reject) => {
    let stdout = "";
    child.stdout?.on("data", (chunk: Buffer) => {
      stdout += chunk;
      if (stdout.includes("\n")) resolve(stdout.split("\n")[0] ?? "");
    });
    child.on("error", reject);
    child.on("exit", (code) => reject(new Error(`alasio ${args.join(" ")} exited with ${code} before it said anything`)));
  });
  return { child, said };
}

/** A read-only query of the lake, as alasio lake runs one: its rows. */
export async function lakeQuery(sql: string): Promise<Record<string, unknown>[]> {
  const stdout = await alasioOk("lake", "--format", "json", sql);
  return stdout.trim().split("\n").filter(Boolean).map((line): Record<string, unknown> => JSON.parse(line));
}

/**
 * The command lines the run started that have not ended: a test that timed out leaves its
 * own running, which tearDown ends, so none keeps the run alive past its tests.
 */
const running = new Set<ChildProcess>();

/** What `child` writes and exits with; what it says on stderr, its progress, is said here too, as it says it. */
function capture(child: ChildProcess): Promise<Ran> {
  running.add(child);
  child.on("close", () => running.delete(child));
  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (chunk: Buffer) => {
    stdout += chunk;
  });
  child.stderr?.on("data", (chunk: Buffer) => {
    stderr += chunk;
    process.stderr.write(chunk);
  });
  return new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("close", (code) => resolve({ code: code ?? 1, stdout, stderr }));
  });
}

/** `sudo -n ...args`, which must succeed. */
const sudo = (...args: string[]) => run("sudo", ["-n", ...args], { maxBuffer: 64 * 1024 * 1024 });

/** `docker ...args`, which must succeed. */
const docker = (...args: string[]) => run("docker", args, { cwd: root, maxBuffer: 64 * 1024 * 1024 });

/** A port of the loopback no one listens on now. */
async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  // A server listening on a TCP port has an address of its own.
  const { port } = server.address() as AddressInfo;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

/**
 * `text` with every bearer credential in it hidden, and every signed token (a base64url JSON object
 * and its signature, as alasio's callers' tokens are): a command run in a pod is said in full where
 * it fails, so a token it was given as an argument would otherwise reach the run's logs.
 */
export const withoutBearers = (text: string): string =>
  text
    .replace(/(bearer\s+)[A-Za-z0-9._~+/=-]+/giu, (_, said: string) => `${said}<hidden>`)
    .replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)?/gu, () => "<hidden>");

/** The objects of the cluster, through the API its kubeconfig reaches, as alasio's command line reaches them. */
let api: KubeApi["Service"] | undefined;
let kubeConfig: KubeConfig | undefined;

const kubeApi = (): KubeApi["Service"] => {
  if (!api) throw new Error("the end-to-end run has no cluster yet");
  return api;
};

/** The reference of the object of `kindName` named `name`, in `namespace` unless it is cluster-scoped. */
export const ref = (kindName: KindName, name: string, namespace?: string): ObjectRef => ({ ...kind(kindName), namespace, name });

/** A local port forwarded to a port of a Service's pods, a connection at a time, each to a pod of it that is ready then. */
export interface Forward {
  readonly base: string;
  readonly port: number;
  close(): void;
}

/** What `exec` runs a command with: the container (the pod's first unless given), and what it reads. */
interface ExecOptions {
  readonly container?: string;
  readonly stdin?: string;
}

/** The cluster, as the tests reach it. */
export const kube = {
  get: <T extends KubernetesObject>(target: ObjectRef): Promise<T | null> => Effect.runPromise(kubeApi().get<T>(target)),
  list: <T extends KubernetesObject>(kindName: KindName, options?: ListOptions): Promise<readonly T[]> => Effect.runPromise(kubeApi().list<T>(kind(kindName), options)),
  apply: (object: KubernetesObject): Promise<KubernetesObject> => Effect.runPromise(kubeApi().apply(object)),
  remove: (target: ObjectRef): Promise<void> => Effect.runPromise(kubeApi().remove(target)),
  patch: (target: ObjectRef, patch: object): Promise<KubernetesObject> => Effect.runPromise(kubeApi().patch(target, patch, "alasio-e2e")),

  /** Kills the pod at once, as a crash would, without the grace it is given otherwise. */
  kill: async (namespace: string, pod: string): Promise<void> => {
    if (!kubeConfig) throw new Error("the end-to-end run has no cluster yet");
    await kubeConfig.makeApiClient(CoreV1Api).deleteNamespacedPod({ namespace, name: pod, gracePeriodSeconds: 0 });
  },

  /** Waits until each of `refs` is ready, as alasio up waits: a workload rolled out, a Job complete. */
  awaitReady: (refs: readonly ObjectRef[], timeout: Duration.Input): Promise<void> =>
    Effect.runPromise(Effect.provideService(awaitReady(refs, { timeout, poll: "2 seconds" }), KubeApi, kubeApi())),

  /** What the container has logged, or, `previous`, its instance before this one, which ended. */
  logs: (namespace: string, pod: string, container: string, { previous = false, timestamps = false }: { readonly previous?: boolean; readonly timestamps?: boolean } = {}): Promise<string> =>
    Effect.runPromise(kubeApi().logs(namespace, pod, { container, previous, timestamps }).pipe(Stream.decodeText(), Stream.mkString)),

  /**
   * All the container logs until it ends, as one that stops logs as it does, whose log goes
   * with its pod: what it had logged when its log ended, however it ended.
   */
  followedLogs: async (namespace: string, pod: string, container: string): Promise<string> => {
    let logged = "";
    await Effect.runPromise(kubeApi().logs(namespace, pod, { container, follow: true }).pipe(
      Stream.decodeText(),
      Stream.runForEach((text) => Effect.sync(() => (logged += text))),
      Effect.ignore,
    ));
    return logged;
  },

  /** Runs `command` in the pod. */
  exec: async (namespace: string, pod: string, command: readonly string[], { container, stdin }: ExecOptions = {}): Promise<Ran> => {
    const name = container ?? (await kube.get<V1Pod>(ref("Pod", pod, namespace)))?.spec?.containers[0]?.name ?? "";
    const out: Buffer[] = [];
    const err: Buffer[] = [];
    const collect = (into: Buffer[]) =>
      new Writable({
        write: (chunk: Buffer, _encoding, done) => {
          into.push(chunk);
          done();
        },
      });
    const code = await Effect.runPromise(kubeApi().exec({ namespace, pod, container: name }, command, {
      stdin: stdin === undefined ? null : Readable.from([stdin]),
      stdout: collect(out),
      stderr: collect(err),
      tty: false,
    }));
    return { code, stdout: Buffer.concat(out).toString("utf8"), stderr: Buffer.concat(err).toString("utf8") };
  },

  /** Runs `command` in the pod, which must succeed: what it printed. */
  execOk: async (namespace: string, pod: string, command: readonly string[], options?: ExecOptions): Promise<string> => {
    const ran = await kube.exec(namespace, pod, command, options);
    if (ran.code !== 0) throw new Error(withoutBearers(`${command.join(" ")} in ${namespace}/${pod} exited with ${ran.code}: ${ran.stderr}${ran.stdout}`));
    return ran.stdout;
  },

  /** The newest pod of the Deployment that runs ready. */
  runningPod: async (namespace: string, deployment: string): Promise<string> => {
    const workload = await kube.get<V1Deployment>(ref("Deployment", deployment, namespace));
    // No selector would be every pod of the namespace.
    if (!workload?.spec?.selector.matchLabels) throw new Error(`the deployment ${namespace}/${deployment} selects no pod by its labels: ${workload ? "it has no matchLabels" : "there is none"}`);
    const pods = await kube.list<V1Pod>("Pod", { namespace, labelSelector: selectorOf(workload.spec.selector.matchLabels) });
    const running = pods
      .filter((pod) => pod.status?.phase === "Running" && !pod.metadata?.deletionTimestamp && podProblems(pod).length === 0)
      .sort((a, b) => new Date(b.metadata?.creationTimestamp ?? 0).getTime() - new Date(a.metadata?.creationTimestamp ?? 0).getTime());
    const name = running[0]?.metadata?.name;
    if (!name) throw new Error(`no pod of deployment ${namespace}/${deployment} runs: ${pods.flatMap(podProblems).join("; ") || "it has none"}`);
    return name;
  },

  /** The value of `key` of the Secret. */
  secret: async (namespace: string, name: string, key: string): Promise<string> => {
    const value = (await kube.get<V1Secret>(ref("Secret", name, namespace)))?.data?.[key];
    if (value === undefined) throw new Error(`the Secret ${namespace}/${name} has no ${key}`);
    return Buffer.from(value, "base64").toString("utf8");
  },

  /** A local port forwarded to `port` of the Service's pods, once one answers there. */
  forward: async (namespace: string, service: string, port: number): Promise<Forward> => {
    if (!kubeConfig) throw new Error("the end-to-end run has no cluster yet");
    const forwarder = new PortForward(kubeConfig);
    const connections = new Set<Socket>();
    const server: Server = createServer((socket) => {
      connections.add(socket);
      // A connection's end, either side's, is its forward's, and the other's.
      socket.on("error", () => socket.destroy());
      socket.on("close", () => connections.delete(socket));
      forwarder.portForwardService(namespace, service, [port], socket, null, socket).then(
        (opened) => {
          // Without retries, a forward is its websocket.
          const forwarded = typeof opened === "function" ? opened() : opened;
          forwarded?.on("close", () => socket.destroy());
          forwarded?.on("error", () => socket.destroy());
          socket.on("close", () => forwarded?.close());
        },
        () => socket.destroy(),
      );
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    // A server listening on a TCP port has an address of its own.
    const { port: local } = server.address() as AddressInfo;
    return {
      base: `http://127.0.0.1:${local}`,
      port: local,
      close: () => {
        server.close();
        for (const socket of connections) socket.destroy();
      },
    };
  },
};

/** Runs a Job of the CronJob's template, as one it starts on its schedule is, until it completes: what its pods logged. */
export async function runCronJob(cronJob: string): Promise<string> {
  const name = `${cronJob}-e2e-${Date.now()}`;
  const template = (await kube.get<V1CronJob>(ref("CronJob", cronJob, NAMESPACE)))?.spec?.jobTemplate;
  if (!template?.spec) throw new Error(`there is no CronJob ${cronJob}`);
  const job: V1Job = { apiVersion: "batch/v1", kind: "Job", metadata: { name, namespace: NAMESPACE, labels: template.metadata?.labels ?? {} }, spec: template.spec };
  await kube.apply(job);
  const logs = async () => {
    const pods = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "job-name": name }) });
    const logged = await Promise.all(pods.flatMap((pod) =>
      [...(pod.spec?.initContainers ?? []), ...(pod.spec?.containers ?? [])].map((container) =>
        kube.logs(NAMESPACE, pod.metadata?.name ?? "", container.name).catch((error: unknown) => (error instanceof Error ? error.message : String(error)))
      )
    ));
    return logged.join("");
  };
  try {
    await kube.awaitReady([ref("Job", name, NAMESPACE)], "10 minutes").catch(async (error: unknown) => {
      throw new Error(`${error instanceof Error ? error.message : String(error)}\n${await logs()}`);
    });
    return await logs();
  } finally {
    await kube.remove(ref("Job", name, NAMESPACE)).catch(() => {});
  }
}

/** The namespace of main's sessions, its sandbox pools'. */
export const SESSIONS = "alasio-sessions";

/** A session's internet, as its alasio records it, and binds it with. */
export type SessionMode = "none" | "full";

/** The internet of each session the run made, as alasio recorded it: what a token of it is minted with, none unless said. */
const sessionModes = new Map<string, SessionMode>();

/** Records the internet alasio made the session `volumeId` with, as tokens of it are minted with. */
export const recordSession = (volumeId: string, mode: SessionMode): void => void sessionModes.set(volumeId, mode);

/** A caller's token of the workspace `workspace` of main's sessions, signed by main's own key, as main's alasio signs one, of its internet. */
export async function mainToken(workspace: string, mode: SessionMode = sessionModes.get(workspace) ?? "none"): Promise<string> {
  const callerKey = await kube.secret(NAMESPACE, GATEWAY, "caller-key");
  return mintToken(environmentKey(Buffer.from(callerKey), "main"), { key: sessionKey(workspace), expires: Date.now() + 9 * 60_000, options: { environment: "main", mode } });
}

/** A caller's token of main's environment for `key`, good for nine minutes, within the longest the gateway takes, as main's alasio signs one: of a folder conversation's instance, say. */
export async function mainKeyToken(key: string): Promise<string> {
  const callerKey = await kube.secret(NAMESPACE, GATEWAY, "caller-key");
  return mintToken(environmentKey(Buffer.from(callerKey), "main"), { key, expires: Date.now() + 9 * 60_000, options: { environment: "main" } });
}

/**
 * What the gateway answers `method path` with, asked as alasio asks it, from its pod, with `token`, `body` sent as it is: its status, what it said, and when it asked to be asked again (its Retry-After), if it did.
 * The token goes in on the command's input, its first line, and to curl as a header file, so it is in no command's arguments.
 */
export async function gatewayCall(token: string, method: string, path: string, body?: string, { bind = true } = {}): Promise<{ readonly status: number; readonly said: string; readonly retryAfter: string }> {
  const alasioPod = await kube.runningPod(NAMESPACE, RELEASE);
  const printed = await kube.execOk(NAMESPACE, alasioPod, [
    "sh",
    "-c",
    [
      // The shell's read takes the first line alone, so curl reads the body from what follows it.
      "IFS= read -r token",
      `curl -sS -w '\n%header{retry-after}\n%{http_code}' -X ${method} -H @/dev/fd/3${bind ? "" : ` -H '${NO_BIND_HEADER}: never'`}${body === undefined ? "" : " --data-binary @-"} "http://${GATEWAY}.${NAMESPACE}.svc:${GATEWAY_PORT}$1" 3<<HEADER`,
      "authorization: Bearer $token",
      "HEADER",
    ].join("\n"),
    "sh",
    path,
  ], { container: "alasio", stdin: `${token}\n${body ?? ""}` });
  const lines = printed.split("\n");
  return { status: Number(lines.at(-1)), said: lines.slice(0, -2).join("\n"), retryAfter: lines.at(-2) ?? "" };
}

/** The pod of `namespace`'s sessions, main's unless said, bound to `workspace`, not one being deleted: its name and node; null where none is. */
export async function boundPodOf(workspace: string, namespace = SESSIONS): Promise<{ readonly name: string; readonly node: string } | null> {
  const [bound] = (await kube.list<V1Pod>("Pod", { namespace, labelSelector: `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}` })).filter(({ metadata }) => !metadata?.deletionTimestamp);
  return bound?.metadata?.name ? { name: bound.metadata.name, node: bound.spec?.nodeName ?? "" } : null;
}

/** Binds main's session `volumeId` through the gateway, as alasio binds it, asked again while its pool has no pod for it: the pod it is bound to. */
export async function bindSession(volumeId: string, timeoutMs = 300_000): Promise<{ readonly name: string; readonly node: string }> {
  const token = await mainToken(volumeId);
  let last = { status: 0, said: "" };
  for (const deadline = Date.now() + timeoutMs; ; await new Promise((resolve) => setTimeout(resolve, 2_000))) {
    last = await gatewayCall(token, "POST", "/bind");
    // 200 and the binding.
    const bound = last.status === 200 ? await boundPodOf(volumeId) : null;
    if (bound) return bound;
    if (Date.now() >= deadline) throw new Error(`the gateway did not bind ${volumeId} within ${timeoutMs / 1000}s: it last answered ${last.status} ${last.said}`);
  }
}

/** Node run in main's session's bayma container, the session bound first, as a turn binds it: its stdout. */
export const inSession = async (volumeId: string, code: string): Promise<string> =>
  kube.execOk(SESSIONS, (await bindSession(volumeId)).name, ["node", "-e", code], { container: "bayma" });

/**
 * Node code that prints whether a TCP connection to `host` (an expression) on `port` opens:
 * "open", or "blocked" where it times out or a socket error ends it, as a network policy's
 * REJECT or DROP does; and nothing else.
 */
const tcp = (host: string, port: number): string =>
  `const s=require("net").connect({host:${host},port:${port},timeout:3000});s.on("connect",()=>{console.log("open");process.exit()});s.on("timeout",()=>{console.log("blocked");process.exit()});s.on("error",()=>{console.log("blocked");process.exit()})`;

/** How many times reachability asks before it gives up on a probe that answers neither "open" nor "blocked". */
const PROBE_TRIES = 3;

/**
 * Whether a TCP connection to `host` (an expression) on `port` opens where `run` runs Node code
 * (tcp), as it answers: exactly "open" or "blocked". An exec that fails, or prints anything else,
 * as nothing, tells nothing, so it is never taken for either: it is asked again, 2 s apart,
 * PROBE_TRIES times at most, and then fails, saying each raw answer.
 */
export async function reachability(run: (code: string) => Promise<string>, host: string, port: number): Promise<"open" | "blocked"> {
  const answers: string[] = [];
  for (let tries = 0; tries < PROBE_TRIES; tries += 1) {
    if (tries > 0) await new Promise((resolve) => setTimeout(resolve, 2_000));
    const answer = await run(tcp(host, port)).then((said) => said.trim(), (error: unknown) => `the probe's exec failed: ${error instanceof Error ? error.message : String(error)}`);
    if (answer === "open" || answer === "blocked") return answer;
    answers.push(JSON.stringify(answer));
  }
  throw new Error(`the probe of ${host}:${port} answered neither "open" nor "blocked", ${PROBE_TRIES} times: ${answers.join(", ")}`);
}

/** Runs Node code in the container `container` of the pod `pod` of `namespace`, as reachability runs it. */
export const inContainer = (namespace: string, pod: string, container: string) => (code: string): Promise<string> =>
  kube.execOk(namespace, pod, ["node", "-e", code], { container });

/** Runs `command` in alasio's container, in the pod it runs in now, which must succeed: what it printed. */
export const inAlasioContainer = async (command: readonly string[], stdin?: string): Promise<string> =>
  kube.execOk(NAMESPACE, await kube.runningPod(NAMESPACE, RELEASE), command, { container: "alasio", ...(stdin === undefined ? {} : { stdin }) });

/**
 * Runs one of test/e2e's scripts by node in the container of the pod, which runs alasio's
 * image: its last line, parsed, which the script prints in the shape `Seen` names.
 */
async function scriptIn<Seen>(namespace: string, pod: string, container: string, script: string, args: readonly string[]): Promise<Seen> {
  // The scripts import alasio's modules by their paths in the repository, for the type
  // checker; in alasio's image those are under /opt/alasio, and a script read from stdin
  // resolves its imports from its working directory, not from where it was read. So they
  // are rewritten to the image's paths as the script is piped in.
  const source = readFileSync(new URL(script, import.meta.url), "utf8").replaceAll('from "../../src/', 'from "/opt/alasio/src/');
  const stdout = await kube.execOk(namespace, pod, ["sh", "-c", 'cd /opt/alasio && node --input-type=module-typescript - "$@"', "node", ...args], { container, stdin: source });
  // split always returns at least one part.
  return JSON.parse(stdout.trim().split("\n").at(-1)!);
}

/** Runs one of test/e2e's scripts in alasio's pod, with alasio's code and ServiceAccount, as scriptIn says. */
export async function inAlasio<Seen>(script: string, ...args: string[]): Promise<Seen> {
  return scriptIn(NAMESPACE, await kube.runningPod(NAMESPACE, RELEASE), "alasio", script, args);
}

/** The pods of the node agents that run ready, by the names of their nodes. */
export async function nodeAgents(): Promise<ReadonlyMap<string, string>> {
  const pods = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("node-agent")) });
  return new Map(pods.flatMap((pod) =>
    pod.spec?.nodeName && pod.metadata?.name && !pod.metadata.deletionTimestamp && podProblems(pod).length === 0 ? [[pod.spec.nodeName, pod.metadata.name] as const] : []
  ));
}

/** Runs one of test/e2e's scripts in the pod of the node agent on `node`, with its bucket and token in its environment, as scriptIn says. */
export async function inNodeAgent<Seen>(node: string, script: string, ...args: string[]): Promise<Seen> {
  const pod = (await nodeAgents()).get(node);
  if (!pod) throw new Error(`no node agent runs ready on ${node}`);
  return scriptIn(NAMESPACE, pod, "node-agent", script, args);
}

/** Restarts the container of the node `node`, in Docker, as the machine it stands for restarts: every process of it ended, what its volumes hold kept. */
export async function restartNodeContainer(node: string): Promise<void> {
  await docker("restart", node);
}

/**
 * Runs `script` with sh as root on the cluster's node `node`, which must succeed: what it
 * printed. A node in Docker is a container of this machine's Docker; on the host, this
 * machine is the node.
 */
export async function onNode(node: string, script: string): Promise<string> {
  return (TARGET === "host" ? await sudo("sh", "-c", script) : await docker("exec", node, "sh", "-c", script)).stdout;
}

/** Builds the image `name`, tagged `tag`, with `args` for docker build besides. */
async function build(name: keyof typeof IMAGES, tag: string, args: readonly string[] = []): Promise<void> {
  console.error(`# building ${tag} from ${IMAGES[name]}`);
  await docker("build", "--quiet", ...(name === "alasio-node" ? NODE_BUILD_ARGS : []), ...args, "--tag", tag, IMAGES[name]);
}

/**
 * Starts the run's registry on the cluster's network, published on a port of the loopback,
 * which Docker pushes to over plain HTTP as it does to any registry there: the registry's
 * host as this machine reaches it.
 */
async function startRegistry(): Promise<string> {
  console.error(`# starting the registry ${REGISTRY}`);
  // One a kept run left goes first; what its volume holds is pushed over.
  await docker("rm", "--force", REGISTRY).catch(() => undefined);
  await docker("run", "--detach", "--name", REGISTRY, "--network", CLUSTER, "--publish", "127.0.0.1::5000", "--volume", `${REGISTRY}:/var/lib/registry`, REGISTRY_IMAGE);
  // The published address, as `127.0.0.1:PORT`.
  return (await docker("port", REGISTRY, "5000/tcp")).stdout.trim();
}

/** Builds the image `name`, with `args` for docker build besides, and pushes it to the registry at `pushHost`, then removes it here, with Docker's build cache. */
async function push(pushHost: string, name: keyof typeof IMAGES, args: readonly string[] = []): Promise<void> {
  const tag = `${pushHost}/${imageOf(name)}`;
  await build(name, tag, args);
  console.error(`# pushing ${tag}`);
  await docker("push", "--quiet", tag);
  await docker("image", "rm", tag);
  await docker("builder", "prune", "--all", "--force");
}

/**
 * The install configuration the run starts alasio with: its images, pulled from where they
 * were pushed, alasio's with the stand-in for Codex, the stand-ins, small volumes, and
 * requests small enough that the whole of it schedules on a CI runner's two CPUs. Of the
 * previous release, its own images, which its package pins, but for alasio's, the stand-in
 * built on it, and the settings it has, as its alasio init writes them. A setting the
 * previous release does not have is given to this checkout's alone, as that release refuses
 * a configuration with a setting it does not know: 10.0.3 has every setting this checkout
 * has, so far.
 */
function installation(of: "previous" | "release"): Record<string, unknown> {
  const pushed = (name: keyof typeof IMAGES) => ({ ...pulledOf(name), digest: "" });
  const on = (node: string) => (AGENTS >= 2 ? { nodeSelector: { "kubernetes.io/hostname": `${CLUSTER}-${node}` } } : {});
  return {
    images: of === "previous"
      ? { alasio: { repository: `${PUSHED}/alasio-harness-stand-ins-previous`, tag: "e2e", digest: "" }, pullPolicy: "IfNotPresent" }
      : { alasio: pushed("alasio-harness-stand-ins"), agent: pushed("alasio-agent"), lake: pushed("alasio-lake"), grafana: pushed("alasio-grafana"), log: pushed("alasio-log"), collector: pushed("alasio-collector"), webhook: pushed("alasio-webhook"), pullPolicy: "IfNotPresent" },
    alasio: {
      env: {
        TELEGRAM_API_ROOT: urlOf(TELEGRAM),
        ALASIO_CODEX_BIN: CODEX_STAND_IN,
        ALASIO_CLAUDE_BIN: CLAUDE_STAND_IN,
      },
      // The release before's as much as the box's asks for.
      resources: { requests: { cpu: "50m", memory: of === "previous" ? "2560Mi" : "256Mi" } },
      // What KEDA scales of an environment's, its relay of its Codex login among it, back to none within the agents and wake shards' runs.
      ...(of === "release" && (SHARD === "agents" || SHARD === "wake") ? { cooldownSeconds: 30 } : {}),
      // One, as the suites reach its pod as one, but where the agents shard makes its scale a range and fixes it again, and the
      // wake shard's gate gives it the default scale, KEDA's, which its press wakes from none; and in the upgrade shard, whose
      // cut-over upgrades to the default scale, as an installation of the release before's does.
      ...(of === "release" && SHARD !== "upgrade" ? { scaling: { min: 1, max: 1 } } : {}),
      // On one node but in the wake shard, whose gate wakes it onto one it never ran on.
      ...(SHARD === "wake" ? {} : on("agent-1")),
    },
    // The release before's folder instances let go after four hours idle, as the box's configuration says. This checkout's limited
    // to 1536Mi, for this run alone, so that a runaway exec's process runs past it without filling the runner, and the 800 MiB its
    // sizing holds still fits.
    host: {
      resources: { requests: { cpu: "20m", memory: "128Mi" }, ...(of === "previous" ? {} : { limits: { memory: "1536Mi" } }) },
      ...(of === "previous" ? { releaseAfter: "4h" } : {}),
    },
    // Conversations' harnesses' pools, of this checkout's configuration alone, as the release before has none:
    // released within the run in the agents and wake shards alone, as the other suites' turns are not written to wait on a pool's
    // pod: the wake shard's alasio sleeps only once nothing the command line's suite left keeps it awake.
    ...(of === "release"
      ? {
        harness: {
          pools: { scaling: { min: 1, max: 2 }, releaseAfter: SHARD === "agents" || SHARD === "wake" ? `${HARNESS_RELEASE_AFTER_SECONDS}s` : "24h" },
          session: { resources: { requests: { cpu: "20m", memory: "128Mi" } } },
          folder: { resources: { requests: { cpu: "20m", memory: "128Mi" } } },
        },
      }
      : {}),
    sessions: {
      storage: { size: "1Gi" },
      // The snapshots shard's sessions hold a REPL of BIG_REPL_MEBIBYTES live, with room for bayma and gVisor.
      resources: { requests: { cpu: "50m", memory: "128Mi" }, limits: { cpu: "1", memory: SHARD === "snapshots" ? "3Gi" : "1Gi" } },
      // Never released within the run but in the snapshots, upgrade and wake shards: the other suites
      // reach sessions by exec in their pods, which is no exec of bayma's, and a release would end
      // their pods under them.
      snapshots: {
        releaseAfter: SHARD === "snapshots" || SHARD === "upgrade" || SHARD === "wake" ? `${RELEASE_AFTER_SECONDS}s` : "24h",
        // Saved live in the snapshots shard within its test of it, as an exec runs on.
        ...(SHARD === "snapshots" ? { liveEvery: `${LIVE_EVERY_SECONDS}s` } : {}),
      },
      // On one agent, but in the snapshots shard, where a session runs on any node, as it may be restored on another than it was saved on.
      ...(SHARD === "snapshots" ? {} : on("agent-0")),
    },
    // Three, where the run proves the log a quorum: each node's access applier writes the
    // credentials through whichever is KRaft's active controller, and its node is ready.
    ...(AGENTS >= 2 && inShard("telemetry") ? { log: { scaling: { min: 3, max: 3 } } } : {}),
    // The release before's log as much as the box's asks for, on the one node the upgrade shard runs.
    ...(of === "previous" ? { log: { resources: { requests: { memory: "1Gi" } } } } : {}),
    neon: {
      safekeepers: { storage: { size: "2Gi" }, resources: { requests: { cpu: "20m", memory: "64Mi" } } },
      // Two, where the run proves Neon's storage scaled out: its tenant split across them.
      pageserver: {
        ...(AGENTS >= 2 && inShard("neon") ? { scaling: { min: 2, max: 2 } } : {}),
        storage: { size: "5Gi" },
        resources: { requests: { cpu: "50m", memory: "128Mi" } },
      },
      readReplica: { resources: { requests: { cpu: "20m", memory: "128Mi" } } },
      storageController: { resources: { requests: { cpu: "10m", memory: "64Mi" } } },
      storageBroker: { resources: { requests: { cpu: "10m", memory: "32Mi" } } },
      controllerDb: { resources: { requests: { cpu: "10m", memory: "64Mi" } } },
      control: { resources: { requests: { cpu: "10m", memory: "32Mi" } } },
      compute: { resources: { requests: { cpu: "50m", memory: "256Mi" } } },
      ...on("server-0"),
    },
    // In Docker, the server node's address, where the intake is; on the machine, the one alasio init finds, which takes
    // its port from Telegram's ranges and, as Telegram is the stand-in here, from the cluster's pods.
    webhook: {
      ...(TARGET === "docker" ? { publicAddress: subnetAddress(DOCKER_SUBNET, 2) } : { sources: ["149.154.160.0/20", "91.108.4.0/22", "10.42.0.0/16"] }),
      resources: { requests: { cpu: "10m", memory: "32Mi" } },
    },
    telemetry: {
      collector: { gateway: { resources: { requests: { cpu: "10m", memory: "64Mi" } } }, scraper: { resources: { requests: { cpu: "10m", memory: "64Mi" } } } },
    },
    objectStore: {
      bundled: {
        // On three nodes, three masters in Raft, each object kept twice on three volume
        // servers, and two filers on their store with two gateways, so losing one of a part
        // loses nothing.
        ...(AGENTS >= 2 ? { replication: "001" } : {}),
        masters: { ...(AGENTS >= 2 ? { scaling: { min: 3, max: 3 } } : {}), resources: { requests: { cpu: "10m", memory: "32Mi" } } },
        volumeServers: {
          ...(AGENTS >= 2 ? { scaling: { min: 3, max: 3 } } : {}),
          storage: { size: "10Gi" },
          volumes: 30,
          // A runner's disk is mostly images; the store needs only a little of what is left.
          minFreeSpace: "1GiB",
          resources: { requests: { cpu: "10m", memory: "64Mi" } },
        },
        filers: { ...(AGENTS >= 2 ? { scaling: { min: 2, max: 2 } } : {}), resources: { requests: { cpu: "10m", memory: "64Mi" } } },
        gateways: { ...(AGENTS >= 2 ? { scaling: { min: 2, max: 2 } } : {}), resources: { requests: { cpu: "10m", memory: "64Mi" } } },
        filerStore: { resources: { requests: { cpu: "10m", memory: "16Mi" } } },
      },
    },
    lake: {
      loader: { resources: { requests: { cpu: "20m", memory: "256Mi" } } },
      maintenance: { resources: { requests: { cpu: "10m", memory: "128Mi" } } },
      // As many as two, so the run sees them scale out.
      query: { scaling: { min: 1, max: 2 }, resources: { requests: { cpu: "10m", memory: "128Mi" } } },
      intake: { resources: { requests: { cpu: "20m", memory: "128Mi" } } },
    },
    // Two, where the run proves Grafana highly available. An idle Grafana, its alert rules
    // evaluated each minute, takes 10 to 13m of CPU (measured in CI): its request well above,
    // so KEDA's CPU scaler, at 80 % of it, scales it on load rather than on that idle work.
    grafana: { ...(inShard("scaling") ? { scaling: { min: 2, max: 2 } } : {}), resources: { requests: { cpu: "40m", memory: "192Mi" } } },
    // JuiceFS's metadata dumped as often as it allows, so one lands within the run.
    workspaceStorage: { backupInterval: "5m" },
  };
}

/**
 * Sets the run up, as the header says. `alasio init` checks the bot's token with
 * Telegram before the cluster, and the stand-in in it, exist, so it is given one of its
 * own, here, which answers for the same bot.
 */
export async function setUp(): Promise<void> {
  const work = mkdtempSync(join(tmpdir(), "alasio-e2e-"));
  const configHome = join(work, "config");
  const configFile = join(configHome, "alasio", "config.json");
  const storage = join(work, "storage");
  // Written by the user folder workspaces run as, as well as by this one.
  const home = join(work, "home");
  mkdirSync(home);
  chmodSync(home, 0o777);
  const env: NodeJS.ProcessEnv = { ...process.env, XDG_CONFIG_HOME: configHome, XDG_DATA_HOME: join(work, "data") };
  for (const name of ["KUBECONFIG", "TELEGRAM_BOT_TOKEN", "TELEGRAM_API_ROOT", "CLAUDE_CODE_OAUTH_TOKEN"]) delete env[name];

  if (TARGET === "host" && (!PUSHED || AGENTS > 0)) throw new Error("the host target's run is of one node, from images pushed already (ALASIO_E2E_REGISTRY)");
  const previous = SHARD === "upgrade";
  if (previous && (TARGET !== "host" || !PREVIOUS)) throw new Error("the upgrade shard upgrades k3s on this machine, as the box's, from the release ALASIO_E2E_PREVIOUS names");
  const prefix = join(work, "prefix");
  console.error(`# packing alasio's package and installing it in ${prefix}`);
  const release = await installCli((await packCli(work)).tarball, prefix);
  let bin = release;
  if (previous) {
    console.error(`# installing alasio ${PREVIOUS}'s package in ${join(work, "previous")}`);
    bin = await installCli(`alasio@${PREVIOUS}`, join(work, "previous"));
  }
  setup = { work, bin, release, env, configFile, kubeconfig: join(configHome, "alasio", "kubeconfig"), storage, home };

  if (!PUSHED) await build("alasio-node", NODE_IMAGE);
  mkdirSync(join(configHome, "alasio"), { recursive: true, mode: 0o700 });
  const target = TARGET === "host"
    ? { host: { storagePath: storage } }
    : {
      docker: {
        name: CLUSTER,
        apiPort: await freePort(),
        storagePath: storage,
        // A subnet of the run's own, whose server node's address the webhook intake is reached at (DOCKER_SUBNET).
        subnet: DOCKER_SUBNET,
        agents: AGENTS,
        image: NODE_IMAGE,
        ...(PUSHED ? {} : { registries: { mirrors: { [REGISTRY_HOST]: { endpoint: [`http://${REGISTRY_HOST}`] } } } }),
      },
    };
  writeFileSync(configFile, JSON.stringify({ target, install: installation(previous ? "previous" : "release") }), { mode: 0o600 });
  const botToken = join(work, "bot-token");
  writeFileSync(botToken, "123:e2e", { mode: 0o600 });
  const telegram = createTelegramStub();
  await new Promise<void>((resolve) => telegram.server.listen(0, "127.0.0.1", resolve));
  try {
    // A server listening on a TCP port has an address of its own.
    const { port } = telegram.server.address() as AddressInfo;
    const folders = HOST_PROFILE ? ["--folder-workspaces", "--user", `${HOST_USER}:${HOST_USER}`, "--home", home] : ["--no-folder-workspaces"];
    const ran = await capture(
      spawn(current().bin, ["init", "--non-interactive", "--bot-token-file", botToken, "--allowed-user-ids", `${OPERATOR},${OTHER_OPERATOR}`, ...folders, "--telemetry-endpoint", urlOf(OTLP), "--no-up"], {
        env: { ...env, TELEGRAM_API_ROOT: `http://127.0.0.1:${port}` },
        stdio: ["ignore", "pipe", "pipe"],
      }),
    );
    if (ran.code !== 0) throw new Error(`alasio init exited with ${ran.code}:\n${ran.stderr}`);
  } finally {
    await new Promise((resolve) => telegram.server.close(resolve));
  }

  kubeConfig = new KubeConfig();
  kubeConfig.loadFromFile(current().kubeconfig);
  api = await Effect.runPromise(Effect.provide(Effect.gen(function*() { return yield* KubeApi; }), KubeApi.layer({ path: current().kubeconfig })));

  if (!PUSHED) {
    const pushHost = await startRegistry();
    for (const name of ["alasio", "alasio-agent", "alasio-lake", "alasio-grafana", "alasio-log", "alasio-collector", "alasio-webhook"] as const) await push(pushHost, name);
    // On alasio's image, which its build pulls back from the registry.
    await push(pushHost, "alasio-harness-stand-ins", ["--file", "test/e2e/harness-stand-ins.Dockerfile", "--build-arg", `ALASIO=${pushHost}/${imageOf("alasio")}`]);
  }

  console.error("# applying the stand-ins");
  const standIns = standInObjects();
  for (const object of standIns) await kube.apply(object);
  await kube.awaitReady(
    standIns.filter((object) => object.kind === "Deployment").map((object) => ref("Deployment", object.metadata?.name ?? "", object.metadata?.namespace)),
    "5 minutes",
  );

  // The command line asks the Telegram stand-in what it would ask Telegram: main's bot is registered with it as alasio up runs.
  botApi = await kube.forward(STAND_INS, TELEGRAM.service, TELEGRAM.port);
  setup = { ...current(), env: { ...current().env, TELEGRAM_API_ROOT: botApi.base } };
  await alasioOk("up", "--timeout", "20m");
  await trustTelegramStandIn();
}

/**
 * Makes Grafana take the Telegram stand-in for api.telegram.org, where its Telegram
 * integration alone sends, over HTTPS (./telegram-tls.ts): the name resolves, in its pod,
 * to the stand-in's Service, Grafana trusts the CA that signed the stand-in's
 * certificate, and may reach it. The run's own objects and patch, beside alasio's, as no
 * setting of alasio's points Grafana elsewhere; then Grafana's new pod is waited for.
 */
async function trustTelegramStandIn(): Promise<void> {
  const grafana = componentName("grafana");
  const ca = "alasio-e2e-telegram-ca";
  const caMap: V1ConfigMap = { apiVersion: "v1", kind: "ConfigMap", metadata: { name: ca, namespace: NAMESPACE }, data: { "ca.pem": CA } };
  const egress: V1NetworkPolicy = {
    apiVersion: "networking.k8s.io/v1",
    kind: "NetworkPolicy",
    metadata: { name: "alasio-e2e-grafana-to-telegram", namespace: NAMESPACE },
    spec: {
      podSelector: { matchLabels: selectorLabels("grafana") },
      policyTypes: ["Egress"],
      egress: [{
        to: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": STAND_INS } }, podSelector: { matchLabels: { app: TELEGRAM.name } } }],
        ports: [{ protocol: "TCP", port: TELEGRAM_TLS_PORT }],
      }],
    },
  };
  for (const object of [caMap, egress]) await kube.apply(object);
  const service = await kube.get<V1Service>(ref("Service", TELEGRAM.service, STAND_INS));
  const deployment = await kube.get<V1Deployment>(ref("Deployment", grafana, NAMESPACE));
  const pod = deployment?.spec?.template.spec;
  if (!service?.spec?.clusterIP || !pod) throw new Error("Grafana, or the Telegram stand-in's Service, is not there");
  await kube.patch(ref("Deployment", grafana, NAMESPACE), {
    spec: {
      template: {
        spec: {
          hostAliases: [{ ip: service.spec.clusterIP, hostnames: ["api.telegram.org"] }],
          volumes: [...(pod.volumes ?? []), { name: "telegram-ca", configMap: { name: ca } }],
          containers: pod.containers.map((container) => ({
            ...container,
            env: [...(container.env ?? []), { name: "SSL_CERT_FILE", value: "/etc/alasio-e2e/ca.pem" }],
            volumeMounts: [...(container.volumeMounts ?? []), { name: "telegram-ca", mountPath: "/etc/alasio-e2e", readOnly: true }],
          })),
        },
      },
    },
  });
  await kube.awaitReady([ref("Deployment", grafana, NAMESPACE)], "10 minutes");
}

/** A word of sh, quoted. */
const shellWord = (word: string): string => `'${word.replaceAll("'", `'\\''`)}'`;

/**
 * The upgrade shard's cut-over, as the box runs it: upgrades/from-<PREVIOUS>.sh, as root,
 * kubectl reaching the cluster by the run's kubeconfig, its UPGRADE this checkout's
 * `alasio upgrade` as this user, the run's install configuration of this checkout laid over
 * the configuration first (./release-config.ts), as this checkout's command line, of no
 * release, pins no images of its own, and its UPGRADE_CHECK this checkout's
 * `alasio upgrade --check` as this user, on a copy of the configuration laid over so, as the
 * configuration itself is laid over only as UPGRADE runs, and sudo asking this user for no
 * password; failing at `failAt`, a point the script names, when given. What it came to; from
 * its success on, the run runs this checkout's `alasio`.
 */
export async function cutOver({ failAt }: { readonly failAt?: string } = {}): Promise<Ran> {
  const { work, env, kubeconfig, release } = current();
  const configHome = env["XDG_CONFIG_HOME"] ?? "";
  const script = join(root, "upgrades", `from-${PREVIOUS}.sh`);
  if (!existsSync(script)) throw new Error(`no upgrades/from-${PREVIOUS}.sh says how an installation of ${PREVIOUS} upgrades to this release: write one, if only to run UPGRADE`);
  const installFile = join(work, "release-install.json");
  writeFileSync(installFile, JSON.stringify(installation("release")), { mode: 0o600 });
  const user = userInfo().username;
  // The release's command line reaches the run's Telegram stand-in as the run's own does: it registers main's bot with it.
  const owner = (configHome: string) =>
    ["sudo", "-u", user, "env", ...["HOME", "PATH", "XDG_DATA_HOME", "TELEGRAM_API_ROOT"].map((name) => `${name}=${env[name] ?? ""}`), `XDG_CONFIG_HOME=${configHome}`];
  const command = (...words: string[]) => words.map(shellWord).join(" ");
  const configured = (configHome: string) =>
    command(...owner(configHome), process.execPath, join(root, "test", "e2e", "release-config.ts"), join(configHome, "alasio", "config.json"), installFile);
  const upgrade = `${configured(configHome)} && ${command(...owner(configHome), release, "upgrade", "--timeout", "30m")}`;
  const copy = join(work, "check-config");
  const check = [
    command(...owner(configHome), "rm", "-rf", copy),
    command(...owner(configHome), "cp", "-a", configHome, copy),
    configured(copy),
    command(...owner(copy), release, "upgrade", "--check"),
    command("sudo", "-u", user, "sudo", "-n", "true"),
  ].join(" && ");
  const out = cutOverRecord();
  console.error(`# the cut-over from ${PREVIOUS}: upgrades/from-${PREVIOUS}.sh, recording in ${out}`);
  const ran = await capture(spawn("sudo", ["-n", "env", `PATH=${process.env["PATH"] ?? ""}`, `KUBECONFIG=${kubeconfig}`, `OUT=${out}`, `CONFIG=${join(configHome, "alasio", "config.json")}`, `UPGRADE=${upgrade}`, `UPGRADE_CHECK=${check}`, ...(failAt === undefined ? [] : [`FAIL_AT=${failAt}`]), "bash", script], {
    stdio: ["ignore", "pipe", "pipe"],
  }));
  if (ran.code === 0) current().bin = release;
  // What the upgrade and its check said, which the cut-over keeps in its record alone: a failure of theirs says only where to look.
  for (const name of ["upgrade-check.log", "upgrade.log"]) {
    const kept = await inCutOverRecord(`if [ -f ${name} ]; then tail -n 80 ${name}; else echo "(none)"; fi`).catch((error: unknown) => `could not be read: ${String(error)}`);
    console.error(`# the cut-over's ${name}, its last lines:\n${kept.trimEnd()}`);
  }
  return ran;
}

/** Where the upgrade shard's cut-over keeps its record (its OUT), which root owns. */
function cutOverRecord(): string {
  return join(current().work, "cut-over");
}

/** sh `script`, as root, in the record the upgrade shard's cut-over keeps, as its operator would change it: what it printed. */
export async function inCutOverRecord(script: string): Promise<string> {
  return (await sudo("sh", "-c", `cd ${shellWord(cutOverRecord())} && ${script}`)).stdout;
}

/** A line k3s, containerd, stargz-snapshotter's daemon or kubelet logs as an error: logrus's level, as text or JSON, or klog's E or F. */
export const LOGGED_ERROR = /level=(?:error|fatal)|"level":"(?:error|fatal)"|[EF]\d{4} \d{2}:/u;

/**
 * How long the dump waits for any one thing it reads before it says it is still being read and
 * goes on: a read can hang, as a stat of a mount whose FUSE session no process serves does, and a
 * dump that hangs on one says nothing of the rest, the hang itself among it.
 */
const DUMP_READ_MS = 30_000;

/** What `reading` comes to, or what it was where it fails, or, past DUMP_READ_MS, that it was still being read. */
function bounded(reading: Promise<string>): Promise<string> {
  let timer: NodeJS.Timeout | undefined;
  const late = new Promise<string>((resolve) => (timer = setTimeout(() => resolve(`still being read after ${DUMP_READ_MS / 1000} s`), DUMP_READ_MS)));
  return Promise.race([reading.catch((error: unknown) => `could not be read: ${error instanceof Error ? error.message : String(error)}`), late]).finally(() => clearTimeout(timer));
}

/** What the container `container` of `namespace`/`pod` logged, before it restarted where `previous`, as bounded reads it. */
const logsOf = (namespace: string, pod: string, container: string, options: { readonly previous?: boolean } = {}): Promise<string> =>
  bounded(kube.logs(namespace, pod, container, options));

/**
 * What each of the cluster's nodes that `which` names, or each when it names none, says of
 * itself, which the API cannot say of a node that never joined: its container's state, in
 * Docker, the errors k3s logged (its agent's kubelet's among them) and its last lines, and
 * the last lines of its containerd's log, where gVisor's shim logs too; on the machine, also
 * stargz-snapshotter's daemon's errors and last lines and its FUSE manager's, which serve the
 * layers pods read lazily.
 */
async function dumpNodes(which: ((node: string) => boolean) | undefined): Promise<string[]> {
  const said: string[] = [];
  const say = (line: string) => said.push(line);
  const lastOf = (heading: string, logged: string, errorsOnly = false) => {
    const lines = logged.trimEnd().split("\n").filter((line) => !errorsOnly || LOGGED_ERROR.test(line)).slice(errorsOnly ? -40 : -30);
    say(`${heading}${lines.length === 0 ? ": none" : ":"}`);
    for (const line of lines) say(`  ${line}`);
  };
  const containerdLog = "tail -n 30 /var/lib/rancher/k3s/agent/containerd/containerd.log";
  try {
    if (TARGET === "host") {
      if (which && !which(hostname())) return said;
      const logged = await bounded(sudo("journalctl", "--unit=k3s", "--no-pager", "--lines=2000").then(({ stdout }) => stdout));
      say(`--- this machine, the node ${hostname()}`);
      lastOf("k3s's errors", logged, true);
      lastOf("k3s's last lines", logged);
      lastOf("containerd's last lines", await bounded(sudo("sh", "-c", containerdLog).then(({ stdout }) => stdout)));
      const daemon = await bounded(sudo("journalctl", "--unit=stargz-snapshotter", "--no-pager", "--lines=2000").then(({ stdout }) => stdout));
      lastOf("stargz-snapshotter's errors", daemon, true);
      lastOf("stargz-snapshotter's last lines", daemon);
      lastOf("its FUSE manager's last lines", await bounded(sudo("tail", "-n", "30", "/var/lib/containerd-stargz-grpc/stargz-fuse-manager.log").then(({ stdout }) => stdout)));
      return said;
    }
    // What the machine's kernel killed for its memory, as a node's container may be.
    lastOf("the kernel's kills for memory", await bounded(sudo("dmesg", "--ctime").then(({ stdout: out }) => out.split("\n").filter((line) => /out of memory|oom-kill|killed process/iu.test(line)).join("\n"))));
    const stdout = await bounded(docker("ps", "--all", "--filter", `label=alasio.cluster=${CLUSTER}`, "--format", "{{.Names}}").then(({ stdout: listed }) => listed));
    for (const node of stdout.split("\n").filter(Boolean).sort()) {
      if (which && !which(node)) continue;
      const state = await bounded(docker("inspect", "--format", "{{.State.Status}}, started {{.State.StartedAt}}, exit code {{.State.ExitCode}}{{if .State.OOMKilled}}, killed for its memory{{end}}{{if .State.Error}}, {{.State.Error}}{{end}}, restarted {{.RestartCount}} times", node)
        .then(({ stdout: inspected }) => inspected.trim()));
      say(`--- node ${node}: its container ${state}`);
      // k3s logs on stderr, which docker logs says as such.
      const logged = await bounded(docker("logs", "--tail", "2000", node).then(({ stdout: out, stderr }) => `${out}${stderr}`));
      lastOf("k3s's errors", logged, true);
      lastOf("k3s's last lines", logged);
      lastOf("containerd's last lines", await bounded(docker("exec", node, "sh", "-c", containerdLog).then(({ stdout: out }) => out)));
    }
  } catch (error) {
    say(`the cluster's nodes could not be listed: ${error instanceof Error ? error.message : String(error)}`);
  }
  return said;
}

/**
 * What the cluster is doing, said on stderr, folded in GitHub's log: its nodes (with how
 * much of each one's CPU its pods request, of what it has), pods (with what each requests,
 * and each container's restarts) and latest events, why each pod that is not ready is
 * not, with its logs, and the latest logs of the pods a compute's wake goes through, its
 * proxy's, neon-control's and each compute's, ready or not: a compute's Postgres restarting
 * within its container leaves its pod ready, and says why in its log alone; the lake's and
 * its intake's, whose queries and writes wait on the catalog there unseen; alasio's, which a
 * reply that never comes says nothing of anywhere else, and the log's, whose access applier
 * says what it made of the topics; and of any
 * other ready pod a container of which restarted, as one killed for its memory is ready
 * again by the time this is said; the log's nodes', their access appliers' among them, and
 * the collector's gateway's, which telemetry and Telegram's updates go through; and the
 * warnings and errors of the object store's parts.
 * Before the API answers, or for a node not ready, what the node itself logs, as dumpNodes
 * says.
 */
export async function dumpClusterState(): Promise<void> {
  if (!setup) return;
  if (!api) {
    process.stderr.write(`${["::group::cluster state", ...await dumpNodes(undefined), "::endgroup::"].join("\n")}\n`);
    return;
  }
  // Said as it is read, so a dump cut short, as a hook that outlasts its timeout is, still says what it read.
  const say = (line: string) => void process.stderr.write(`${line}\n`);
  say("::group::cluster state");
  const ready = (conditions: readonly { readonly type: string; readonly status: string }[] | undefined) =>
    conditions?.find(({ type }) => type === "Ready")?.status === "True";
  try {
    const nodes = await kube.list<V1Node>("Node");
    const pods = await kube.list<V1Pod>("Pod");
    // What a pod asks of its node's CPU, as the scheduler counts it: its containers' requests
    // summed, or its largest init container's where that is more; none once it has ended.
    const requested = (pod: V1Pod): number => {
      if (pod.status?.phase === "Succeeded" || pod.status?.phase === "Failed") return 0;
      const of = (containers: readonly { readonly resources?: { readonly requests?: Record<string, string> } }[] | undefined) =>
        (containers ?? []).map(({ resources }) => cpuMillis(resources?.requests?.["cpu"] ?? "0"));
      return Math.max(of(pod.spec?.containers).reduce((sum, each) => sum + each, 0), ...of(pod.spec?.initContainers));
    };
    for (const node of nodes) {
      const condition = node.status?.conditions?.find(({ type }) => type === "Ready");
      const asked = pods.filter((pod) => pod.spec?.nodeName === node.metadata?.name).reduce((sum, pod) => sum + requested(pod), 0);
      const allocatable = cpuMillis(node.status?.allocatable?.["cpu"] ?? "0");
      say(`node ${node.metadata?.name}: ${ready(node.status?.conditions) ? "ready" : `not ready${condition ? ` (${condition.reason}: ${condition.message})` : ""}`}, ${cpuText(asked)} of ${cpuText(allocatable)} requested`);
    }
    const notReady = new Set(nodes.filter((node) => !ready(node.status?.conditions)).map((node) => node.metadata?.name ?? ""));
    // The machine's always, whose services serve its pods' layers and mounts as no API says.
    for (const line of await dumpNodes((node) => TARGET === "host" || notReady.has(node) || !nodes.some((each) => each.metadata?.name === node))) say(line);
    for (const pod of pods) {
      const { namespace, name } = pod.metadata ?? {};
      const restarts = (pod.status?.containerStatuses ?? []).filter(({ restartCount }) => restartCount > 0).map(({ name: container, restartCount, lastState }) =>
        `${container} restarted ${restartCount} times${lastState?.terminated ? `, last exiting with ${lastState.terminated.exitCode}${lastState.terminated.reason ? ` (${lastState.terminated.reason})` : ""}` : ""}`
      );
      say(`pod ${namespace}/${name} on ${pod.spec?.nodeName ?? "no node"}: ${pod.status?.phase}${ready(pod.status?.conditions) ? ", ready" : ""}, requesting ${cpuText(requested(pod))}${restarts.map((each) => `, ${each}`).join("")}`);
    }
    const events = [...(await kube.list<CoreV1Event>("Event"))]
      .sort((a, b) => new Date(a.lastTimestamp ?? a.eventTime ?? 0).getTime() - new Date(b.lastTimestamp ?? b.eventTime ?? 0).getTime());
    for (const { metadata, involvedObject, type, reason, message } of events.slice(-60)) {
      say(`event ${metadata.namespace}/${involvedObject.kind}/${involvedObject.name}: ${type} ${reason}: ${message?.trim()}`);
    }
    for (const pod of pods.filter((each) => each.status?.phase !== "Succeeded" && podProblems(each).length > 0)) {
      const { namespace = "", name = "" } = pod.metadata ?? {};
      say(`--- ${namespace}/${name}`);
      for (const problem of podProblems(pod)) say(problem);
      for (const event of events.filter(({ involvedObject }) => involvedObject.name === name).slice(-10)) say(`${event.type} ${event.reason}: ${event.message?.trim()}`);
      for (const container of [...(pod.spec?.initContainers ?? []), ...(pod.spec?.containers ?? [])]) {
        const logged = await logsOf(namespace, name, container.name);
        say(`logs of ${container.name}:`);
        for (const line of logged.trimEnd().split("\n").slice(-40)) say(`  ${line}`);
        // A readiness command run again, each step it takes said, and how long it took, as kubelet's says only that it failed.
        const [shell, flag, script] = container.readinessProbe?.exec?.command ?? [];
        if (shell === "sh" && flag === "-c" && script !== undefined) {
          const started = Date.now();
          const ran = await bounded(kube.exec(namespace, name, ["sh", "-xc", script], { container: container.name }).then(({ code, stdout, stderr }) => `exited with ${code}\n${stderr}${stdout}`));
          say(`its readiness command, run again, in ${Date.now() - started} ms: ${ran.trimEnd()}`);
        }
      }
    }
    // Ready or not, each node's mount of workspace storage and its agent: what the mount's juicefs said, its container
    // before it restarted too, and what each, the agent among them, which every bind reaches the mount through, sees
    // of alasio's directory of the node's: the workspaces' root's file system, asked of for 5 s at most, as a stat of a
    // FUSE session no process serves never answers, and every mount under it, with its propagation.
    const view = [
      `timeout -s KILL 5 stat -f -c "%T" ${WORKSPACES_ROOT} 2>&1 || echo "stat -f of ${WORKSPACES_ROOT} did not answer in 5 s"`,
      `grep " /var/lib/alasio" /proc/self/mountinfo || echo "no mount there"`,
    ].join("; ");
    // And, of a mount's pod, which shares the node's PIDs: the juicefs log it keeps, then each of the node's juicefs
    // processes and each process waiting uninterruptibly, as one whose request a FUSE session leaves unanswered does:
    // its state, what it waits in, its parent, and its kernel stack, each of its threads' too, as a Go process's main
    // thread idles while another waits on what holds it up, and what it holds open of FUSE and of sockets; and the
    // node's FUSE connections, with the requests each leaves unanswered.
    const processes = [
      "tail -n 80 /run/alasio/juicefs.log 2>&1",
      "for proc in /proc/[0-9]*; do",
      `  fields=$(sed 's/.*) //' "$proc/stat" 2>/dev/null) || continue`,
      `  state=$(echo "$fields" | cut -d ' ' -f 1)`,
      `  command=$(tr '\\0' ' ' < "$proc/cmdline" 2>/dev/null)`,
      // By its name too, and any of its threads' waiting uninterruptibly: a juicefs whose main
      // thread has ended, a zombie, has no command line, as one of its threads waits on yet.
      `  name=$(awk '/^Name:/ { print $2; exit }' "$proc/status" 2>/dev/null)`,
      `  waiting=$(cat "$proc"/task/*/stat 2>/dev/null | sed 's/.*) //' | cut -d ' ' -f 1 | grep -c '^D')`,
      `  case "$state:$name:$command" in D:*|*:juicefs*:*|*juicefs*) ;; *) [ "$waiting" -gt 0 ] || continue ;; esac`,
      `  echo "pid \${proc#/proc/} $state, named $name, $waiting of its threads waiting uninterruptibly, waiting in $(cat "$proc/wchan" 2>/dev/null), its parent $(echo "$fields" | cut -d ' ' -f 2): $command"`,
      `  sed 's/^/    /' "$proc/stack" 2>/dev/null`,
      `  for task in "$proc"/task/*; do`,
      `    [ "\${task##*/}" = "\${proc#/proc/}" ] && continue`,
      `    echo "    thread \${task##*/} $(sed 's/.*) //' "$task/stat" 2>/dev/null | cut -d ' ' -f 1), waiting in $(cat "$task/wchan" 2>/dev/null)"`,
      `    sed 's/^/        /' "$task/stack" 2>/dev/null`,
      "  done",
      `  for fd in "$proc"/fd/*; do target=$(readlink "$fd" 2>/dev/null) || continue; case "$target" in *fuse*|socket:*) echo "    fd \${fd##*/} $target";; esac; done`,
      "done",
      // fusectl's mount made where the node has none, its connections otherwise unseen.
      `nsenter -t 1 -m -- sh -c 'grep -qF " /sys/fs/fuse/connections " /proc/self/mountinfo || mount -t fusectl none /sys/fs/fuse/connections; for connection in /sys/fs/fuse/connections/*; do [ -e "$connection/waiting" ] && echo "FUSE connection \${connection##*/}: $(cat "$connection/waiting") waiting"; done' 2>&1`,
      // And which mount each is, by its device's minor.
      `nsenter -t 1 -m -- awk '{ for (i = 6; i < NF; i += 1) if ($i == "-") { if ($(i + 1) ~ /^fuse(\\.|$)/) { split($3, device, ":"); print "FUSE connection " device[2] " is the mount at " $5 }; break } }' /proc/self/mountinfo 2>&1`,
    ].join("\n");
    // Of a mount's juicefs that has not said it serves two minutes after it started, as one held up taking a mount
    // over does: what each of its goroutines waits on, as Go says it on SIGQUIT, which ends it, for kubelet to start
    // its mount again; said once its log has it.
    const goroutines = [
      `pid=$(cat /run/alasio/juicefs.pid 2>/dev/null) || exit 0`,
      `grep -qF 'is ready at' /run/alasio/juicefs.log && exit 0`,
      // Its age: the system's uptime less when it started, in clock ticks since boot, a hundred a second.
      `started=$(sed 's/.*) //' "/proc/$pid/stat" 2>/dev/null | cut -d ' ' -f 20) || exit 0`,
      `age=$(( $(cut -d . -f 1 /proc/uptime) - started / 100 ))`,
      `[ "$age" -ge 120 ] || exit 0`,
      `lines=$(wc -l < /run/alasio/juicefs.log)`,
      `kill -QUIT "$pid" && sleep 3`,
      `echo "juicefs $pid, mounting for $age s without serving, its goroutines as SIGQUIT ended it:"`,
      `tail -n +"$(( lines + 1 ))" /run/alasio/juicefs.log | head -n 400`,
    ].join("\n");
    for (const pod of pods.filter((each) => each.status?.phase === "Running" && ["workspace-mount", "node-agent"].includes(each.metadata?.labels?.["app.kubernetes.io/component"] ?? ""))) {
      const { namespace = "", name = "" } = pod.metadata ?? {};
      const container = pod.spec?.containers[0]?.name ?? "";
      const restarted = (pod.status?.containerStatuses?.[0]?.restartCount ?? 0) > 0;
      for (const previous of restarted ? [true, false] : [false]) {
        const logged = await logsOf(namespace, name, container, { previous });
        say(`--- logs of ${container} of ${namespace}/${name}${previous ? ", before it restarted" : ""}:`);
        for (const line of logged.trimEnd().split("\n").slice(-40)) say(`  ${line}`);
      }
      const seen = await bounded(kube.exec(namespace, name, ["sh", "-c", view], { container }).then(({ stdout, stderr }) => `${stdout}${stderr}`));
      say(`--- alasio's directory of the node's as ${namespace}/${name} sees it:\n${seen.trimEnd()}`);
      if (pod.metadata?.labels?.["app.kubernetes.io/component"] === "workspace-mount") {
        const running = await bounded(kube.exec(namespace, name, ["sh", "-c", processes], { container }).then(({ stdout, stderr }) => `${stdout}${stderr}`));
        say(`--- the juicefs log of ${namespace}/${name}, then the node's juicefs processes and those waiting uninterruptibly:\n${running.trimEnd()}`);
        const stuck = await bounded(kube.exec(namespace, name, ["sh", "-c", goroutines], { container }).then(({ stdout, stderr }) => `${stdout}${stderr}`));
        if (stuck.trim() !== "") say(`--- ${namespace}/${name}'s ${stuck.trimEnd()}`);
      }
    }
    // Each Valkey's last lines, ready or not, and before it restarted: whether it waits for the lock of its volume,
    // has loaded its file, or serves, which workspace storage's mount waits on as it starts.
    for (const pod of pods.filter((each) => each.status?.phase === "Running" && (each.spec?.containers ?? []).some(({ name: container }) => container === "valkey"))) {
      const { namespace = "", name = "" } = pod.metadata ?? {};
      const restarted = (pod.status?.containerStatuses ?? []).some(({ name: container, restartCount }) => container === "valkey" && restartCount > 0);
      for (const previous of restarted ? [true, false] : [false]) {
        const logged = await logsOf(namespace, name, "valkey", { previous });
        say(`--- logs of valkey of ${namespace}/${name}${previous ? ", before it restarted" : ""}:`);
        for (const line of logged.trimEnd().split("\n").slice(-40)) say(`  ${line}`);
      }
    }
    const waking = new Set(["neon-proxy", "neon-control", "neon-compute", "lake-loader", "lake-query", "lake-intake", "alasio", "log"]);
    const restarts = (pod: V1Pod) => (pod.status?.containerStatuses ?? []).some(({ restartCount }) => restartCount > 0);
    const told = (pod: V1Pod) => waking.has(pod.metadata?.labels?.["app.kubernetes.io/component"] ?? "") || restarts(pod);
    for (const pod of pods.filter((each) => each.status?.phase === "Running" && told(each) && podProblems(each).length === 0)) {
      const { namespace = "", name = "" } = pod.metadata ?? {};
      for (const container of pod.spec?.containers ?? []) {
        // A container restarted, what it logged as it ended too, which is why.
        const restarted = (pod.status?.containerStatuses ?? []).some(({ name: each, restartCount }) => each === container.name && restartCount > 0);
        for (const previous of restarted ? [true, false] : [false]) {
          const logged = await logsOf(namespace, name, container.name, { previous });
          say(`--- logs of ${container.name} of ${namespace}/${name}${previous ? ", before it restarted" : ""}:`);
          for (const line of logged.trimEnd().split("\n").slice(-40)) say(`  ${line}`);
        }
      }
    }
    // Ready as they stay, the log's nodes and the collector's gateway say why telemetry or
    // Telegram's updates did not go through them in their logs alone: each node's access
    // applier, whether and when it applied every principal's ACLs, credential and topics, or
    // why not yet; Kafka's warnings and errors, its authorizer's denials and the sign-ins it
    // refused; and the gateway's last lines, which say why an export failed. Each container
    // restarted, what it logged before it did too.
    const carrying = new Set([LOG_COMPONENT, COLLECTOR_COMPONENT]);
    const refused = (line: string) => / (WARN|ERROR) |is Denied|Failed authentication/u.test(line);
    for (const pod of pods.filter((each) => each.status?.phase === "Running" && carrying.has(each.metadata?.labels?.["app.kubernetes.io/component"] ?? ""))) {
      const { namespace = "", name = "" } = pod.metadata ?? {};
      for (const container of pod.spec?.containers ?? []) {
        const kafka = container.name === LOG_COMPONENT;
        const restarted = (pod.status?.containerStatuses ?? []).some(({ name: each, restartCount }) => each === container.name && restartCount > 0);
        for (const previous of restarted ? [true, false] : [false]) {
          const logged = await logsOf(namespace, name, container.name, { previous });
          say(`--- ${kafka ? "the warnings, errors and refusals" : "logs"} of ${container.name} of ${namespace}/${name}${previous ? ", before it restarted" : ""}:`);
          for (const line of logged.trimEnd().split("\n").filter((each) => !kafka || refused(each)).slice(-60)) say(`  ${line}`);
        }
      }
    }
    // Ready as they stay, the object store's parts say why a request failed in their logs
    // alone: an S3 gateway's InternalError, the filer's or a volume server's failing under it.
    // Their warnings and errors, as glog marks them, of each of their containers.
    const objectStore = new Set<string>(Object.values(OBJECT_STORE_COMPONENTS));
    for (const pod of pods.filter((each) => each.status?.phase === "Running" && objectStore.has(each.metadata?.labels?.["app.kubernetes.io/component"] ?? ""))) {
      const { namespace = "", name = "" } = pod.metadata ?? {};
      for (const container of pod.spec?.containers ?? []) {
        const logged = await logsOf(namespace, name, container.name);
        say(`--- ${namespace}/${name}: the warnings and errors of ${container.name}`);
        for (const line of logged.trimEnd().split("\n").filter((each) => /^[EW]\d{4} /u.test(each)).slice(-40)) say(`  ${line}`);
      }
    }
    // Ready as they may be, the sandbox gateway and the node agents log why a snapshot was not
    // taken, a session not restored from one, or a bound pod not reached: the gateway in its
    // scopes "snapshots" and "binder", the agents in theirs.
    for (const [component, scopes] of [[GATEWAY_COMPONENT, ["[snapshots]", "[binder]"]], ["node-agent", ["[node-agent]"]]] as const) {
      for (const pod of await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels(component)) })) {
        const name = pod.metadata?.name ?? "";
        const logged = await logsOf(NAMESPACE, name, component);
        say(`--- ${NAMESPACE}/${name}: its last lines of ${scopes.join(" and ")}`);
        for (const line of logged.trimEnd().split("\n").filter((each) => scopes.some((scope) => each.startsWith(scope))).slice(-80)) say(`  ${line}`);
      }
    }
    // The gateway and alasio log why a harness was not bound, reached, or released: each line of
    // theirs naming a harness's key, and the gateway's own, its binder's said above.
    for (const component of [GATEWAY_COMPONENT, "alasio"] as const) {
      for (const pod of await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels(component)) })) {
        const name = pod.metadata?.name ?? "";
        const logged = await logsOf(NAMESPACE, name, component);
        say(`--- ${NAMESPACE}/${name}: its last lines of harnesses`);
        // alasio's of its turns' drives, reattaches and recoveries too, which name the conversation, not its harness's key, and
        // of its timing of its start's first answer (src/telegram/wake-latency.ts).
        const told = (each: string) =>
          each.includes("harness:") || each.includes("harness's") || each.startsWith("[sandbox-gateway]") ||
          (component === "alasio" && /harness|turn-controller|was lost|attach|the start's first update/iu.test(each));
        for (const line of logged.trimEnd().split("\n").filter(told).slice(-120)) say(`  ${line}`);
      }
    }
    // And each bound harness pod's own last lines, which a pod ready as it serves shows nowhere else.
    for (const pod of pods.filter((each) => each.metadata?.annotations?.["alasio.dev/binding-key"]?.startsWith("harness:") && podProblems(each).length === 0)) {
      const { namespace = "", name = "" } = pod.metadata ?? {};
      const logged = await logsOf(namespace, name, "harness");
      say(`--- ${namespace}/${name}: the last lines of its harness host`);
      for (const line of logged.trimEnd().split("\n").slice(-40)) say(`  ${line}`);
    }
    // And each folder instance's bayma, whole, ready as it serves, and before it restarted where it did: its own account
    // of the REPL sessions it saved as its instance was let go and restored as one was made anew, which nothing else
    // says, a snapshot it does not restore said why on its stderr, and the image it runs.
    for (const pod of pods.filter((each) => each.metadata?.labels?.[POOL_LABEL] === FOLDER_POOL)) {
      const { namespace = "", name = "" } = pod.metadata ?? {};
      const restarted = (pod.status?.containerStatuses ?? []).some(({ name: container, restartCount }) => container === BAYMA_CONTAINER && restartCount > 0);
      for (const previous of restarted ? [true, false] : [false]) {
        const logged = await logsOf(namespace, name, BAYMA_CONTAINER, { previous });
        const image = pod.status?.containerStatuses?.find(({ name: container }) => container === BAYMA_CONTAINER)?.imageID ?? "an image not yet said";
        say(`--- logs of ${BAYMA_CONTAINER} of the folder instance ${namespace}/${name}, running ${image}${previous ? ", before it restarted" : ""}:`);
        for (const line of logged.trimEnd().split("\n")) say(`  ${line}`);
      }
    }
    // And each session's bayma bound in a sandbox pool's pod, its last lines, and before it restarted where it did: its own
    // account of the MCP sessions its clients held, which it closes where they answer no ping, and of its sandbox's saves.
    for (const pod of pods.filter((each) => each.metadata?.labels?.[BINDING_LABEL] !== undefined && each.metadata.labels[POOL_LABEL] !== FOLDER_POOL && (each.spec?.containers ?? []).some(({ name }) => name === BAYMA_CONTAINER))) {
      const { namespace = "", name = "" } = pod.metadata ?? {};
      const restarted = (pod.status?.containerStatuses ?? []).some(({ name: container, restartCount }) => container === BAYMA_CONTAINER && restartCount > 0);
      for (const previous of restarted ? [true, false] : [false]) {
        const logged = await logsOf(namespace, name, BAYMA_CONTAINER, { previous });
        say(`--- logs of ${BAYMA_CONTAINER} of the sandbox ${namespace}/${name}${previous ? ", before it restarted" : ""}:`);
        for (const line of logged.trimEnd().split("\n").slice(-200)) say(`  ${line}`);
      }
    }
  } catch (error) {
    say(`the cluster's state could not be read: ${error instanceof Error ? error.message : String(error)}`);
    // Its API gone, what its nodes say of themselves, their containers' state among it.
    for (const line of await dumpNodes(undefined)) say(line);
  }
  say("::endgroup::");
}

/** Removes what the run made, unless it is kept: the registry, the cluster, with all it holds, the node image, and the run's directory. */
export async function tearDown(): Promise<void> {
  for (const child of running) {
    console.error(`# ended ${child.spawnargs.join(" ")}, still running as the run ends`);
    child.kill("SIGTERM");
  }
  botApi?.close();
  botApi = undefined;
  if (!setup) return;
  const { work, configFile } = setup;
  if (KEEP) {
    const registry = PUSHED ? "" : `docker rm --force ${REGISTRY} && docker volume rm ${REGISTRY} removes the registry, and then `;
    console.error(`# kept ${THE_CLUSTER}${PUSHED ? "" : `, the registry ${REGISTRY}`} and ${work}: ${registry}${setup.bin} --config ${configFile} uninstall --purge --yes the cluster`);
    return;
  }
  // First, as Docker removes no network a container is still on.
  if (!PUSHED) {
    await docker("rm", "--force", REGISTRY).catch(() => undefined);
    await docker("volume", "rm", REGISTRY).catch(() => undefined);
  }
  const removed = await alasio("uninstall", "--purge", "--yes").catch((error: unknown) => ({ code: 1, stderr: error instanceof Error ? error.message : String(error) }));
  if (removed.code !== 0) console.error(`# ${THE_CLUSTER} may be left: alasio uninstall --purge failed: ${removed.stderr.trim()}`);
  if (TARGET === "docker") await docker("image", "rm", "--force", NODE_IMAGE).catch(() => undefined);
  try {
    rmSync(work, { recursive: true, force: true });
  } catch (error) {
    // What folder workspaces wrote in their home is their user's, which this one may not remove.
    console.error(`# left ${work}: ${error instanceof Error ? error.message : String(error)}`);
  }
}
