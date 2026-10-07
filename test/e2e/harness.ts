/**
 * alasio's end-to-end run, set up as an operator sets alasio up: its npm package packed
 * and installed (tooling/cli-package.ts), its images built here, a cluster on this
 * machine made by `alasio init` from the node image built here, alasio's own images
 * pushed to a registry of the run's own beside the cluster, which the cluster's
 * `registries` have its nodes pull from, the stand-ins it talks to instead of Telegram
 * and a telemetry backend applied (./stand-ins.ts), and alasio started by `alasio up`.
 * The tests then drive it through its command line (`alasio`), the stand-ins, and the
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
 */
import { type ChildProcess, execFile, spawn } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { type AddressInfo, createServer, type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Readable, Writable } from "node:stream";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { CoreV1Api, type CoreV1Event, KubeConfig, type KubernetesObject, PortForward, type V1Deployment, type V1Node, type V1Pod, type V1Secret } from "@kubernetes/client-node";
import { type Duration, Effect, Stream } from "effect";

import { kind, type KindName, KubeApi, type ListOptions, type ObjectRef } from "../../cli/src/kube/api.ts";
import { awaitReady, podProblems, selectorOf } from "../../cli/src/kube/rollout.ts";
import { NAMESPACE, RELEASE } from "../../cli/src/manifests/common.ts";
import { installCli, packCli } from "../../tooling/cli-package.ts";
import { NODE_BUILD_ARGS } from "../../tooling/node-image.ts";
import { OTLP, standInObjects, TELEGRAM, urlOf } from "./stand-ins.ts";
import { createTelegramStub } from "./telegram-stub.ts";

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
/** The user folder workspaces run as. */
export const HOST_USER = 1000;

/** The target `value` names: the cluster in Docker unless it names the host. */
export function targetOf(value: string | undefined): "host" | "docker" {
  if (!value || value === "docker") return "docker";
  if (value === "host") return "host";
  throw new Error(`ALASIO_E2E_TARGET is ${value}, not host or docker`);
}

/** The shards of the suites, each run on a cluster of its own. */
export const SHARDS = ["sessions", "neon"] as const;
export type Shard = (typeof SHARDS)[number];

/** The shard `value` names, or none, every suite, when it is unset or empty. */
export function shardOf(value: string | undefined): Shard | undefined {
  if (!value) return undefined;
  const shard = SHARDS.find((name) => name === value);
  if (!shard) throw new Error(`ALASIO_E2E_SHARD is ${value}, not one of ${SHARDS.join(", ")}`);
  return shard;
}

/**
 * The shard of the suites the run runs, ALASIO_E2E_SHARD's: `sessions`, alasio on
 * Kubernetes and workspaces on JuiceFS, which works on the sessions the other made, or
 * `neon`, alasio's Neon, which needs neither; every suite unless set. Each shard runs the
 * command line's suite, and uninstall's, as well.
 */
export const SHARD = shardOf(process.env["ALASIO_E2E_SHARD"]);

/** Whether the run runs the suites of `shard`. */
export const inShard = (shard: Shard): boolean => SHARD === undefined || SHARD === shard;

/** The images built here: their names, and what they are built from. */
const IMAGES = { "alasio": ".", "alasio-agent": "sandbox/agent", "alasio-lake": "neon/lake", "alasio-node": "cluster/node" } as const;

/** The registry the images were pushed to already, when the run builds none. */
const PUSHED = process.env["ALASIO_E2E_REGISTRY"] || undefined;

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
  /** The `alasio` installed from the package. */
  readonly bin: string;
  /** The environment alasio runs in: its config, and the cluster's storage, in the run's directory. */
  readonly env: NodeJS.ProcessEnv;
  readonly configFile: string;
  readonly kubeconfig: string;
  readonly storage: string;
  /** The home of the operator folder workspaces work as. */
  readonly home: string;
}

let setup: Setup | undefined;

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
  const ran = await alasio(...args);
  if (ran.code !== 0) throw new Error(`alasio ${args.join(" ")} exited with ${ran.code}:\n${ran.stderr}`);
  return ran.stdout;
}

/** What `child` writes and exits with; what it says on stderr, its progress, is said here too, as it says it. */
function capture(child: ChildProcess): Promise<Ran> {
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

  /** What the container has logged. */
  logs: (namespace: string, pod: string, container: string): Promise<string> =>
    Effect.runPromise(kubeApi().logs(namespace, pod, { container }).pipe(Stream.decodeText(), Stream.mkString)),

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
    if (ran.code !== 0) throw new Error(`${command.join(" ")} in ${namespace}/${pod} exited with ${ran.code}: ${ran.stderr}${ran.stdout}`);
    return ran.stdout;
  },

  /** The newest pod of the Deployment that runs ready. */
  runningPod: async (namespace: string, deployment: string): Promise<string> => {
    const workload = await kube.get<V1Deployment>(ref("Deployment", deployment, namespace));
    const pods = await kube.list<V1Pod>("Pod", { namespace, labelSelector: selectorOf(workload?.spec?.selector.matchLabels ?? {}) });
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

/** The namespace of session filesystems' Sandboxes. */
export const SESSIONS = "alasio-sessions";

/** Node run in a session's bayma container: its stdout. */
export const inSession = (volumeId: string, code: string): Promise<string> => kube.execOk(SESSIONS, volumeId, ["node", "-e", code], { container: "bayma" });

/** Node code that prints whether a TCP connection to `host` (an expression) on `port` opens. */
export const tcp = (host: string, port: number): string =>
  `const s=require("net").connect({host:${host},port:${port},timeout:3000});s.on("connect",()=>{console.log("open");process.exit()});s.on("timeout",()=>{console.log("blocked");process.exit()});s.on("error",()=>{console.log("blocked");process.exit()})`;

/** Runs `command` in alasio's container, in the pod it runs in now, which must succeed: what it printed. */
export const inAlasioContainer = async (command: readonly string[], stdin?: string): Promise<string> =>
  kube.execOk(NAMESPACE, await kube.runningPod(NAMESPACE, RELEASE), command, { container: "alasio", ...(stdin === undefined ? {} : { stdin }) });

/**
 * Runs one of test/e2e's scripts in alasio's pod, with alasio's code and ServiceAccount: its
 * last line, parsed, which the script prints in the shape `Seen` names.
 */
export async function inAlasio<Seen>(script: string, ...args: string[]): Promise<Seen> {
  // The scripts import alasio's modules by their paths in the repository, for the type
  // checker; in alasio's image those are under /opt/alasio, and a script read from stdin
  // resolves its imports from its working directory, not from where it was read. So they
  // are rewritten to the image's paths as the script is piped in.
  const source = readFileSync(new URL(script, import.meta.url), "utf8").replaceAll('from "../../src/', 'from "/opt/alasio/src/');
  const stdout = await inAlasioContainer(["sh", "-c", 'cd /opt/alasio && node --input-type=module-typescript - "$@"', "node", ...args], source);
  // split always returns at least one part.
  return JSON.parse(stdout.trim().split("\n").at(-1)!);
}

/**
 * Runs `script` with sh as root on the cluster's node `node`, which must succeed: what it
 * printed. A node in Docker is a container of this machine's Docker; on the host, this
 * machine is the node.
 */
export async function onNode(node: string, script: string): Promise<string> {
  return (TARGET === "host" ? await sudo("sh", "-c", script) : await docker("exec", node, "sh", "-c", script)).stdout;
}

/** Builds the image `name`, tagged `tag`. */
async function build(name: keyof typeof IMAGES, tag: string): Promise<void> {
  console.error(`# building ${tag} from ${IMAGES[name]}`);
  await docker("build", "--quiet", ...(name === "alasio-node" ? NODE_BUILD_ARGS : []), "--tag", tag, IMAGES[name]);
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

/** Builds the image `name` and pushes it to the registry at `pushHost`, then removes it here, with Docker's build cache. */
async function push(pushHost: string, name: keyof typeof IMAGES): Promise<void> {
  const tag = `${pushHost}/${imageOf(name)}`;
  await build(name, tag);
  console.error(`# pushing ${tag}`);
  await docker("push", "--quiet", tag);
  await docker("image", "rm", tag);
  await docker("builder", "prune", "--all", "--force");
}

/** The install configuration the run starts alasio with: its images, pulled from where they were pushed, the stand-ins, small volumes, and requests small enough that the whole of it schedules on a CI runner's two CPUs. */
function installation(): Record<string, unknown> {
  const pushed = (name: keyof typeof IMAGES) => ({ ...pulledOf(name), digest: "" });
  const on = (node: string) => (AGENTS >= 2 ? { nodeSelector: { "kubernetes.io/hostname": `${CLUSTER}-${node}` } } : {});
  return {
    images: { alasio: pushed("alasio"), agent: pushed("alasio-agent"), lake: pushed("alasio-lake"), pullPolicy: "IfNotPresent" },
    alasio: {
      env: { TELEGRAM_API_ROOT: urlOf(TELEGRAM) },
      persistence: { size: "2Gi" },
      resources: { requests: { cpu: "50m", memory: "256Mi" } },
      ...on("agent-1"),
    },
    host: { resources: { requests: { cpu: "20m", memory: "128Mi" } } },
    sessions: {
      storage: { size: "1Gi" },
      resources: { requests: { cpu: "50m", memory: "128Mi" }, limits: { cpu: "1", memory: "1Gi" } },
      ...on("agent-0"),
    },
    neon: {
      safekeepers: { storage: { size: "2Gi" }, resources: { requests: { cpu: "20m", memory: "64Mi" } } },
      pageserver: { storage: { size: "5Gi" }, resources: { requests: { cpu: "50m", memory: "128Mi" } } },
      storageController: { resources: { requests: { cpu: "10m", memory: "64Mi" } } },
      storageBroker: { resources: { requests: { cpu: "10m", memory: "32Mi" } } },
      controllerDb: { resources: { requests: { cpu: "10m", memory: "64Mi" } } },
      control: { resources: { requests: { cpu: "10m", memory: "32Mi" } } },
      compute: { resources: { requests: { cpu: "50m", memory: "256Mi" } } },
      ...on("server-0"),
    },
    telemetry: { collector: { resources: { requests: { cpu: "10m", memory: "64Mi" } } } },
    objectStore: {
      bundled: {
        storage: { size: "10Gi" },
        volumes: 30,
        // A runner's disk is mostly images; the store needs only a little of what is left.
        minFreeSpace: "1GiB",
        resources: { requests: { cpu: "20m", memory: "128Mi" } },
      },
    },
    lake: { resources: { requests: { cpu: "20m", memory: "256Mi" } } },
    // JuiceFS's metadata dumped as often as it allows, so one lands within the run.
    workspaceStorage: { backupInterval: "5m" },
    agentSandbox: { resources: { requests: { cpu: "10m", memory: "32Mi" } } },
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
  const prefix = join(work, "prefix");
  setup = { work, bin: join(prefix, "bin", "alasio"), env, configFile, kubeconfig: join(configHome, "alasio", "kubeconfig"), storage, home };

  console.error(`# packing alasio's package and installing it in ${prefix}`);
  await installCli((await packCli(work)).tarball, prefix);

  if (!PUSHED) await build("alasio-node", NODE_IMAGE);
  mkdirSync(join(configHome, "alasio"), { recursive: true, mode: 0o700 });
  const target = TARGET === "host"
    ? { host: { storagePath: storage } }
    : {
      docker: {
        name: CLUSTER,
        apiPort: await freePort(),
        storagePath: storage,
        agents: AGENTS,
        image: NODE_IMAGE,
        ...(PUSHED ? {} : { registries: { mirrors: { [REGISTRY_HOST]: { endpoint: [`http://${REGISTRY_HOST}`] } } } }),
      },
    };
  writeFileSync(configFile, JSON.stringify({ target, install: installation() }), { mode: 0o600 });
  const botToken = join(work, "bot-token");
  writeFileSync(botToken, "123:e2e", { mode: 0o600 });
  const telegram = createTelegramStub();
  await new Promise<void>((resolve) => telegram.server.listen(0, "127.0.0.1", resolve));
  try {
    // A server listening on a TCP port has an address of its own.
    const { port } = telegram.server.address() as AddressInfo;
    const folders = HOST_PROFILE ? ["--folder-workspaces", "--user", `${HOST_USER}:${HOST_USER}`, "--home", home] : ["--no-folder-workspaces"];
    const ran = await capture(
      spawn(current().bin, ["init", "--non-interactive", "--bot-token-file", botToken, "--allowed-user-ids", OPERATOR, ...folders, "--telemetry-endpoint", urlOf(OTLP), "--no-up"], {
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
    for (const name of ["alasio", "alasio-agent", "alasio-lake"] as const) await push(pushHost, name);
  }

  console.error("# applying the stand-ins");
  const standIns = standInObjects();
  for (const object of standIns) await kube.apply(object);
  await kube.awaitReady(
    standIns.filter((object) => object.kind === "Deployment").map((object) => ref("Deployment", object.metadata?.name ?? "", object.metadata?.namespace)),
    "5 minutes",
  );

  await alasioOk("up", "--timeout", "20m");
}

/** What the cluster is doing, said on stderr, folded in GitHub's log: its nodes, pods and latest events, and why each pod that is not ready is not, with its logs. */
export async function dumpClusterState(): Promise<void> {
  if (!api) return;
  const said: string[] = ["::group::cluster state"];
  const say = (line: string) => said.push(line);
  const ready = (conditions: readonly { readonly type: string; readonly status: string }[] | undefined) =>
    conditions?.find(({ type }) => type === "Ready")?.status === "True";
  try {
    for (const node of await kube.list<V1Node>("Node")) say(`node ${node.metadata?.name}: ${ready(node.status?.conditions) ? "ready" : "not ready"}`);
    const pods = await kube.list<V1Pod>("Pod");
    for (const pod of pods) {
      const { namespace, name } = pod.metadata ?? {};
      say(`pod ${namespace}/${name} on ${pod.spec?.nodeName ?? "no node"}: ${pod.status?.phase}${ready(pod.status?.conditions) ? ", ready" : ""}`);
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
        const logged = await kube.logs(namespace, name, container.name).catch((error: unknown) => (error instanceof Error ? error.message : String(error)));
        say(`logs of ${container.name}:`);
        for (const line of logged.trimEnd().split("\n").slice(-40)) say(`  ${line}`);
      }
    }
  } catch (error) {
    say(`the cluster's state could not be read: ${error instanceof Error ? error.message : String(error)}`);
  }
  say("::endgroup::");
  process.stderr.write(`${said.join("\n")}\n`);
}

/** Removes what the run made, unless it is kept: the registry, the cluster, with all it holds, the node image, and the run's directory. */
export async function tearDown(): Promise<void> {
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
