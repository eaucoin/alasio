/**
 * alasio's end-to-end run, set up as an operator sets alasio up: its npm package packed
 * and installed (tooling/cli-package.ts), its images built here, a cluster on this
 * machine made by `alasio init` from the node image built here, alasio's own images
 * loaded into the cluster's nodes through the local cluster's driver, the stand-ins it
 * talks to instead of Telegram and a telemetry backend applied (./stand-ins.ts), and
 * alasio started by `alasio up`. The tests then drive it through its command line
 * (`alasio`), the stand-ins, and the cluster's API (`kube`); the run is torn down by
 * `alasio uninstall --purge`, after what the cluster was doing is said, when something
 * of the run failed.
 *
 * ALASIO_E2E_AGENTS is the number of agent nodes beside the server (0 unless set), and
 * ALASIO_E2E_KEEP=1 keeps the cluster and the run's directory afterwards. Needs Docker
 * and npm. Once loaded, the images built are removed, and Docker's build cache with
 * them, as a CI runner's disk holds the cluster's copies and little more.
 */
import { type ChildProcess, execFile, spawn } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { type AddressInfo, createServer, type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Readable, Writable } from "node:stream";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { NodeServices } from "@effect/platform-node";
import { CoreV1Api, type CoreV1Event, KubeConfig, type KubernetesObject, PortForward, type V1Deployment, type V1Node, type V1Pod, type V1Secret } from "@kubernetes/client-node";
import { type Duration, Effect, Stream } from "effect";

import { LocalCluster } from "../../cli/src/cluster/local.ts";
import { readConfig } from "../../cli/src/config.ts";
import { kind, type KindName, KubeApi, type ListOptions, type ObjectRef } from "../../cli/src/kube/api.ts";
import { awaitReady, podProblems, selectorOf } from "../../cli/src/kube/rollout.ts";
import { localCluster, resolveTarget } from "../../cli/src/target.ts";
import { installCli, packCli } from "../../tooling/cli-package.ts";
import { OTLP, standInObjects, TELEGRAM, urlOf } from "./stand-ins.ts";
import { createTelegramStub } from "./telegram-stub.ts";

const run = promisify(execFile);
const root = fileURLToPath(new URL("../..", import.meta.url));

/** Agent nodes beside the server. */
export const AGENTS = Number(process.env["ALASIO_E2E_AGENTS"] ?? 0);
/** Whether the cluster, and the run's directory, are kept afterwards. */
export const KEEP = process.env["ALASIO_E2E_KEEP"] === "1";
/** The cluster: one of its own for each number of agents, so runs of each can be kept side by side. */
export const CLUSTER = `alasio-e2e-${AGENTS}`;
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

/** The images built here: their names, and what they are built from. */
const IMAGES = { "alasio": ".", "alasio-agent": "sandbox/agent", "alasio-lake": "neon/lake", "alasio-node": "cluster/node" } as const;
const imageOf = (name: keyof typeof IMAGES): string => `alasio-e2e/${name}:e2e`;

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

/** Builds the image `name`, tagged imageOf(name). */
async function build(name: keyof typeof IMAGES): Promise<void> {
  console.error(`# building ${imageOf(name)} from ${IMAGES[name]}`);
  await docker("build", "--quiet", "--tag", imageOf(name), IMAGES[name]);
}

/** Loads the image `name` into every node of the cluster, through the local cluster's driver, then removes it here, with Docker's build cache. */
async function load(configFile: string, name: keyof typeof IMAGES): Promise<void> {
  await Effect.runPromise(
    Effect.gen(function*() {
      const config = yield* readConfig(configFile);
      if (!config) return yield* Effect.die(new Error(`there is no config at ${configFile}`));
      const target = yield* resolveTarget(config);
      if (target._tag !== "Local") return yield* Effect.die(new Error("the run's cluster is not one on this machine"));
      yield* Effect.flatMap(LocalCluster, (cluster) => cluster.loadImages([imageOf(name)])).pipe(Effect.provide(localCluster(target.cluster)));
    }).pipe(Effect.provide(NodeServices.layer)),
  );
  await docker("image", "rm", imageOf(name));
  await docker("builder", "prune", "--all", "--force");
}

/** The install configuration the run starts alasio with: its images built here, the stand-ins, small volumes, and requests small enough that the whole of it schedules on a CI runner's two CPUs. */
function installation(): Record<string, unknown> {
  const local = (name: keyof typeof IMAGES) => ({ repository: `alasio-e2e/${name}`, tag: "e2e", digest: "" });
  const on = (node: string) => (AGENTS >= 2 ? { nodeSelector: { "kubernetes.io/hostname": `${CLUSTER}-${node}` } } : {});
  return {
    images: { alasio: local("alasio"), agent: local("alasio-agent"), lake: local("alasio-lake"), pullPolicy: "Never" },
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
      collector: { resources: { requests: { cpu: "10m", memory: "64Mi" } } },
      ...on("server-0"),
    },
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

  const prefix = join(work, "prefix");
  setup = { work, bin: join(prefix, "bin", "alasio"), env, configFile, kubeconfig: join(configHome, "alasio", "kubeconfig"), storage, home };

  console.error(`# packing alasio's package and installing it in ${prefix}`);
  await installCli((await packCli(work)).tarball, prefix);

  await build("alasio-node");
  mkdirSync(join(configHome, "alasio"), { recursive: true, mode: 0o700 });
  writeFileSync(
    configFile,
    JSON.stringify({ target: { local: { name: CLUSTER, apiPort: await freePort(), storagePath: storage, agents: AGENTS, image: imageOf("alasio-node") } }, install: installation() }),
    { mode: 0o600 },
  );
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

  for (const name of ["alasio", "alasio-agent", "alasio-lake"] as const) {
    await build(name);
    await load(configFile, name);
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

/** Removes what the run made, unless it is kept: the cluster, with all it holds, the node image, and the run's directory. */
export async function tearDown(): Promise<void> {
  if (!setup) return;
  const { work, configFile } = setup;
  if (KEEP) {
    console.error(`# kept the cluster ${CLUSTER} and ${work}: ${setup.bin} --config ${configFile} uninstall --purge --yes removes the cluster`);
    return;
  }
  const removed = await alasio("uninstall", "--purge", "--yes").catch((error: unknown) => ({ code: 1, stderr: error instanceof Error ? error.message : String(error) }));
  if (removed.code !== 0) console.error(`# the cluster ${CLUSTER} may be left: alasio uninstall --purge failed: ${removed.stderr.trim()}`);
  await docker("image", "rm", "--force", imageOf("alasio-node")).catch(() => undefined);
  try {
    rmSync(work, { recursive: true, force: true });
  } catch (error) {
    // What folder workspaces wrote in their home is their user's, which this one may not remove.
    console.error(`# left ${work}: ${error instanceof Error ? error.message : String(error)}`);
  }
}
