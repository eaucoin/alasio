import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import { NodeServices } from "@effect/platform-node";
import { KubeConfig } from "@kubernetes/client-node";
import { Effect, Layer, Logger } from "effect";

import { DockerEngine } from "../src/cluster/docker.ts";
import { LocalCluster, type LocalClusterOptions, localKubeconfig, nodeHostsWith, subnetAddress } from "../src/cluster/local.ts";
import { type FakeContainer, type FakeDocker, type FakeExecResult, serveFakeDocker } from "./support/fake-docker.ts";

/** The kubeconfig k3s writes in a server node. */
const K3S_YAML = `apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: Q0E=
    server: https://127.0.0.1:6443
  name: default
contexts:
- context:
    cluster: default
    user: default
  name: default
current-context: default
kind: Config
users:
- name: default
  user:
    client-certificate-data: Q0VSVA==
    client-key-data: S0VZ
`;

const TOKEN = "K10abc::server:secret";

/** What k3s writes in a server node that the cluster reads. */
const SERVER_FILES: Readonly<Record<string, string>> = {
  "/etc/rancher/k3s/k3s.yaml": K3S_YAML,
  "/var/lib/rancher/k3s/server/token": `${TOKEN}\n`,
};

/** k3s as the fake's nodes run it: kubectl in the server, over an API server in memory, and ctr in every node. */
class FakeK3s {
  apiReady = true;
  nodeHosts = "";
  resourceVersion = 1;
  readonly patches: unknown[] = [];
  readonly applied: unknown[] = [];
  readonly imported = new Map<string, string>();
  containers: ReadonlyMap<string, FakeContainer> = new Map();

  readonly exec = (container: string, command: readonly string[], stdin: Buffer): FakeExecResult => {
    const [tool, ...args] = command;
    if (tool === "ctr") {
      this.imported.set(container, stdin.toString("utf8"));
      return { exitCode: 0 };
    }
    if (!this.apiReady) return { exitCode: 1, stderr: "The connection to the server 127.0.0.1:6443 was refused" };
    const verb = args.find((arg) => !arg.startsWith("--"));
    if (args.includes("--raw=/readyz")) return { exitCode: 0, stdout: "ok" };
    if (verb === "get" && args.includes("nodes")) {
      // Every running node has registered and is ready.
      const items = [...this.containers].filter(([, node]) => node.state === "running").map(([name]) => ({
        metadata: { name },
        status: { conditions: [{ type: "Ready", status: "True" }] },
      }));
      return { exitCode: 0, stdout: JSON.stringify({ items }) };
    }
    if (verb === "get" && args.includes("configmap")) {
      return { exitCode: 0, stdout: JSON.stringify({ metadata: { resourceVersion: String(this.resourceVersion) }, data: { NodeHosts: this.nodeHosts } }) };
    }
    if (verb === "patch") {
      const patch = JSON.parse(args.find((arg) => arg.startsWith("--patch="))?.slice("--patch=".length) ?? "") as {
        metadata: { resourceVersion: string };
        data: { NodeHosts: string };
      };
      this.patches.push(patch);
      if (patch.metadata.resourceVersion !== String(this.resourceVersion)) return { exitCode: 1, stderr: "the object has been modified" };
      this.nodeHosts = patch.data.NodeHosts;
      this.resourceVersion += 1;
      return { exitCode: 0 };
    }
    if (verb === "apply") {
      this.applied.push(JSON.parse(stdin.toString("utf8")));
      return { exitCode: 0 };
    }
    return { exitCode: 1, stderr: `unknown command ${command.join(" ")}` };
  };
}

/** A fake Docker with k3s in its nodes, a directory for the cluster's files, and a way to run a LocalCluster on them. */
interface Rig {
  readonly fake: FakeDocker;
  readonly k3s: FakeK3s;
  readonly kubeconfig: string;
  readonly options: LocalClusterOptions;
  readonly run: <A, E>(body: (cluster: LocalCluster["Service"]) => Effect.Effect<A, E>, options?: Partial<LocalClusterOptions>) => Promise<A>;
}

async function rig(t: TestContext): Promise<Rig> {
  const k3s = new FakeK3s();
  const fake = await serveFakeDocker({
    onExec: k3s.exec,
    files: (container, path) => {
      const content = container.endsWith("-server-0") ? SERVER_FILES[path] : undefined;
      return content === undefined ? null : Buffer.from(content);
    },
    pullable: (reference) => !reference.endsWith(":absent"),
  });
  k3s.containers = fake.containers;
  const directory = mkdtempSync(join(tmpdir(), "alasio-cluster-"));
  t.after(async () => {
    await fake.close();
    rmSync(directory, { recursive: true, force: true });
  });
  const options: LocalClusterOptions = {
    name: "dev",
    apiPort: 7443,
    storagePath: join(directory, "storage"),
    image: "ghcr.io/eaucoin/alasio-node:test",
    subnet: "172.30.9.0/24",
    hostAliases: [{ ip: "172.30.9.250", hostnames: ["otelcol.observability"] }],
    readyTimeout: "300 millis",
    poll: "5 millis",
  };
  return {
    fake,
    k3s,
    kubeconfig: join(directory, "config", "kubeconfig"),
    options,
    run: (body, overrides = {}) =>
      Effect.runPromise(
        LocalCluster.pipe(
          Effect.flatMap(body),
          Effect.provide(
            LocalCluster.layer({ ...options, ...overrides }).pipe(
              Layer.provide(DockerEngine.layer({ DOCKER_HOST: fake.host })),
              Layer.provide(NodeServices.layer),
            ),
          ),
          Effect.provide(Logger.layer([])),
        ),
      ),
  };
}

/** The body of the request `METHOD path` the fake was sent last. */
const sent = (fake: FakeDocker, method: string, path: string): unknown => fake.requests.findLast((request) => request.method === method && request.path === path)?.body;

test("up makes the cluster: its image, network, volumes and server, waits for it, configures it, and writes its kubeconfig", async (t) => {
  const { fake, k3s, kubeconfig, options, run } = await rig(t);
  k3s.nodeHosts = "172.30.9.2 dev-server-0";
  await run((cluster) => cluster.up(kubeconfig));

  assert.deepEqual(fake.changes(), [
    "POST /images/create",
    "POST /networks/create",
    "POST /volumes/create",
    "POST /volumes/create",
    "POST /volumes/create",
    "POST /volumes/create",
    "POST /containers/create",
    "POST /containers/dev-server-0/start",
  ]);
  assert.equal(fake.requests.find(({ path }) => path === "/images/create")?.query.get("fromImage"), "ghcr.io/eaucoin/alasio-node:test");
  assert.deepEqual(sent(fake, "POST", "/networks/create"), {
    Name: "dev",
    Driver: "bridge",
    IPAM: { Config: [{ Subnet: "172.30.9.0/24" }] },
    Labels: { "alasio.cluster": "dev" },
  });
  assert.deepEqual([...fake.volumes.keys()], ["dev-server-0-k3s", "dev-server-0-kubelet", "dev-server-0-cni", "dev-server-0-log"]);

  const { Labels, ...server } = sent(fake, "POST", "/containers/create") as { Labels: Record<string, string> };
  assert.equal(Labels["alasio.cluster"], "dev");
  assert.equal(Labels["alasio.role"], "server");
  assert.match(Labels["alasio.spec"] ?? "", /^[0-9a-f]{64}$/u);
  assert.deepEqual(server, {
    Image: "ghcr.io/eaucoin/alasio-node:test",
    Hostname: "dev-server-0",
    Cmd: [
      "server",
      "--disable=traefik",
      "--tls-san=127.0.0.1",
      `--default-local-storage-path=${options.storagePath}`,
      "--kubelet-arg=eviction-hard=imagefs.available<5%,nodefs.available<5%",
      "--kubelet-arg=eviction-minimum-reclaim=imagefs.available=1%,nodefs.available=1%",
      "--kubelet-arg=image-gc-high-threshold=98",
      "--kubelet-arg=image-gc-low-threshold=95",
    ],
    Env: [],
    ExposedPorts: { "6443/tcp": {} },
    HostConfig: {
      Privileged: true,
      Init: true,
      CgroupnsMode: "private",
      RestartPolicy: { Name: "unless-stopped" },
      SecurityOpt: ["label=disable"],
      Tmpfs: { "/run": "", "/var/run": "" },
      Mounts: [
        { Type: "volume", Source: "dev-server-0-k3s", Target: "/var/lib/rancher/k3s" },
        { Type: "volume", Source: "dev-server-0-kubelet", Target: "/var/lib/kubelet" },
        { Type: "volume", Source: "dev-server-0-cni", Target: "/var/lib/cni" },
        { Type: "volume", Source: "dev-server-0-log", Target: "/var/log" },
        { Type: "bind", Source: options.storagePath, Target: options.storagePath },
      ],
      ExtraHosts: ["otelcol.observability:172.30.9.250"],
      PortBindings: { "6443/tcp": [{ HostIp: "127.0.0.1", HostPort: "7443" }] },
    },
    NetworkingConfig: { EndpointsConfig: { dev: { IPAMConfig: { IPv4Address: "172.30.9.2" } } } },
  });
  assert.ok(statSync(options.storagePath).isDirectory());

  assert.equal(k3s.nodeHosts, "172.30.9.2 dev-server-0\n172.30.9.250 otelcol.observability");
  assert.deepEqual(k3s.patches, [{ metadata: { resourceVersion: "1" }, data: { NodeHosts: k3s.nodeHosts } }]);
  assert.deepEqual(k3s.applied, [{ apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: "gvisor" }, handler: "runsc" }]);

  assert.equal(statSync(kubeconfig).mode & 0o777, 0o600);
  const written = new KubeConfig();
  written.loadFromString(readFileSync(kubeconfig, "utf8"));
  assert.equal(written.getCurrentContext(), "dev");
  assert.equal(written.getCurrentCluster()?.server, "https://127.0.0.1:7443");
  assert.equal(written.getCurrentCluster()?.caData, "Q0E=");
  assert.equal(written.getCurrentUser()?.keyData, "S0VZ");
});

test("up again changes nothing in Docker or CoreDNS", async (t) => {
  const { fake, k3s, kubeconfig, run } = await rig(t);
  await run((cluster) => cluster.up(kubeconfig));
  const changes = fake.changes().length;
  const patches = k3s.patches.length;
  await run((cluster) => cluster.up(kubeconfig));
  assert.deepEqual(fake.changes().slice(changes), []);
  assert.equal(k3s.patches.length, patches);
  assert.equal(k3s.applied.length, 2);
});

test("up starts a stopped cluster, and down stops it, keeping everything", async (t) => {
  const { fake, kubeconfig, run } = await rig(t);
  await run((cluster) => cluster.up(kubeconfig));
  await run((cluster) => cluster.down);
  assert.equal(fake.containers.get("dev-server-0")?.state, "exited");
  const changes = fake.changes().length;
  await run((cluster) => cluster.up(kubeconfig));
  assert.deepEqual(fake.changes().slice(changes), ["POST /containers/dev-server-0/start"]);
});

test("up makes a node made from other settings anew, keeping its volumes", async (t) => {
  const { fake, kubeconfig, run } = await rig(t);
  await run((cluster) => cluster.up(kubeconfig));
  const changes = fake.changes().length;
  await run((cluster) => cluster.up(kubeconfig), { mounts: [{ source: "/home", target: "/home" }] });
  assert.deepEqual(fake.changes().slice(changes), [
    "POST /containers/dev-server-0/stop",
    "DELETE /containers/dev-server-0",
    "POST /volumes/create",
    "POST /volumes/create",
    "POST /volumes/create",
    "POST /volumes/create",
    "POST /containers/create",
    "POST /containers/dev-server-0/start",
  ]);
  const { HostConfig } = sent(fake, "POST", "/containers/create") as { HostConfig: { Mounts: unknown[] } };
  assert.deepEqual(HostConfig.Mounts.at(-1), { Type: "bind", Source: "/home", Target: "/home", ReadOnly: false });
  assert.equal(fake.volumes.size, 4);
});

test("agents join the server with the token it made, at the addresses after it, and stop before it", async (t) => {
  const { fake, kubeconfig, run } = await rig(t);
  await run((cluster) => cluster.up(kubeconfig), { agents: 2 });
  const agents = fake.requests.filter(({ path, query }) => path === "/containers/create" && query.get("name")?.includes("agent"));
  assert.deepEqual(agents.map(({ query }) => query.get("name")), ["dev-agent-0", "dev-agent-1"]);
  const agent = agents[1]?.body as {
    Cmd: string[];
    Env: string[];
    Labels: Record<string, string>;
    ExposedPorts?: unknown;
    HostConfig: { PortBindings?: unknown };
    NetworkingConfig: unknown;
  };
  assert.equal(agent.Cmd[0], "agent");
  assert.deepEqual(agent.Env, ["K3S_URL=https://dev-server-0:6443", `K3S_TOKEN=${TOKEN}`]);
  assert.equal(agent.Labels["alasio.role"], "agent");
  assert.equal(agent.ExposedPorts, undefined);
  assert.equal(agent.HostConfig.PortBindings, undefined);
  assert.deepEqual(agent.NetworkingConfig, { EndpointsConfig: { dev: { IPAMConfig: { IPv4Address: "172.30.9.4" } } } });
  // The server is ready before its token is read and the agents are made.
  const firstAgent = fake.requests.findIndex(({ path, query }) => path === "/containers/create" && query.get("name") === "dev-agent-0");
  const token = fake.requests.findIndex(({ path }) => path === "/containers/dev-server-0/archive");
  assert.ok(token >= 0 && token < firstAgent);

  const changes = fake.changes().length;
  await run((cluster) => cluster.down, { agents: 2 });
  assert.deepEqual(fake.changes().slice(changes), [
    "POST /containers/dev-agent-1/stop",
    "POST /containers/dev-agent-0/stop",
    "POST /containers/dev-server-0/stop",
  ]);
});

test("up touches nothing of its name that is not the cluster's", async (t) => {
  const { fake, kubeconfig, run } = await rig(t);
  fake.containers.set("dev-server-0", { body: { Labels: {} }, state: "running" });
  const error = await run((cluster) => Effect.flip(cluster.up(kubeconfig)));
  assert.equal(error._tag, "ClusterUnusable");
  assert.equal(error.message, "cluster dev cannot be made: a container named dev-server-0 is not one of its nodes");
  assert.ok(!fake.changes().some((change) => change.includes("/containers/dev-server-0")));
  assert.equal(fake.containers.get("dev-server-0")?.state, "running");
});

test("up refuses a network of the cluster's with another subnet than its settings'", async (t) => {
  const { fake, kubeconfig, run } = await rig(t);
  fake.networks.set("dev", { Name: "dev", Labels: { "alasio.cluster": "dev" }, Subnet: "172.30.8.0/24" });
  const error = await run((cluster) => Effect.flip(cluster.up(kubeconfig)));
  assert.equal(error.message, "cluster dev cannot be made: network dev has the subnet 172.30.8.0/24, not 172.30.9.0/24");
});

test("up gives up on an API server that does not answer, saying why", async (t) => {
  const { k3s, kubeconfig, run } = await rig(t);
  k3s.apiReady = false;
  const error = await run((cluster) => Effect.flip(cluster.up(kubeconfig)));
  assert.equal(error._tag, "ClusterNotReady");
  assert.match(
    error.message,
    /^cluster dev was not ready within 0\.3s, waiting for its API server: kubectl get --raw=\/readyz in dev-server-0 exited with 1: The connection to the server 127\.0\.0\.1:6443 was refused$/u,
  );
});

test("a pull that fails fails up before anything is made", async (t) => {
  const { fake, kubeconfig, run } = await rig(t);
  const error = await run((cluster) => Effect.flip(cluster.up(kubeconfig)), { image: "ghcr.io/eaucoin/alasio-node:absent" });
  assert.equal(error.message, "Docker failed POST /images/create: denied");
  assert.deepEqual(fake.changes(), ["POST /images/create"]);
});

test("remove deletes the nodes and the network, and the volumes only when asked", async (t) => {
  const { fake, kubeconfig, run } = await rig(t);
  fake.volumes.set("unrelated", {});
  await run((cluster) => cluster.up(kubeconfig));
  await run((cluster) => cluster.remove());
  assert.equal(fake.containers.size, 0);
  assert.equal(fake.networks.size, 0);
  assert.equal(fake.volumes.size, 5);
  await run((cluster) => cluster.remove({ volumes: true }));
  assert.deepEqual([...fake.volumes.keys()], ["unrelated"]);
});

test("status is Docker's version and the cluster's nodes, the server first", async (t) => {
  const { kubeconfig, run } = await rig(t);
  assert.deepEqual(await run((cluster) => cluster.status), { docker: "29.0.0", nodes: [] });
  await run((cluster) => cluster.up(kubeconfig), { agents: 1 });
  await run((cluster) => cluster.down, { agents: 1 });
  assert.deepEqual(await run((cluster) => cluster.status), {
    docker: "29.0.0",
    nodes: [{ name: "dev-server-0", role: "server", state: "exited" }, { name: "dev-agent-0", role: "agent", state: "exited" }],
  });
});

test("loadImages streams Docker's export of the images into every node's containerd", async (t) => {
  const { fake, k3s, kubeconfig, run } = await rig(t);
  await run((cluster) => cluster.up(kubeconfig).pipe(Effect.andThen(cluster.loadImages(["alasio:e2e", "alasio-agent:e2e"]))), { agents: 1 });
  assert.deepEqual(Object.fromEntries(k3s.imported), {
    "dev-server-0": "archive of alasio:e2e alasio-agent:e2e",
    "dev-agent-0": "archive of alasio:e2e alasio-agent:e2e",
  });
  const imports = fake.requests.filter(({ path }) => /^\/containers\/[^/]+\/exec$/u.test(path)).map(({ body }) => (body as { Cmd: string[] }).Cmd);
  assert.deepEqual(imports.at(-1), ["ctr", "--namespace=k8s.io", "images", "import", "-"]);
});

test("subnetAddress counts from the subnet's network address, within its host addresses", () => {
  assert.equal(subnetAddress("172.31.252.0/24", 2), "172.31.252.2");
  assert.equal(subnetAddress("10.1.0.0/16", 258), "10.1.1.2");
  assert.equal(subnetAddress("10.1.2.3/16", 2), "10.1.0.2");
  assert.equal(subnetAddress("192.168.0.0/30", 2), "192.168.0.2");
  assert.equal(subnetAddress("192.168.0.0/30", 3), null);
  assert.equal(subnetAddress("192.168.0.0/24", 0), null);
  assert.equal(subnetAddress("300.1.0.0/16", 2), null);
  assert.equal(subnetAddress("fd00::/64", 2), null);
});

test("nodeHostsWith keeps k3s's lines for the nodes and replaces every other with the aliases", () => {
  const nodeHosts = "172.31.252.250 otelcol.observability\n172.31.252.1 host.k3d.internal\n172.31.252.3 dev-server-0";
  assert.equal(
    nodeHostsWith(nodeHosts, ["dev-server-0"], [{ ip: "172.31.252.251", hostnames: ["otelcol.observability", "otelcol"] }]),
    "172.31.252.3 dev-server-0\n172.31.252.251 otelcol.observability otelcol",
  );
  assert.equal(nodeHostsWith(nodeHosts, ["dev-server-0"], []), "172.31.252.3 dev-server-0");
});

test("localKubeconfig is null for what is not yet a kubeconfig with a cluster and a user", () => {
  assert.equal(localKubeconfig("", "dev", 7443), null);
  assert.equal(localKubeconfig("apiVersion: v1\nclusters: [", "dev", 7443), null);
  assert.ok(localKubeconfig(K3S_YAML, "dev", 7443)?.includes("https://127.0.0.1:7443"));
});
