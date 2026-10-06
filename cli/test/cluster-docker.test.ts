import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import { NodeServices } from "@effect/platform-node";
import { KubeConfig } from "@kubernetes/client-node";
import { Effect, Layer, Logger } from "effect";

import { DockerEngine } from "../src/cluster/docker-engine.ts";
import { DockerCluster, type DockerClusterOptions, freeSubnet, subnetAddress } from "../src/cluster/docker.ts";
import type { FakeDocker } from "./support/fake-docker.ts";
import { type FakeK3s, K3S_TOKEN, serveK3sInDocker } from "./support/fake-k3s.ts";

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

/** A fake Docker with k3s in its nodes, a directory for the cluster's files, and a way to run a DockerCluster on them. */
interface Rig {
  readonly fake: FakeDocker;
  readonly k3s: FakeK3s;
  readonly kubeconfig: string;
  readonly options: DockerClusterOptions;
  readonly run: <A, E>(body: (cluster: DockerCluster["Service"]) => Effect.Effect<A, E>, options?: Partial<DockerClusterOptions>) => Promise<A>;
}

/** A cluster on a fake Docker; its network's subnet given in its settings, unless `subnet` is null. */
async function rig(t: TestContext, subnet: string | null = "172.30.9.0/24"): Promise<Rig> {
  const { fake, k3s } = await serveK3sInDocker(K3S_YAML, { pullable: (reference) => !reference.endsWith(":absent") });
  const directory = mkdtempSync(join(tmpdir(), "alasio-cluster-"));
  t.after(async () => {
    await fake.close();
    rmSync(directory, { recursive: true, force: true });
  });
  const options: DockerClusterOptions = {
    name: "dev",
    apiPort: 7443,
    storagePath: join(directory, "storage"),
    image: "ghcr.io/eaucoin/alasio-node:test",
    ...(subnet === null ? {} : { subnet, hostAliases: [{ ip: "172.30.9.250", hostnames: ["otelcol.observability"] }] }),
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
        DockerCluster.pipe(
          Effect.flatMap(body),
          Effect.provide(
            DockerCluster.layer({ ...options, ...overrides }).pipe(
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
  assert.deepEqual(agent.Env, ["K3S_URL=https://dev-server-0:6443", `K3S_TOKEN=${K3S_TOKEN}`]);
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

test("up gives a network made without a subnet the first 172.30.N.0/24 no other network has, so its nodes' addresses are fixed", async (t) => {
  const { fake, kubeconfig, run } = await rig(t, null);
  fake.networks.set("bridge", { Name: "bridge", Labels: {}, Subnet: "172.17.0.0/16" });
  fake.networks.set("other", { Name: "other", Labels: {}, Subnet: "172.30.0.0/24" });
  await run((cluster) => cluster.up(kubeconfig));
  assert.equal(fake.networks.get("dev")?.Subnet, "172.30.1.0/24");
  const created = fake.requests.filter(({ path }) => path === "/containers/create").map(({ body }) => (body as { NetworkingConfig: { EndpointsConfig: Record<string, { IPAMConfig: { IPv4Address: string } }> } }).NetworkingConfig.EndpointsConfig["dev"]?.IPAMConfig.IPv4Address);
  assert.deepEqual(created, ["172.30.1.2"]);
});

test("freeSubnet takes the first 172.30.N.0/24 that overlaps no network, and none when all do", () => {
  assert.equal(freeSubnet([]), "172.30.0.0/24");
  assert.equal(freeSubnet(["172.30.0.0/24", "172.30.1.0/25", "fd00::/64", "172.17.0.0/16"]), "172.30.2.0/24");
  assert.equal(freeSubnet(["172.30.0.0/23"]), "172.30.2.0/24");
  assert.equal(freeSubnet(["172.16.0.0/12"]), null);
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

test("up waits for nodes reported ready since they started, not before", async (t) => {
  const { k3s, kubeconfig, run } = await rig(t);
  await run((cluster) => cluster.up(kubeconfig));
  await run((cluster) => cluster.down);
  // The Ready status the server node had when it stopped, which it keeps until its kubelet reports again.
  k3s.heartbeat = new Date(Date.now() - 60_000).toISOString();
  const error = await run((cluster) => Effect.flip(cluster.up(kubeconfig)));
  assert.equal(error.message, "cluster dev was not ready within 0.3s, waiting for its nodes: not reported ready since started: dev-server-0");
  k3s.heartbeat = null;
  await run((cluster) => cluster.up(kubeconfig));
});

test("a pull that fails fails up before anything is made", async (t) => {
  const { fake, kubeconfig, run } = await rig(t);
  const error = await run((cluster) => Effect.flip(cluster.up(kubeconfig)), { image: "ghcr.io/eaucoin/alasio-node:absent" });
  assert.equal(error.message, "Docker failed POST /images/create: denied");
  assert.deepEqual(fake.changes(), ["POST /images/create"]);
});

test("remove aborts the FUSE connections of each running node's mounts, deletes the nodes and the network, and the volumes only when asked", async (t) => {
  const { fake, k3s, kubeconfig, run } = await rig(t);
  fake.volumes.set("unrelated", {});
  await run((cluster) => cluster.up(kubeconfig), { agents: 1 });
  const agent = fake.containers.get("dev-agent-0");
  assert.ok(agent);
  agent.state = "exited";
  await run((cluster) => cluster.remove(), { agents: 1 });
  assert.deepEqual(k3s.aborted, ["dev-server-0"]);
  assert.equal(fake.containers.size, 0);
  assert.equal(fake.networks.size, 0);
  assert.equal(fake.volumes.size, 9);
  await run((cluster) => cluster.remove({ volumes: true }));
  assert.deepEqual([...fake.volumes.keys()], ["unrelated"]);
});

test("remove takes other containers off the network before removing it, and leaves them be", async (t) => {
  const { fake, kubeconfig, run } = await rig(t);
  await run((cluster) => cluster.up(kubeconfig));
  fake.containers.set("collector", { body: { Labels: {} }, state: "running" });
  const network = fake.networks.get("dev");
  assert.ok(network);
  network.attached = ["collector"];
  await run((cluster) => cluster.remove());
  assert.equal(fake.networks.size, 0);
  assert.deepEqual(fake.requests.filter(({ path }) => path === "/networks/dev/disconnect").map(({ body }) => body), [{ Container: "collector", Force: true }]);
  assert.deepEqual([...fake.containers.keys()], ["collector"]);
  assert.equal(fake.containers.get("collector")?.state, "running");
});

test("remove with the storage empties it in the server node, as root, again while pods write in it, then removes it", async (t) => {
  const { fake, k3s, kubeconfig, options, run } = await rig(t);
  await run((cluster) => cluster.up(kubeconfig).pipe(Effect.andThen(cluster.down)));
  k3s.findFailures = 2;
  await run((cluster) => cluster.remove({ volumes: true, storage: true }));
  const commands = fake.requests.filter(({ path }) => path === "/containers/dev-server-0/exec").map(({ body }) => (body as { Cmd: string[] }).Cmd);
  assert.deepEqual(commands.filter(([tool]) => tool === "find"), Array.from({ length: 3 }, () => ["find", options.storagePath, "-mindepth", "1", "-delete"]));
  assert.equal(fake.containers.size, 0);
  assert.throws(() => statSync(options.storagePath), /ENOENT/u);
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

test("every node is made with the registries, as k3s's registries.yaml, and made anew when they change", async (t) => {
  const { fake, kubeconfig, run } = await rig(t);
  const registries = { mirrors: { "registry.example:5000": { endpoint: ["http://registry.example:5000"] } } };
  await run((cluster) => cluster.up(kubeconfig), { agents: 1, registries });
  const environments = () =>
    fake.requests.filter(({ path }) => path === "/containers/create").map(({ query, body }) => [query.get("name"), (body as { Env: string[] }).Env.filter((variable) => variable.startsWith("ALASIO_REGISTRIES="))]);
  assert.deepEqual(environments(), [
    ["dev-server-0", [`ALASIO_REGISTRIES={"mirrors":{"registry.example:5000":{"endpoint":["http://registry.example:5000"]}},"configs":{}}`]],
    ["dev-agent-0", [`ALASIO_REGISTRIES={"mirrors":{"registry.example:5000":{"endpoint":["http://registry.example:5000"]}},"configs":{}}`]],
  ]);

  const changes = fake.changes().length;
  await run((cluster) => cluster.up(kubeconfig), { agents: 1, registries: { ...registries, configs: { "registry.example:5000": { tls: { insecureSkipVerify: true } } } } });
  assert.deepEqual(fake.changes().slice(changes).filter((change) => !change.startsWith("POST /volumes")), [
    "POST /containers/dev-server-0/stop",
    "DELETE /containers/dev-server-0",
    "POST /containers/create",
    "POST /containers/dev-server-0/start",
    "POST /containers/dev-agent-0/stop",
    "DELETE /containers/dev-agent-0",
    "POST /containers/create",
    "POST /containers/dev-agent-0/start",
  ]);
  assert.match(environments().at(-1)?.[1]?.[0] ?? "", /"configs":\{"registry\.example:5000":\{"tls":\{"insecure_skip_verify":true\}\}\}/u);
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
