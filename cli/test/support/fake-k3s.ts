/**
 * k3s in the nodes of a fake Docker (./fake-docker.ts): kubectl in the server node, over
 * an API server in memory (nodes, CoreDNS's ConfigMap, and what is applied), find in
 * every node, and the files k3s writes in the server node, its kubeconfig and token.
 */
import { type FakeContainer, type FakeDocker, type FakeDockerOptions, type FakeExecResult, serveFakeDocker } from "./fake-docker.ts";

/** The token the server makes, which agents join with. */
export const K3S_TOKEN = "K10abc::server:secret";

/** k3s as the fake's nodes run it: kubectl in the server, over an API server in memory, and find in every node. */
export class FakeK3s {
  apiReady = true;
  /** When the nodes' kubelets last reported them ready; now, unless a test says. */
  heartbeat: string | null = null;
  nodeHosts = "";
  resourceVersion = 1;
  readonly patches: unknown[] = [];
  readonly applied: unknown[] = [];
  containers: ReadonlyMap<string, FakeContainer> = new Map();

  readonly exec = (_container: string, command: readonly string[], stdin: Buffer): FakeExecResult => {
    const [tool, ...args] = command;
    if (tool === "find") return { exitCode: 0 };
    if (!this.apiReady) return { exitCode: 1, stderr: "The connection to the server 127.0.0.1:6443 was refused" };
    const verb = args.find((arg) => !arg.startsWith("--"));
    if (args.includes("--raw=/readyz")) return { exitCode: 0, stdout: "ok" };
    if (verb === "get" && args.includes("nodes")) {
      // Every running node has registered and is ready.
      const items = [...this.containers].filter(([, node]) => node.state === "running").map(([name]) => ({
        metadata: { name },
        status: { conditions: [{ type: "Ready", status: "True", lastHeartbeatTime: this.heartbeat ?? new Date().toISOString() }] },
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

/** A fake Docker whose nodes run k3s, whose server node holds `k3sYaml` as its kubeconfig. */
export async function serveK3sInDocker(k3sYaml: string, options: Pick<FakeDockerOptions, "pullable"> = {}): Promise<{ readonly fake: FakeDocker; readonly k3s: FakeK3s }> {
  const files: Readonly<Record<string, string>> = {
    "/etc/rancher/k3s/k3s.yaml": k3sYaml,
    "/var/lib/rancher/k3s/server/token": `${K3S_TOKEN}\n`,
  };
  const k3s = new FakeK3s();
  const fake = await serveFakeDocker({
    ...options,
    onExec: k3s.exec,
    files: (container, path) => {
      const content = container.endsWith("-server-0") ? files[path] : undefined;
      return content === undefined ? null : Buffer.from(content);
    },
  });
  k3s.containers = fake.containers;
  return { fake, k3s };
}
