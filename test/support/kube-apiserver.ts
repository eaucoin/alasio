/**
 * A throwaway Kubernetes API server for tests, as controller-runtime's envtest runs one: the
 * kube-apiserver and etcd of an envtest release, pinned by digest, downloaded once into the
 * user's cache and started as children of the test process on free ports of the loopback,
 * with no controller, scheduler or kubelet beside them. What a test makes is stored and
 * served, with the API's own conflicts, conditioned updates and watches, and nothing acts
 * on it. A pod is made without a ServiceAccount, as nothing makes the default one. One
 * server per test process, started by the first startKubeApiServer and stopped as the test
 * file's tests end.
 */
import { type ChildProcess, execFile, spawn } from "node:child_process";
import { createHash, generateKeyPairSync, randomBytes } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { request } from "node:https";
import { type AddressInfo, createServer } from "node:net";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { after } from "node:test";
import { promisify } from "node:util";

/** The envtest release, Kubernetes' version of the k3s alasio pins (cluster/node/pins.json), and its archive's digest. */
const RELEASE = "v1.37.0";
const ARCHIVE = `https://github.com/kubernetes-sigs/controller-tools/releases/download/envtest-${RELEASE}/envtest-${RELEASE}-linux-amd64.tar.gz`;
const ARCHIVE_SHA256 = "f03faedfc68d2a78cee5cb02618bb44e7f67044f4c5fc957f72d3307fda61a9c";

/** A running API server: its address, a kubeconfig of its administrator, and that administrator's token; and the last it wrote, for a test to say what it did. */
export interface TestKubeApiServer {
  readonly server: string;
  readonly kubeconfig: string;
  readonly token: string;
  readonly output: () => string;
}

let started: Promise<{ readonly api: TestKubeApiServer; readonly stop: () => Promise<void> }> | undefined;

after(async () => {
  const running = await started?.catch(() => null);
  started = undefined;
  await running?.stop();
});

/** A port of the loopback no one listens on now. */
async function freePort(): Promise<number> {
  const probe = createServer();
  await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
  // A server listening on a TCP port has an address of its own.
  const { port } = probe.address() as AddressInfo;
  await new Promise((resolve) => probe.close(resolve));
  return port;
}

/** The directory of the release's binaries, downloaded and unpacked once, its archive checked against its digest. */
async function binaries(): Promise<string> {
  const cache = join(process.env["XDG_CACHE_HOME"] || join(homedir(), ".cache"), "alasio-envtest", RELEASE);
  const directory = join(cache, "controller-tools", "envtest");
  if (existsSync(join(directory, "kube-apiserver"))) return directory;
  mkdirSync(cache, { recursive: true });
  const response = await fetch(ARCHIVE);
  if (!response.ok) throw new Error(`downloading ${ARCHIVE} failed: ${response.status}`);
  const archive = Buffer.from(await response.arrayBuffer());
  const digest = createHash("sha256").update(archive).digest("hex");
  if (digest !== ARCHIVE_SHA256) throw new Error(`${ARCHIVE} is not the archive pinned: its sha256 is ${digest}`);
  const file = join(cache, "envtest.tar.gz");
  writeFileSync(file, archive);
  await promisify(execFile)("tar", ["-xzf", file, "-C", cache]);
  rmSync(file);
  return directory;
}

/** Starts `binary` with `args`, its output kept to say why it ended, should it end before it is stopped. */
function run(binary: string, args: readonly string[]): { readonly child: ChildProcess; readonly output: () => string } {
  const child = spawn(binary, args, { stdio: ["ignore", "pipe", "pipe"] });
  let output = "";
  const keep = (chunk: Buffer) => void (output = (output + chunk.toString("utf8")).slice(-8192));
  child.stdout?.on("data", keep);
  child.stderr?.on("data", keep);
  return { child, output: () => output };
}

/** Whether the API server at `port`, whose certificate is `ca`, answers `/readyz` with 200 to `token`. */
const ready = (port: number, ca: Buffer, token: string): Promise<boolean> =>
  new Promise((resolve) => {
    const asked = request({ host: "127.0.0.1", port, path: "/readyz", ca, headers: { authorization: `Bearer ${token}` } }, (response) => {
      response.resume();
      resolve(response.statusCode === 200);
    });
    asked.on("error", () => resolve(false));
    asked.end();
  });

async function start(): Promise<{ readonly api: TestKubeApiServer; readonly stop: () => Promise<void> }> {
  const bin = await binaries();
  const directory = mkdtempSync(join(tmpdir(), "alasio-kube-apiserver-"));
  const [etcdPort, peerPort, apiPort] = [await freePort(), await freePort(), await freePort()];
  const etcd = run(join(bin, "etcd"), [
    `--data-dir=${join(directory, "etcd")}`,
    `--listen-client-urls=http://127.0.0.1:${etcdPort}`,
    `--advertise-client-urls=http://127.0.0.1:${etcdPort}`,
    `--listen-peer-urls=http://127.0.0.1:${peerPort}`,
    `--initial-advertise-peer-urls=http://127.0.0.1:${peerPort}`,
    `--initial-cluster=default=http://127.0.0.1:${peerPort}`,
    "--unsafe-no-fsync",
  ]);
  const token = randomBytes(24).toString("hex");
  writeFileSync(join(directory, "tokens.csv"), `${token},admin,admin,system:masters\n`);
  const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  writeFileSync(join(directory, "sa.key"), privateKey.export({ type: "pkcs8", format: "pem" }));
  writeFileSync(join(directory, "sa.pub"), publicKey.export({ type: "spki", format: "pem" }));
  const certs = join(directory, "certs");
  const apiServer = run(join(bin, "kube-apiserver"), [
    `--etcd-servers=http://127.0.0.1:${etcdPort}`,
    "--bind-address=127.0.0.1",
    "--advertise-address=127.0.0.1",
    // The loopback, which no Endpoints of the kubernetes Service may name: none is kept.
    "--endpoint-reconciler-type=none",
    `--secure-port=${apiPort}`,
    `--cert-dir=${certs}`,
    `--token-auth-file=${join(directory, "tokens.csv")}`,
    "--authorization-mode=RBAC",
    "--service-cluster-ip-range=10.0.0.0/24",
    "--service-account-issuer=https://kubernetes.default.svc",
    `--service-account-key-file=${join(directory, "sa.pub")}`,
    `--service-account-signing-key-file=${join(directory, "sa.key")}`,
    // Nothing makes a namespace's default ServiceAccount, which its admission would have each pod name.
    "--disable-admission-plugins=ServiceAccount",
    "--allow-privileged=true",
  ]);
  const stop = async () => {
    for (const { child } of [apiServer, etcd]) {
      if (child.exitCode !== null) continue;
      const exited = new Promise((resolve) => child.once("exit", resolve));
      child.kill("SIGTERM");
      await exited;
    }
    rmSync(directory, { recursive: true, force: true });
  };
  try {
    const certificate = join(certs, "apiserver.crt");
    for (const deadline = Date.now() + 60_000; ; await new Promise((resolve) => setTimeout(resolve, 250))) {
      for (const [name, { child, output }] of [["etcd", etcd], ["kube-apiserver", apiServer]] as const) {
        if (child.exitCode !== null) throw new Error(`${name} ended (${child.exitCode}): ${output()}`);
      }
      if (existsSync(certificate) && (await ready(apiPort, readFileSync(certificate), token))) break;
      if (Date.now() > deadline) throw new Error(`kube-apiserver was not ready within a minute: ${apiServer.output()}`);
    }
    const server = `https://127.0.0.1:${apiPort}`;
    const kubeconfig = join(directory, "kubeconfig");
    writeFileSync(kubeconfig, JSON.stringify({
      apiVersion: "v1",
      kind: "Config",
      clusters: [{ name: "test", cluster: { server, "certificate-authority-data": readFileSync(certificate).toString("base64") } }],
      users: [{ name: "admin", user: { token } }],
      contexts: [{ name: "test", context: { cluster: "test", user: "admin" } }],
      "current-context": "test",
    }));
    return { api: { server, kubeconfig, token, output: apiServer.output }, stop };
  } catch (error) {
    await stop();
    throw error;
  }
}

/** The test process's API server, started at the first call. */
export function startKubeApiServer(): Promise<TestKubeApiServer> {
  started ??= start();
  return started.then(({ api }) => api);
}
