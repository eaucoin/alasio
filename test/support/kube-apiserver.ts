/**
 * A throwaway Kubernetes API server for tests, as controller-runtime's envtest runs one: the
 * kube-apiserver and etcd of an envtest release, pinned by digest, downloaded once into the
 * user's cache and started as children of the test process on free ports of the loopback,
 * with no controller, scheduler or kubelet beside them. A port is found free by probing it,
 * and is bound by its server only once that starts: another process may take it in between, as
 * the test files' processes run beside each other, so a start whose server found its port
 * taken is made again on ports found free anew (STARTS). What a test makes is stored and
 * served, with the API's own conflicts, conditioned updates and watches, and nothing acts
 * on it. A pod is made without a ServiceAccount, as nothing makes the default one. One
 * server per test process, started by the first startKubeApiServer and stopped as the test
 * file's tests end.
 */
import { type ChildProcess, execFile, spawn } from "node:child_process";
import { createHash, generateKeyPairSync, randomBytes } from "node:crypto";
import { once } from "node:events";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
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

/** How many times the servers are started, each time on ports found free anew, where one found its port taken as it started. */
const STARTS = 5;

/** What a server that found its port taken as it started says, as etcd's and kube-apiserver's listeners do. */
const PORT_TAKEN = /address already in use/u;

/** A port of the loopback no one listens on now. */
async function freePort(): Promise<number> {
  const probe = createServer();
  await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
  // A server listening on a TCP port has an address of its own.
  const { port } = probe.address() as AddressInfo;
  await new Promise((resolve) => probe.close(resolve));
  return port;
}

/** How long the release's archive may take to fetch, as a download that stalls would wait forever. */
const FETCH_TIMEOUT_MS = 120_000;

/** The directory of the release's binaries, downloaded and unpacked once, its archive checked against its digest. */
async function binaries(): Promise<string> {
  const cache = join(process.env["XDG_CACHE_HOME"] || join(homedir(), ".cache"), "alasio-envtest", RELEASE);
  if (existsSync(unpackedIn(cache))) return unpackedIn(cache);
  const response = await fetch(ARCHIVE, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
  if (!response.ok) throw new Error(`downloading ${ARCHIVE} failed: ${response.status}`);
  const archive = Buffer.from(await response.arrayBuffer());
  const digest = createHash("sha256").update(archive).digest("hex");
  if (digest !== ARCHIVE_SHA256) throw new Error(`${ARCHIVE} is not the archive pinned: its sha256 is ${digest}`);
  return unpack(cache, archive);
}

/** Where `cache` holds the release's binaries, once unpacked there. */
const unpackedIn = (cache: string): string => join(cache, "controller-tools", "envtest");

/**
 * The binaries of `archive`, an envtest release's, unpacked into `cache`: in a directory of
 * this call's own, then moved into place whole, so a test file's process that fetches the
 * release beside another, as each of a run's does with nothing in its cache, neither runs a
 * binary the other is still writing, which fails to start (ETXTBSY), nor writes over one the
 * other runs. Where another moved its own into place first, that one is kept.
 */
export async function unpack(cache: string, archive: Buffer): Promise<string> {
  mkdirSync(join(cache, "controller-tools"), { recursive: true });
  const unpacking = mkdtempSync(join(cache, ".unpacking-"));
  try {
    writeFileSync(join(unpacking, "envtest.tar.gz"), archive);
    await promisify(execFile)("tar", ["-xzf", join(unpacking, "envtest.tar.gz"), "-C", unpacking]);
    try {
      renameSync(unpackedIn(unpacking), unpackedIn(cache));
    } catch (error) {
      if (!existsSync(unpackedIn(cache))) throw error;
    }
    return unpackedIn(cache);
  } finally {
    rmSync(unpacking, { recursive: true, force: true });
  }
}

/**
 * Starts `binary` with `args`, its output kept to say why it ended, should it end before it is
 * stopped; one that could not be started ended, saying why.
 */
function run(binary: string, args: readonly string[]): { readonly child: ChildProcess; readonly output: () => string } {
  const child = spawn(binary, args, { stdio: ["ignore", "pipe", "pipe"] });
  let output = "";
  const keep = (chunk: Buffer) => void (output = (output + chunk.toString("utf8")).slice(-8192));
  child.stdout?.on("data", keep);
  child.stderr?.on("data", keep);
  child.on("error", (error) => keep(Buffer.from(`${error.message}\n`)));
  return { child, output: () => output };
}

/** How long a probe of the API server's readiness waits for its answer. */
const READY_TIMEOUT_MS = 2_000;

/** How long a server stopped is given to end on SIGTERM, before SIGKILL ends it. */
const STOP_TIMEOUT_MS = 10_000;

/** Whether `child` has ended: with a code, or by a signal, which leaves its code null. */
const ended = (child: ChildProcess): boolean => child.exitCode !== null || child.signalCode !== null;

/** Ends `child`, by SIGTERM, then SIGKILL where it has not ended within STOP_TIMEOUT_MS; at once where it has ended already. */
export async function end(child: ChildProcess): Promise<void> {
  if (ended(child)) return;
  const exited = once(child, "exit");
  child.kill("SIGTERM");
  const killing = setTimeout(() => child.kill("SIGKILL"), STOP_TIMEOUT_MS);
  try {
    await exited;
  } finally {
    clearTimeout(killing);
  }
}

/**
 * Whether the API server at `port`, whose certificate is `ca`, answers `/readyz` with 200 to
 * `token` within READY_TIMEOUT_MS. Bounded, as what holds the port may be no API server: one
 * that took it before kube-apiserver could bind it may take the connection and say nothing.
 */
export const ready = (port: number, ca: Buffer, token: string): Promise<boolean> =>
  new Promise((resolve) => {
    const asked = request({ host: "127.0.0.1", port, path: "/readyz", ca, headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(READY_TIMEOUT_MS) }, (response) => {
      response.resume();
      resolve(response.statusCode === 200);
    });
    asked.on("error", () => resolve(false));
    asked.end();
  });

/** A server that ended as it started, as it found its port taken: started again on another. */
class PortTaken extends Error {}

/**
 * An API server on ports `ports` finds free, started again on others where one of its servers
 * found its port taken as it started, STARTS times at most.
 */
export async function startApiServer(ports: () => Promise<number> = freePort, bin: () => Promise<string> = binaries): Promise<{ readonly api: TestKubeApiServer; readonly stop: () => Promise<void> }> {
  for (let attempt = 1; ; attempt += 1) {
    try {
      return await startOnce(ports, bin);
    } catch (error) {
      if (!(error instanceof PortTaken) || attempt === STARTS) throw error;
    }
  }
}

async function startOnce(ports: () => Promise<number>, binariesOf: () => Promise<string>): Promise<{ readonly api: TestKubeApiServer; readonly stop: () => Promise<void> }> {
  const bin = await binariesOf();
  const directory = mkdtempSync(join(tmpdir(), "alasio-kube-apiserver-"));
  const [etcdPort, peerPort, apiPort] = [await ports(), await ports(), await ports()];
  // Each server started, ended as the start fails, however it fails: one left running would hold the test file's process open
  // after its tests, a hang no test of it fails by.
  const servers: { readonly child: ChildProcess }[] = [];
  const stop = async () => {
    for (const { child } of servers.toReversed()) await end(child);
    rmSync(directory, { recursive: true, force: true });
  };
  try {
    const etcd = run(join(bin, "etcd"), [
      `--data-dir=${join(directory, "etcd")}`,
      `--listen-client-urls=http://127.0.0.1:${etcdPort}`,
      `--advertise-client-urls=http://127.0.0.1:${etcdPort}`,
      `--listen-peer-urls=http://127.0.0.1:${peerPort}`,
      `--initial-advertise-peer-urls=http://127.0.0.1:${peerPort}`,
      `--initial-cluster=default=http://127.0.0.1:${peerPort}`,
      "--unsafe-no-fsync",
    ]);
    servers.push(etcd);
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
    servers.push(apiServer);
    const certificate = join(certs, "apiserver.crt");
    for (const deadline = Date.now() + 60_000; ; await new Promise((resolve) => setTimeout(resolve, 250))) {
      for (const [name, { child, output }] of [["etcd", etcd], ["kube-apiserver", apiServer]] as const) {
        if (ended(child)) throw new (PORT_TAKEN.test(output()) ? PortTaken : Error)(`${name} ended (${child.exitCode ?? child.signalCode}): ${output()}`);
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
  started ??= startApiServer();
  return started.then(({ api }) => api);
}
