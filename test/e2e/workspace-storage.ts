/**
/**
 * Workspaces on JuiceFS as alasio installs it: a session's workspace a directory of the
 * file system, made by JuiceFS's CSI driver, doing in gVisor what tools ask of a file
 * system within the claim's size; its files kept through the session suspended, Valkey
 * and the object store killed mid-write, its mount pod deleted and its mount lost, and,
 * on several nodes, its moving to another; neither Valkey nor the object store, nor their
 * credentials, within a session's reach; a volume deleted with its claim, its directory
 * with it; and the file system's metadata dumped, backed up nightly, and restored from
 * the backup with the same files.
 *
 * The end-to-end run's workspaces shard (test/e2e/alasio.test.ts), registered with
 * workspaceStorage(): it makes its sessions as alasio does, one with no internet, which
 * it works on, and one with internet.
 */
import assert from "node:assert/strict";
import { before, describe, test } from "node:test";

import type {
  V1CronJob,
  V1Deployment,
  V1Job,
  V1PersistentVolume,
  V1PersistentVolumeClaim,
  V1Pod,
  V1Service,
  V1StorageClass,
} from "@kubernetes/client-node";

import { selectorOf } from "../../cli/src/kube/rollout.ts";
import { componentName, NAMESPACE, neonName } from "../../cli/src/manifests/common.ts";
import { CSI_DRIVER, JUICEFS_PODS_SELECTOR, MOUNT_POD_LABELS, NODE_POD_LABELS } from "../../cli/src/manifests/juicefs-csi.ts";
import { VALKEY } from "../../cli/src/manifests/valkey.ts";
import { JUICEFS_ADMIN, WORKSPACES_CREDENTIALS } from "../../cli/src/manifests/workspace-storage.ts";
import type { NetMode } from "../../src/sandbox/index.ts";
import { AGENTS, inAlasio, inSession, kube, onNode, type Ran, ref, SESSIONS, tcp } from "./harness.ts";
import type { BroughtUp } from "./session-bring-up.ts";
import type { Made } from "./session-volumes.ts";

/** Where the installation's JuiceFS driver runs, and makes its mount pods: its default namespace. */
const DRIVER_NAMESPACE = "kube-system";
const SEAWEEDFS = componentName("seaweedfs");

/** Where the suite works in a session's workspace, and the files it writes first and expects everywhere after. */
const WORKDIR = "/workspace/juicefs-e2e";
const FIXTURE = `${WORKDIR}/fixture`;

/** The digest of the files under the current directory, which two copies of it share only when they hold the same. */
const DIGEST = "find . -type f | LC_ALL=C sort | xargs sha256sum | sha256sum | cut -c1-16";

/** What alasio looks at to tell a session's workspace mount is broken (src/sandbox/index.ts). */
const MOUNT_CHECK = "env LC_ALL=C stat --file-system --format=%T /workspace";

/**
 * The file operations workspaces' tools rely on, in the session's workspace, as its agent,
 * each saying `PASS name` or `FAIL name: why`. Writes a 100 MiB file and deletes it.
 */
const FILE_OPERATIONS = String.raw`
set -u
W=${WORKDIR}/operations; rm -rf "$W"; mkdir -p "$W"; cd "$W" || exit 1
check() { name=$1; shift; if "$@" >/tmp/out 2>&1; then echo "PASS $name"; else echo "FAIL $name: $(tail -3 /tmp/out | tr '\n' ' ')"; fi; }
t_fsync() { dd if=/dev/urandom of=synced bs=64k count=4 conv=fsync 2>/dev/null && [ "$(stat -c %s synced)" = 262144 ]; }
t_rename() { echo a > r1 && mv r1 r2 && [ ! -e r1 ] && [ "$(cat r2)" = a ] && echo b > r3 && mv -f r3 r2 && [ "$(cat r2)" = b ]; }
t_hardlink() { echo h > h1 && ln h1 h2 && [ "$(stat -c %h h1)" = 2 ] && [ "$(stat -c %i h1)" = "$(stat -c %i h2)" ] && echo more >> h2 && cmp h1 h2; }
t_symlink() { echo s > s1 && ln -s s1 s2 && [ "$(readlink s2)" = s1 ] && cmp s1 s2; }
t_chmod() { echo c > c1 && chmod 600 c1 && [ "$(stat -c %a c1)" = 600 ] && chmod 755 c1 && [ "$(stat -c %a c1)" = 755 ]; }
t_flock() { (flock -x 9 && sleep 3) 9>lock & sleep 1; if flock -n -x lock true; then wait; return 1; fi; wait && flock -n -x lock true; }
t_mmap() {
  cat > /tmp/mmap.c <<'EOF'
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
int main(int argc, char **argv) {
  int fd = open(argv[1], O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (argc < 2 || fd < 0 || ftruncate(fd, 65536) != 0) return 1;
  char *mapped = mmap(0, 65536, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (mapped == MAP_FAILED) return 1;
  memcpy(mapped, "hello", 5);
  memcpy(mapped + 60000, "world", 5);
  return msync(mapped, 65536, MS_SYNC) != 0 || munmap(mapped, 65536) != 0 || close(fd) != 0;
}
EOF
  gcc -o /tmp/mmap /tmp/mmap.c && /tmp/mmap mapped && [ "$(head -c 5 mapped)" = hello ] && [ "$(tail -c +60001 mapped | head -c 5)" = world ]
}
t_large() { dd if=/dev/urandom of=large bs=1M count=100 2>/dev/null && sync large && a=$(sha256sum < large) && [ "$(stat -c %s large)" = 104857600 ] && b=$(cat large | sha256sum) && [ "$a" = "$b" ] && rm large && [ ! -e large ]; }
t_small() { mkdir small && i=0 && while [ $i -lt 1000 ]; do echo $i > small/$i || return 1; i=$((i + 1)); done && [ "$(ls small | wc -l)" = 1000 ] && [ "$(cat small/999)" = 999 ]; }
t_git() {
  git init -q repo && cd repo && echo one > f && git add f && git -c user.name=e2e -c user.email=e2e@example.invalid commit -qm one \
    && echo two >> f && git -c user.name=e2e -c user.email=e2e@example.invalid commit -qam two && git fsck --strict && [ "$(git rev-list --count HEAD)" = 2 ]
  r=$?; cd ..; return $r
}
check fsync t_fsync
check rename t_rename
check hardlink t_hardlink
check symlink t_symlink
check chmod t_chmod
check flock t_flock
check mmap t_mmap
check large-file t_large
check small-files t_small
check git t_git
`;

/**
 * Node, in a session: writes files in the directory argv[1], each fsynced, for argv[2]
 * milliseconds, printing the number of each once its fsync has returned; one whose write
 * fails is said on stderr, and the next is tried half a second later.
 */
const WRITER = `const fs = require("node:fs");
const [dir, ms] = process.argv.slice(1);
fs.mkdirSync(dir, { recursive: true });
const end = Date.now() + Number(ms);
for (let i = 0; Date.now() < end; i++) {
  try {
    const fd = fs.openSync(dir + "/" + i, "w");
    fs.writeSync(fd, (i + "\\n").repeat(4096));
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    console.log(i);
  } catch (error) {
    console.error(String(error));
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 500);
  }
}`;

/** Node, in a session: the names of the files in the directory argv[1] that hold what WRITER wrote, as JSON. */
const WRITTEN = `const fs = require("node:fs");
const dir = process.argv[1];
console.log(JSON.stringify(fs.readdirSync(dir).filter((name) => fs.readFileSync(dir + "/" + name, "utf8") === (name + "\\n").repeat(4096))));`;

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Asks `check` every two seconds until it answers something, for up to `timeoutMs`: what
 * it answered. Should it not, `explain` says what there was instead.
 */
async function until<T>(what: string, check: () => Promise<T | undefined>, timeoutMs = 300_000, explain = async () => ""): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const answer = await check();
    if (answer !== undefined) return answer;
    if (Date.now() >= deadline) assert.fail(`${what} did not happen within ${timeoutMs / 1000}s${await explain()}`);
    await sleep(2000);
  }
}

/** sh run in the session's bayma container, as its agent. */
const inWorkspace = (volumeId: string, script: string): Promise<Ran> => kube.exec(SESSIONS, volumeId, ["sh", "-c", script], { container: "bayma" });
const inWorkspaceOk = (volumeId: string, script: string): Promise<string> => kube.execOk(SESSIONS, volumeId, ["sh", "-c", script], { container: "bayma" });

/**
 * What MOUNT_CHECK comes to in the session, given ten seconds, and a minute in all: a
 * mount whose client is gone but whose connection is held may answer nothing at all.
 */
async function checkMount(volumeId: string): Promise<Ran> {
  const hung: Ran = { code: 124, stdout: "", stderr: "the look at the workspace's mount did not end" };
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      inWorkspace(volumeId, `timeout -k 5 10 ${MOUNT_CHECK}`),
      new Promise<Ran>((resolve) => {
        timer = setTimeout(() => resolve(hung), 60_000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

/** The digest of the fixture, as the session sees it. */
const fixtureDigest = async (volumeId: string) => (await inWorkspaceOk(volumeId, `cd ${FIXTURE} && timeout -k 5 120 sh -c '${DIGEST}'`)).trim();

/** Brings the session up as a turn does (./session-bring-up.ts): the pod it runs in then. */
const bringUp = (volumeId: string, ...where: string[]) => inAlasio<BroughtUp>("./session-bring-up.ts", volumeId, ...where);

const isReady = (pod: V1Pod | null | undefined) => pod?.status?.conditions?.some(({ type, status }) => type === "Ready" && status === "True") ?? false;

/** Makes the session `volumeId` with `netMode` as alasio does (./session-volumes.ts). */
const create = (volumeId: string, netMode: NetMode) => inAlasio<Made>("./session-volumes.ts", "create", volumeId, netMode);

/** The session's pod, which runs. */
async function podOf(volumeId: string): Promise<V1Pod> {
  const pod = await kube.get<V1Pod>(ref("Pod", volumeId, SESSIONS));
  assert.ok(pod?.spec?.nodeName, `the session ${volumeId} runs in no pod`);
  return pod;
}

/** The session's claim, and its volume. */
async function volumeOf(volumeId: string): Promise<{ claim: V1PersistentVolumeClaim; volume: V1PersistentVolume }> {
  const claimName = (await podOf(volumeId)).spec?.volumes?.find((volume) => volume.persistentVolumeClaim)?.persistentVolumeClaim?.claimName;
  assert.ok(claimName, `the session ${volumeId} has no claim`);
  const claim = await kube.get<V1PersistentVolumeClaim>(ref("PersistentVolumeClaim", claimName, SESSIONS));
  assert.ok(claim?.spec?.volumeName, `the claim ${claimName} is bound to no volume`);
  const volume = await kube.get<V1PersistentVolume>(ref("PersistentVolume", claim.spec.volumeName));
  assert.ok(volume);
  return { claim, volume };
}

/** The directory of the file system that is the session's volume, named by the StorageClass's pathPattern. */
const directoryOf = async (volumeId: string) => `${SESSIONS}-${(await volumeOf(volumeId)).claim.metadata?.name}`;

/** The mount pod that serves the session's volume on its node, unless it is being deleted. */
async function mountPodOf(volumeId: string): Promise<V1Pod | undefined> {
  const node = (await podOf(volumeId)).spec?.nodeName;
  const volume = (await volumeOf(volumeId)).volume.metadata?.name ?? "";
  const pods = await kube.list<V1Pod>("Pod", { namespace: DRIVER_NAMESPACE, labelSelector: selectorOf(MOUNT_POD_LABELS) });
  // A mount pod mounts the file system at a path named after the volume.
  return pods.find((pod) => pod.spec?.nodeName === node && !pod.metadata?.deletionTimestamp && JSON.stringify(pod.spec).includes(volume));
}

/** The driver's mount pods, and what its node service on `node` last logged. */
async function driverState(node: string): Promise<string> {
  const pods = await kube.list<V1Pod>("Pod", { namespace: DRIVER_NAMESPACE, labelSelector: JUICEFS_PODS_SELECTOR });
  const mountPods = pods.filter(({ metadata }) => metadata?.labels?.["app.kubernetes.io/name"] === MOUNT_POD_LABELS["app.kubernetes.io/name"]);
  const plugin = pods.find(({ metadata, spec }) => metadata?.labels?.["app"] === NODE_POD_LABELS["app"] && spec?.nodeName === node)?.metadata?.name ?? "";
  const logged = await kube.logs(DRIVER_NAMESPACE, plugin, "juicefs-plugin").catch((error: unknown) => (error instanceof Error ? error.message : String(error)));
  return [
    ...mountPods.map((pod) =>
      `\nmount pod ${pod.metadata?.name} (${pod.metadata?.uid}) on ${pod.spec?.nodeName}: ${pod.status?.phase}${isReady(pod) ? ", ready" : ""}${pod.metadata?.deletionTimestamp ? ", being deleted" : ""}`
    ),
    `\nthe node service ${plugin} last logged:`,
    ...logged.trimEnd().split("\n").slice(-40).map((line) => `\n  ${line}`),
  ].join("");
}

/** What the session's workspace is in the session, its volume's mounts on `node`, and the driver's state there. */
async function mountState(volumeId: string, node: string, volume: string): Promise<string> {
  const seen = await inWorkspace(volumeId, "stat --file-system --format=%T /workspace; ls -la /workspace /workspace/juicefs-e2e");
  const mounts = await onNode(node, `grep '${volume}' /proc/self/mountinfo || true`);
  return `in the session: ${seen.stdout}${seen.stderr}\non the node: ${mounts}${await driverState(node)}`;
}

/** Runs a Job of the CronJob's template, as one it starts on its schedule is, until it completes: what its pods logged. */
async function runJob(cronJob: string): Promise<string> {
  const name = `${cronJob}-e2e-${Date.now()}`;
  const template = (await kube.get<V1CronJob>(ref("CronJob", cronJob, NAMESPACE)))?.spec?.jobTemplate;
  assert.ok(template?.spec, `there is no CronJob ${cronJob}`);
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

/**
 * A pod of alasio's labelled as its JuiceFS admin pods are, which reach Valkey and the
 * object store, placed where the stack runs: `s3`, the backup's uploader, with the
 * object store's admin keys and the backup's settings, and `juicefs`, JuiceFS's command
 * line on the mount image, with the file system's credentials, privileged to mount it;
 * `/backup` is theirs to share. Removed once `use` is done with it.
 */
async function withAdminPod<T>(use: (inPod: (container: "s3" | "juicefs", script: string) => Promise<string>) => Promise<T>): Promise<T> {
  const upload = (await kube.get<V1CronJob>(ref("CronJob", neonName("backup"), NAMESPACE)))?.spec?.jobTemplate.spec?.template.spec?.containers[0];
  const juicefs = (await kube.get<V1CronJob>(ref("CronJob", componentName("juicefs-quota-check"), NAMESPACE)))?.spec?.jobTemplate.spec?.template.spec?.containers[0];
  assert.ok(upload?.image && juicefs?.image);
  const compute = await kube.get<V1Deployment>(ref("Deployment", neonName("compute"), NAMESPACE));
  const credential = (name: string, key: string) => ({ name, valueFrom: { secretKeyRef: { name: WORKSPACES_CREDENTIALS, key } } });
  const name = `juicefs-admin-${Date.now()}`;
  const pod: V1Pod = {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, namespace: NAMESPACE, labels: JUICEFS_ADMIN },
    spec: {
      nodeSelector: compute?.spec?.template.spec?.nodeSelector ?? {},
      terminationGracePeriodSeconds: 0,
      containers: [
        {
          name: "s3",
          image: upload.image,
          command: ["sleep", "infinity"],
          env: upload.env ?? [],
          envFrom: upload.envFrom ?? [],
          securityContext: { runAsNonRoot: true, runAsUser: 1000, runAsGroup: 1000, allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } },
          volumeMounts: [{ name: "backup", mountPath: "/backup" }, { name: "tmp", mountPath: "/tmp" }],
        },
        {
          name: "juicefs",
          image: juicefs.image,
          command: ["sleep", "infinity"],
          env: [credential("META_URL", "metaurl"), credential("ACCESS_KEY", "access-key"), credential("SECRET_KEY", "secret-key")],
          securityContext: { privileged: true },
          volumeMounts: [{ name: "backup", mountPath: "/backup" }],
        },
      ],
      volumes: [{ name: "backup", emptyDir: {} }, { name: "tmp", emptyDir: {} }],
    },
  };
  await kube.apply(pod);
  try {
    await until(`the admin pod ${name} running`, async () => (isReady(await kube.get<V1Pod>(ref("Pod", name, NAMESPACE))) ? true : undefined));
    return await use((container, script) => kube.execOk(NAMESPACE, name, ["sh", "-c", script], { container }));
  } finally {
    await kube.remove(ref("Pod", name, NAMESPACE)).catch(() => {});
  }
}

/** The names of the objects under `prefix`, an s3:// URL in the admin pod's shell, that match `pattern`. */
async function listed(inPod: (container: "s3", script: string) => Promise<string>, prefix: string, pattern: RegExp): Promise<string[]> {
  const listing = await inPod("s3", `aws s3 --endpoint-url "$S3_ENDPOINT" ls "${prefix}" || true`);
  return listing.split("\n").map((line) => line.trim().split(/\s+/u).at(-1) ?? "").filter((object) => pattern.test(object)).sort();
}

/** When JuiceFS made the dump `dump-YYYY-MM-DD-hhmmss.json.gz`, which it names by UTC. */
function dumpedAt(dump: string): number {
  const [, year, month, day, hour, minute, second] = (/dump-(\d{4})-(\d{2})-(\d{2})-(\d{2})(\d{2})(\d{2})\.json\.gz$/u.exec(dump) ?? []).map(Number);
  assert.ok(year && month && day, `${dump} names no time`);
  return Date.UTC(year, month - 1, day, hour ?? 0, minute ?? 0, second ?? 0);
}

/** Registers the suite, which runs once the end-to-end run has made its sessions. */
export function workspaceStorage(): void {
  describe("workspaces on JuiceFS", () => {
    const sessions: Record<NetMode, string> = { none: "fs-e2e-none", full: "fs-e2e-full" };
    const none = sessions.none;
    let fixture: string;
    let fixtureWrittenAt: number;

    before(async () => {
      for (const netMode of ["none", "full"] as const) assert.deepEqual(await create(sessions[netMode], netMode), { volumeId: sessions[netMode], netMode });
      fixture = (await inWorkspaceOk(none, [
        "set -e",
        `mkdir -p ${FIXTURE} && cd ${FIXTURE}`,
        "i=0; while [ $i -lt 20 ]; do head -c 4096 /dev/urandom > small-$i; i=$((i + 1)); done",
        "head -c 5242880 /dev/urandom > large",
        DIGEST,
      ].join("\n"))).trim();
      fixtureWrittenAt = Date.now();
    });

    test("a session's workspace is a volume of the workspaces' class, made by JuiceFS's driver, whose mount pod is ready", async () => {
      const { claim, volume } = await volumeOf(none);
      const storageClass = await kube.get<V1StorageClass>(ref("StorageClass", claim.spec?.storageClassName ?? ""));
      assert.equal(storageClass?.provisioner, CSI_DRIVER);
      assert.equal(volume.spec?.csi?.driver, CSI_DRIVER);
      await until("the session's mount pod ready", async () => (isReady(await mountPodOf(none)) ? true : undefined), 60_000, async () => driverState((await podOf(none)).spec?.nodeName ?? ""));
    });

    test("a workspace on JuiceFS does in gVisor what tools ask of a file system", async () => {
      assert.match(await inSession(none, 'console.log(require("fs").readFileSync("/proc/version","utf8"))'), /gvisor/u);
      const said = (await inWorkspaceOk(none, FILE_OPERATIONS)).trim().split("\n");
      assert.deepEqual(said, ["fsync", "rename", "hardlink", "symlink", "chmod", "flock", "mmap", "large-file", "small-files", "git"].map((name) => `PASS ${name}`));
    });

    test("a workspace takes no more than its claim's size", async () => {
      const said = await inWorkspaceOk(none, [
        `cd ${WORKDIR}`,
        'fallocate -l 900M quota-a && echo "took 900M"',
        'fallocate -l 300M quota-b 2>&1 && echo "took 300M more"',
        "rm -f quota-a quota-b",
        "echo after > after-quota && cat after-quota",
      ].join("; "));
      assert.match(said, /^took 900M$/mu);
      assert.match(said, /quota exceeded/iu);
      assert.doesNotMatch(said, /took 300M more/u);
      assert.match(said, /^after$/mu);
    });

    test("a session reaches neither Valkey nor the object store, and holds none of their credentials", async () => {
      const addresses: (readonly [string, number])[] = [];
      for (const [name, port] of [[VALKEY, 6379], [SEAWEEDFS, 8333]] as const) {
        const service = await kube.get<V1Service>(ref("Service", name, NAMESPACE));
        const pod = await kube.get<V1Pod>(ref("Pod", `${name}-0`, NAMESPACE));
        assert.ok(service?.spec?.clusterIP && pod?.status?.podIP, `${name} has no address`);
        addresses.push([service.spec.clusterIP, port], [pod.status.podIP, port]);
      }
      const secrets = await Promise.all([
        kube.secret(NAMESPACE, VALKEY, "password"),
        kube.secret(NAMESPACE, WORKSPACES_CREDENTIALS, "metaurl"),
        kube.secret(NAMESPACE, WORKSPACES_CREDENTIALS, "access-key"),
        kube.secret(NAMESPACE, WORKSPACES_CREDENTIALS, "secret-key"),
      ]);
      for (const netMode of ["none", "full"] as const) {
        const session = sessions[netMode];
        for (const [address, port] of addresses) {
          assert.equal((await inSession(session, tcp(JSON.stringify(address), port))).trim(), "blocked", `${netMode}: ${address}:${port}`);
        }
        // Its environment, every process's, and its mounts.
        const seen = await inWorkspaceOk(session, "env; for f in /proc/[0-9]*/environ; do tr '\\0' '\\n' < \"$f\"; done 2>/dev/null; cat /proc/mounts /proc/self/mountinfo");
        assert.ok(seen.includes("/workspace"));
        for (const secret of secrets) assert.ok(!seen.includes(secret), `the session with ${netMode} internet holds a credential of the file system`);
        assert.doesNotMatch(seen, /redis:\/\//u);
      }
    });

    test("a workspace suspended resumes with its files", async () => {
      await kube.patch(ref("Sandbox", none, SESSIONS), { spec: { operatingMode: "Suspended" } });
      await until("the session's pod gone", async () => ((await kube.get<V1Pod>(ref("Pod", none, SESSIONS))) ? undefined : true));
      await bringUp(none);
      assert.equal(await fixtureDigest(none), fixture);
    });

    for (const component of [VALKEY, SEAWEEDFS]) {
      test(`a workspace keeps every file written through ${component} being killed mid-write`, async () => {
        const dir = `${WORKDIR}/through-${component}`;
        const writing = kube.execOk(SESSIONS, none, ["node", "-e", WRITER, dir, "30000"], { container: "bayma" });
        await sleep(3000);
        const name = `${component}-0`;
        const killed = await kube.get<V1Pod>(ref("Pod", name, NAMESPACE));
        await kube.kill(NAMESPACE, name);
        await until(`${name} made anew`, async () => ((await kube.get<V1Pod>(ref("Pod", name, NAMESPACE)))?.metadata?.uid === killed?.metadata?.uid ? undefined : true));
        await kube.awaitReady([ref("StatefulSet", component, NAMESPACE)], "10 minutes");
        const written = (await writing).trim().split("\n").filter(Boolean);
        assert.ok(written.length > 0, "nothing was written");
        const kept = new Set<string>(JSON.parse(await kube.execOk(SESSIONS, none, ["node", "-e", WRITTEN, dir], { container: "bayma" })));
        assert.deepEqual(written.filter((file) => !kept.has(file)), []);
        assert.equal(await fixtureDigest(none), fixture);
      });
    }

    test("a workspace keeps working through its mount pod's client crashing, as the driver holds the mount for the client started again", async () => {
      const running = await podOf(none);
      const node = running.spec?.nodeName ?? "";
      const volume = (await volumeOf(none)).volume.metadata?.name;
      const mountPod = await mountPodOf(none);
      assert.ok(mountPod?.metadata?.name && volume, `the session ${none} has no mount pod`);
      const name = mountPod.metadata.name;
      const restarts = mountPod.status?.containerStatuses?.[0]?.restartCount ?? 0;
      // The client mounts the volume at a path of the driver's named after it, which its
      // command line names; the bracket keeps the pattern from matching the shell's own.
      await onNode(node, [
        "set -eu",
        `clients=$(for process in /proc/[0-9]*; do grep -q '[/]jfs/${volume}-' "$process/cmdline" 2>/dev/null && echo "\${process#/proc/}"; done; true)`,
        '[ -n "$clients" ]',
        "kill -9 $clients",
      ].join("\n"));
      await until("the mount pod's client started again", async () => {
        const status = (await kube.get<V1Pod>(ref("Pod", name, DRIVER_NAMESPACE)))?.status?.containerStatuses?.[0];
        return (status?.restartCount ?? 0) > restarts && status?.state?.running ? true : undefined;
      }, 120_000, () => driverState(node));
      const checked = await checkMount(none);
      const after = await bringUp(none);
      assert.deepEqual({ answered: checked.code, uid: after.uid }, { answered: 0, uid: running.metadata?.uid }, checked.stderr);
      assert.equal(await fixtureDigest(none), fixture);
    });

    test("a session keeps its files through its mount pod being deleted: on the mount the driver holds for the one it makes anew, or restarted as it is brought up", async () => {
      const running = await podOf(none);
      const node = running.spec?.nodeName ?? "";
      const mountPod = await mountPodOf(none);
      assert.ok(mountPod?.metadata?.name, `the session ${none} has no mount pod`);
      await kube.remove(ref("Pod", mountPod.metadata.name, DRIVER_NAMESPACE));
      await until("its mount pod made anew", async () => {
        const made = await mountPodOf(none);
        return made?.metadata?.uid !== mountPod.metadata?.uid && made?.status?.phase === "Running" ? true : undefined;
      }, 300_000, () => driverState(node));
      const checked = await checkMount(none);
      const after = await bringUp(none);
      const restarted = after.uid !== running.metadata?.uid;
      assert.equal(restarted, checked.code !== 0, `restarted: ${restarted}, as its workspace answered ${checked.code} ${checked.stderr}`);
      assert.equal(await fixtureDigest(none), fixture);
    });

    test("a session whose workspace's mount is lost is restarted as it is brought up, at once, on a JuiceFS mount made anew, its files kept", async () => {
      const running = await podOf(none);
      const node = running.spec?.nodeName ?? "";
      const volume = (await volumeOf(none)).volume.metadata?.name ?? "";
      const lost = await mountPodOf(none);
      assert.ok(lost?.metadata?.name, `the session ${none} has no mount pod`);
      /** The file system types of what is mounted at the pod `uid`'s volumes on the node. */
      const mountedAt = async (uid: string | undefined) => onNode(node, `awk '$5 ~ /\\/pods\\/${uid}\\/volumes\\// { print $(NF - 2) }' /proc/self/mountinfo`);
      // The FUSE connection of the session's mount on the node aborted, as the kernel does
      // to a mount whose client is gone for good: the major:minor of its mount names it.
      await onNode(node, [
        "set -eu",
        `device=$(awk '$5 ~ /\\/pods\\/${running.metadata?.uid}\\/volumes\\// && / - fuse\\.juicefs / { print $3; exit }' /proc/self/mountinfo)`,
        '[ -n "$device" ]',
        'connection=$(echo "$device" | cut -d: -f2)',
        '[ -e "/sys/fs/fuse/connections/$connection" ] || mount -t fusectl fusectl /sys/fs/fuse/connections',
        'echo 1 > "/sys/fs/fuse/connections/$connection/abort"',
      ].join("\n"));
      const broken = await until("the session's workspace broken", async () => {
        const checked = await checkMount(none);
        return checked.code === 0 ? undefined : checked.stderr;
      }, 60_000);
      assert.match(broken, /Transport endpoint is not connected|Software caused connection abort|Input\/output error/u);
      const after = await bringUp(none);
      assert.notEqual(after.uid, running.metadata?.uid);
      // The broken mount's pod is gone, and the session's new pod has its volume from JuiceFS,
      // not the node's directory under the mount point.
      const remaining = await kube.get<V1Pod>(ref("Pod", lost.metadata.name, DRIVER_NAMESPACE));
      assert.ok(remaining?.metadata?.uid !== lost.metadata.uid || remaining?.metadata?.deletionTimestamp, `${lost.metadata.name} still serves`);
      const state = await mountState(none, node, volume);
      assert.deepEqual([...new Set((await mountedAt(after.uid)).trim().split("\n"))].filter((type) => type !== "tmpfs"), ["fuse.juicefs"], state);
      const files = await inWorkspace(none, `cd ${FIXTURE} && timeout -k 5 120 sh -c '${DIGEST}'`);
      assert.equal(files.stdout.trim(), fixture, state);
    });

    test("a workspace moves to another node with its files, as a JuiceFS volume is no node's", { skip: AGENTS < 2 && "the run has one node" }, async () => {
      const full = sessions.full;
      const written = (await inWorkspaceOk(full, `mkdir -p ${WORKDIR} && head -c 1048576 /dev/urandom > ${WORKDIR}/moving && sha256sum < ${WORKDIR}/moving`)).trim();
      const node = (await podOf(full)).spec?.nodeName ?? "";
      await kube.patch(ref("Node", node), { spec: { unschedulable: true } });
      try {
        const moved = await bringUp(full, "anywhere");
        assert.ok(moved.node && moved.node !== node, `the session stayed on ${node}`);
        assert.equal((await inWorkspaceOk(full, `sha256sum < ${WORKDIR}/moving`)).trim(), written);
      } finally {
        await kube.patch(ref("Node", node), { spec: { unschedulable: null } });
      }
    });

    test("the daily quota check checks each workspace's quota", async () => {
      const said = await runJob(componentName("juicefs-quota-check"));
      for (const netMode of ["none", "full"] as const) {
        const directory = await directoryOf(sessions[netMode]);
        assert.match(said, new RegExp(`quota of /${directory} is consistent|/${directory}: quota\\(`, "u"), directory);
      }
    });

    test("a workspace's volume is deleted with its claim, and its directory with it", async () => {
      const { claim: session } = await volumeOf(none);
      const image = (await kube.get<V1CronJob>(ref("CronJob", componentName("juicefs-quota-check"), NAMESPACE)))?.spec?.jobTemplate.spec?.template.spec?.containers[0]?.image;
      assert.ok(image);
      const name = `juicefs-e2e-deleted-${Date.now()}`;
      const claim: V1PersistentVolumeClaim = {
        apiVersion: "v1",
        kind: "PersistentVolumeClaim",
        metadata: { name, namespace: NAMESPACE },
        spec: { storageClassName: session.spec?.storageClassName ?? "", accessModes: session.spec?.accessModes ?? [], resources: { requests: { storage: "1Gi" } } },
      };
      const writer: V1Pod = {
        apiVersion: "v1",
        kind: "Pod",
        metadata: { name, namespace: NAMESPACE },
        spec: {
          restartPolicy: "Never",
          terminationGracePeriodSeconds: 0,
          containers: [{ name: "writer", image, command: ["sh", "-c", "head -c 1048576 /dev/urandom > /data/written && sync"], volumeMounts: [{ name: "data", mountPath: "/data" }] }],
          volumes: [{ name: "data", persistentVolumeClaim: { claimName: name } }],
        },
      };
      await kube.apply(claim);
      await kube.apply(writer);
      try {
        await until(`${name} written in its volume`, async () => {
          const phase = (await kube.get<V1Pod>(ref("Pod", name, NAMESPACE)))?.status?.phase;
          assert.notEqual(phase, "Failed", `${name} could not write in its volume`);
          return phase === "Succeeded" ? true : undefined;
        });
      } finally {
        await kube.remove(ref("Pod", name, NAMESPACE));
      }
      const volume = (await kube.get<V1PersistentVolumeClaim>(ref("PersistentVolumeClaim", name, NAMESPACE)))?.spec?.volumeName;
      assert.ok(volume, `the claim ${name} is bound to no volume`);
      await kube.remove(ref("PersistentVolumeClaim", name, NAMESPACE));
      await until(`the volume ${volume} deleted`, async () => ((await kube.get<V1PersistentVolume>(ref("PersistentVolume", volume))) ? undefined : true), 300_000, async () => {
        const left = await kube.get<V1PersistentVolume>(ref("PersistentVolume", volume));
        const logged = await kube.logs(DRIVER_NAMESPACE, "juicefs-csi-controller-0", "juicefs-plugin").catch((error: unknown) => (error instanceof Error ? error.message : String(error)));
        return `: it is ${left?.status?.phase}; the controller last logged:\n${logged.trimEnd().split("\n").slice(-20).join("\n")}`;
      });
      const listing = await withAdminPod((inPod) =>
        inPod("juicefs", [
          "set -eu",
          "mkdir -p /mounted",
          'juicefs mount --background --read-only --no-bgjob --cache-size 0 "$META_URL" /mounted',
          "ls -a /mounted",
          "umount /mounted",
        ].join("\n"))
      );
      assert.ok(listing.split("\n").includes(await directoryOf(none)), `the file system holds no workspace's directory: ${listing}`);
      assert.ok(!listing.split("\n").includes(`${NAMESPACE}-${name}`), `the deleted volume's directory remains: ${listing}`);
    });

    test("JuiceFS dumps its metadata to its bucket, the nightly backup copies the newest, and the copy restores the same files", async () => {
      await withAdminPod(async (inPod) => {
        const meta = "s3://$WORKSPACES_BUCKET/$WORKSPACES_NAME/meta/";
        const dumps = () => listed(inPod, meta, /^dump-.*\.json\.gz$/u);
        // A dump made since the fixture was written, which it holds: at most one interval, 5m, away.
        const newest = await until("a dump of the file system's metadata since the fixture", async () => {
          const last = (await dumps()).at(-1);
          return last && dumpedAt(last) > fixtureWrittenAt ? last : undefined;
        }, 12 * 60_000);
        await runJob(neonName("backup"));
        const name = (await inPod("s3", 'printf %s "$WORKSPACES_NAME"')).trim();
        const copies = await listed(inPod, "s3://$BUCKET/", new RegExp(`^${name}-dump-.*\\.json\\.gz$`, "u"));
        // The newest when the backup ran: that one, or one made since.
        const copy = copies.at(-1) ?? "";
        assert.ok([newest, (await dumps()).at(-1)].map((dump) => `${name}-${dump}`).includes(copy), `the backups hold ${copies.join(", ") || "no dump"}`);

        // Restored as the README says: loaded into an empty database, given the bucket's
        // keys, checked, and mounted read-only beside the file system it was copied from.
        await inPod("s3", `aws s3 --endpoint-url "$S3_ENDPOINT" cp "s3://$BUCKET/${copy}" /backup/dump.json.gz`);
        const restored = await inPod("juicefs", [
          "set -eu",
          'restored="${META_URL%/1}/2"',
          'juicefs load "$restored" /backup/dump.json.gz',
          'juicefs config "$restored" --access-key "$ACCESS_KEY" --secret-key "$SECRET_KEY" --force',
          'juicefs fsck "$restored"',
          "mkdir -p /restored",
          'juicefs mount --background --read-only --no-bgjob --cache-size 0 "$restored" /restored',
          `cd /restored/${await directoryOf(none)}${FIXTURE}`,
          DIGEST,
          "cd / && umount /restored",
        ].join("\n"));
        assert.equal(restored.trim().split("\n").at(-1), fixture);
      });
    });
  });
}
