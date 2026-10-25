/**
 * Workspaces on JuiceFS as alasio installs it: a session's workspace a directory of the
 * file system, made through the sandbox gateway by a node agent with its quota, on the
 * nodes' mount of it (cli/src/manifests/sandbox-gateway.ts workspaceMount), doing in gVisor
 * what tools ask of a file system within its quota; its files kept through the session
 * released and bound again, and Valkey and the object store killed mid-write; a file's fsync or close failing while Valkey is away; a session forked, its fork a clone of its files
 * with their owners, its own quota, and neither touched by what is done in the other or by
 * its deletion; neither Valkey nor the object store, nor their credentials, within a
 * session's reach (its moving to another node is the snapshots shard's); a workspace deleted, its directory with it; the file system checked and
 * collected daily; and its metadata dumped, backed up nightly, and restored from the backup
 * with the same files. The nodes' mount through a handover and a reboot is the sandbox
 * service's suite's (./sandbox-service.ts).
 *
 * The end-to-end run's workspaces shard (test/e2e/alasio.test.ts), registered with
 * workspaceStorage(): it makes its sessions as alasio does, one with no internet, which
 * it works on, and one with internet.
 */
import assert from "node:assert/strict";
import { before, describe, test } from "node:test";

import type { V1CronJob, V1Deployment, V1Pod, V1Service, V1StatefulSet } from "@kubernetes/client-node";

import { selectorOf } from "../../cli/src/kube/rollout.ts";
import { componentName, NAMESPACE, neonName, selectorLabels } from "../../cli/src/manifests/common.ts";
import { FILER, FILER_STORE, FILER_STORE_USERS, GATEWAY, MASTER, VOLUME_SERVER } from "../../cli/src/manifests/object-store.ts";
import { JUICEFS_ADMIN, VALKEY, WORKSPACES_CREDENTIALS } from "../../cli/src/manifests/workspace-storage.ts";
import { WORKSPACES_ROOT } from "../../src/node-agent/names.ts";
import type { NetMode } from "../../src/sandbox/index.ts";
import { bindSession, boundPodOf, gatewayCall, inAlasio, inSession, kube, mainToken, nodeAgents, reachability, recordSession, ref, runCronJob, SESSIONS } from "./harness.ts";
import type { Made } from "./session-volumes.ts";

/**
 * What a workspace's writes go through, of the stack's: its metadata's Valkey, and the
 * object store's parts its data does, each a StatefulSet's but its gateways, a
 * Deployment's; and the port each takes them on.
 */
const STORES: readonly Store[] = [
  { name: VALKEY, kind: "StatefulSet", port: 6379, server: "valkey" },
  { name: GATEWAY, kind: "Deployment", port: 8333 },
  { name: FILER, kind: "StatefulSet", port: 8888 },
  { name: VOLUME_SERVER, kind: "StatefulSet", port: 8080 },
  { name: FILER_STORE, kind: "StatefulSet", port: 6379, server: "valkey" },
];

/** A workload of the stack's a workspace's data goes through, or might: its name, its kind, a port it takes connections on, and the container whose log says how it started again, where one does. */
interface Store {
  readonly name: string;
  readonly kind: "StatefulSet" | "Deployment";
  readonly port: number;
  readonly server?: string;
}

/** What a session must not reach: every store, and the object store's masters, which its writes go through to none of. */
const UNREACHED: readonly Store[] = [...STORES, { name: MASTER, kind: "StatefulSet", port: 9333 }];

/** A pod of the store `store`'s that runs now. */
const podOfStore = async ({ name, kind }: Store) => (kind === "Deployment" ? kube.runningPod(NAMESPACE, name) : `${name}-0`);

/** Where the suite works in a session's workspace, and the files it writes first and expects everywhere after. */
const WORKDIR = "/workspace/juicefs-e2e";
const FIXTURE = `${WORKDIR}/fixture`;

/** Where the suite writes the files a fork is to keep, beside one in the home. */
const FORKED = `${WORKDIR}/forked`;
const FORKED_HOME = "/home/agent/forked";

/** The digest of the files under the current directory, which two copies of it share only when they hold the same. */
const DIGEST = "find . -type f | LC_ALL=C sort | xargs sha256sum | sha256sum | cut -c1-16";

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

/**
 * Node, in a session: opens argv[3] files in the directory argv[1], then, once the file
 * argv[2] is there, writes each as WRITER does, fsyncs it and closes it, printing of each, as a
 * line of JSON, its name and, should it have failed, the step that did and its error's code.
 */
const AWAY_WRITER = `const fs = require("node:fs");
const [dir, go, count] = process.argv.slice(1);
fs.mkdirSync(dir, { recursive: true });
const fds = Array.from({ length: Number(count) }, (_, i) => fs.openSync(dir + "/" + i, "w"));
while (!fs.existsSync(go)) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
for (const [i, fd] of fds.entries()) {
  let step = "write";
  try {
    fs.writeSync(fd, (i + "\\n").repeat(4096));
    step = "fsync";
    fs.fsyncSync(fd);
    step = "close";
    fs.closeSync(fd);
    console.log(JSON.stringify({ file: String(i) }));
  } catch (error) {
    console.log(JSON.stringify({ file: String(i), failed: step, code: error.code }));
    // Closed all the same, what it says already said by the step that failed.
    if (step !== "close") try { fs.closeSync(fd); } catch {}
  }
}`;

/** Node, in a session: of each file argv[2...] of the directory argv[1], whether it is there, its size and its first bytes, as JSON. */
const SEEN = `const fs = require("node:fs");
const [dir, ...names] = process.argv.slice(1);
console.log(JSON.stringify(names.map((name) => {
  try {
    const held = fs.readFileSync(dir + "/" + name);
    return { name, size: held.length, starts: held.subarray(0, 16).toString("latin1") };
  } catch (error) {
    return { name, error: String(error) };
  }
})));`;

/** Node, in a session: the names of the files in the directory argv[1] that hold what WRITER wrote, as JSON. */
const WRITTEN = `const fs = require("node:fs");
const dir = process.argv[1];
console.log(JSON.stringify(fs.readdirSync(dir).filter((name) => fs.readFileSync(dir + "/" + name, "utf8") === (name + "\\n").repeat(4096))));`;

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Asks `check` every two seconds until it answers something, for up to `timeoutMs`: what
 * it answered. Should it not, `explain` says what there was instead.
 */
export async function until<T>(what: string, check: () => Promise<T | undefined>, timeoutMs = 300_000, explain = async () => ""): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const answer = await check();
    if (answer !== undefined) return answer;
    if (Date.now() >= deadline) assert.fail(`${what} did not happen within ${timeoutMs / 1000}s${await explain()}`);
    await sleep(2000);
  }
}

/** The pod the session is bound to, it bound first as a turn binds it: its name and node. */
const bringUp = (volumeId: string) => bindSession(volumeId);

/** sh run in the session's bayma container, as its agent, the session bound first. */
const inWorkspaceOk = async (volumeId: string, script: string): Promise<string> => kube.execOk(SESSIONS, (await bringUp(volumeId)).name, ["sh", "-c", script], { container: "bayma" });

/** The digest of the fixture, as the session sees it. */
const fixtureDigest = async (volumeId: string) => (await inWorkspaceOk(volumeId, `cd ${FIXTURE} && timeout -k 5 120 sh -c '${DIGEST}'`)).trim();

const isReady = (pod: V1Pod | null | undefined) => pod?.status?.conditions?.some(({ type, status }) => type === "Ready" && status === "True") ?? false;

/** Makes the session `volumeId` with `netMode` as alasio does (./session-volumes.ts), recorded so, as tokens of it are minted. */
async function create(volumeId: string, netMode: NetMode): Promise<Made> {
  recordSession(volumeId, netMode);
  return inAlasio<Made>("./session-volumes.ts", "create", volumeId, netMode);
}

/** Forks the session `source` into `volumeId` as alasio does (./session-volumes.ts). */
const fork = (source: string, volumeId: string) => inAlasio<Made>("./session-volumes.ts", "fork", source, volumeId);

/** Deletes the session `volumeId` as alasio does (./session-volumes.ts), its binding let go first. */
const destroy = (volumeId: string) => inAlasio<Made>("./session-volumes.ts", "destroy", volumeId, "none");

/** Lets the session's binding go, as the gateway releases one idle: its pod gone once it is. */
async function release(volumeId: string): Promise<void> {
  const released = await gatewayCall(await mainToken(volumeId), "POST", "/release");
  assert.equal(released.status, 204, released.said);
  await until(`the release of ${volumeId}`, async () => ((await boundPodOf(volumeId)) === null ? true : undefined), 180_000);
}

/** The directory of the file system that is the session's workspace, as a node agent makes it, of its environment's. */
const directoryOf = (volumeId: string) => `${SESSIONS}/${volumeId}`;

/** The session's workspace's quota, as the gateway reads it back: its capacity and what it holds, in bytes. */
async function quotaOf(volumeId: string): Promise<{ readonly capacityBytes: number; readonly usedBytes: number }> {
  const read = await gatewayCall(await mainToken(volumeId), "GET", "/workspace/quota");
  assert.equal(read.status, 200, read.said);
  return JSON.parse(read.said) as { capacityBytes: number; usedBytes: number };
}

/**
 * A pod of alasio's labelled as its JuiceFS admin pods are, which reach Valkey and the
 * object store, placed where the stack runs: `s3`, the backup's uploader, with the
 * backups' keys and the backup's settings, and `juicefs`, JuiceFS's command
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
    const forked = "fs-e2e-fork";
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

    test("a session's workspace is a directory of the file system, of a quota its own, on the mount of the node it is bound on, whose pod is ready", async () => {
      assert.deepEqual((await quotaOf(none)).capacityBytes, 1024 ** 3);
      const { node } = await bringUp(none);
      const mounts = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("workspace-mount")) });
      assert.ok(isReady(mounts.find(({ spec }) => spec?.nodeName === node)), `the mount of ${node} is not ready`);
      // On the node, as its agent sees it: the workspace's files under its directory of the file system.
      const agent = (await nodeAgents()).get(node);
      assert.ok(agent, `no node agent runs ready on ${node}`);
      const listed = await kube.execOk(NAMESPACE, agent, ["sh", "-c", `ls ${WORKSPACES_ROOT}/${directoryOf(none)}/files`], { container: "node-agent" });
      assert.deepEqual(listed.trim().split("\n").sort(), ["home", "workspace"]);
    });

    test("a workspace on JuiceFS does in gVisor what tools ask of a file system", async () => {
      assert.match(await inSession(none, 'console.log(require("fs").readFileSync("/proc/version","utf8"))'), /gvisor/u);
      const said = (await inWorkspaceOk(none, FILE_OPERATIONS)).trim().split("\n");
      assert.deepEqual(said, ["fsync", "rename", "hardlink", "symlink", "chmod", "flock", "mmap", "large-file", "small-files", "git"].map((name) => `PASS ${name}`));
    });

    test("a workspace takes no more than its quota", async () => {
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

    test("a session reaches neither Valkey nor any part of the object store, and holds none of their credentials", async () => {
      const addresses: (readonly [string, number])[] = [];
      for (const store of UNREACHED) {
        const service = await kube.get<V1Service>(ref("Service", store.name, NAMESPACE));
        const pod = await kube.get<V1Pod>(ref("Pod", await podOfStore(store), NAMESPACE));
        assert.ok(pod?.status?.podIP, `${store.name} has no address`);
        // A headless Service's name is its pods' addresses.
        addresses.push(...(service?.spec?.clusterIP && service.spec.clusterIP !== "None" ? [[service.spec.clusterIP, store.port] as const] : []), [pod.status.podIP, store.port]);
      }
      const secrets = await Promise.all([
        kube.secret(NAMESPACE, VALKEY, "password"),
        kube.secret(NAMESPACE, FILER_STORE_USERS.filer.secret, "password"),
        kube.secret(NAMESPACE, FILER_STORE_USERS.collector.secret, "password"),
        kube.secret(NAMESPACE, WORKSPACES_CREDENTIALS, "metaurl"),
        kube.secret(NAMESPACE, WORKSPACES_CREDENTIALS, "access-key"),
        kube.secret(NAMESPACE, WORKSPACES_CREDENTIALS, "secret-key"),
      ]);
      for (const netMode of ["none", "full"] as const) {
        const session = sessions[netMode];
        for (const [address, port] of addresses) {
          assert.equal(await reachability((code) => inSession(session, code), JSON.stringify(address), port), "blocked", `${netMode}: ${address}:${port}`);
        }
        // Its environment, every process's, and its mounts.
        const seen = await inWorkspaceOk(session, "env; for f in /proc/[0-9]*/environ; do tr '\\0' '\\n' < \"$f\"; done 2>/dev/null; cat /proc/mounts /proc/self/mountinfo");
        assert.ok(seen.includes("/workspace"));
        for (const secret of secrets) assert.ok(!seen.includes(secret), `the session with ${netMode} internet holds a credential of the file system`);
        assert.doesNotMatch(seen, /redis:\/\//u);
      }
    });

    test("a workspace released is bound again with its files", async () => {
      const before = await bringUp(none);
      await release(none);
      assert.notEqual((await bringUp(none)).name, before.name);
      assert.equal(await fixtureDigest(none), fixture);
    });

    test("a fork holds its source's files, with their owners, modes and symlinks, counted by its own quota, its source running on", async () => {
      await inWorkspaceOk(none, [
        "set -e",
        `mkdir -p ${FORKED}/private && cd ${FORKED}`,
        "echo secret > private/key && chmod 600 private/key && chmod 700 private",
        "printf '#!/bin/sh\\necho ran\\n' > run && chmod 755 run",
        "ln -s private/key link && ln -s /nowhere dangling",
        `echo home > ${FORKED_HOME}`,
      ].join("\n"));
      /** The files the fork is to keep, each with its owner, mode, type and target. */
      const listing = (volumeId: string) => inWorkspaceOk(volumeId, `find ${FORKED} ${FORKED_HOME} -printf '%p %U:%G %m %y %l\\n' | LC_ALL=C sort`);
      const listed = await listing(none);
      assert.match(listed, /\/private\/key 1000:1000 600 f \n/u);
      assert.match(listed, /\/link 1000:1000 777 l private\/key\n/u);
      const running = (await bringUp(none)).name;

      assert.deepEqual(await fork(none, forked), { volumeId: forked, netMode: "none" });
      // Its source runs on, bound as it was, with its files.
      assert.equal((await boundPodOf(none))?.name, running);
      assert.equal(await listing(none), listed);
      // The fork waits to be bound, its quota its own, counting what it was cloned with.
      assert.equal(await boundPodOf(forked), null);
      const quota = await quotaOf(forked);
      assert.ok(quota.capacityBytes === 1024 ** 3 && quota.usedBytes > 0, JSON.stringify(quota));
      const directory = directoryOf(forked);
      const checked = await withAdminPod((inPod) => inPod("juicefs", `juicefs quota check "$META_URL" --path /${directory} 2>&1`));
      assert.match(checked, new RegExp(`quota of /${directory} is consistent`, "u"));
      await bringUp(forked);
      assert.equal(await listing(forked), listed);
      assert.equal(await fixtureDigest(forked), fixture);
      assert.equal((await inWorkspaceOk(forked, `${FORKED}/run`)).trim(), "ran");
    });

    test("a fork and its source see nothing of what is written in the other", async () => {
      await inWorkspaceOk(forked, `cd ${FORKED} && echo fork > from-fork && echo changed > private/key && rm run`);
      await inWorkspaceOk(none, `cd ${FORKED} && echo source > from-source && echo home again >> ${FORKED_HOME}`);
      const seen = (volumeId: string) => inWorkspaceOk(volumeId, `cd ${FORKED} && ls && cat private/key ${FORKED_HOME}`);
      assert.equal(await seen(none), "dangling\nfrom-source\nlink\nprivate\nrun\nsecret\nhome\nhome again\n");
      assert.equal(await seen(forked), "dangling\nfrom-fork\nlink\nprivate\nchanged\nhome\n");
    });

    test("a fork takes no more than its own quota", async () => {
      const said = await inWorkspaceOk(forked, [
        `cd ${WORKDIR}`,
        'fallocate -l 900M quota-a && echo "took 900M"',
        'fallocate -l 300M quota-b 2>&1 && echo "took 300M more"',
        "rm -f quota-a quota-b",
      ].join("; "));
      assert.match(said, /^took 900M$/mu);
      assert.match(said, /quota exceeded/iu);
      assert.doesNotMatch(said, /took 300M more/u);
    });

    test("deleting a fork leaves whole both the session it was forked from and its own fork", async () => {
      const again = `${forked}-again`;
      assert.deepEqual(await fork(forked, again), { volumeId: again, netMode: "none" });
      await destroy(forked);
      const gone = await gatewayCall(await mainToken(forked), "GET", "/workspace/quota");
      assert.equal(gone.status, 404, gone.said);
      assert.equal(await fixtureDigest(none), fixture);
      assert.equal(await fixtureDigest(again), fixture);
      assert.equal(await inWorkspaceOk(again, `cat ${FORKED}/private/key`), "changed\n");
      await destroy(again);
    });

    for (const store of STORES) {
      test(`a workspace keeps every file written through ${store.name} being killed mid-write`, async () => {
        const dir = `${WORKDIR}/through-${store.name}`;
        const pod = (await bringUp(none)).name;
        const writing = kube.execOk(SESSIONS, pod, ["node", "-e", WRITER, dir, "30000"], { container: "bayma" });
        await sleep(3000);
        const name = await podOfStore(store);
        const killed = await kube.get<V1Pod>(ref("Pod", name, NAMESPACE));
        await kube.kill(NAMESPACE, name);
        // The killed pod is gone: a StatefulSet's made anew under its name, a Deployment's for another, of a name of its own.
        await until(`${name} gone`, async () => {
          const now = await kube.get<V1Pod>(ref("Pod", name, NAMESPACE));
          const replaced = store.kind === "Deployment" ? now === null : now !== null && now.metadata?.uid !== killed?.metadata?.uid;
          return replaced ? true : undefined;
        });
        await kube.awaitReady([ref(store.kind, store.name, NAMESPACE)], "10 minutes");
        const written = (await writing).trim().split("\n").filter(Boolean);
        assert.ok(written.length > 0, "nothing was written");
        const kept = new Set<string>(JSON.parse(await kube.execOk(SESSIONS, pod, ["node", "-e", WRITTEN, dir], { container: "bayma" })));
        const lost = written.filter((file) => !kept.has(file));
        if (lost.length > 0) {
          const seen = await kube.execOk(SESSIONS, pod, ["node", "-e", SEEN, dir, ...lost], { container: "bayma" });
          const started = store.server ? await kube.logs(NAMESPACE, await podOfStore(store), store.server) : "";
          assert.fail(`the files ${lost.join(", ")}, written, were lost: ${seen}\n${store.name} as it started again:\n${started}`);
        }
        assert.equal(await fixtureDigest(none), fixture);
      });
    }

    test("a file written while Valkey is away fails its fsync or its close, as JuiceFS fails to commit it", async () => {
      const dir = `${WORKDIR}/while-valkey-away`;
      const go = "/tmp/valkey-away";
      const files = 2;
      const pod = (await bringUp(none)).name;
      const inPod = (command: readonly string[]) => kube.execOk(SESSIONS, pod, [...command], { container: "bayma" });
      const writing = inPod(["node", "-e", AWAY_WRITER, dir, go, String(files)]);
      await until(`${files} files open in ${dir}`, async () => ((await inPod(["sh", "-c", `ls ${dir} 2>/dev/null | wc -l`])).trim() === String(files) ? true : undefined));
      const valkey = ref("StatefulSet", VALKEY, NAMESPACE);
      const name = `${VALKEY}-0`;
      await kube.patch(valkey, { spec: { replicas: 0 } });
      try {
        await until(`${name} gone`, async () => ((await kube.get<V1Pod>(ref("Pod", name, NAMESPACE))) === null ? true : undefined));
        await inPod(["touch", go]);
        // Each fsync waits out JuiceFS's retries of Valkey.
        const told = (await writing).trim().split("\n").map((line) => JSON.parse(line) as { readonly file: string; readonly failed?: string; readonly code?: string });
        assert.deepEqual(told.filter(({ failed }) => failed !== "fsync" && failed !== "close"), [], "a file was told written, or failed but not by its fsync or close");
      } finally {
        await kube.patch(valkey, { spec: { replicas: 1 } });
        await kube.awaitReady([valkey], "10 minutes");
      }
      assert.equal(await fixtureDigest(none), fixture);
    });

    test("a second Valkey started on alasio-valkey's volume, as a pod deleted outright is made anew while its server runs, serves nothing until the first has ended", async () => {
      const template = (await kube.get<V1StatefulSet>(ref("StatefulSet", VALKEY, NAMESPACE)))?.spec?.template.spec;
      const server = template?.containers.find(({ name }) => name === "valkey");
      assert.ok(template && server, "alasio-valkey has no server");
      const second = `${VALKEY}-second`;
      // Its server alone, unprobed, as nothing waits for it to be ready.
      const { readinessProbe: _readiness, livenessProbe: _liveness, startupProbe: _startup, ...unprobed } = server;
      const pod: V1Pod = {
        apiVersion: "v1",
        kind: "Pod",
        metadata: { name: second, namespace: NAMESPACE },
        spec: {
          ...template,
          containers: [unprobed],
          volumes: [...(template.volumes ?? []), { name: "data", persistentVolumeClaim: { claimName: `data-${VALKEY}-0` } }],
        },
      };
      await kube.apply(pod);
      try {
        await until(`${second} running`, async () => ((await kube.get<V1Pod>(ref("Pod", second, NAMESPACE)))?.status?.containerStatuses?.[0]?.state?.running ? true : undefined));
        // Long past what a server that started anyway, loading the append-only file, would take to listen.
        await sleep(30_000);
        // No server runs in it, and none takes connections: one loading the file would answer LOADING.
        const servers = await kube.execOk(NAMESPACE, second, ["sh", "-c", "cat /proc/[0-9]*/comm"]);
        assert.doesNotMatch(servers, /valkey-server/u);
        const answered = await kube.execOk(NAMESPACE, second, ["sh", "-c", "valkey-cli ping 2>&1 || true"]);
        assert.match(answered, /Could not connect|Connection refused/u);
        assert.match(await kube.logs(NAMESPACE, second, "valkey"), /waiting for the Valkey or restore that holds \/data\/valkey\.lock to end/u);
        // The first serves on, whole.
        assert.equal(await fixtureDigest(none), fixture);
      } finally {
        await kube.remove(ref("Pod", second, NAMESPACE));
      }
    });

    test("the daily quota check checks each workspace's quota, which is consistent after it", async () => {
      await runCronJob(componentName("juicefs-quota-check"));
      const checked = await withAdminPod((inPod) =>
        inPod("juicefs", (["none", "full"] as const).map((netMode) => `juicefs quota check "$META_URL" --path /${directoryOf(sessions[netMode])} 2>&1`).join("\n")));
      for (const netMode of ["none", "full"] as const) assert.match(checked, new RegExp(`quota of /${directoryOf(sessions[netMode])} is consistent`, "u"), netMode);
    });

    test("the daily collection collects the file system", async () => {
      await runCronJob(componentName("juicefs-gc"));
    });

    test("a workspace deleted through the gateway is gone from the file system, its directory with it", async () => {
      const deleted = "fs-e2e-deleted";
      assert.deepEqual(await create(deleted, "none"), { volumeId: deleted, netMode: "none" });
      await inWorkspaceOk(deleted, "head -c 1048576 /dev/urandom > /workspace/written && sync");
      await destroy(deleted);
      const listing = await withAdminPod((inPod) =>
        inPod("juicefs", [
          "set -eu",
          "mkdir -p /mounted",
          // It prints a dot on its standard output each time it finds the mount not ready yet, and no newline after.
          'juicefs mount --background --read-only --no-bgjob --cache-size 0 "$META_URL" /mounted >/dev/null',
          `ls -a /mounted/${SESSIONS}`,
          "umount /mounted",
        ].join("\n"))
      );
      assert.ok(listing.split("\n").includes(none), `the file system holds no workspace's directory: ${listing}`);
      assert.ok(!listing.split("\n").includes(deleted), `the deleted workspace's directory remains: ${listing}`);
    });

    test("a lock a client that died held lets go once the job that cleans stale sessions has run, its session gone", async () => {
      const { node } = await bringUp(none);
      const mountPods = async () => (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("workspace-mount")) })).filter(({ spec }) => spec?.nodeName === node);
      const [mount] = await mountPods();
      assert.ok(mount?.metadata?.name, `${node} runs no mount's pod`);
      const sessions = () => withAdminPod((inPod) => inPod("juicefs", 'juicefs status "$META_URL" 2>/dev/null')).then((said) => [...said.matchAll(/"Sid": *([0-9]+)/gu)].map(([, sid]) => sid));
      const before = await sessions();
      // Locked through the node's mount, as its agent sees it, by a process that holds it; then the mount's client killed, as a reboot ends it.
      const agent = (await nodeAgents()).get(node);
      assert.ok(agent, `no node agent runs ready on ${node}`);
      const inAgent = (script: string) => kube.exec(NAMESPACE, agent, ["sh", "-c", script], { container: "node-agent" });
      const locked = `${WORKSPACES_ROOT}/${directoryOf(none)}/files${WORKDIR}/locked`;
      const tryLock = async () => ((await inAgent(`flock -n -x ${locked} true`)).code === 0 ? "free" : "held");
      await inAgent(`touch ${locked}`);
      const holding = inAgent(`flock -x ${locked} sleep 120`).catch(() => undefined);
      await until("the lock held", async () => ((await tryLock()) === "held" ? true : undefined), 30_000);
      const killed = Date.now();
      await kube.kill(NAMESPACE, mount.metadata.name);
      // Its holder ends as its sleep does, its lock the dead client's session's meanwhile.
      void holding;
      await until("the node's mount served again by a new pod", async () =>
        (await mountPods()).some(({ metadata, status }) => metadata?.uid !== mount.metadata?.uid && status?.conditions?.some(({ type, status: is }) => type === "Ready" && is === "True")) ? true : undefined, 240_000);
      // Held still, by the session of the client that died, until it is cleaned.
      assert.equal(await tryLock(), "held");
      // Stale once its client has sent no heartbeat for a minute; then cleaned by the job.
      await sleep(Math.max(0, killed + 75_000 - Date.now()));
      await runCronJob(componentName("juicefs-sessions"));
      assert.equal(await tryLock(), "free");
      const after = await sessions();
      assert.ok(before.some((sid) => !after.includes(sid)), `no session of before is gone: ${before.join(", ")} then ${after.join(", ")}`);
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
        await runCronJob(neonName("backup"));
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
          'juicefs mount --background --read-only --no-bgjob --cache-size 0 "$restored" /restored >/dev/null',
          `cd /restored/${directoryOf(none)}/files${FIXTURE}`,
          DIGEST,
          "cd / && umount /restored",
        ].join("\n"));
        assert.equal(restored.trim().split("\n").at(-1), fixture);
      });
    });
  });
}
