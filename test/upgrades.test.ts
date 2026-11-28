/**
 * The cut-over of an installation of the release before (upgrades/from-10.0.27.sh), in how it
 * runs, without a cluster: its checks and refusals, its point of no return, and how a run
 * again goes on from where one before stopped. The end-to-end run's upgrade shard runs it on
 * an installation of that release (test/e2e/alasio.test.ts).
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { appendFileSync, chmodSync, closeSync, existsSync, mkdirSync, mkdtempSync, openSync, readdirSync, readFileSync, renameSync, rmSync, statSync, symlinkSync, writeFileSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { Result } from "effect";

import { installConfigOf } from "../cli/src/config.ts";
import { imageReference } from "../cli/src/images.ts";
import { componentName, neonName } from "../cli/src/manifests/common.ts";
import { STORE_SNAPSHOT_CONFIGMAP } from "../cli/src/object-store.ts";

const SCRIPT = fileURLToPath(new URL("../upgrades/from-10.0.27.sh", import.meta.url));

/** The PATH the cut-over runs with: `bin`'s, and the system's. */
const pathOf = (bin: string) => [bin, "/usr/bin", "/bin"].join(delimiter);

/** The bundled object store's parts, by their StatefulSets, as the cut-over stops them. */
const STORE_PARTS = ["alasio-seaweedfs-filer", "alasio-seaweedfs-volume", "alasio-seaweedfs-master", "alasio-seaweedfs-filer-store"];
/** The same, as the cut-over starts them again: each once those it needs are ready. */
const STARTED = [...STORE_PARTS].reverse();
/** Neon's parts that hold what they write to the store, by their StatefulSets, which a restore stops. */
const NEON_WRITERS = ["alasio-neon-safekeeper", "alasio-neon-pageserver"];
/** The SeaweedFS the cut-over pulls onto the machine before its point of no return, as it names it. */
const STORE_IMAGE = /^STORE_IMAGE=(\S+)$/mu.exec(readFileSync(SCRIPT, "utf8"))?.[1] ?? "";
/** The claims of the parts that keep a volume: the masters', the volume servers' and the filers' store's. */
const MASTER = "data-alasio-seaweedfs-master-0";
const VOLUME = "data-alasio-seaweedfs-volume-0";
const FILER_STORE = "data-alasio-seaweedfs-filer-store-0";
const CLAIMS = [MASTER, VOLUME, FILER_STORE];
/** The image the masters run, as the cut-over reads it. */
const IMAGE = "chrislusf/seaweedfs:4.48@sha256:0123";
/** What the cut-over keeps of a volume's data file besides its link: its first bytes. */
const HEAD_BYTES = 8192;
/** The floor the cut-over keeps free of the snapshot's filesystem unless told otherwise: 60 GiB. */
const FLOOR_BYTES = 60 * 1024 ** 3;

/** `length` bytes, from `seed` on: a file's content that no other file has. */
const bytesOf = (length: number, seed: number) => Buffer.from(Array.from({ length }, (_, at) => (seed + at) % 251));

/** Every file under `directory`, by its path under it, and what it holds; each directory by its path, as null. */
function tree(directory: string): Record<string, Buffer | null> {
  return Object.fromEntries(readdirSync(directory, { recursive: true, encoding: "utf8" }).sort().map((path) => {
    const at = join(directory, path);
    return [path, statSync(at).isDirectory() ? null : readFileSync(at)];
  }));
}

/** A df that says every path is on the filesystem mounted at /, but the claims' volumes on `claimsOn`, with `avail` bytes free. */
const df = (avail: number, claimsOn = "/") =>
  `#!/bin/sh\ncase "$*" in\n  *--output=target*/volumes/*) printf 'Mounted on\\n${claimsOn}\\n' ;;\n  *--output=target*) printf 'Mounted on\\n/\\n' ;;\n  *) printf 'Avail\\n${avail}\\n' ;;\nesac\n`;

/**
 * A machine for the cut-over: a kubectl that reaches its cluster and lists the namespaces
 * `namespaces` names, `<name>\t<branch label>` each, and the bundled object store's claims, each
 * a directory of the machine's, whose parts it stops and starts as it is told, saying so in
 * `stopped` and `started`, a part named in `unready` never ready, and whose pods run while
 * `running` names one; that keeps what it was asked of ConfigMaps in `configMaps`, the ConfigMap's
 * path `named` says, and what the store's shell was given in `shell`, failing while `shellFails` is
 * there; a crictl of k3s's that keeps what it pulled in `pulled`, failing while `pullFails` is
 * there; and an UPGRADE and an UPGRADE_CHECK, each saying it ran, UPGRADE failing while
 * `upgradeFails` is there.
 */
function machine() {
  const root = mkdtempSync(join(tmpdir(), "alasio-cut-over-"));
  const bin = join(root, "bin");
  const ran = join(root, "ran");
  const namespaces = join(root, "namespaces");
  const claims = join(root, "claims");
  const volumes = join(root, "volumes");
  const counts = join(root, "counts");
  const stopped = join(root, "stopped");
  const started = join(root, "started");
  const unready = join(root, "unready");
  const running = join(root, "running");
  const configMaps = join(root, "config-maps");
  const named = join(root, "named");
  const configMapsFail = join(root, "config-maps-fail");
  const changes = join(root, "changes");
  const unreadable = join(root, "unreadable");
  const active = join(root, "active");
  const shell = join(root, "shell");
  const shellFails = join(root, "shell-fails");
  const pulled = join(root, "pulled");
  const pullFails = join(root, "pull-fails");
  const upgradeFails = join(root, "upgrade-fails");
  const config = join(root, "config.json");
  for (const directory of [bin, volumes, counts, active]) mkdirSync(directory);
  writeFileSync(namespaces, "");
  writeFileSync(claims, "");
  writeFileSync(config, JSON.stringify({ target: { host: {} }, install: { host: { enabled: true } } }), { mode: 0o600 });
  // The claims, '<claim>\t<volume>' a line, each volume's directory volumes/<volume>; each part's count counts/<part>.
  writeFileSync(join(bin, "kubectl"), `#!/bin/sh
case "$*" in
  "get --raw=/readyz") exit 0 ;;
  "get namespace -l alasio.dev/branch -o jsonpath="*) cat '${namespaces}' ;;
  "-n alasio get pvc -o jsonpath="*) cat '${claims}' ;;
  "get pv "*" -o jsonpath="*) printf '%s' '${volumes}'/"$3" ;;
  "-n alasio get statefulset "*" -o jsonpath={.spec.replicas}") ! grep -qx "$5" '${unreadable}' 2>/dev/null && { cat '${counts}'/"$5" 2>/dev/null || echo 1; } ;;
  "-n alasio patch cronjob "*" --type merge -p "*|"-n alasio delete job "*" --cascade=foreground --wait") echo "$*" >>'${changes}' ;;
  "-n alasio get cronjob "*" -o jsonpath={range .status.active"*) cat '${active}'/"$5" 2>/dev/null || true ;;
  "-n alasio get statefulset alasio-seaweedfs-master -o jsonpath={.spec.template.spec.containers[?(@.name==\\"master\\")].image}") printf '%s' '${IMAGE}' ;;
  "-n alasio get statefulset "*" -o jsonpath={.spec.replicas}/{.status.replicas}")
    count=$(cat '${counts}'/"$5" 2>/dev/null || echo 1); echo "$count/$count" ;;
  "-n alasio get statefulset "*" -o jsonpath={.spec.replicas}/{.status.readyReplicas}"|"-n alasio get deployment alasio-seaweedfs -o jsonpath={.spec.replicas}/{.status.readyReplicas}")
    count=$(cat '${counts}'/"$5" 2>/dev/null || echo 1)
    if grep -qx "$5" '${unready}' 2>/dev/null; then echo "$count/"; else echo "$count/$count"; fi ;;
  "-n alasio get deployment alasio-seaweedfs -o jsonpath={.spec.template.spec.containers[?(@.name==\\"buckets\\")].command[2]}")
    printf '%s' 'shell() { printf "%s\\n" "$1" | weed shell -master=alasio-seaweedfs-master-0.alasio-seaweedfs-master.alasio:9333 -filer=alasio-seaweedfs-filer:8888; }' ;;
  "-n alasio exec -i deployment/alasio-seaweedfs -c buckets -- sh -c weed shell -master=alasio-seaweedfs-master-0.alasio-seaweedfs-master.alasio:9333 -filer=alasio-seaweedfs-filer:8888")
    cat >>'${shell}'; [ ! -e '${shellFails}' ] || echo "error: the shell fails, as the test asks" ;;
  "-n alasio get configmap ${STORE_SNAPSHOT_CONFIGMAP} -o jsonpath={.data.path} --ignore-not-found") cat '${named}' 2>/dev/null || true ;;
  "-n alasio patch configmap ${STORE_SNAPSHOT_CONFIGMAP} --type merge -p "*) echo "$*" >>'${configMaps}' ;;
  "-n alasio scale statefulset "*" --replicas="*)
    count=\${6#--replicas=}
    echo "$count" >'${counts}'/"$5"
    echo "$*" >>'${changes}'
    if [ "$count" = 0 ]; then echo "$5" >>'${stopped}'; else echo "$5" >>'${started}'; fi ;;
  "-n alasio rollout status statefulset "*" --timeout="*) ! grep -qx "$6" '${unready}' 2>/dev/null ;;
  "-n alasio get pod -l app.kubernetes.io/component in ("*") -o name") cat '${running}' 2>/dev/null || true ;;
  "-n alasio get pod -l "*" -o name") ;;
  "-n alasio delete configmap "*" --ignore-not-found") [ ! -e '${configMapsFail}' ] && echo "$*" >>'${configMaps}' ;;
  "-n alasio create configmap "*) echo "$*" >>'${configMaps}' ;;
  *) echo "kubectl $* is not the cut-over's" >&2; exit 2 ;;
esac
`, { mode: 0o755 });
  writeFileSync(join(bin, "k3s"), `#!/bin/sh
[ "$1 $2" = "crictl pull" ] && [ ! -e '${pullFails}' ] && echo "$3" >>'${pulled}'
`, { mode: 0o755 });
  const out = join(root, "out");
  const snapshot = join(out, "object-store");
  /** The cut-over, failing at `failAt` if given, with `env` besides, keeping none of the snapshot's filesystem free unless told: what it came to. */
  const cutOver = (failAt?: string, env: Readonly<Record<string, string>> = {}) => {
    const run = spawnSync("bash", [SCRIPT], {
      encoding: "utf8",
      env: {
        PATH: pathOf(bin),
        OUT: out,
        CONFIG: config,
        UPGRADE: `echo upgrade >>'${ran}' && [ ! -e '${upgradeFails}' ]`,
        UPGRADE_CHECK: `echo check >>'${ran}'`,
        STORE_FLOOR_BYTES: "0",
        ...(failAt === undefined ? {} : { FAIL_AT: failAt }),
        ...env,
      },
    });
    return { code: run.status, said: run.stderr };
  };
  /** The script `name` the snapshot carries, run as the machine's root runs it: what it came to. */
  const carried = (name: "restore.sh" | "delete.sh" | "pause.sh", ...args: string[]) => {
    const run = spawnSync("bash", [join(snapshot, name), ...args], { encoding: "utf8", env: { PATH: pathOf(bin) } });
    return { code: run.status, said: run.stderr };
  };
  /** The directory of `claim`'s volume. */
  const volume = (claim: string) => join(volumes, `pv-${claim}`);
  /** What of `file` was written since the last time it was asked, a line each. */
  const since = (file: string) => () => {
    const read = existsSync(file) ? readFileSync(file, "utf8").split("\n").filter(Boolean) : [];
    rmSync(file, { force: true });
    return read;
  };
  return {
    cutOver,
    /** What UPGRADE and UPGRADE_CHECK ran, in order, since the last time it was asked. */
    ran: since(ran),
    /** The cut-over's markers, of what it recorded. */
    marks: () => (existsSync(out) ? readdirSync(out).filter((name) => ["point-of-no-return", "upgraded"].includes(name)).sort() : []),
    namespaces: (listed: string) => writeFileSync(namespaces, listed),
    /**
     * Gives the cluster the bundled object store's claims, beside another's, each a volume of its own: the volume servers'
     * three volumes, each its data, its index and its info, the first's data longer than the head the cut-over keeps; the
     * masters' log, in a directory of its own; and the filers' store's dump.
     */
    store: () => {
      writeFileSync(claims, [...CLAIMS.map((claim) => `${claim}\tpv-${claim}`), "data-alasio-log-0\tpv-log", ""].join("\n"));
      // Each made by anyone to write, as local-path makes a volume's directory.
      for (const claim of CLAIMS) {
        mkdirSync(volume(claim), { recursive: true });
        chmodSync(volume(claim), 0o777);
      }
      for (const [id, length] of [[1, 10_000], [2, 3000], [3, 500]] as const) {
        writeFileSync(join(volume(VOLUME), `neon_${id}.dat`), bytesOf(length, id));
        writeFileSync(join(volume(VOLUME), `neon_${id}.idx`), bytesOf(16 * id, 100 + id));
        writeFileSync(join(volume(VOLUME), `neon_${id}.vif`), `{"version":3,"id":${id}}`);
      }
      mkdirSync(join(volume(MASTER), "m"), { recursive: true });
      writeFileSync(join(volume(MASTER), "m", "log"), bytesOf(700, 7));
      writeFileSync(join(volume(FILER_STORE), "dump.rdb"), bytesOf(900, 9));
    },
    volume,
    snapshot,
    /** Takes the directory of `claim`'s volume away, as of a volume kept elsewhere than this machine. */
    unkept: (claim: string) => rmSync(volume(claim), { recursive: true, force: true }),
    /** The parts the cut-over stopped, in order, since the last time it was asked. */
    stopped: since(stopped),
    /** The parts the cut-over started again, in order, since the last time it was asked. */
    started: since(started),
    /** Makes `part` never ready once started, or, given none, every part ready. */
    unready: (part?: string) => (part === undefined ? rmSync(unready, { force: true }) : writeFileSync(unready, `${part}\n`)),
    /** Makes the store's pod `pod` run, or, given none, none of its pods. */
    running: (pod?: string) => (pod === undefined ? rmSync(running, { force: true }) : writeFileSync(running, `pod/${pod}\n`)),
    /** What kubectl was asked of ConfigMaps, in order, since the last time it was asked. */
    configMaps: since(configMaps),
    /** The counts the cut-over kept of the object store's parts as it stopped them, a `<part>\t<count>` each. */
    replicas: () => readFileSync(join(out, "object-store-replicas"), "utf8").split("\n").filter(Boolean),
    restore: (...args: string[]) => carried("restore.sh", ...args),
    deleteSnapshot: (...args: string[]) => carried("delete.sh", ...args),
    pause: () => carried("pause.sh"),
    /** What kubectl was asked to change of the store's and Neon's StatefulSets, the CronJobs and their Jobs, in order, since the last time it was asked. */
    changes: since(changes),
    /** Makes the count of the StatefulSet `part` unreadable, or, given none, every one readable. */
    unreadable: (part?: string) => (part === undefined ? rmSync(unreadable, { force: true }) : writeFileSync(unreadable, `${part}\n`)),
    /** Makes the Jobs `jobs` of the CronJob `cronJob` run. */
    active: (cronJob: string, jobs: readonly string[]) => writeFileSync(join(active, cronJob), jobs.map((job) => `${job}\n`).join("")),
    /** Stops each part of the store `parts` names, as one would by hand: its StatefulSet at none. */
    stop: (parts: readonly string[]) => { for (const part of parts) writeFileSync(join(counts, part), "0\n"); },
    configMapsFail: (fails: boolean) => (fails ? writeFileSync(configMapsFail, "") : rmSync(configMapsFail, { force: true })),
    /** Makes the ConfigMap name the snapshot at `path`, or none. */
    named: (path?: string) => (path === undefined ? rmSync(named, { force: true }) : writeFileSync(named, path)),
    /** What the store's shell was given, since the last time it was asked, a line each. */
    shell: since(shell),
    shellFails: (fails: boolean) => (fails ? writeFileSync(shellFails, "") : rmSync(shellFails, { force: true })),
    /** The images pulled onto the machine, since the last time it was asked. */
    pulled: since(pulled),
    pullFails: (fails: boolean) => (fails ? writeFileSync(pullFails, "") : rmSync(pullFails, { force: true })),
    bin,
    upgradeFails: (fails: boolean) => (fails ? writeFileSync(upgradeFails, "") : rmSync(upgradeFails, { force: true })),
    remove: () => rmSync(root, { recursive: true, force: true }),
  };
}

test("the cut-over checks, upgrades past its point of no return, and says it is through", () => {
  const at = machine();
  try {
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(at.ran(), ["check", "upgrade"]);
    assert.deepEqual(at.marks(), ["point-of-no-return", "upgraded"]);
    // Of an installation whose object store is not the bundled one: none of it stopped, nor snapshotted.
    assert.match(run.said, /cutting over from 10\.0\.27[^\n]*\n[^\n]* checking that no branch environment exists[^\n]*\n[^\n]* checking that the upgrade can run[^\n]*\n[^\n]* the object store is not the bundled one: none of it to snapshot\n[^\n]* past the point of no return[^\n]*\n[^\n]* upgrading, from the cut-over begun at [^\n]*\n[^\n]* upgraded at \S+\n[^\n]* through\n$/u);
    assert.deepEqual([at.stopped(), at.configMaps()], [[], []]);
  } finally {
    at.remove();
  }
});

test("the cut-over stops the bundled object store past its point of no return, writers first, snapshots it, each volume's data linked, its size and its head kept, every other file copied, every directory of it root's to write, and starts it again, each part once its peers are ready, its compaction paused, before the upgrade and after it, once", () => {
  const at = machine();
  try {
    at.store();
    const before = Object.fromEntries(CLAIMS.map((claim) => [claim, tree(at.volume(claim))]));
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(at.ran(), ["check", "upgrade"]);
    assert.deepEqual(at.stopped(), STORE_PARTS);
    assert.deepEqual(at.started(), STARTED);
    // SeaweedFS 4.54 pulled onto the machine before the point of no return, as the release pins it.
    assert.deepEqual(at.pulled(), [STORE_IMAGE]);
    // Each claim's files as they were: each volume's data the very file the store keeps, every other file a copy.
    for (const claim of CLAIMS) {
      const files = join(at.snapshot, claim, "files");
      assert.deepEqual(tree(files), before[claim], claim);
      for (const [path, held] of Object.entries(before[claim] ?? {})) {
        if (held !== null) assert.equal(statSync(join(files, path)).ino === statSync(join(at.volume(claim), path)).ino, path.endsWith(".dat"), `${claim}/${path}`);
      }
    }
    assert.equal(existsSync(join(at.snapshot, "data-alasio-log-0")), false);
    assert.deepEqual(readFileSync(join(at.snapshot, VOLUME, "sizes"), "utf8").split("\n").filter(Boolean).sort(), ["neon_1.dat\t10000", "neon_2.dat\t3000", "neon_3.dat\t500"]);
    assert.deepEqual(tree(join(at.snapshot, VOLUME, "heads")), { "neon_1.dat": bytesOf(HEAD_BYTES, 1), "neon_2.dat": bytesOf(3000, 2), "neon_3.dat": bytesOf(500, 3) });
    for (const claim of [MASTER, FILER_STORE]) assert.equal(readFileSync(join(at.snapshot, claim, "sizes"), "utf8"), "", claim);
    // What the restore and the deletion read beside, each written mode 0700.
    assert.equal(readFileSync(join(at.snapshot, "image"), "utf8"), IMAGE);
    assert.deepEqual(readFileSync(join(at.snapshot, "counts"), "utf8").split("\n").filter(Boolean), STORE_PARTS.map((part) => `${part}\t1`));
    assert.deepEqual(readFileSync(join(at.snapshot, "claims"), "utf8").split("\n").filter(Boolean).map((line) => line.split("\t")[0]).sort(), [...CLAIMS].sort());
    assert.equal(readFileSync(join(at.snapshot, "floor"), "utf8"), "0\n");
    for (const name of ["restore.sh", "delete.sh", "pause.sh"]) assert.equal(statSync(join(at.snapshot, name)).mode & 0o777, 0o700, name);
    // Every directory of it, and OUT, written by its owner alone, the claims' own made 0777 among them, and searchable by the
    // configuration owner's group alone; no file it copied read by any other account; each volume's data, which it shares with
    // the store, as the store keeps it.
    for (const [path, held] of Object.entries({ "..": null, ...tree(at.snapshot) })) {
      const { mode, gid } = statSync(join(at.snapshot, path));
      if (held === null) {
        assert.deepEqual([mode & 0o077, gid], [0o010, process.getgid?.()], path);
      } else if (/(^|\/)files\/[^/]*\.dat$/u.test(path)) {
        assert.equal(mode & 0o777, 0o644, path);
      } else {
        assert.equal(mode & 0o007, 0, path);
      }
    }
    // The ConfigMap alasio status reads, made anew: the files the snapshot links, and what it copied, the volumes' indexes and
    // infos, the masters' log and the filers' store's dump; and, once the upgrade is through, from when the compaction is paused.
    assert.deepEqual(readFileSync(join(at.snapshot, "volumes"), "utf8").split("\n").filter(Boolean).sort(), [1, 2, 3].map((id) => join(at.snapshot, VOLUME, "files", `neon_${id}.dat`)));
    const taken = readFileSync(join(at.snapshot, "taken"), "utf8").trim();
    const keepUntil = new Date(Date.parse(taken) + 48 * 3600_000).toISOString().replace(/\.000Z$/u, "Z");
    const copied = 16 * (1 + 2 + 3) + [1, 2, 3].reduce((total, id) => total + `{"version":3,"id":${id}}`.length, 0) + 700 + 900;
    const [deleted, made, patched, ...more] = at.configMaps();
    assert.deepEqual([deleted, made, more], [
      `-n alasio delete configmap ${STORE_SNAPSHOT_CONFIGMAP} --ignore-not-found`,
      `-n alasio create configmap ${STORE_SNAPSHOT_CONFIGMAP} --from-literal=path=${at.snapshot} --from-literal=taken=${taken} --from-literal=keepUntil=${keepUntil} --from-literal=floorBytes=0 --from-literal=copiedBytes=${copied} --from-file=volumes=${join(at.snapshot, "volumes")}`,
      [],
    ]);
    assert.match(patched ?? "", new RegExp(`^-n alasio patch configmap ${STORE_SNAPSHOT_CONFIGMAP} --type merge -p \\{"data":\\{"vacuumPausedAt":"\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\dZ"\\}\\}$`, "u"));
    // The compaction paused once the store started again on 4.48, and again on 4.54, the upgrade's masters knowing nothing of it.
    assert.deepEqual(at.shell(), ["lock", "volume.vacuum.disable", "unlock", "lock", "volume.vacuum.disable", "unlock"]);
    assert.match(run.said, /checking that the upgrade can run[^\n]*\n[^\n]* checking that the object store can be snapshotted, to \S+\/object-store\n[^\n]* the object store's snapshot links 3 volumes' data and copies 26332 bytes, leaving \d+ free on \S+, of \d+\n[^\n]* pulling SeaweedFS 4\.54 onto this machine \(chrislusf\/seaweedfs:4\.54@sha256:[0-9a-f]{64}\), for the store's parts to start on it at once\n[^\n]* past the point of no return[^\n]*\n[^\n]* stopping the object store[^\n]*\n/u);
    assert.match(run.said, new RegExp(`(snapshotted the object store's data-alasio-seaweedfs-[a-z-]+-0, \\d volumes' data linked and \\d+ bytes copied, in \\d+ s, to \\S+\\n[^\\n]* ){3}the object store snapshotted, to \\S+: restore\\.sh --disaster-recovery there puts it back, its parts stopped; delete\\.sh may delete it from \\S+\\n[^\\n]* the object store started again, each part ready, on ${IMAGE.replaceAll(".", "\\.")}\\n[^\\n]* the object store's compaction paused, so its snapshot holds no more of the disk\\n[^\\n]* upgrading, from the cut-over[^\\n]*\\n[^\\n]* the object store's compaction paused on SeaweedFS 4\\.54, until its snapshot is deleted\\n`, "u"));
    // Once: a run again snapshots, stops and starts nothing, and pauses the compaction again, which a master started anew forgets.
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.deepEqual([at.stopped(), at.started(), at.pulled()], [[], [], []]);
    assert.doesNotMatch(again.said, /snapshotted/u);
    assert.deepEqual(at.shell(), ["lock", "volume.vacuum.disable", "unlock"]);
  } finally {
    at.remove();
  }
});

test("the cut-over pulls onto the machine the SeaweedFS its release pins, and refuses one it cannot pull before its point of no return, stopping nothing", () => {
  const config = installConfigOf({}, { claude: false, target: "host" });
  assert.ok(Result.isSuccess(config));
  assert.equal(STORE_IMAGE, imageReference(config.success.objectStore.bundled.image));
  const at = machine();
  try {
    at.store();
    at.pullFails(true);
    const refused = at.cutOver();
    assert.notEqual(refused.code, 0);
    assert.match(refused.said, /FAILED: SeaweedFS 4\.54 \(chrislusf\/seaweedfs:4\.54@sha256:[0-9a-f]{64}\) could not be pulled onto this machine \(k3s crictl pull\), which the upgrade starts the object store's parts on: nothing is changed, and the cut-over run again pulls it again\n$/u);
    assert.deepEqual([at.marks(), at.stopped(), at.ran()], [[], [], ["check"]]);
  } finally {
    at.remove();
  }
});

test("the cut-over whose snapshot of a claim fails at any step, or is not whole, fails before it is renamed whole or said taken, and a run again snapshots it again", () => {
  const at = machine();
  try {
    at.store();
    // A file it cannot read, as of a copy that fails: set -e holds no step of a function its caller tests.
    chmodSync(join(at.volume(VOLUME), "neon_2.idx"), 0o000);
    const unread = at.cutOver();
    chmodSync(join(at.volume(VOLUME), "neon_2.idx"), 0o644);
    assert.notEqual(unread.code, 0);
    assert.match(unread.said, /FAILED: the claim data-alasio-seaweedfs-volume-0 could not be snapshotted whole from \S+, and its snapshot is not taken: the cut-over run again snapshots it again\n$/u);
    // Failed by the copy itself, which says why, the snapshot never checked.
    assert.match(unread.said, /cp: [^\n]*neon_2\.idx[^\n]*Permission denied/u);
    assert.doesNotMatch(unread.said, /the snapshot of /u);
    assert.equal(existsSync(join(at.snapshot, VOLUME)), false);
    assert.equal(existsSync(join(at.snapshot, "..", "object-store-snapshotted")), false);
    assert.deepEqual(at.configMaps(), []);
    // A copy that says it copied, and did not, leaving the index linked to the store's own: not whole.
    writeFileSync(join(at.bin, "cp"), `#!/bin/sh\ncase "$*" in *--remove-destination*neon_2.idx*) exit 0 ;; esac\nexec /bin/cp "$@"\n`, { mode: 0o755 });
    const linked = at.cutOver();
    assert.notEqual(linked.code, 0);
    assert.match(linked.said, /the snapshot of \S+\/neon_2\.idx is linked to it, not a copy\n[^\n]* FAILED: the claim data-alasio-seaweedfs-volume-0 could not be snapshotted whole/u);
    assert.equal(existsSync(join(at.snapshot, VOLUME)), false);
    rmSync(join(at.bin, "cp"));
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.deepEqual(tree(join(at.snapshot, VOLUME, "files")), tree(at.volume(VOLUME)));
  } finally {
    at.remove();
  }
});

test("the cut-over refuses an object store's volume it cannot snapshot, one on another filesystem than the snapshot, or a filesystem the snapshot would leave less free than the floor, before its point of no return, stopping nothing", () => {
  const at = machine();
  try {
    at.store();
    at.unkept(VOLUME);
    const unkept = at.cutOver();
    assert.notEqual(unkept.code, 0);
    assert.match(unkept.said, /FAILED: the claim data-alasio-seaweedfs-volume-0 is kept in no directory of this machine/u);
    assert.deepEqual([at.marks(), at.stopped()], [[], []]);
    at.store();
    // The claims on a filesystem of their own.
    writeFileSync(join(at.bin, "df"), df(FLOOR_BYTES * 2, "/mnt/claims"), { mode: 0o755 });
    const elsewhere = at.cutOver();
    assert.notEqual(elsewhere.code, 0);
    assert.match(elsewhere.said, /FAILED: the claim data-alasio-seaweedfs-[a-z-]+-0 is kept on \/mnt\/claims, and \S+ on \/, where its volumes cannot be hard-linked: give STORE_SNAPSHOT a directory on \/mnt\/claims\n$/u);
    assert.deepEqual([at.marks(), at.stopped()], [[], []]);
    // 10 MB free, which the snapshot's copies, a tenth more with them, would leave under a floor of 9.999 MB.
    writeFileSync(join(at.bin, "df"), df(10_000_000), { mode: 0o755 });
    const short = at.cutOver(undefined, { STORE_FLOOR_BYTES: "9999000" });
    assert.notEqual(short.code, 0);
    assert.match(short.said, /FAILED: the snapshot of the object store copies 28965 bytes with a tenth more, which would leave 9971035 of the 10000000 free on \/, under the 9999000 kept free \(STORE_FLOOR_BYTES\): make room there\n$/u);
    assert.deepEqual([at.marks(), at.stopped()], [[], []]);
    // The floor kept unless told otherwise, 60 GiB: refused a byte short of it, taken with room.
    // The snapshot's copies: the volumes' indexes and infos, the masters' log, the filers' store's dump and each volume's head, a tenth more.
    const needed = 28_965;
    writeFileSync(join(at.bin, "df"), df(FLOOR_BYTES + needed - 1), { mode: 0o755 });
    const unfloored = at.cutOver(undefined, { STORE_FLOOR_BYTES: "" });
    assert.match(unfloored.said, new RegExp(`FAILED: the snapshot of the object store [^\\n]* under the ${FLOOR_BYTES} kept free`, "u"));
    writeFileSync(join(at.bin, "df"), df(FLOOR_BYTES + needed), { mode: 0o755 });
    const floored = at.cutOver("before-the-point-of-no-return", { STORE_FLOOR_BYTES: "" });
    assert.match(floored.said, new RegExp(`leaving ${FLOOR_BYTES} free on /, of ${FLOOR_BYTES + needed}\\n[^\\n]* pulling SeaweedFS 4\\.54[^\\n]*\\n[^\\n]* FAILED: a failure injected before the point of no return\\n$`, "u"));
    assert.deepEqual([at.marks(), at.stopped()], [[], []]);
  } finally {
    at.remove();
  }
});

test("the cut-over whose object store's part is not ready again fails, and a run again starts it again, snapshotting nothing again; an upgrade failing leaves the store running as its StatefulSets say, which it says, with its snapshot", () => {
  const at = machine();
  try {
    at.store();
    at.unready("alasio-seaweedfs-master");
    const unready = at.cutOver();
    assert.notEqual(unready.code, 0);
    assert.match(unready.said, /FAILED: the object store's alasio-seaweedfs-master was not ready within 600 s of its start \(kubectl -n alasio describe statefulset alasio-seaweedfs-master\): the cut-over run again starts it\n$/u);
    assert.deepEqual([at.ran(), at.stopped(), at.started()], [["check"], STORE_PARTS, STARTED.slice(0, 2)]);
    assert.deepEqual(at.replicas(), STORE_PARTS.map((part) => `${part}\t1`));
    at.unready();
    at.upgradeFails(true);
    const failed = at.cutOver();
    assert.notEqual(failed.code, 0);
    assert.ok(failed.said.endsWith(`FAILED: the upgrade failed (upgrade.log), the object store running as its StatefulSets say, its masters on ${IMAGE}, its snapshot kept in ${at.snapshot}: the cut-over run again goes on with it\n`), failed.said);
    assert.doesNotMatch(failed.said, /snapshotted/u);
    assert.deepEqual([at.ran(), at.stopped(), at.started()], [["upgrade"], [], STARTED]);
    at.upgradeFails(false);
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.deepEqual([at.ran(), at.stopped(), at.started()], [["upgrade"], [], STARTED]);
    // The compaction that cannot be paused once the upgrade is through fails the run, which a run again pauses.
    at.shellFails(true);
    const unpaused = at.cutOver();
    assert.notEqual(unpaused.code, 0);
    assert.match(unpaused.said, /the store's shell did not pause its compaction \(volume\.vacuum\.disable\)\n[^\n]* FAILED: the object store's compaction could not be paused on SeaweedFS 4\.54: the cut-over run again pauses it\n$/u);
  } finally {
    at.remove();
  }
});

test("the snapshot's restore refuses unless told it is a disaster's recovery, while a part of the store is not stopped or runs, or as a symbolic link is in either tree, changing nothing; then stops Neon's writers and puts each claim back as it was snapshotted, saying it put back the store alone, and how Neon and workspace storage recover", () => {
  const at = machine();
  try {
    at.store();
    const before = Object.fromEntries(CLAIMS.map((claim) => [claim, tree(at.volume(claim))]));
    assert.equal(at.cutOver().code, 0);
    at.stopped();
    const volume = at.volume(VOLUME);
    // What SeaweedFS 4.54 does to the volumes after. It appends to a volume's data, and to its index.
    appendFileSync(join(volume, "neon_1.dat"), bytesOf(4000, 50));
    appendFileSync(join(volume, "neon_1.idx"), bytesOf(16, 51));
    // It writes a volume's superblock, as its replication is configured.
    const superblock = openSync(join(volume, "neon_2.dat"), "r+");
    writeSync(superblock, Buffer.from([3, 1]), 0, 2, 0);
    closeSync(superblock);
    // It compacts a volume, writing it anew and renaming it over the old one.
    writeFileSync(join(volume, "neon_3.cpd"), bytesOf(200, 60));
    renameSync(join(volume, "neon_3.cpd"), join(volume, "neon_3.dat"));
    writeFileSync(join(volume, "neon_3.idx"), bytesOf(8, 61));
    // It removes a file, and makes a volume the masters as they were know nothing of.
    rmSync(join(volume, "neon_2.idx"));
    writeFileSync(join(volume, "neon_4.dat"), bytesOf(100, 70));
    writeFileSync(join(volume, "neon_4.idx"), bytesOf(16, 71));
    mkdirSync(join(volume, "made"));
    writeFileSync(join(volume, "made", "since"), "4.54");
    // Its masters and its filers' store write their own state, a directory of it removed.
    rmSync(join(at.volume(MASTER), "m"), { recursive: true });
    writeFileSync(join(at.volume(FILER_STORE), "dump.rdb"), bytesOf(800, 80));
    const changed = Object.fromEntries(CLAIMS.map((claim) => [claim, tree(at.volume(claim))]));
    for (const claim of CLAIMS) assert.notDeepEqual(changed[claim], before[claim], claim);
    const unchanged = (said: string) => {
      for (const claim of CLAIMS) assert.deepEqual(tree(at.volume(claim)), changed[claim], `${claim}: ${said}`);
      assert.deepEqual(at.stopped(), [], said);
    };
    // Not told it is a disaster's recovery.
    const untold = at.restore();
    assert.notEqual(untold.code, 0);
    assert.match(untold.said, /FAILED: this puts back the object store alone, as it was before its upgrade \(\S+\), dropping all it took since: Neon's safekeepers and pageservers, and workspace storage's metadata, would be ahead of it[^\n]*; run it as \S+\/restore\.sh --disaster-recovery where that is what the store's loss calls for\n$/u);
    unchanged(untold.said);
    // A part of the store not stopped, its StatefulSet at its count.
    const counted = at.restore("--disaster-recovery");
    assert.notEqual(counted.code, 0);
    assert.match(counted.said, /FAILED: the object store's alasio-seaweedfs-filer is at 1 replicas: stop each of its parts first/u);
    unchanged(counted.said);
    // Each at none, but a pod of one runs still.
    at.stop(STORE_PARTS);
    at.running("alasio-seaweedfs-volume-0");
    const refused = at.restore("--disaster-recovery");
    assert.notEqual(refused.code, 0);
    assert.match(refused.said, /FAILED: the object store runs \(pod\/alasio-seaweedfs-volume-0 \): wait until none of its parts' pods is left/u);
    unchanged(refused.said);
    at.running();
    // A symbolic link in a claim's directory, or in its snapshot, which it would write through.
    for (const link of [join(volume, "neon_9.dat"), join(at.snapshot, VOLUME, "heads", "neon_9.dat")]) {
      symlinkSync("/etc/hostname", link);
      const linked = at.restore("--disaster-recovery");
      rmSync(link);
      assert.notEqual(linked.code, 0);
      assert.match(linked.said, /FAILED: the claim data-alasio-seaweedfs-volume-0's snapshot \(\S+\) or its directory \(\S+\) holds a symbolic link, which the restore would write through: nothing is changed\n$/u);
      unchanged(linked.said);
    }
    const restored = at.restore("--disaster-recovery");
    assert.equal(restored.code, 0, restored.said);
    for (const claim of CLAIMS) assert.deepEqual(tree(at.volume(claim)), before[claim], claim);
    // Each volume's data the snapshot's file again, as linked.
    for (const id of [1, 2, 3]) assert.equal(statSync(join(volume, `neon_${id}.dat`)).ino, statSync(join(at.snapshot, VOLUME, "files", `neon_${id}.dat`)).ino, `neon_${id}.dat`);
    // Neon's writers stopped first, their counts kept.
    assert.deepEqual(at.stopped(), NEON_WRITERS);
    assert.equal(readFileSync(join(at.snapshot, "neon-counts"), "utf8"), NEON_WRITERS.map((part) => `${part}\t1\n`).join(""));
    const said = restored.said;
    assert.ok(said.includes(`the object store restored as it was snapshotted, ${readFileSync(join(at.snapshot, "taken"), "utf8").trim()}, and it alone: start it on ${IMAGE}, as 10.0.27 ran it`), said);
    assert.ok(said.includes("its masters, started anew, compact it again"), said);
    assert.ok(said.includes("Neon's safekeepers and pageservers are stopped (alasio-neon-safekeeper=1 alasio-neon-pageserver=1 ): what they hold is ahead of the store, so recover Neon's databases from their newest dump"), said);
    assert.ok(said.includes("workspace storage's metadata, in its Valkey, is ahead of the store too: load its newest JuiceFS dump"), said);
  } finally {
    at.remove();
  }
});

test("the snapshot's deletion refuses until it has been kept 48 hours, but told --under-floor while its disk is under the floor and the store is ready, or as its ConfigMap names another; then resumes the compaction, deletes it, and its ConfigMap last, and nothing of the store's", () => {
  const at = machine();
  try {
    at.store();
    assert.equal(at.cutOver(undefined, { STORE_FLOOR_BYTES: "5000000" }).code, 0);
    at.configMaps();
    at.shell();
    const kept = () => {
      assert.ok(existsSync(join(at.snapshot, VOLUME, "files", "neon_1.dat")));
      assert.deepEqual([at.configMaps(), at.shell()], [[], []]);
    };
    const early = at.deleteSnapshot();
    assert.notEqual(early.code, 0);
    assert.match(early.said, /FAILED: the snapshot \S+ is kept until \S+, for the store to be seen well on SeaweedFS 4\.54 first: run this again then, or sooner as \S+\/delete\.sh --under-floor, which deletes it only while its disk has less than 5000000 free and every part of the store is ready\n$/u);
    kept();
    // Told --under-floor: refused while its disk has room above the floor, and while a part of the store is not ready.
    writeFileSync(join(at.bin, "df"), df(5_000_000), { mode: 0o755 });
    const roomy = at.deleteSnapshot("--under-floor");
    assert.notEqual(roomy.code, 0);
    assert.match(roomy.said, /FAILED: --under-floor, but its disk has 5000000 free, not under the floor of 5000000: it is kept until \S+\n$/u);
    kept();
    writeFileSync(join(at.bin, "df"), df(4_999_999), { mode: 0o755 });
    at.unready("alasio-seaweedfs-volume");
    const unready = at.deleteSnapshot("--under-floor");
    assert.notEqual(unready.code, 0);
    assert.match(unready.said, /alasio-seaweedfs-volume has 1\/ of its replicas ready\nFAILED: --under-floor, but the object store is not ready, which its snapshot may yet be needed for: it is kept\n$/u);
    kept();
    at.unready();
    // Its ConfigMap naming another snapshot.
    at.named("/elsewhere/object-store");
    const other = at.deleteSnapshot("--under-floor");
    assert.notEqual(other.code, 0);
    assert.match(other.said, /FAILED: the ConfigMap alasio-object-store-snapshot names the snapshot \/elsewhere\/object-store, not \S+: nothing is deleted\n$/u);
    kept();
    at.named(at.snapshot);
    // The compaction not resumed, as the store does not answer: kept.
    at.shellFails(true);
    const unanswered = at.deleteSnapshot("--under-floor");
    assert.notEqual(unanswered.code, 0);
    assert.match(unanswered.said, /FAILED: the object store's compaction could not be resumed \(volume\.vacuum\.enable, in weed shell\), so the snapshot is kept: run this again once the store answers\n$/u);
    assert.ok(existsSync(join(at.snapshot, VOLUME, "files", "neon_1.dat")));
    assert.deepEqual(at.configMaps(), []);
    at.shell();
    at.shellFails(false);
    const deleted = at.deleteSnapshot("--under-floor");
    assert.equal(deleted.code, 0, deleted.said);
    assert.equal(existsSync(at.snapshot), false);
    assert.deepEqual([at.shell(), at.configMaps()], [["lock", "volume.vacuum.enable", "unlock"], [`-n alasio delete configmap ${STORE_SNAPSHOT_CONFIGMAP} --ignore-not-found`]]);
    assert.deepEqual(readFileSync(join(at.volume(VOLUME), "neon_1.dat")), bytesOf(10_000, 1));
  } finally {
    at.remove();
  }
});

test("the snapshot's deletion, once it has been kept 48 hours, needs no word of the floor; and deletes the snapshot before its ConfigMap, which names it while it is there", () => {
  for (const configMapFails of [false, true]) {
    const at = machine();
    try {
      at.store();
      assert.equal(at.cutOver().code, 0);
      at.configMaps();
      // Taken 47 hours ago, refused still; 49 hours ago, deleted.
      const ago = (hours: number) => writeFileSync(join(at.snapshot, "taken"), `${new Date(Date.now() - hours * 3600_000).toISOString()}\n`);
      ago(47);
      assert.notEqual(at.deleteSnapshot().code, 0);
      ago(49);
      at.configMapsFail(configMapFails);
      const deleted = at.deleteSnapshot();
      assert.equal(existsSync(at.snapshot), false);
      if (configMapFails) {
        assert.notEqual(deleted.code, 0);
        assert.match(deleted.said, /FAILED: the snapshot \S+ is deleted, but its ConfigMap alasio-object-store-snapshot could not be: delete it \(kubectl -n alasio delete configmap alasio-object-store-snapshot\)\n$/u);
        assert.deepEqual(at.configMaps(), []);
      } else {
        assert.equal(deleted.code, 0, deleted.said);
        assert.deepEqual(at.configMaps(), [`-n alasio delete configmap ${STORE_SNAPSHOT_CONFIGMAP} --ignore-not-found`]);
      }
    } finally {
      at.remove();
    }
  }
});

test("the snapshot's pause, for a master that restarted and forgot the compaction's pause, pauses it again and tells the ConfigMap from when; failing as the store does not answer", () => {
  const at = machine();
  try {
    at.store();
    assert.equal(at.cutOver().code, 0);
    at.shell();
    at.configMaps();
    const paused = at.pause();
    assert.equal(paused.code, 0, paused.said);
    assert.match(paused.said, /^the object store's compaction paused again, until its snapshot \S+ is deleted\n$/u);
    assert.deepEqual(at.shell(), ["lock", "volume.vacuum.disable", "unlock"]);
    const [patched, ...more] = at.configMaps();
    assert.deepEqual(more, []);
    assert.match(patched ?? "", new RegExp(`^-n alasio patch configmap ${STORE_SNAPSHOT_CONFIGMAP} --type merge -p \\{"data":\\{"vacuumPausedAt":"\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\dZ"\\}\\}$`, "u"));
    at.shellFails(true);
    const unanswered = at.pause();
    assert.notEqual(unanswered.code, 0);
    assert.match(unanswered.said, /the store's shell did not pause its compaction \(volume\.vacuum\.disable\)\nFAILED: the object store's compaction could not be paused: run this again once the store answers\n$/u);
    assert.deepEqual(at.configMaps(), []);
  } finally {
    at.remove();
  }
});

test("the snapshot's restore suspends the CronJobs that make the dumps its recovery reads, ending a Job of theirs that runs, before it stops Neon; keeps Neon's counts whole or not at all; and says how to resume the CronJobs", () => {
  assert.equal(/^DUMP_WRITERS="([^"]*)"$/mu.exec(readFileSync(SCRIPT, "utf8"))?.[1], `${neonName("backup")} ${componentName("juicefs-dump")}`);
  const at = machine();
  try {
    at.store();
    assert.equal(at.cutOver().code, 0);
    at.stop(STORE_PARTS);
    at.stopped();
    at.changes();
    // A count of Neon's it cannot read, the second: nothing of the store changed, no count kept, none in part.
    at.unreadable("alasio-neon-pageserver");
    const unread = at.restore("--disaster-recovery");
    assert.notEqual(unread.code, 0);
    assert.match(unread.said, /FAILED: Neon's alasio-neon-pageserver could not be read: nothing of the store is changed\n$/u);
    assert.equal(existsSync(join(at.snapshot, "neon-counts")), false);
    assert.deepEqual(at.stopped(), []);
    at.changes();
    at.unreadable();
    at.active("alasio-neon-backup", ["alasio-neon-backup-29400000"]);
    const restored = at.restore("--disaster-recovery");
    assert.equal(restored.code, 0, restored.said);
    const suspend = (job: string) => `-n alasio patch cronjob ${job} --type merge -p {"spec":{"suspend":true}}`;
    assert.deepEqual(at.changes(), [
      suspend("alasio-neon-backup"),
      "-n alasio delete job alasio-neon-backup-29400000 --cascade=foreground --wait",
      suspend("alasio-juicefs-dump"),
      ...NEON_WRITERS.map((part) => `-n alasio scale statefulset ${part} --replicas=0`),
    ]);
    assert.equal(readFileSync(join(at.snapshot, "neon-counts"), "utf8"), NEON_WRITERS.map((part) => `${part}\t1\n`).join(""));
    assert.ok(restored.said.includes(`The CronJobs that make those dumps (alasio-neon-backup alasio-juicefs-dump) are suspended, so none replaces what the recovery reads: once it is through, resume each (kubectl -n alasio patch cronjob <name> --type merge -p '{"spec":{"suspend":false}}')`), restored.said);
  } finally {
    at.remove();
  }
});

test("the cut-over refuses a branch environment before it changes anything, naming each to delete, and no namespace that is no branch's own", () => {
  const at = machine();
  try {
    at.namespaces("alasio-branch-one\tone\nalasio-branch-two\ttwo\nalasio-sessions\tone\nother\t\n");
    const refused = at.cutOver();
    assert.notEqual(refused.code, 0);
    assert.match(refused.said, /FAILED: alasio upgrades main alone, and the branch environments 10\.0\.27 made [^\n]*\(alasio branch delete one, alasio branch delete two\)/u);
    assert.deepEqual([at.ran(), at.marks()], [[], []]);
    // A namespace labelled for a branch that is not that branch's own is no branch environment.
    at.namespaces("alasio-sessions\tone\n");
    assert.equal(at.cutOver().code, 0);
  } finally {
    at.remove();
  }
});

test("the cut-over failing before its point of no return leaves none, and a run again starts over", () => {
  const at = machine();
  try {
    const failed = at.cutOver("before-the-point-of-no-return");
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /checking that the upgrade can run[^\n]*\n[^\n]* the object store is not the bundled one[^\n]*\n[^\n]* FAILED: a failure injected before the point of no return\n$/u);
    assert.deepEqual([at.ran(), at.marks()], [["check"], []]);
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.match(again.said, /checking that no branch environment exists/u);
    assert.deepEqual([at.ran(), at.marks()], [["check", "upgrade"], ["point-of-no-return", "upgraded"]]);
  } finally {
    at.remove();
  }
});

test("the cut-over failing past its point of no return, or in its upgrade, goes on with the upgrade when run again, checking nothing again", () => {
  const at = machine();
  try {
    const failed = at.cutOver("past-the-point-of-no-return");
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /FAILED: a failure injected past the point of no return, before the upgrade\n$/u);
    assert.deepEqual([at.ran(), at.marks()], [["check"], ["point-of-no-return"]]);
    at.upgradeFails(true);
    const broken = at.cutOver();
    assert.notEqual(broken.code, 0);
    assert.match(broken.said, /FAILED: the upgrade failed \(upgrade\.log\): the cut-over run again goes on with it\n$/u);
    assert.deepEqual([at.ran(), at.marks()], [["upgrade"], ["point-of-no-return"]]);
    at.upgradeFails(false);
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.doesNotMatch(again.said, /checking that/u);
    assert.deepEqual([at.ran(), at.marks()], [["upgrade"], ["point-of-no-return", "upgraded"]]);
  } finally {
    at.remove();
  }
});

test("the cut-over failing once its upgrade is through goes on with what follows the upgrade alone when run again", () => {
  const at = machine();
  try {
    const failed = at.cutOver("after-the-upgrade");
    assert.notEqual(failed.code, 0);
    assert.match(failed.said, /FAILED: a failure injected once the upgrade was through\n$/u);
    assert.deepEqual([at.ran(), at.marks()], [["check", "upgrade"], ["point-of-no-return", "upgraded"]]);
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.doesNotMatch(again.said, /checking that|upgrading, from the cut-over/u);
    assert.match(again.said, /^[^\n]* upgraded at \S+\n[^\n]* through\n$/u);
    assert.deepEqual(at.ran(), []);
  } finally {
    at.remove();
  }
});

test("the cut-over refuses to run without what it needs, or a cluster to reach, before it changes anything", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-cut-over-"));
  try {
    const run = spawnSync("bash", [SCRIPT], { encoding: "utf8", env: { PATH: "/usr/bin:/bin" } });
    assert.notEqual(run.status, 0);
    assert.match(run.stderr, /UPGRADE: the upgrade of the release/u);
    const unchecked = spawnSync("bash", [SCRIPT], { encoding: "utf8", env: { PATH: "/usr/bin:/bin", UPGRADE: "true" } });
    assert.notEqual(unchecked.status, 0);
    assert.match(unchecked.stderr, /UPGRADE_CHECK: a command proving UPGRADE can run/u);
    const needs = { PATH: pathOf(join(root, "bin")), OUT: join(root, "out"), UPGRADE: "true", UPGRADE_CHECK: "true" };
    const unconfigured = spawnSync("bash", [SCRIPT], { encoding: "utf8", env: needs });
    assert.notEqual(unconfigured.status, 0);
    assert.match(unconfigured.stderr, /CONFIG: the file of the configuration/u);
    // A kubectl that reaches no cluster.
    mkdirSync(join(root, "bin"));
    writeFileSync(join(root, "bin", "kubectl"), "#!/bin/sh\nexit 1\n", { mode: 0o755 });
    const unread = spawnSync("bash", [SCRIPT], { encoding: "utf8", env: { ...needs, CONFIG: join(root, "none.json") } });
    assert.notEqual(unread.status, 0);
    assert.match(unread.stderr, /FAILED: CONFIG \S+none\.json cannot be read/u);
    writeFileSync(join(root, "config.json"), "{}");
    const unreached = spawnSync("bash", [SCRIPT], { encoding: "utf8", env: { ...needs, CONFIG: join(root, "config.json") } });
    assert.notEqual(unreached.status, 0);
    assert.match(unreached.stderr, /FAILED: kubectl does not reach the cluster/u);
    assert.ok(!existsSync(join(root, "out", "point-of-no-return")));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
