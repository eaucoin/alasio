/**
 * The cut-over of an installation of the release before (upgrades/from-10.0.26.sh), in how it
 * runs, without a cluster: its checks and refusals, its point of no return, and how a run
 * again goes on from where one before stopped. The end-to-end run's upgrade shard runs it on
 * an installation of that release (test/e2e/alasio.test.ts).
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { appendFileSync, closeSync, existsSync, mkdirSync, mkdtempSync, openSync, readdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { STORE_SNAPSHOT_CONFIGMAP } from "../cli/src/object-store.ts";

const SCRIPT = fileURLToPath(new URL("../upgrades/from-10.0.26.sh", import.meta.url));

/** The PATH the cut-over runs with: `bin`'s, and the system's. */
const pathOf = (bin: string) => [bin, "/usr/bin", "/bin"].join(delimiter);

/** The bundled object store's parts, by their StatefulSets, as the cut-over stops them. */
const STORE_PARTS = ["alasio-seaweedfs-filer", "alasio-seaweedfs-volume", "alasio-seaweedfs-master", "alasio-seaweedfs-filer-store"];
/** The same, as the cut-over starts them again: each once those it needs are ready. */
const STARTED = [...STORE_PARTS].reverse();
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
 * `running` names one; that keeps what it was asked of ConfigMaps in `configMaps`; and an UPGRADE
 * and an UPGRADE_CHECK, each saying it ran, UPGRADE failing while `upgradeFails` is there.
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
  const upgradeFails = join(root, "upgrade-fails");
  const config = join(root, "config.json");
  for (const directory of [bin, volumes, counts]) mkdirSync(directory);
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
  "-n alasio get statefulset "*" -o jsonpath={.spec.replicas}") cat '${counts}'/"$5" 2>/dev/null || echo 1 ;;
  "-n alasio get statefulset alasio-seaweedfs-master -o jsonpath={.spec.template.spec.containers[?(@.name==\\"master\\")].image}") printf '%s' '${IMAGE}' ;;
  "-n alasio scale statefulset "*" --replicas="*)
    count=\${6#--replicas=}
    echo "$count" >'${counts}'/"$5"
    if [ "$count" = 0 ]; then echo "$5" >>'${stopped}'; else echo "$5" >>'${started}'; fi ;;
  "-n alasio rollout status statefulset "*" --timeout="*) ! grep -qx "$6" '${unready}' 2>/dev/null ;;
  "-n alasio get pod -l app.kubernetes.io/component in ("*") -o name") cat '${running}' 2>/dev/null || true ;;
  "-n alasio get pod -l "*" -o name") ;;
  "-n alasio delete configmap "*" --ignore-not-found"|"-n alasio create configmap "*) echo "$*" >>'${configMaps}' ;;
  *) echo "kubectl $* is not the cut-over's" >&2; exit 2 ;;
esac
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
  const carried = (name: "restore.sh" | "delete.sh") => {
    const run = spawnSync("bash", [join(snapshot, name)], { encoding: "utf8", env: { PATH: pathOf(bin) } });
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
      for (const claim of CLAIMS) mkdirSync(volume(claim), { recursive: true });
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
    restore: () => carried("restore.sh"),
    deleteSnapshot: () => carried("delete.sh"),
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
    assert.match(run.said, /cutting over from 10\.0\.26[^\n]*\n[^\n]* checking that no branch environment exists[^\n]*\n[^\n]* checking that the upgrade can run[^\n]*\n[^\n]* the object store is not the bundled one: none of it to snapshot\n[^\n]* past the point of no return[^\n]*\n[^\n]* upgrading, from the cut-over begun at [^\n]*\n[^\n]* upgraded at \S+\n[^\n]* through\n$/u);
    assert.deepEqual([at.stopped(), at.configMaps()], [[], []]);
  } finally {
    at.remove();
  }
});

test("the cut-over stops the bundled object store past its point of no return, writers first, snapshots it, each volume's data linked, its size and its head kept, every other file copied, and starts it again, each part once its peers are ready, before the upgrade, once", () => {
  const at = machine();
  try {
    at.store();
    const before = Object.fromEntries(CLAIMS.map((claim) => [claim, tree(at.volume(claim))]));
    const run = at.cutOver();
    assert.equal(run.code, 0, run.said);
    assert.deepEqual(at.ran(), ["check", "upgrade"]);
    assert.deepEqual(at.stopped(), STORE_PARTS);
    assert.deepEqual(at.started(), STARTED);
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
    for (const name of ["restore.sh", "delete.sh"]) assert.equal(statSync(join(at.snapshot, name)).mode & 0o777, 0o700, name);
    // The ConfigMap alasio status reads, made anew: the files the snapshot links, and what it copied, the volumes' indexes and
    // infos, the masters' log and the filers' store's dump.
    assert.deepEqual(readFileSync(join(at.snapshot, "volumes"), "utf8").split("\n").filter(Boolean).sort(), [1, 2, 3].map((id) => join(at.snapshot, VOLUME, "files", `neon_${id}.dat`)));
    const taken = readFileSync(join(at.snapshot, "taken"), "utf8").trim();
    const keepUntil = new Date(Date.parse(taken) + 48 * 3600_000).toISOString().replace(/\.000Z$/u, "Z");
    const copied = 16 * (1 + 2 + 3) + [1, 2, 3].reduce((total, id) => total + `{"version":3,"id":${id}}`.length, 0) + 700 + 900;
    assert.deepEqual(at.configMaps(), [
      `-n alasio delete configmap ${STORE_SNAPSHOT_CONFIGMAP} --ignore-not-found`,
      `-n alasio create configmap ${STORE_SNAPSHOT_CONFIGMAP} --from-literal=path=${at.snapshot} --from-literal=taken=${taken} --from-literal=keepUntil=${keepUntil} --from-literal=copiedBytes=${copied} --from-file=volumes=${join(at.snapshot, "volumes")}`,
    ]);
    // Searchable by the owner as alasio status runs, down to the data files, none listable.
    for (const directory of [join(at.snapshot, ".."), at.snapshot, join(at.snapshot, VOLUME)]) assert.equal(statSync(directory).mode & 0o077, 0o011, directory);
    assert.match(run.said, /checking that the upgrade can run[^\n]*\n[^\n]* checking that the object store can be snapshotted, to \S+\/object-store\n[^\n]* the object store's snapshot links 3 volumes' data and copies 26332 bytes, leaving \d+ free on \S+, of \d+\n[^\n]* past the point of no return[^\n]*\n[^\n]* stopping the object store[^\n]*\n/u);
    assert.match(run.said, /(snapshotted the object store's data-alasio-seaweedfs-[a-z-]+-0, \d volumes' data linked and \d+ bytes copied, in \d+ s, to \S+\n[^\n]* ){3}the object store snapshotted, to \S+: restore\.sh there puts it back, its parts stopped; delete\.sh deletes it from \S+\n[^\n]* the object store started again, each part ready, on SeaweedFS 4\.48, as it was, for the upgrade to start it on 4\.54\n[^\n]* upgrading, from the cut-over/u);
    // Once: a run again snapshots, stops and starts nothing.
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.deepEqual([at.stopped(), at.started(), at.configMaps()], [[], [], []]);
    assert.doesNotMatch(again.said, /snapshotted/u);
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
    assert.match(floored.said, new RegExp(`leaving ${FLOOR_BYTES} free on /, of ${FLOOR_BYTES + needed}\\n[^\\n]* FAILED: a failure injected before the point of no return\\n$`, "u"));
    assert.deepEqual([at.marks(), at.stopped()], [[], []]);
  } finally {
    at.remove();
  }
});

test("the cut-over whose object store's part is not ready again fails, and a run again starts it again, snapshotting nothing again; an upgrade failing leaves the store running as before", () => {
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
    assert.match(failed.said, /FAILED: the upgrade failed \(upgrade\.log\): the cut-over run again goes on with it\n$/u);
    assert.doesNotMatch(failed.said, /snapshotted/u);
    assert.deepEqual([at.ran(), at.stopped(), at.started()], [["upgrade"], [], STARTED]);
    at.upgradeFails(false);
    const again = at.cutOver();
    assert.equal(again.code, 0, again.said);
    assert.deepEqual([at.ran(), at.stopped(), at.started()], [["upgrade"], [], STARTED]);
  } finally {
    at.remove();
  }
});

test("the snapshot's restore, refused while the store runs, puts each claim back as it was snapshotted: what the store made since removed, each file but a volume's data copied back, each volume's data linked back where it was replaced, cut to its size and its head written back", () => {
  const at = machine();
  try {
    at.store();
    const before = Object.fromEntries(CLAIMS.map((claim) => [claim, tree(at.volume(claim))]));
    assert.equal(at.cutOver().code, 0);
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
    for (const claim of CLAIMS) assert.notDeepEqual(tree(at.volume(claim)), before[claim], claim);
    // Refused while a part of the store runs, changing nothing.
    at.running("alasio-seaweedfs-volume-0");
    const refused = at.restore();
    assert.notEqual(refused.code, 0);
    assert.match(refused.said, /FAILED: the object store runs \(pod\/alasio-seaweedfs-volume-0 \): stop each of its parts first/u);
    assert.equal(statSync(join(volume, "neon_1.dat")).size, 14_000);
    at.running();
    const restored = at.restore();
    assert.equal(restored.code, 0, restored.said);
    for (const claim of CLAIMS) assert.deepEqual(tree(at.volume(claim)), before[claim], claim);
    // Each volume's data the snapshot's file again, as linked.
    for (const id of [1, 2, 3]) assert.equal(statSync(join(volume, `neon_${id}.dat`)).ino, statSync(join(at.snapshot, VOLUME, "files", `neon_${id}.dat`)).ino, `neon_${id}.dat`);
    assert.ok(restored.said.includes(`the object store restored as it was snapshotted, ${readFileSync(join(at.snapshot, "taken"), "utf8").trim()}; start it on ${IMAGE}, as 10.0.26 ran it`), restored.said);
  } finally {
    at.remove();
  }
});

test("the snapshot's deletion refuses until it has been kept 48 hours, then deletes it and its ConfigMap, and nothing of the store's", () => {
  const at = machine();
  try {
    at.store();
    assert.equal(at.cutOver().code, 0);
    at.configMaps();
    const early = at.deleteSnapshot();
    assert.notEqual(early.code, 0);
    assert.match(early.said, /FAILED: the snapshot \S+ is kept until \S+, for the store to be seen well on SeaweedFS 4\.54 first: run this again then\n$/u);
    assert.ok(existsSync(join(at.snapshot, VOLUME, "files", "neon_1.dat")));
    assert.deepEqual(at.configMaps(), []);
    // Taken 47 hours ago, refused still; 49 hours ago, deleted.
    const ago = (hours: number) => writeFileSync(join(at.snapshot, "taken"), `${new Date(Date.now() - hours * 3600_000).toISOString()}\n`);
    ago(47);
    assert.notEqual(at.deleteSnapshot().code, 0);
    ago(49);
    const deleted = at.deleteSnapshot();
    assert.equal(deleted.code, 0, deleted.said);
    assert.equal(existsSync(at.snapshot), false);
    assert.deepEqual(at.configMaps(), [`-n alasio delete configmap ${STORE_SNAPSHOT_CONFIGMAP} --ignore-not-found`]);
    assert.deepEqual(readFileSync(join(at.volume(VOLUME), "neon_1.dat")), bytesOf(10_000, 1));
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
    assert.match(refused.said, /FAILED: alasio upgrades main alone, and the branch environments 10\.0\.26 made [^\n]*\(alasio branch delete one, alasio branch delete two\)/u);
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
