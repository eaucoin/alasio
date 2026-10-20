import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, type StatsFs, statSync, symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";

import { Effect } from "effect";

import { holdsQuota, lookOnNode, mountPoints, reaching, WorkspaceRefused } from "../src/node-agent/workspaces.ts";
import { workspacesNode } from "./support/node-workspaces.ts";

const refusal = <A>(effect: Effect.Effect<A, unknown>) => Effect.runPromise(Effect.flip(effect)).then((error) => (error instanceof WorkspaceRefused ? error.status : error));

test("mountinfo's mount points are read with their escapes undone", () => {
  assert.deepEqual(mountPoints("36 35 98:0 /mnt1 /mnt/a\\040b rw - ext3 /dev/root rw\n37 35 0:50 / /var rw - tmpfs tmpfs rw\n"), ["/mnt/a b", "/var"]);
});

test("what is mounted is unreachable where reading it answers ENOTCONN or EIO, as FUSE answers of a mount whose process ended, and reachable otherwise, or where it does not answer in time", async () => {
  const failing = (code: string) => () => Promise.reject(Object.assign(new Error(code), { code }));
  assert.equal(await reaching(failing("ENOTCONN"))("/p"), false);
  assert.equal(await reaching(failing("EIO"))("/p"), false);
  assert.equal(await reaching(failing("EACCES"))("/p"), true);
  assert.equal(await reaching(() => Promise.resolve([]))("/p"), true);
  assert.equal(await reaching(() => new Promise(() => {}), 50)("/p"), true);
});

test("what is attached into a pod is looked at in the node's mount namespace, where its mount is, and unreachable as ls there says its mount is not connected, or fails to read it", async () => {
  const ran: string[] = [];
  const said = new Map([
    ["/attach/pool-0/ws", { exitCode: 0, stdout: "/attach/pool-0/ws\n", stderr: "" }],
    ["/attach/pool-1/ws", { exitCode: 1, stdout: "", stderr: "ls: /attach/pool-1/ws: Transport endpoint is not connected\n" }],
    ["/attach/pool-2/ws", { exitCode: 2, stdout: "", stderr: "ls: cannot access '/attach/pool-2/ws': Input/output error\n" }],
    ["/attach/pool-3/ws", { exitCode: 1, stdout: "", stderr: "ls: /attach/pool-3/ws: Permission denied\n" }],
  ]);
  const host = { run: (command: readonly string[]) => Effect.sync(() => (ran.push(command.join(" ")), said.get(command.at(-1) ?? "") ?? { exitCode: 0, stdout: "", stderr: "" })) };
  const reaches = reaching(lookOnNode(host));
  assert.deepEqual(await Promise.all([0, 1, 2, 3].map((n) => reaches(`/attach/pool-${n}/ws`))), [true, false, false, true]);
  assert.equal(ran[0], "ls -d -- /attach/pool-0/ws");
});

test("the pods a workspace is attached into are listed each by whether it is reachable, and none other of the node's mounts", async () => {
  const at = workspacesNode();
  try {
    at.files("fs-a");
    at.files("fs-b");
    at.pod("pool-0");
    at.pod("pool-1");
    await Effect.runPromise(at.workspaces.attach({ environment: "main", workspace: "fs-a" }, "pool-0", "t", {}));
    await Effect.runPromise(at.workspaces.attach({ environment: "main", workspace: "fs-b" }, "pool-1", "t", {}));
    at.mounts.push(join(at.attachRoot, "main", "pool-1", "ws", "deeper"), join(at.root, "elsewhere", "ws"));
    at.unreachable.add("main/pool-1");
    assert.deepEqual(await Effect.runPromise(at.workspaces.attachments), [{ namespace: "main", pod: "pool-0", reachable: true }, { namespace: "main", pod: "pool-1", reachable: false }]);
  } finally {
    at.done();
  }
});

test("pods of two namespaces sharing a name each have a directory of their own, each attached into and detached from apart", async () => {
  const at = workspacesNode();
  try {
    at.files("fs-a");
    mkdirSync(join(at.workspacesRoot, "alasio-branch-try-sessions", "fs-a", "files", "workspace"), { recursive: true });
    at.pod("pool-0");
    at.pod("pool-0", "alasio-branch-try-sessions");
    await Effect.runPromise(at.workspaces.attach({ environment: "main", workspace: "fs-a" }, "pool-0", "main-token", {}));
    await Effect.runPromise(at.workspaces.attach({ environment: "alasio-branch-try-sessions", workspace: "fs-a" }, "pool-0", "branch-token", {}));
    assert.deepEqual(at.mounts, [join(at.attachRoot, "main", "pool-0", "ws"), join(at.attachRoot, "alasio-branch-try-sessions", "pool-0", "ws")]);
    assert.deepEqual(
      ["main", "alasio-branch-try-sessions"].map((namespace) => readFileSync(join(at.attachRoot, namespace, "pool-0", "token"), "utf8")),
      ["main-token", "branch-token"],
    );
    await Effect.runPromise(at.workspaces.detach("alasio-branch-try-sessions", "pool-0", []));
    assert.deepEqual(at.mounts, [join(at.attachRoot, "main", "pool-0", "ws")]);
    assert.deepEqual((await Effect.runPromise(at.workspaces.attachments)).map(({ namespace, pod }) => `${namespace}/${pod}`), ["main/pool-0"]);
  } finally {
    at.done();
  }
});

test("a workspace is made with its quota, which is read back, its directories its agent's, JuiceFS told the metadata engine's password out of its arguments", async () => {
  const at = workspacesNode();
  try {
    const ref = { environment: "main", workspace: "fs-made" };
    await Effect.runPromise(at.workspaces.create(ref, 10 * 1024 ** 3 - 1));
    const files = join(at.workspacesRoot, "main", "fs-made", "files");
    for (const directory of [files, join(files, "workspace"), join(files, "home")]) assert.equal(statSync(directory).uid, process.getuid?.() ?? 1000, directory);
    assert.deepEqual(await Effect.runPromise(at.workspaces.quota(ref)), { capacityBytes: 10 * 1024 ** 3, usedBytes: 12 * 1024 ** 2 });
    // Made again: as it was, its quota set anew.
    await Effect.runPromise(at.workspaces.create(ref, 20 * 1024 ** 3));
    assert.deepEqual((await Effect.runPromise(at.workspaces.quota(ref)))?.capacityBytes, 20 * 1024 ** 3);
    assert.deepEqual(at.juicefsCalls(), [
      "quota set redis://valkey.alasio.svc:6379/1 --path /main/fs-made --capacity 10 META_PASSWORD=the password",
      "quota get redis://valkey.alasio.svc:6379/1 --path /main/fs-made META_PASSWORD=the password",
      "quota set redis://valkey.alasio.svc:6379/1 --path /main/fs-made --capacity 20 META_PASSWORD=the password",
      "quota get redis://valkey.alasio.svc:6379/1 --path /main/fs-made META_PASSWORD=the password",
    ]);
    // Of names that are none, or a quota that is none: refused, nothing made.
    assert.equal(await refusal(at.workspaces.create({ environment: "main", workspace: "../etc" }, 1024)), 400);
    assert.equal(await refusal(at.workspaces.create({ environment: "main", workspace: "fs-other" }, 0)), 400);
    assert.equal(existsSync(join(at.workspacesRoot, "main", "fs-other")), false);
    // Of one not there, none; of one there whose quota JuiceFS does not answer, its refusal, as the agent says it.
    assert.equal(await Effect.runPromise(at.workspaces.quota({ environment: "main", workspace: "fs-never" })), null);
    mkdirSync(join(at.workspacesRoot, "main", "fs-bare", "files"), { recursive: true });
    const unanswered = await Effect.runPromise(Effect.flip(at.workspaces.quota({ environment: "main", workspace: "fs-bare" })));
    assert.match(unanswered.message, /^juicefs quota get failed: no quota of \/main\/fs-bare$/u);
  } finally {
    at.done();
  }
});

test("a workspace is attached into a pod's directory with its binding's token beside it, and never into one that holds a mount", async () => {
  const at = workspacesNode();
  try {
    const files = at.files("fs-abc123");
    at.files("fs-def456");
    at.pod("pool-0");
    await Effect.runPromise(at.workspaces.attach({ environment: "main", workspace: "fs-abc123" }, "pool-0", "binding-token", { OTEL_EXPORTER_OTLP_ENDPOINT: "http://10.43.0.9:4318", OTEL_NOTE: "it's quoted" }));
    assert.deepEqual(at.mounts, [join(at.attachRoot, "main", "pool-0", "ws")]);
    assert.ok(at.ran.includes(`mount --bind ${files} ${join(at.attachRoot, "main", "pool-0", "ws")}`));
    // Its token written once its workspace is there, as its bayma starts, and a restore is restored, once it is.
    assert.deepEqual(at.tokenAtMount, [false]);
    const token = join(at.attachRoot, "main", "pool-0", "token");
    assert.equal(readFileSync(token, "utf8"), "binding-token");
    assert.equal(statSync(token).mode & 0o777, 0o400);
    // Its bayma's variables, as a shell reads them.
    const env = execFileSync("sh", ["-c", `. ${join(at.attachRoot, "main", "pool-0", "env")}; printf '%s|%s' "$OTEL_EXPORTER_OTLP_ENDPOINT" "$OTEL_NOTE"`], { encoding: "utf8" });
    assert.equal(env, "http://10.43.0.9:4318|it's quoted");
    // Another workspace into the same pod: refused, nothing mounted over the first.
    assert.equal(await refusal(at.workspaces.attach({ environment: "main", workspace: "fs-def456" }, "pool-0", "other-token", {})), 409);
    assert.equal(at.mounts.length, 1);
    assert.equal(readFileSync(token, "utf8"), "binding-token");
  } finally {
    at.done();
  }
});

test("an attach of a workspace there is not, or by names that are none, is refused", async () => {
  const at = workspacesNode();
  try {
    at.files("fs-abc123");
    at.pod("pool-0");
    assert.equal(await refusal(at.workspaces.attach({ environment: "main", workspace: "fs-abc123" }, "pool-0", "t", { "NOT A NAME": "x" })), 400);
    assert.equal(await refusal(at.workspaces.attach({ environment: "main", workspace: "fs-missing" }, "pool-0", "t", {})), 404);
    for (const [environment, workspace, pod] of [["main", "../etc", "pool-0"], ["Main", "fs-abc123", "pool-0"], ["main", "fs-abc123", "../pool"], ["main", "fs-abc123", "a/b"]] as const) {
      assert.equal(await refusal(at.workspaces.attach({ environment, workspace }, pod, "t", {})), 400, `${environment}/${workspace} into ${pod}`);
    }
    assert.deepEqual(at.mounts, []);
  } finally {
    at.done();
  }
});

test("a mount holds a directory's quota where a statfs of the directory says another size than of its root, as JuiceFS's client answers once it has loaded the quota", async () => {
  const sized = (sizes: Readonly<Record<string, number>>) => (path: string) =>
    sizes[path] === undefined ? Promise.reject(Object.assign(new Error(`${path}: no such file`), { code: "ENOENT" })) : Promise.resolve({ blocks: sizes[path], bsize: 4096 } as StatsFs);
  // Loaded: the directory's size is its quota's, 1 GiB, the root's its file system's, 1 PiB.
  assert.equal(await holdsQuota(sized({ "/w/main/fs-a": 262_144, "/w": 2 ** 38 }))("/w/main/fs-a", "/w"), true);
  // Not yet: the directory's is the root's.
  assert.equal(await holdsQuota(sized({ "/w/main/fs-a": 2 ** 38, "/w": 2 ** 38 }))("/w/main/fs-a", "/w"), false);
  // Neither read: not held.
  assert.equal(await holdsQuota(sized({ "/w": 2 ** 38 }))("/w/main/fs-a", "/w"), false);
});

test("a workspace is attached only once the node's mount holds its quota, waited for, and refused where it does not in time, nothing mounted", async () => {
  const at = workspacesNode({ quotaHeldWithinMs: 1_200 });
  try {
    const files = at.files("fs-abc123");
    at.files("fs-def456");
    at.pod("pool-0");
    at.pod("pool-1");
    at.unheld.add("fs-abc123");
    assert.equal(await refusal(at.workspaces.attach({ environment: "main", workspace: "fs-abc123" }, "pool-0", "t", {})), 503);
    assert.deepEqual(at.mounts, []);
    assert.equal(existsSync(join(at.attachRoot, "main", "pool-0", "token")), false);
    // The mount's own look, at the workspace's directory, which the quota is set on, beside the root.
    assert.equal(at.looks[0], `${join(files, "..")} ${at.workspacesRoot}`);
    // Held a moment after the attach began: attached then.
    at.unheld.add("fs-def456");
    setTimeout(() => at.unheld.delete("fs-def456"), 150);
    await Effect.runPromise(at.workspaces.attach({ environment: "main", workspace: "fs-def456" }, "pool-1", "t", {}));
    assert.deepEqual(at.mounts, [join(at.attachRoot, "main", "pool-1", "ws")]);
  } finally {
    at.done();
  }
});

test("a file is written into a workspace only once the node's mount holds its quota, and refused at once where it does not, nothing written, to be asked again", async () => {
  const at = workspacesNode({ quotaHeldWithinMs: 1_200 });
  try {
    const files = at.files("fs-abc123");
    const ref = { environment: "main", workspace: "fs-abc123" };
    at.unheld.add("fs-abc123");
    const asked = Date.now();
    assert.equal(await refusal(at.workspaces.write(ref, "home/.alasio/received/1/photo.jpg", Buffer.from("jpg"))), 503);
    assert.ok(Date.now() - asked < 1_200);
    assert.equal(existsSync(join(files, "home", ".alasio")), false);
    at.unheld.delete("fs-abc123");
    await Effect.runPromise(at.workspaces.write(ref, "home/.alasio/received/1/photo.jpg", Buffer.from("jpg")));
    assert.equal(readFileSync(join(files, "home", ".alasio", "received", "1", "photo.jpg"), "utf8"), "jpg");
    // Into one not there, nothing waited for: refused as one not there is.
    at.unheld.add("fs-missing");
    const started = Date.now();
    assert.equal(await refusal(at.workspaces.write({ environment: "main", workspace: "fs-missing" }, "home/note", Buffer.from("x"))), 403);
    assert.ok(Date.now() - started < 1_200);
  } finally {
    at.done();
  }
});

test("a workspace is cloned into another, of any environment, its files' owners and modes kept, its quota its own; listed by environment; and deleted, its quota with it", async () => {
  const at = workspacesNode();
  try {
    const source = { environment: "main", workspace: "fs-source" };
    await Effect.runPromise(at.workspaces.create(source, 1024 ** 3));
    const sourceFiles = join(at.workspacesRoot, "main", "fs-source", "files");
    writeFileSync(join(sourceFiles, "workspace", "kept"), "from the source");
    chmodSync(join(sourceFiles, "workspace", "kept"), 0o640);
    const fork = { environment: "alasio-branch-try-sessions", workspace: "fs-source" };
    // Its quota found off once after its first repair, as the clone's counts settle.
    at.inconsistentChecks(1);
    await Effect.runPromise(at.workspaces.clone(source, fork, 3 * 1024 ** 3));
    const forked = join(at.workspacesRoot, "alasio-branch-try-sessions", "fs-source", "files", "workspace", "kept");
    assert.equal(readFileSync(forked, "utf8"), "from the source");
    assert.equal(statSync(forked).mode & 0o777, 0o640);
    assert.equal((await Effect.runPromise(at.workspaces.quota(fork)))?.capacityBytes, 3 * 1024 ** 3);
    // Of one not there, or into one there already: refused.
    assert.equal(await refusal(at.workspaces.clone({ environment: "main", workspace: "fs-never" }, { environment: "main", workspace: "fs-other" }, 1024)), 404);
    assert.equal(await refusal(at.workspaces.clone(source, fork, 1024)), 409);
    // Listed by environment, of what are workspaces' names alone; of one with none, none.
    mkdirSync(join(at.workspacesRoot, "main", ".partial"), { recursive: true });
    assert.deepEqual(await Effect.runPromise(at.workspaces.list("main")), ["fs-source"]);
    assert.deepEqual(await Effect.runPromise(at.workspaces.list("alasio-branch-try-sessions")), ["fs-source"]);
    assert.deepEqual(await Effect.runPromise(at.workspaces.list("elsewhere")), []);
    assert.equal(await refusal(at.workspaces.list("../etc")), 400);
    // Deleted, its quota with it; again, nothing.
    await Effect.runPromise(at.workspaces.remove(fork));
    assert.equal(existsSync(join(at.workspacesRoot, "alasio-branch-try-sessions", "fs-source")), false);
    await Effect.runPromise(at.workspaces.remove(fork));
    assert.ok(at.juicefsCalls().some((call) => call.startsWith("quota delete redis://valkey.alasio.svc:6379/1 --path /alasio-branch-try-sessions/fs-source ")));
    // Cloned, its quota set, then its usage counted anew until a check finds it holds.
    const cloned = at.juicefsCalls().findIndex((call) => call.startsWith(`clone --preserve ${sourceFiles} `));
    assert.ok(cloned >= 0);
    assert.deepEqual(at.juicefsCalls().slice(cloned + 1, cloned + 6).map((call) => call.split(" META_PASSWORD")[0]), [
      "quota set redis://valkey.alasio.svc:6379/1 --path /alasio-branch-try-sessions/fs-source --capacity 3",
      "quota check redis://valkey.alasio.svc:6379/1 --path /alasio-branch-try-sessions/fs-source --repair",
      "quota check redis://valkey.alasio.svc:6379/1 --path /alasio-branch-try-sessions/fs-source",
      "quota check redis://valkey.alasio.svc:6379/1 --path /alasio-branch-try-sessions/fs-source --repair",
      "quota check redis://valkey.alasio.svc:6379/1 --path /alasio-branch-try-sessions/fs-source",
    ]);
    assert.equal(readFileSync(join(sourceFiles, "workspace", "kept"), "utf8"), "from the source");
  } finally {
    at.done();
  }
});

test("an attach for a restore's pod yet to be made makes its directory, and attaches into it, but never for a pool's pod, nor into one there already, nor through a symlink", async () => {
  const at = workspacesNode();
  try {
    at.files("fs-abc123");
    const ref = { environment: "main", workspace: "fs-abc123" };
    await Effect.runPromise(at.workspaces.attach(ref, "sandboxes-none-restore-0abc123", "t", {}, { made: true }));
    assert.deepEqual(at.mounts, [join(at.attachRoot, "main", "sandboxes-none-restore-0abc123", "ws")]);
    assert.equal(readFileSync(join(at.attachRoot, "main", "sandboxes-none-restore-0abc123", "token"), "utf8"), "t");
    // A pool's pod's, its directory kubelet's: refused by its name, nothing attached into it.
    at.pod("sandboxes-none-5d4f8b7c9-x2k8q");
    assert.equal(await refusal(at.workspaces.attach(ref, "sandboxes-none-5d4f8b7c9-x2k8q", "t", {}, { made: true })), 400);
    // A restore's there already, or a symlink to another's: refused, nothing attached.
    at.pod("sandboxes-none-restore-1abc123");
    symlinkSync(join(at.attachRoot, "main", "sandboxes-none-5d4f8b7c9-x2k8q"), join(at.attachRoot, "main", "sandboxes-none-restore-2abc123"));
    for (const pod of ["sandboxes-none-restore-1abc123", "sandboxes-none-restore-2abc123"]) assert.equal(await refusal(at.workspaces.attach(ref, pod, "t", {}, { made: true })), 409, pod);
    assert.equal(at.mounts.length, 1);
    assert.equal(existsSync(join(at.attachRoot, "main", "sandboxes-none-5d4f8b7c9-x2k8q", "token")), false);
  } finally {
    at.done();
  }
});

test("an attach into a pod's directory kubelet has not made, or that is no directory, is refused, and none is made", async () => {
  const at = workspacesNode();
  try {
    at.files("fs-abc123");
    assert.equal(await refusal(at.workspaces.attach({ environment: "main", workspace: "fs-abc123" }, "pool-0", "t", {})), 404);
    assert.equal(existsSync(join(at.attachRoot, "main", "pool-0")), false);
    // One whose name is a symlink, to a directory of another's.
    at.pod("pool-1");
    symlinkSync(join(at.attachRoot, "main", "pool-1"), join(at.attachRoot, "main", "pool-2"));
    assert.equal(await refusal(at.workspaces.attach({ environment: "main", workspace: "fs-abc123" }, "pool-2", "t", {})), 404);
    assert.deepEqual([at.mounts, existsSync(join(at.attachRoot, "main", "pool-1", "token"))], [[], false]);
  } finally {
    at.done();
  }
});

test("a detach whose unmount did not take removes nothing of the pod's directory, the workspace's files beneath it whole; and one that did leaves what no attach wrote there, and the directory with it", async () => {
  const at = workspacesNode();
  try {
    at.files("fs-abc123");
    at.pod("pool-0");
    await Effect.runPromise(at.workspaces.attach({ environment: "main", workspace: "fs-abc123" }, "pool-0", "t", {}));
    // The workspace's files, as the pod's directory shows them through its mount.
    const ws = join(at.attachRoot, "main", "pool-0", "ws");
    mkdirSync(join(ws, "workspace"), { recursive: true });
    writeFileSync(join(ws, "workspace", "precious"), "the user's");
    at.stuck.add(ws);
    assert.equal(await refusal(at.workspaces.detach("main", "pool-0", [])), 409);
    assert.equal(readFileSync(join(ws, "workspace", "precious"), "utf8"), "the user's");
    assert.ok(existsSync(join(at.attachRoot, "main", "pool-0", "token")));
    // Unmounted at last: what the attach wrote removed, what it did not left, and said.
    at.stuck.clear();
    rmSync(join(ws, "workspace"), { recursive: true });
    writeFileSync(join(at.attachRoot, "main", "pool-0", "stray"), "no attach wrote this");
    await Effect.runPromise(at.workspaces.detach("main", "pool-0", []));
    assert.deepEqual(readdirSync(join(at.attachRoot, "main", "pool-0")), ["stray"]);
    assert.equal(readFileSync(join(at.attachRoot, "main", "pool-0", "stray"), "utf8"), "no attach wrote this");
  } finally {
    at.done();
  }
});

test("a detach, by an agent started after the attach, unmounts what its own namespace copied of the pod's directory too, and removes it", async () => {
  const at = workspacesNode();
  try {
    at.files("fs-again");
    at.pod("pool-0");
    await Effect.runPromise(at.workspaces.attach({ environment: "main", workspace: "fs-again" }, "pool-0", "t", {}));
    // Its own namespace's copy, as its container was made after the attach.
    at.ownMounts.push(join(at.attachRoot, "main", "pool-0", "ws"));
    await Effect.runPromise(at.workspaces.detach("main", "pool-0", []));
    assert.deepEqual([at.mounts, at.ownMounts], [[], []]);
    assert.equal(existsSync(join(at.attachRoot, "main", "pool-0")), false);
  } finally {
    at.done();
  }
});

test("a detach is refused while the pod's sandbox runs, and once it has ended unmounts lazily all under its directory, deepest first, and removes it", async () => {
  const at = workspacesNode();
  try {
    at.files("fs-abc123");
    at.pod("pool-0");
    await Effect.runPromise(at.workspaces.attach({ environment: "main", workspace: "fs-abc123" }, "pool-0", "t", {}));
    // A mount beneath, as a sandbox's own might leave one.
    at.mounts.push(join(at.attachRoot, "main", "pool-0", "ws", "workspace", "inner"));
    at.running.add("container-1");
    assert.equal(await refusal(at.workspaces.detach("main", "pool-0", ["container-0", "container-1"])), 409);
    assert.equal(at.mounts.length, 2);
    at.running.delete("container-1");
    await Effect.runPromise(at.workspaces.detach("main", "pool-0", ["container-0", "container-1"]));
    assert.deepEqual(at.ran.filter((command) => command.startsWith("umount")), [
      `umount -l ${join(at.attachRoot, "main", "pool-0", "ws", "workspace", "inner")}`,
      `umount -l ${join(at.attachRoot, "main", "pool-0", "ws")}`,
    ]);
    assert.deepEqual(at.mounts, []);
    assert.equal(existsSync(join(at.attachRoot, "main", "pool-0")), false);
  } finally {
    at.done();
  }
});

test("a workspace's file is read by its path from the workspace's root alone, never through a symlink, nor past its size", async () => {
  const at = workspacesNode();
  try {
    const files = at.files("fs-abc123");
    const ref = { environment: "main", workspace: "fs-abc123" };
    writeFileSync(join(files, "workspace", "out.png"), "png");
    mkdirSync(join(files, "workspace", "dir"));
    writeFileSync(join(at.root, "secret"), "secret");
    symlinkSync(join(at.root, "secret"), join(files, "workspace", "link"));
    symlinkSync(at.root, join(files, "workspace", "away"));
    assert.equal((await Effect.runPromise(at.workspaces.read(ref, "workspace/out.png", 100))).toString(), "png");
    assert.equal(await refusal(at.workspaces.read(ref, "workspace/link", 100)), 403);
    assert.equal(await refusal(at.workspaces.read(ref, "workspace/away/secret", 100)), 403);
    assert.equal(await refusal(at.workspaces.read(ref, "workspace/dir", 100)), 403);
    assert.equal(await refusal(at.workspaces.read(ref, "workspace/out.png", 2)), 413);
    assert.equal(await refusal(at.workspaces.read(ref, "workspace/missing", 100)), 404);
    for (const path of ["../../secret", "/etc/passwd", "workspace/../../x", "other/file", "workspace/"]) {
      assert.equal(await refusal(at.workspaces.read(ref, path, 100)), 400, path);
    }
  } finally {
    at.done();
  }
});

test("a file received is written into a workspace, its directories made, never through a symlink", async () => {
  const at = workspacesNode();
  try {
    const files = at.files("fs-abc123");
    const ref = { environment: "main", workspace: "fs-abc123" };
    await Effect.runPromise(at.workspaces.write(ref, "home/.alasio/received/1/photo.jpg", Buffer.from("jpg")));
    assert.equal(readFileSync(join(files, "home", ".alasio", "received", "1", "photo.jpg"), "utf8"), "jpg");
    symlinkSync(at.root, join(files, "home", "away"));
    assert.equal(await refusal(at.workspaces.write(ref, "home/away/planted", Buffer.from("x"))), 403);
    assert.equal(existsSync(join(at.root, "planted")), false);
    symlinkSync(join(at.root, "target"), join(files, "home", "file-link"));
    assert.equal(await refusal(at.workspaces.write(ref, "home/file-link", Buffer.from("x"))), 403);
    assert.equal(existsSync(join(at.root, "target")), false);
    assert.equal(await refusal(at.workspaces.write(ref, "../outside", Buffer.from("x"))), 400);
  } finally {
    at.done();
  }
});

test("a FIFO in a workspace is neither read nor written, nor waited on", async () => {
  const at = workspacesNode();
  try {
    const files = at.files("fs-abc123");
    const ref = { environment: "main", workspace: "fs-abc123" };
    execFileSync("mkfifo", [join(files, "workspace", "pipe")]);
    const started = Date.now();
    assert.equal(await refusal(at.workspaces.read(ref, "workspace/pipe", 100)), 403);
    assert.equal(await refusal(at.workspaces.write(ref, "workspace/pipe", Buffer.from("x"))), 403);
    assert.ok(Date.now() - started < 5_000);
  } finally {
    at.done();
  }
});

test("a workspace records the snapshot it was released into beside its files, out of its sandbox's reach, replaced whole, and none once cleared", async () => {
  const at = workspacesNode();
  try {
    const ref = { environment: "main", workspace: "fs-released" };
    const files = at.files("fs-released");
    assert.equal(await Effect.runPromise(at.workspaces.released(ref)), null);
    await Effect.runPromise(at.workspaces.setReleased(ref, { snapshot: "20261017T120000000Z-0000000a", release: "20260921.0-eaucoin.8" }));
    assert.deepEqual(await Effect.runPromise(at.workspaces.released(ref)), { snapshot: "20261017T120000000Z-0000000a", release: "20260921.0-eaucoin.8" });
    await Effect.runPromise(at.workspaces.setReleased(ref, { snapshot: "20261017T130000000Z-0000000b", release: "20260921.0-eaucoin.8" }));
    assert.deepEqual(await Effect.runPromise(at.workspaces.released(ref)), { snapshot: "20261017T130000000Z-0000000b", release: "20260921.0-eaucoin.8" });
    // Beside its files, which its sandbox sees, not among them; nothing staged left beside it.
    assert.deepEqual(readdirSync(join(files, "..")).sort(), ["files", "released.json"]);
    assert.deepEqual(readdirSync(files).sort(), ["home", "workspace"]);
    await Effect.runPromise(at.workspaces.setReleased(ref, null));
    assert.equal(await Effect.runPromise(at.workspaces.released(ref)), null);
    await Effect.runPromise(at.workspaces.setReleased(ref, null));
    // Of a workspace there is not, or by names that are none, refused; one whose record is none, refused too.
    assert.equal(await refusal(at.workspaces.setReleased({ environment: "main", workspace: "fs-never" }, { snapshot: "20261017T120000000Z-0000000a", release: "r" })), 404);
    assert.equal(await refusal(at.workspaces.released({ environment: "main", workspace: "../etc" })), 400);
    writeFileSync(join(files, "..", "released.json"), "{\"snapshot\":\"../../x\"}");
    assert.equal(await refusal(at.workspaces.released(ref)), 500);
  } finally {
    at.done();
  }
});

test("nothing is made, cloned, attached or recorded where workspace storage is not mounted at the root, as for the moment its mount is handed over, nor written beneath it", async () => {
  const at = workspacesNode();
  try {
    at.files("fs-there");
    at.pod("pool-0");
    at.storage.mounted = false;
    const ref = { environment: "main", workspace: "fs-new" };
    assert.equal(await refusal(at.workspaces.create(ref, 1024 ** 3)), 503);
    assert.equal(existsSync(join(at.workspacesRoot, "main", "fs-new")), false);
    assert.equal(await refusal(at.workspaces.clone({ environment: "main", workspace: "fs-there" }, ref, 1024 ** 3)), 503);
    assert.equal(await refusal(at.workspaces.attach({ environment: "main", workspace: "fs-there" }, "pool-0", "token", {})), 503);
    assert.deepEqual(at.mounts, []);
    assert.equal(await refusal(at.workspaces.setReleased({ environment: "main", workspace: "fs-there" }, { snapshot: "20261017T120000000Z-0000000a", release: "r" })), 503);
    assert.deepEqual(at.juicefsCalls(), []);
    // Mounted again: made.
    at.storage.mounted = true;
    await Effect.runPromise(at.workspaces.create(ref, 1024 ** 3));
    assert.ok(existsSync(join(at.workspacesRoot, "main", "fs-new", "files", "workspace")));
  } finally {
    at.done();
  }
});

test("bayma of a bound pod of a pool on this node is told to go on with its stop, by its hook run in its container, and no other container nor pod's is", async () => {
  const at = workspacesNode();
  try {
    at.pod("pool-0", "alasio-sessions");
    const pod = (name: string, container = "bayma", namespace = "alasio-sessions") => ({ "io.kubernetes.cri.sandbox-namespace": namespace, "io.kubernetes.cri.sandbox-name": name, "io.kubernetes.cri.container-name": container });
    for (const [id, annotations] of [["c-bayma", pod("pool-0")], ["c-other", pod("pool-0", "sidecar")], ["c-elsewhere", pod("other-0")]] as const) {
      at.running.add(id);
      at.annotations.set(id, annotations);
    }
    const hook = ["node", "/opt/bayma/bayma.js", "sandbox-hook", "stop", "--state-dir", "/home/agent/.bayma"];
    const stop = (containerId: string, of = "pool-0", argv: readonly string[] = hook) => at.workspaces.stop({ namespace: "alasio-sessions", pod: of, containerId, hook: argv });
    await Effect.runPromise(stop("c-bayma"));
    assert.deepEqual(at.execs, [`r c-bayma ${hook.join(" ")}`]);
    // Not its pod's bayma; not the pod it says; of no pool here, as kubelet made it no directory; a hook but stop's; a container that runs not.
    assert.equal(await refusal(stop("c-other")), 403);
    assert.equal(await refusal(stop("c-bayma", "pool-1")), 403);
    assert.equal(await refusal(stop("c-elsewhere", "other-0")), 403);
    assert.equal(await refusal(stop("c-bayma", "pool-0", ["sh", "-c", "rm -rf /"])), 400);
    assert.equal(await refusal(stop("c-gone")), 404);
    assert.equal(at.execs.length, 1);
    // A hook that fails, its failure said.
    at.hooks.fail = true;
    assert.equal(await refusal(stop("c-bayma")), 502);
  } finally {
    at.done();
  }
});

test("the sandbox of a bound pod's bayma of a pool on this node is told to write its files through, by sync run in its container, and no other container nor pod's is", async () => {
  const at = workspacesNode();
  try {
    at.pod("pool-0", "alasio-sessions");
    const pod = (name: string, container = "bayma", namespace = "alasio-sessions") => ({ "io.kubernetes.cri.sandbox-namespace": namespace, "io.kubernetes.cri.sandbox-name": name, "io.kubernetes.cri.container-name": container });
    for (const [id, annotations] of [["c-bayma", pod("pool-0")], ["c-other", pod("pool-0", "sidecar")], ["c-elsewhere", pod("other-0")]] as const) {
      at.running.add(id);
      at.annotations.set(id, annotations);
    }
    const sync = (containerId: string, of = "pool-0") => at.workspaces.sync({ namespace: "alasio-sessions", pod: of, containerId });
    await Effect.runPromise(sync("c-bayma"));
    assert.deepEqual(at.execs, ["r c-bayma sync"]);
    // Not its pod's bayma; not the pod it says; of no pool here; a container that runs not.
    assert.equal(await refusal(sync("c-other")), 403);
    assert.equal(await refusal(sync("c-bayma", "pool-1")), 403);
    assert.equal(await refusal(sync("c-elsewhere", "other-0")), 403);
    assert.equal(await refusal(sync("c-gone")), 404);
    assert.equal(at.execs.length, 1);
    // One that fails, its failure said.
    at.hooks.fail = true;
    assert.equal(await refusal(sync("c-bayma")), 502);
  } finally {
    at.done();
  }
});
