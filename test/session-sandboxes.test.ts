/**
 * Session filesystems through the sandbox gateway (src/sandbox/index.ts), on a stand-in of the
 * gateway's workspaces that records what it is asked: a session brought up by its bind of the
 * internet alasio records, a branch's inherited one forked from its parent first, a workspace
 * made, forked and deleted, and its files read and written by the paths the session sees.
 */
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Effect } from "effect";

import { GatewayError, type GatewayWorkspaces } from "../src/sandbox/gateway-client.ts";
import { filesPath, type Inheritance, makeSessionSandboxes, type NetMode, SessionInheritError } from "../src/sandbox/index.ts";

const GATEWAY = { url: "http://gateway:8080", environment: "main", callerKey: Buffer.from("key") };

/** The gateway's workspaces, as a stand-in: each call recorded; files read as `files` holds them, and `there` the workspaces it has. */
function gateway({ files = new Map<string, Uint8Array>(), there = new Set<string>(), refuse = new Map<string, number>() } = {}): GatewayWorkspaces & { readonly asked: string[] } {
  const asked: string[] = [];
  const refused = (call: string) => (refuse.has(call) ? Effect.fail(new GatewayError({ call, status: refuse.get(call), reason: "refused" })) : Effect.void);
  return {
    asked,
    endpoint: (volumeId, mode) => Effect.succeed({ url: "http://gateway:8080/mcp", headers: { Authorization: `Bearer ${volumeId}/${mode}` } }),
    bind: (volumeId, mode) => Effect.andThen(Effect.sync(() => void asked.push(`bind ${volumeId} ${mode}`)), refused("bind")),
    release: (volumeId, mode) => Effect.andThen(Effect.sync(() => void asked.push(`release ${volumeId} ${mode}`)), refused("release")),
    create: (volumeId) => Effect.andThen(Effect.sync(() => (asked.push(`create ${volumeId}`), void there.add(volumeId))), refused("create")),
    remove: (volumeId) => Effect.andThen(Effect.sync(() => (asked.push(`remove ${volumeId}`), void there.delete(volumeId))), refused("remove")),
    quota: (volumeId) => Effect.sync(() => (asked.push(`quota ${volumeId}`), there.has(volumeId) ? { capacityBytes: 10, usedBytes: 1 } : null)),
    list: Effect.sync(() => [...there]),
    readFile: (volumeId, path, maxBytes) =>
      Effect.suspend(() => {
        asked.push(`read ${volumeId} ${path} ${maxBytes}`);
        const file = files.get(path);
        if (!file) return Effect.fail(new GatewayError({ call: "read", status: refuse.get(path) ?? 404, reason: "there is none" }));
        return file.length > maxBytes ? Effect.fail(new GatewayError({ call: "read", status: 413, reason: "more than its max" })) : Effect.succeed(file);
      }),
    writeFile: (volumeId, path, bytes) => Effect.sync(() => (asked.push(`write ${volumeId} ${path}`), void files.set(path, bytes))),
    fork: (volumeId, into) => Effect.andThen(Effect.sync(() => void asked.push(`fork ${volumeId} ${into.environment}/${into.workspace}`)), refused("fork")),
    folders: { endpoint: () => Effect.die("no folder"), bind: () => Effect.die("no folder"), release: () => Effect.die("no folder"), starting: () => Effect.die("no folder") },
  };
}

const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect);

test("a path a session sees is named in its files under its workspace, as one relative to it is, or its home, and none outside both", () => {
  assert.equal(filesPath("/workspace/out/chart.png"), "workspace/out/chart.png");
  assert.equal(filesPath("out/chart.png"), "workspace/out/chart.png");
  assert.equal(filesPath("./out/chart.png"), "workspace/out/chart.png");
  assert.equal(filesPath("/home/agent/.alasio/received/1/note.txt"), "home/.alasio/received/1/note.txt");
  for (const outside of ["/etc/passwd", "/workspace", "/workspace/../etc/passwd", "../etc/passwd", "/home/agentx/a", "out//a"]) assert.equal(filesPath(outside), null, outside);
});

test("a session is brought up bound to a pod of the internet alasio records, none where it records none, its bayma reached through the gateway", async () => {
  const stand = gateway();
  const modes = new Map<string, NetMode>([["fs-full01", "full"]]);
  const sessions = makeSessionSandboxes({ gateway: GATEWAY, stateDir: "/state", netModeOf: (volumeId) => Effect.succeed(modes.get(volumeId) ?? null), workspaces: stand });
  assert.deepEqual(await run(sessions.ensureSession("fs-full01")), { bayma: { url: "http://gateway:8080/mcp", headers: { Authorization: "Bearer fs-full01/full" } } });
  await run(sessions.ensureSession("fs-plain1"));
  assert.deepEqual(stand.asked, ["bind fs-full01 full", "bind fs-plain1 none"]);
  // One whose bind the gateway refuses: its refusal.
  const refusing = makeSessionSandboxes({ gateway: GATEWAY, stateDir: "/state", netModeOf: () => Effect.succeed(null), workspaces: gateway({ refuse: new Map([["bind", 503]]) }) });
  const failed = await run(Effect.flip(refusing.ensureSession("fs-plain1")));
  assert.equal(failed._tag === "GatewayError" && failed.status, 503);
});

test("a branch's session it inherited, not here yet, is forked from its parent's first, and recorded with the internet its parent says; one here is not, nor one its parent has not", async () => {
  const stand = gateway({ there: new Set(["fs-here01"]) });
  const recorded: string[] = [];
  const inherit: Inheritance = {
    fork: (volumeId) => Effect.sync(() => (recorded.push(`fork ${volumeId}`), volumeId === "fs-parent1" ? ("full" as const) : null)),
    record: (volumeId, netMode) => Effect.sync(() => void recorded.push(`record ${volumeId} ${netMode}`)),
  };
  const modes = new Map<string, NetMode>();
  const sessions = makeSessionSandboxes({ gateway: GATEWAY, stateDir: "/state", netModeOf: (volumeId) => Effect.succeed(modes.get(volumeId) ?? null), inherit: { ...inherit, record: (volumeId, netMode) => Effect.andThen(inherit.record(volumeId, netMode), Effect.sync(() => void modes.set(volumeId, netMode))) }, workspaces: stand });
  await run(sessions.ensureSession("fs-parent1"));
  await run(sessions.ensureSession("fs-here01"));
  await run(sessions.ensureSession("fs-nobody1"));
  assert.deepEqual(recorded, ["fork fs-parent1", "record fs-parent1 full", "fork fs-nobody1"]);
  assert.deepEqual(stand.asked.filter((asked) => asked.startsWith("bind")), ["bind fs-parent1 full", "bind fs-here01 none", "bind fs-nobody1 none"]);
  // A fork the parent could not make: its refusal, the session not brought up.
  const failing = makeSessionSandboxes({
    gateway: GATEWAY,
    stateDir: "/state",
    netModeOf: () => Effect.succeed(null),
    inherit: { ...inherit, fork: () => Effect.fail(new SessionInheritError({ message: "the parent did not answer" })) },
    workspaces: gateway(),
  });
  assert.equal((await run(Effect.flip(failing.ensureSession("fs-parent1"))))._tag, "SessionInheritError");
});

test("a workspace is made, forked with its source's internet into alasio's environment or another, deleted once its binding is let go, and listed, through the gateway", async () => {
  const stand = gateway();
  const sessions = makeSessionSandboxes({ gateway: GATEWAY, stateDir: "/state", netModeOf: (volumeId) => Effect.succeed(volumeId === "fs-source1" ? "full" : null), workspaces: stand });
  assert.deepEqual(await run(sessions.volumes.create("fs-source1", "full")), { volumeId: "fs-source1", netMode: "full" });
  assert.deepEqual(await run(sessions.volumes.fork("fs-source1", "fs-fork001")), { volumeId: "fs-fork001", netMode: "full" });
  assert.deepEqual(await run(sessions.volumes.fork("fs-source1", "fs-source1", "try")), { volumeId: "fs-source1", netMode: "full" });
  assert.deepEqual(await run(sessions.volumes.list), ["fs-source1"]);
  await run(sessions.volumes.destroy("fs-source1"));
  assert.deepEqual(stand.asked, ["create fs-source1", "fork fs-source1 main/fs-fork001", "fork fs-source1 try/fs-source1", "release fs-source1 full", "remove fs-source1"]);
  // One the gateway would not delete: its refusal, which keeps alasio's record.
  const refusing = makeSessionSandboxes({ gateway: GATEWAY, stateDir: "/state", netModeOf: () => Effect.succeed(null), workspaces: gateway({ refuse: new Map([["remove", 409]]) }) });
  const failed = await run(Effect.flip(refusing.volumes.destroy("fs-source1")));
  assert.equal(failed._tag === "GatewayError" && failed.status, 409);
});

test("a session's file is read and written through the gateway by the path it sees it at, one not there, refused, or outside its files a note, one too large said so", async () => {
  const stand = gateway({ files: new Map([["workspace/out/chart.png", Uint8Array.from([1, 2, 3])], ["workspace/big", new Uint8Array(10)]]), refuse: new Map([["workspace/link", 403]]) });
  const sessions = makeSessionSandboxes({ gateway: GATEWAY, stateDir: "/state", netModeOf: () => Effect.succeed(null), workspaces: stand });
  assert.deepEqual(await run(sessions.readFile("fs-abc123", "/workspace/out/chart.png", 5)), { bytes: Buffer.from([1, 2, 3]) });
  assert.deepEqual(await run(sessions.readFile("fs-abc123", "out/none.png", 5)), { note: "file not found" });
  assert.deepEqual(await run(sessions.readFile("fs-abc123", "link", 5)), { note: "file not found" });
  assert.deepEqual(await run(sessions.readFile("fs-abc123", "/etc/passwd", 5)), { note: "not in the session's files" });
  assert.match((await run(sessions.readFile("fs-abc123", "big", 5))).note ?? "", /over Telegram's \d+ MB limit/u);
  await run(sessions.writeFile("fs-abc123", "/home/agent/.alasio/received/1/note.txt", Buffer.from("hello")));
  assert.equal((await run(Effect.flip(sessions.writeFile("fs-abc123", "/etc/cron.d/x", Buffer.from("no")))))._tag, "SessionFileError");
  assert.deepEqual(stand.asked.filter((asked) => asked.startsWith("write")), ["write fs-abc123 home/.alasio/received/1/note.txt"]);
  assert.ok(!stand.asked.some((asked) => asked.includes("/etc/")));
});

test("a session's harness directory is its own, under the state directory, outside the sandbox, and the gateway is never reached for it", () => {
  const stateDir = mkdtempSync(join(tmpdir(), "alasio-session-sandboxes-"));
  try {
    const stand = gateway();
    const sessions = makeSessionSandboxes({ gateway: GATEWAY, stateDir, netModeOf: () => Effect.die("no internet is read"), workspaces: stand });
    const directory = sessions.harnessDirectory("fs-abc123");
    assert.equal(directory, join(stateDir, "sessionfs", "workspaces", "fs-abc123"));
    assert.ok(existsSync(directory));
    assert.throws(() => sessions.harnessDirectory("../escape"), /invalid session volume id/u);
    assert.deepEqual(stand.asked, []);
  } finally {
    rmSync(stateDir, { recursive: true, force: true });
  }
});
