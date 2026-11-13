/**
 * The node agent's restores into standby pods (src/node-agent/standby.ts), against stand-ins for
 * their shims listening on Unix sockets as the fork's do, in one directory of their user's: the
 * pod's own first, then each of its containers' that has a shim of its own; an image other than
 * one of the workspace's in the snapshot cache refused before any is asked; a standby with no
 * shim listening, or whose shim took a restore already, taken as one that takes none, as is one
 * whose directory is not its shims' user's alone, or with anything but a socket in the place of
 * one of its sockets; and one whose restore fails, failed.
 */
import assert from "node:assert/strict";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";

import { Effect } from "effect";

import { makeStandbyRestores, type StandbyRestoreRequest } from "../src/node-agent/standby.ts";

/** The shims' directory, made as mkdtemp makes one, this user's, which only it may write to; the shims' user, this one. */
let sockets = "";
const owner = process.getuid?.() ?? 0;
const servers: Server[] = [];

before(async () => {
  sockets = await mkdtemp(join(tmpdir(), "standby-"));
});

after(async () => {
  for (const server of servers) server.close();
  await rm(sockets, { recursive: true, force: true });
});

/** The restores of standbys whose shims listen in `directory`, the shims' unless given, as `owner`'s, this user's unless given. */
const restoresIn = (directory = sockets, as = owner) => makeStandbyRestores({ directory, owner: as });

/** A stand-in shim listening on `<namespace>.<pod>.sock`, or `<namespace>.<pod>.<container>.sock`, in `directory`, the shims' unless given: what it was asked, answered as `status` says, with `containers` restored where 200. */
async function shim(socket: string, status: number, containers: readonly string[], asked: string[], directory = sockets): Promise<void> {
  const server = createServer((request, response) => {
    let body = "";
    request.on("data", (chunk: Buffer) => (body += chunk.toString()));
    request.on("end", () => {
      asked.push(`${request.method} ${request.url} ${socket} ${body}`);
      response.writeHead(status, { "content-type": "application/json" }).end(
        status === 200 ? JSON.stringify({ containers, milliseconds: containers.map((_, index) => 10 + index) }) : status === 409 ? "a restore was requested of this standby already" : "restoring container: runsc failed",
      );
    });
  });
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(join(directory, socket), resolve));
}

const IMAGE = "/var/lib/alasio/snapshots/sessions/fs-abc123/20261017T120000000Z-0000000a";
const request = (pod: string, imagePath = IMAGE): StandbyRestoreRequest => ({ environment: "sessions", workspace: "fs-abc123", namespace: "sessions", pod, imagePath });

test("a restore into a standby asks the pod's shim first, then each of its containers' own, each to restore the image paused, and answers what each restored", async () => {
  const asked: string[] = [];
  await shim("sessions.pool-restore-0000001.sock", 200, ["c-pause"], asked);
  await shim("sessions.pool-restore-0000001.bayma.sock", 200, ["c-bayma"], asked);
  // Another pod's, and a pod of the same name's of another namespace, which are not asked.
  await shim("sessions.pool-restore-0000010.sock", 200, ["c-other"], asked);
  await shim("others.pool-restore-0000001.sock", 200, ["c-other"], asked);
  const restored = await Effect.runPromise(restoresIn().restore(request("pool-restore-0000001")));
  assert.deepEqual(restored, { containers: ["c-pause", "c-bayma"], milliseconds: [10, 10] });
  const body = JSON.stringify({ imagePath: IMAGE, paused: true });
  assert.deepEqual(asked, [`POST /restore sessions.pool-restore-0000001.sock ${body}`, `POST /restore sessions.pool-restore-0000001.bayma.sock ${body}`]);
});

test("a restore into a standby whose pod has one shim, as shims grouped are, asks it alone", async () => {
  const asked: string[] = [];
  await shim("sessions.pool-restore-0000002.sock", 200, ["c-pause", "c-bayma"], asked);
  const restored = await Effect.runPromise(restoresIn().restore(request("pool-restore-0000002")));
  assert.deepEqual(restored.containers, ["c-pause", "c-bayma"]);
  assert.equal(asked.length, 1);
});

test("a restore into a standby of an image that is not one of its workspace's in the snapshot cache is refused, no shim asked", async () => {
  const asked: string[] = [];
  await shim("sessions.pool-restore-0000003.sock", 200, ["c-pause"], asked);
  for (const imagePath of ["/etc", "/var/lib/alasio/snapshots/sessions/fs-other/20261017T120000000Z-0000000a", `${IMAGE}/../../fs-other/20261017T120000000Z-0000000a`, `${IMAGE}x`]) {
    const failed = await Effect.runPromise(Effect.flip(restoresIn().restore(request("pool-restore-0000003", imagePath))));
    assert.equal(failed._tag, "StandbyImageRefused", imagePath);
  }
  assert.deepEqual(asked, []);
});

test("a standby no shim of which listens, or whose shim took a restore already, takes none, and one whose shim fails its restore fails, the shims after it unasked", async () => {
  const restores = restoresIn();
  assert.equal((await Effect.runPromise(Effect.flip(restores.restore(request("pool-restore-0000004")))))._tag, "StandbyUnavailable");
  const asked: string[] = [];
  await shim("sessions.pool-restore-0000005.sock", 409, [], asked);
  const taken = await Effect.runPromise(Effect.flip(restores.restore(request("pool-restore-0000005"))));
  assert.equal(taken._tag, "StandbyUnavailable");
  assert.match(taken.message, /took a restore already/u);
  await shim("sessions.pool-restore-0000006.sock", 500, [], asked);
  await shim("sessions.pool-restore-0000006.bayma.sock", 200, ["c-bayma"], asked);
  const failed = await Effect.runPromise(Effect.flip(restores.restore(request("pool-restore-0000006"))));
  assert.equal(failed._tag, "StandbyRestoreFailed");
  assert.match(failed.message, /answered 500: restoring container: runsc failed/u);
  assert.ok(!asked.some((each) => each.includes("sessions.pool-restore-0000006.bayma.sock")), asked.join("\n"));
});

test("a standby whose shims' directory is not their user's alone, or is not there, takes no restore, and none of its shims is asked", async () => {
  const asked: string[] = [];
  await shim("sessions.pool-restore-0000007.sock", 200, ["c-pause"], asked);
  // Another user's.
  const others = await Effect.runPromise(Effect.flip(restoresIn(sockets, owner + 1).restore(request("pool-restore-0000007"))));
  assert.equal(others._tag, "StandbyUnavailable");
  assert.match(others.message, /is not a directory of the shims' user that only it may write to/u);
  // Its user's, which others may write to.
  const open = await mkdtemp(join(tmpdir(), "standby-open-"));
  try {
    await shim("sessions.pool-restore-0000007.sock", 200, ["c-pause"], asked, open);
    await chmod(open, 0o777);
    assert.equal((await Effect.runPromise(Effect.flip(restoresIn(open).restore(request("pool-restore-0000007")))))._tag, "StandbyUnavailable");
    await chmod(open, 0o720);
    assert.equal((await Effect.runPromise(Effect.flip(restoresIn(open).restore(request("pool-restore-0000007")))))._tag, "StandbyUnavailable");
  } finally {
    await rm(open, { recursive: true, force: true });
  }
  // None there.
  const gone = await Effect.runPromise(Effect.flip(restoresIn(join(sockets, "none")).restore(request("pool-restore-0000007"))));
  assert.equal(gone._tag, "StandbyUnavailable");
  assert.deepEqual(asked, []);
});

test("a standby with anything but a socket in the place of its own socket, or of one of its containers', takes no restore, and none of its shims is asked", async () => {
  const asked: string[] = [];
  await writeFile(join(sockets, "sessions.pool-restore-0000008.sock"), "");
  const own = await Effect.runPromise(Effect.flip(restoresIn().restore(request("pool-restore-0000008"))));
  assert.equal(own._tag, "StandbyUnavailable");
  assert.match(own.message, /sessions\.pool-restore-0000008\.sock is not a socket/u);
  await shim("sessions.pool-restore-0000009.sock", 200, ["c-pause"], asked);
  await writeFile(join(sockets, "sessions.pool-restore-0000009.bayma.sock"), "");
  const container = await Effect.runPromise(Effect.flip(restoresIn().restore(request("pool-restore-0000009"))));
  assert.equal(container._tag, "StandbyUnavailable");
  assert.match(container.message, /sessions\.pool-restore-0000009\.bayma\.sock is not a socket/u);
  assert.deepEqual(asked, []);
});
