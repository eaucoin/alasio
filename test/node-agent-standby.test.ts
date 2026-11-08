/**
 * The node agent's restores into standby pods (src/node-agent/standby.ts), against stand-ins for
 * their shims listening on Unix sockets as the fork's do: the pod's own first, then each of its
 * containers' that has a shim of its own; an image other than one of the workspace's in the
 * snapshot cache refused before any is asked; a standby with no shim listening, or whose shim
 * took a restore already, taken as one that takes none; and one whose restore fails, failed.
 */
import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";

import { Effect } from "effect";

import { makeStandbyRestores, type StandbyRestoreRequest } from "../src/node-agent/standby.ts";

let sockets = "";
const servers: Server[] = [];

before(async () => {
  sockets = await mkdtemp(join(tmpdir(), "standby-"));
  await mkdir(join(sockets, "sessions"));
});

after(async () => {
  for (const server of servers) server.close();
  await rm(sockets, { recursive: true, force: true });
});

/** A stand-in shim listening on `<pod>.sock`, or `<pod>.<container>.sock`, under the namespace's sockets: what it was asked, answered as `status` says, with `containers` restored where 200. */
async function shim(socket: string, status: number, containers: readonly string[], asked: string[]): Promise<void> {
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
  await new Promise<void>((resolve) => server.listen(join(sockets, "sessions", socket), resolve));
}

const IMAGE = "/var/lib/alasio/snapshots/sessions/fs-abc123/20261017T120000000Z-0000000a";
const request = (pod: string, imagePath = IMAGE): StandbyRestoreRequest => ({ environment: "sessions", workspace: "fs-abc123", namespace: "sessions", pod, imagePath });

test("a restore into a standby asks the pod's shim first, then each of its containers' own, each to restore the image paused, and answers what each restored", async () => {
  const asked: string[] = [];
  await shim("pool-restore-0000001.sock", 200, ["c-pause"], asked);
  await shim("pool-restore-0000001.bayma.sock", 200, ["c-bayma"], asked);
  // Another pod's, which is not asked.
  await shim("pool-restore-0000010.sock", 200, ["c-other"], asked);
  const restored = await Effect.runPromise(makeStandbyRestores(sockets).restore(request("pool-restore-0000001")));
  assert.deepEqual(restored, { containers: ["c-pause", "c-bayma"], milliseconds: [10, 10] });
  const body = JSON.stringify({ imagePath: IMAGE, paused: true });
  assert.deepEqual(asked, [`POST /restore pool-restore-0000001.sock ${body}`, `POST /restore pool-restore-0000001.bayma.sock ${body}`]);
});

test("a restore into a standby whose pod has one shim, as shims grouped are, asks it alone", async () => {
  const asked: string[] = [];
  await shim("pool-restore-0000002.sock", 200, ["c-pause", "c-bayma"], asked);
  const restored = await Effect.runPromise(makeStandbyRestores(sockets).restore(request("pool-restore-0000002")));
  assert.deepEqual(restored.containers, ["c-pause", "c-bayma"]);
  assert.equal(asked.length, 1);
});

test("a restore into a standby of an image that is not one of its workspace's in the snapshot cache is refused, no shim asked", async () => {
  const asked: string[] = [];
  await shim("pool-restore-0000003.sock", 200, ["c-pause"], asked);
  for (const imagePath of ["/etc", "/var/lib/alasio/snapshots/sessions/fs-other/20261017T120000000Z-0000000a", `${IMAGE}/../../fs-other/20261017T120000000Z-0000000a`, `${IMAGE}x`]) {
    const failed = await Effect.runPromise(Effect.flip(makeStandbyRestores(sockets).restore(request("pool-restore-0000003", imagePath))));
    assert.equal(failed._tag, "StandbyImageRefused", imagePath);
  }
  assert.deepEqual(asked, []);
});

test("a standby no shim of which listens, or whose shim took a restore already, takes none, and one whose shim fails its restore fails, the shims after it unasked", async () => {
  const restores = makeStandbyRestores(sockets);
  assert.equal((await Effect.runPromise(Effect.flip(restores.restore(request("pool-restore-0000004")))))._tag, "StandbyUnavailable");
  const asked: string[] = [];
  await shim("pool-restore-0000005.sock", 409, [], asked);
  const taken = await Effect.runPromise(Effect.flip(restores.restore(request("pool-restore-0000005"))));
  assert.equal(taken._tag, "StandbyUnavailable");
  assert.match(taken.message, /took a restore already/u);
  await shim("pool-restore-0000006.sock", 500, [], asked);
  await shim("pool-restore-0000006.bayma.sock", 200, ["c-bayma"], asked);
  const failed = await Effect.runPromise(Effect.flip(restores.restore(request("pool-restore-0000006"))));
  assert.equal(failed._tag, "StandbyRestoreFailed");
  assert.match(failed.message, /answered 500: restoring container: runsc failed/u);
  assert.ok(!asked.some((each) => each.includes("pool-restore-0000006.bayma.sock")), asked.join("\n"));
});
