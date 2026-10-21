/**
 * The tests' own Postgres (./support/postgres.ts), started on a port another process took
 * between its being found free and the server's start, as a test process starting a server
 * of its own may take it: started on another, or, where every one is taken, failing with
 * what the server said.
 */
import assert from "node:assert/strict";
import { type AddressInfo, createServer, type Server } from "node:net";
import { test } from "node:test";

import pg from "pg";

import { startPostgres } from "./support/postgres.ts";

/** A port of the loopback no one listens on now. */
async function freeNow(): Promise<number> {
  const { port, listener } = await heldPort();
  await new Promise((resolve) => listener.close(resolve));
  return port;
}

/** A port of the loopback held by a listener of the test's. */
async function heldPort(): Promise<{ readonly port: number; readonly listener: Server }> {
  const listener = createServer();
  await new Promise<void>((resolve) => listener.listen(0, "127.0.0.1", resolve));
  return { port: (listener.address() as AddressInfo).port, listener };
}

test("the tests' Postgres, its port taken as it starts, starts on another, and where every one it is given is taken fails saying so", async () => {
  const { port, listener } = await heldPort();
  try {
    await assert.rejects(startPostgres({ ports: async () => port }), (error: Error) => {
      assert.match(error.message, new RegExp(`^Postgres ended as it started on port ${port}: .*could not bind IPv4 address "127\\.0\\.0\\.1": Address already in use`, "u"));
      return true;
    });
    const given: number[] = [];
    const database = await startPostgres({
      ports: async () => {
        const next = given.length === 0 ? port : await freeNow();
        given.push(next);
        return next;
      },
    });
    try {
      assert.deepEqual([given.length, given[0]], [2, port]);
      const client = new pg.Client({ connectionString: database.url });
      await client.connect();
      try {
        assert.equal((await client.query<{ one: number }>("select 1 as one")).rows[0]?.one, 1);
      } finally {
        await client.end();
      }
    } finally {
      await database.stop();
    }
  } finally {
    listener.close();
  }
});
