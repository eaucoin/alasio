/**
 * The tests' own Postgres (./support/postgres.ts), started on a port another process took
 * between its being found free and the server's start, as a test process starting a server
 * of its own may take it: started on another, or, where every one is taken, failing with
 * what the server said. And a database stopped right after the pool on it ended, as a test
 * ends it, before the server has heard its clients end: dropped once they have, so the pool,
 * ended, hears no connection of its terminated.
 */
import assert from "node:assert/strict";
import { type AddressInfo, connect, createServer, type Server, type Socket } from "node:net";
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

/**
 * A proxy of the loopback to `port` of it, which passes what its clients send on at once until
 * `hold` is called, and from then on `heldMs` late, their ends with it; what the server sends,
 * at once, always, a client's end of what it sends kept apart from the server's, as TCP keeps
 * them.
 */
async function delaying(port: number, heldMs: number): Promise<{ readonly port: number; readonly hold: () => void; readonly close: () => Promise<void> }> {
  let held = false;
  const open = new Set<Socket>();
  const proxy = createServer({ allowHalfOpen: true }, (client) => {
    const upstream = connect({ port, host: "127.0.0.1", allowHalfOpen: true });
    client.on("data", (chunk: Buffer) => (held ? void setTimeout(() => upstream.write(chunk), heldMs) : void upstream.write(chunk)));
    client.on("end", () => (held ? void setTimeout(() => upstream.end(), heldMs) : void upstream.end()));
    upstream.pipe(client);
    for (const end of [client, upstream]) {
      open.add(end);
      end.on("close", () => open.delete(end));
      end.on("error", () => (client.destroy(), upstream.destroy()));
    }
  });
  await new Promise<void>((resolve) => proxy.listen(0, "127.0.0.1", resolve));
  return {
    port: (proxy.address() as AddressInfo).port,
    hold: () => void (held = true),
    close: async () => {
      for (const socket of open) socket.destroy();
      await new Promise((resolve) => proxy.close(resolve));
    },
  };
}

test("a database stopped as soon as the pool on it has ended, before the server has heard its clients end, is dropped once they have: the pool hears none of its connections terminated", async () => {
  const database = await startPostgres();
  const url = new URL(database.url);
  // The clients' end reaching the server a second late, as one under load may.
  const proxy = await delaying(Number(url.port), 1_000);
  url.port = String(proxy.port);
  const pool = new pg.Pool({ connectionString: url.toString(), max: 2 });
  const heard: Error[] = [];
  pool.on("error", (error) => void heard.push(error));
  try {
    await Promise.all([pool.query("select pg_sleep(0.05)"), pool.query("select pg_sleep(0.05)")]);
    proxy.hold();
    await pool.end();
    await database.stop();
    // What the server would have sent the clients, had it terminated their connections, come.
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.deepEqual(heard.map(({ message }) => message), []);
  } finally {
    await proxy.close();
  }
});
