/**
 * Throwaway Postgres databases for tests, on a real PostgreSQL 17 server, the major
 * version of alasio's Neon computes, run without Docker: embedded-postgres's binaries,
 * initdb'd in a temporary directory and started as a child of the test process, on a free
 * port of the loopback, with password logins (SCRAM) as Neon takes them. One server per
 * test process, started by the first startPostgres and stopped, its directory removed,
 * once every database it gave is stopped; each startPostgres gets a database of its own.
 * Roles are the server's, and so shared by the databases of one test process.
 */
import { randomBytes } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { type AddressInfo, createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import EmbeddedPostgres from "embedded-postgres";
import pg from "pg";

/** A throwaway database: where to connect, as its owner, and how to remove it. */
export interface TestPostgres {
  readonly url: string;
  stop(): Promise<void>;
}

/** The running server: how to reach it as its superuser, and how many of its databases are in use. */
interface Server {
  readonly postgres: EmbeddedPostgres;
  readonly url: URL;
  databases: number;
}

let server: Promise<Server> | undefined;
let databases = 0;

/** A port of the loopback no one listens on now. */
async function freePort(): Promise<number> {
  const probe = createServer();
  await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
  // A server listening on a TCP port has an address of its own.
  const { port } = probe.address() as AddressInfo;
  await new Promise((resolve) => probe.close(resolve));
  return port;
}

async function startServer(): Promise<Server> {
  const password = randomBytes(12).toString("hex");
  const port = await freePort();
  const postgres = new EmbeddedPostgres({
    databaseDir: mkdtempSync(join(tmpdir(), "alasio-postgres-")),
    port,
    user: "postgres",
    password,
    authMethod: "scram-sha-256",
    persistent: false,
    // On the loopback alone, with no socket file; and nothing a test needs kept through a crash.
    postgresFlags: ["-c", "listen_addresses=127.0.0.1", "-c", "unix_socket_directories=", "-c", "fsync=off", "-c", "full_page_writes=off"],
    onLog: () => {},
  });
  await postgres.initialise();
  await postgres.start();
  return { postgres, url: new URL(`postgresql://postgres:${password}@127.0.0.1:${port}/postgres`), databases: 0 };
}

/** Runs `sql` on the server's own database, as its superuser. */
async function onServer({ url }: Server, sql: string): Promise<void> {
  const client = new pg.Client({ connectionString: url.toString() });
  await client.connect();
  try {
    await client.query(sql);
  } finally {
    await client.end();
  }
}

export async function startPostgres(): Promise<TestPostgres> {
  server ??= startServer();
  const running = await server;
  running.databases++;
  const name = `test_${++databases}`;
  try {
    await onServer(running, `create database ${name}`);
  } catch (error) {
    running.databases--;
    throw error;
  }
  const url = new URL(running.url);
  url.pathname = `/${name}`;
  return {
    url: url.toString(),
    stop: async () => {
      await onServer(running, `drop database ${name} with (force)`);
      if (--running.databases > 0) return;
      server = undefined;
      await running.postgres.stop();
    },
  };
}
