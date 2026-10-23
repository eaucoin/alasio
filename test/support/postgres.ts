/**
 * Throwaway Postgres databases for tests, on a real PostgreSQL 17 server, the major
 * version of alasio's Neon computes, run without Docker: embedded-postgres's binaries,
 * initdb'd in a temporary directory and started as a child of the test process, on a free
 * port of the loopback, with password logins (SCRAM) as Neon takes them. One server per
 * test process, started by the first startPostgres and stopped, its directory removed,
 * once every database it gave is stopped, or else as the test file's tests end, so one that
 * failed before it stopped its database leaves no server keeping the process alive; each
 * startPostgres gets a database of its own.
 * Roles are the server's, and so shared by the databases of one test process.
 *
 * What holds connections to it is closed before it stops (closedBeforeStop): a test file's
 * hooks run in the order they were made, and this module's is made first, as each module that
 * pools connections imports it, so a pool's own hook would end it only after the server had
 * terminated its idle connections, which the pool then fails of with none to hear it.
 */
import { randomBytes } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { type AddressInfo, createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after } from "node:test";

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
  /** Whether it was stopped as the test file's tests ended, its databases with it. */
  ended: boolean;
}

let server: Promise<Server> | undefined;
let databases = 0;
/** What holds connections to the server, closed before it stops as the test file's tests end, in turn. */
const closers: (() => Promise<unknown>)[] = [];

/** Has `close` run as the test file's tests end, before the server stops: a pool's end, say. */
export function closedBeforeStop(close: () => Promise<unknown>): void {
  closers.push(close);
}

after(async () => {
  for (const close of closers.splice(0)) await close();
  const left = await server?.catch(() => null);
  server = undefined;
  if (!left) return;
  left.ended = true;
  await left.postgres.stop();
});

/** A port of the loopback no one listens on now. */
async function freePort(): Promise<number> {
  const probe = createServer();
  await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
  // A server listening on a TCP port has an address of its own.
  const { port } = probe.address() as AddressInfo;
  await new Promise((resolve) => probe.close(resolve));
  return port;
}

/** How many times a server is started on a port another took between freePort and its start, before that is given up on. */
const PORT_TRIES = 5;

/** What Postgres says where its port was taken before it bound it. */
const PORT_TAKEN = /could not bind IPv4 address .*: Address already in use/u;

/**
 * A server, started on a free port of the loopback; on another where one is taken between
 * freePort and its start, as a test process starting a server of its own may take it.
 * embedded-postgres rejects a start whose server ended with no reason, so what the server
 * said is kept to say why.
 */
async function startServer(ports: () => Promise<number>, tries = PORT_TRIES): Promise<Server> {
  const password = randomBytes(12).toString("hex");
  const port = await ports();
  const databaseDir = mkdtempSync(join(tmpdir(), "alasio-postgres-"));
  let said = "";
  const postgres = new EmbeddedPostgres({
    databaseDir,
    port,
    user: "postgres",
    password,
    authMethod: "scram-sha-256",
    persistent: false,
    // On the loopback alone, with no socket file; and nothing a test needs kept through a crash.
    postgresFlags: ["-c", "listen_addresses=127.0.0.1", "-c", "unix_socket_directories=", "-c", "fsync=off", "-c", "full_page_writes=off"],
    onLog: (message) => void (said += message),
  });
  await postgres.initialise();
  try {
    await postgres.start();
  } catch (error) {
    // Its directory, which a server that ended leaves, removed as a stop removes it; its stop
    // itself waits on an exit that has come already.
    rmSync(databaseDir, { recursive: true, force: true });
    if (PORT_TAKEN.test(said) && tries > 1) return startServer(ports, tries - 1);
    throw new Error(`Postgres ended as it started on port ${port}: ${said.trim().split("\n").slice(-5).join(" | ") || "saying nothing"}`, { cause: error });
  }
  return { postgres, url: new URL(`postgresql://postgres:${password}@127.0.0.1:${port}/postgres`), databases: 0, ended: false };
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

/** Where the process's server is started, where it is not running yet: free ports of the loopback, unless a test of the start itself gives others. */
export interface StartPostgresOptions {
  readonly ports?: (() => Promise<number>) | undefined;
}

export async function startPostgres({ ports = freePort }: StartPostgresOptions = {}): Promise<TestPostgres> {
  // A start that failed is not kept, so the next is tried anew.
  server ??= startServer(ports).catch((error: unknown) => {
    server = undefined;
    throw error;
  });
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
      if (running.ended) return;
      await onServer(running, `drop database ${name} with (force)`);
      if (--running.databases > 0) return;
      server = undefined;
      await running.postgres.stop();
    },
  };
}
