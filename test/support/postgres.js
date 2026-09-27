/**
 * A throwaway Postgres for tests: Postgres 17 with pgvector, as alasio's Neon
 * compute has them, pinned, on a random local port, removed when stopped.
 * Tests that need one skip where Docker is not available.
 */
import { execFile, execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { promisify } from "node:util";

import pg from "pg";

const run = promisify(execFile);
const IMAGE = "pgvector/pgvector:0.8.0-pg17@sha256:40b404964359299eefdd5f8518facf1886c562848cf4de13b6eaf91cb70c2b87";

export function dockerAvailable() {
  try {
    execFileSync("docker", ["info"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

export async function startPostgres() {
  const password = randomBytes(12).toString("hex");
  const { stdout } = await run("docker", [
    "run", "--detach", "--rm",
    "--publish", "127.0.0.1::5432",
    "--env", `POSTGRES_PASSWORD=${password}`,
    IMAGE,
  ]);
  const id = stdout.trim();
  const stop = () => run("docker", ["rm", "--force", id]).catch(() => {});
  try {
    const { stdout: port } = await run("docker", ["port", id, "5432/tcp"]);
    const url = `postgresql://postgres:${password}@${port.trim().split("\n")[0]}/postgres`;
    const deadline = Date.now() + 60_000;
    for (;;) {
      const client = new pg.Client({ connectionString: url });
      try {
        await client.connect();
        await client.query("select 1");
        await client.end();
        break;
      } catch (error) {
        await client.end().catch(() => {});
        if (Date.now() > deadline) throw error;
        await new Promise((resolve) => setTimeout(resolve, 250));
      }
    }
    return { url, stop };
  } catch (error) {
    await stop();
    throw error;
  }
}
