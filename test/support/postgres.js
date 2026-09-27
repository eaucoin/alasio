/**
 * A throwaway Postgres for tests: the controller database's pinned image, on
 * a random local port, removed when stopped. Tests that need one skip where
 * Docker is not available.
 */
import { execFile, execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { promisify } from "node:util";

import pg from "pg";

const run = promisify(execFile);
const IMAGE = "postgres:17-bookworm@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652";

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
