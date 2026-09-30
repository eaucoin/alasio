import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, test } from "node:test";

// agent-exec runs inside the session host, whose environment holds the volume's storage
// keys and metadata URL. These tests run the real script with `runsc` replaced by a stub
// that prints the environment it would give the sandboxed command.
const dir = mkdtempSync(join(tmpdir(), "alasio-agent-exec-"));
after(() => rmSync(dir, { recursive: true, force: true }));

const agentEnvFile = join(dir, "agent-env");
const stub = join(dir, "runsc");
writeFileSync(stub, '#!/bin/bash\nwhile [ $# -gt 0 ]; do [ "$1" = --env ] && echo "$2"; shift; done\n');
chmodSync(stub, 0o755);
const script = join(dir, "agent-exec");
writeFileSync(script, readFileSync(new URL("../sandbox/session-host/agent-exec.sh", import.meta.url), "utf8")
  .replaceAll("/opt/gvisor/runsc", stub)
  .replaceAll("/run/agent-env", agentEnvFile));

/** The env the sandboxed command would get, given the session host's env. */
function sandboxEnv(hostEnv) {
  return execFileSync("bash", [script, "true"], { env: { PATH: process.env.PATH, ...hostEnv }, encoding: "utf8" })
    .trim().split("\n");
}

const sessionHostEnv = {
  ACCESS_KEY: "storage-access", SECRET_KEY: "storage-secret",
  JFS_META: "redis://valkey:6379/3", JFS_BUCKET: "http://seaweedfs/sessions/fs-a", META_PASSWORD_FILE: "/run/meta",
  GATEWAY_IP: "172.18.0.1", NET_MODE: "none",
};

test("agent-exec gives the sandbox none of the session host's own environment", () => {
  writeFileSync(agentEnvFile, "ANTHROPIC_BASE_URL=http://gw\n");
  const env = sandboxEnv(sessionHostEnv);
  assert.deepEqual(env, [
    "HOME=/home/agent", "USER=agent", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "ANTHROPIC_BASE_URL=http://gw",
  ]);
});

test("agent-exec forwards exactly the per-spawn variables it is told to", () => {
  rmSync(agentEnvFile, { force: true });
  const env = sandboxEnv({ ...sessionHostEnv, CODEX_HOME: "/home/agent/.codex", TOKEN: "a b=c", AGENT_EXEC_VARS: "CODEX_HOME,TOKEN,HOME" });
  assert.deepEqual(env.slice(3), ["CODEX_HOME=/home/agent/.codex", "TOKEN=a b=c"]);
  assert.ok(!env.some((line) => /KEY|JFS_|META|GATEWAY/.test(line)), env.join("\n"));
});

test("agent-exec refuses a malformed variable name", () => {
  assert.throws(() => sandboxEnv({ AGENT_EXEC_VARS: "OK,BAD-NAME" }), /invalid variable name: BAD-NAME/);
});
