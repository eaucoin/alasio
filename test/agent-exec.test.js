import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, test } from "node:test";

// agent-exec and agent-connect run inside the session host, whose environment holds the
// volume's storage keys and metadata URL. These tests run the real scripts with `runsc`
// replaced by a stub that prints what the sandboxed command would be given.
const dir = mkdtempSync(join(tmpdir(), "alasio-agent-exec-"));
after(() => rmSync(dir, { recursive: true, force: true }));

const stub = join(dir, "runsc");
writeFileSync(stub, '#!/bin/bash\nwhile [ $# -gt 0 ]; do\n  case "$1" in --env) echo "env $2"; shift ;; session) shift; echo "argv $*"; exit 0 ;; esac\n  shift\ndone\n');
chmodSync(stub, 0o755);
const script = (name) => {
  const path = join(dir, name);
  writeFileSync(path, readFileSync(new URL(`../sandbox/session-host/${name}.sh`, import.meta.url), "utf8")
    .replaceAll("/opt/gvisor/runsc", stub)
    .replaceAll("exec agent-exec ", `exec ${join(dir, "agent-exec")} `));
  chmodSync(path, 0o755);
  return path;
};
const agentExec = script("agent-exec");
const agentConnect = script("agent-connect");

const sessionHostEnv = {
  ACCESS_KEY: "storage-access", SECRET_KEY: "storage-secret",
  JFS_META: "redis://valkey:6379/3", JFS_BUCKET: "http://seaweedfs/sessions/fs-a", META_PASSWORD_FILE: "/run/meta",
  NET_MODE: "none",
};
const run = (path, args, env = {}) =>
  execFileSync("bash", [path, ...args], { env: { PATH: process.env.PATH, ...sessionHostEnv, ...env }, encoding: "utf8" }).trim().split("\n");

test("agent-exec gives the sandboxed command only the agent's own HOME, USER and PATH", () => {
  // Nothing of the session host's environment, and nothing a `docker exec -e` adds.
  assert.deepEqual(run(agentExec, ["sh", "-c", "env"], { EXTRA: "from docker exec -e" }), [
    "env HOME=/home/agent",
    "env USER=agent",
    "env PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "argv sh -c env",
  ]);
});

test("agent-connect pipes to a port on the sandbox's own loopback, as the agent, and only to a port", () => {
  const out = run(agentConnect, ["7290"]).join("\n");
  assert.doesNotMatch(out, /KEY|JFS_|META/);
  // The command is node with the pipe program (several lines) and the port.
  const argv = out.slice(out.indexOf("argv "));
  assert.match(argv, /^argv node -e /);
  assert.match(argv, /connect\(Number\(process\.argv\[1\]\), "127\.0\.0\.1"\)/);
  assert.match(argv, / 7290$/);
  for (const bad of ["", "72a", "1;id", "-1"]) {
    assert.throws(() => run(agentConnect, [bad]), undefined, bad);
  }
});
