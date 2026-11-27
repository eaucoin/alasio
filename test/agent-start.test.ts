/**
 * What a sandbox pool's pod runs before bayma (sandbox/agent/start.sh), run by sh on
 * directories of the test's own: bayma's server once in a pod, its hook as often as it is asked.
 */
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { SANDBOX_STOP_REASON_VARIABLE } from "../src/kube/sandbox-shape.ts";
import { SAVE_HOOK_SECONDS } from "../src/node-agent/names.ts";

const START = new URL("../sandbox/agent/start.sh", import.meta.url).pathname;

/** A pod's directories, of a test's own, with its binding's token and variables written, and bayma's hook in them: `hook`, a script. */
function pod(hook: string): { root: string; workspace: string; env: Record<string, string> } {
  const root = mkdtempSync(join(tmpdir(), "alasio-agent-start-"));
  const attach = join(root, "attach");
  const workspace = join(root, "workspace");
  for (const directory of [attach, workspace, join(root, "home")]) mkdirSync(directory);
  writeFileSync(join(attach, "env"), "BAYMA_EXAMPLE='bound'\n");
  writeFileSync(join(attach, "token"), "binding");
  writeFileSync(join(root, "hook"), `#!/bin/sh\n${hook}\n`, { mode: 0o755 });
  return {
    root,
    workspace,
    env: { PATH: process.env["PATH"] ?? "", HOME: join(root, "home"), ALASIO_ATTACH: attach, ALASIO_WORKSPACE: workspace, ALASIO_BAYMA_HOOK: join(root, "hook") },
  };
}

test("bayma's server is started once in a pod, its container started again running nothing of the workspace, and its hook as often as it is asked", () => {
  const { root, workspace, env } = pod("exit 0");
  try {
    const start = (...args: string[]) => spawnSync("sh", [START, "sh", "-c", 'echo "$BAYMA_EXAMPLE in $(pwd)"', ...args], { env, encoding: "utf8" });
    const served = start("mcp-http");
    assert.deepEqual([served.status, served.stdout.trim()], [0, `bound in ${workspace}`]);
    const again = start("mcp-http");
    assert.equal(again.status, 1);
    assert.match(again.stderr, /has served this pod's binding already/u);
    for (const _ of [1, 2]) assert.equal(start("sandbox-hook", "stop").status, 0);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("bayma's hook is the image's without Node, given what follows sandbox-hook, and none of the workspace's setup", () => {
  const { root, env } = pod('echo "$# $* ${BAYMA_EXAMPLE:-unset} in $(pwd)"');
  try {
    // As the agent and gVisor run it: after bayma's command, which the hook does not start.
    const hook = (mode: string | null, ...args: string[]) =>
      spawnSync("sh", [START, "node", "--expose-gc", "/opt/bayma/bayma.js", "sandbox-hook", ...args], {
        env: mode === null ? env : { ...env, GVISOR_SAVE_RESTORE_AUTO_EXEC_MODE: mode },
        cwd: root,
        encoding: "utf8",
      });
    const ran = (result: ReturnType<typeof hook>) => [result.status, result.stdout.trim(), result.stderr];
    assert.deepEqual(ran(hook(null, "stop", "--state-dir", "/attach/ws/home/.bayma")), [0, `3 stop --state-dir /attach/ws/home/.bayma unset in ${root}`, ""]);
    for (const mode of ["save", "resume", "restore"]) {
      assert.deepEqual(ran(hook(mode, "--state-dir", "/attach/ws/home/.bayma", "--leave-running")), [
        0,
        `3 --state-dir /attach/ws/home/.bayma --leave-running unset in ${root}`,
        "",
      ]);
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("bayma's hook, run as the sandbox is restored, is told why it stopped, as the binding's variables say it, and of them that alone", () => {
  const { root, env } = pod('echo "${BAYMA_SANDBOX_STOP_REASON:-unsaid} ${BAYMA_EXAMPLE:-unset}"');
  try {
    const hook = (mode: string) =>
      spawnSync("sh", [START, "node", "/opt/bayma/bayma.js", "sandbox-hook", "--state-dir", "/attach/ws/home/.bayma"], {
        env: { ...env, GVISOR_SAVE_RESTORE_AUTO_EXEC_MODE: mode },
        encoding: "utf8",
      }).stdout.trim();
    // A restore of a sandbox its host stopped for no reason it says is told none.
    assert.equal(hook("restore"), "unsaid unset");
    writeFileSync(join(root, "attach", "env"), `BAYMA_EXAMPLE='bound'\n${SANDBOX_STOP_REASON_VARIABLE}='it ran out of its memory (1Gi)'\n`);
    assert.equal(hook("restore"), "it ran out of its memory (1Gi) unset");
    // A save is told nothing of it.
    assert.equal(hook("save"), "unsaid unset");
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("bayma's hook is killed past its bound as it readies the sandbox for a save, or goes on after one taken live, and has runsc's longer one as the sandbox is restored", () => {
  // A hook that takes 2 s, its bound for a save 1 s.
  const { root, env } = pod("sleep 2");
  try {
    const hook = (mode: string) =>
      spawnSync("sh", [START, "node", "/opt/bayma/bayma.js", "sandbox-hook", "--state-dir", "/attach/ws/home/.bayma"], {
        env: { ...env, ALASIO_SAVE_HOOK_SECONDS: "1", GVISOR_SAVE_RESTORE_AUTO_EXEC_MODE: mode },
        encoding: "utf8",
      });
    // Ended as timeout(1) ends what outlasts it: 124.
    assert.deepEqual(["save", "resume", "restore"].map((mode) => hook(mode).status), [124, 124, 0]);
    // Its bound for a save, but for tests, the agent's.
    assert.equal(/ALASIO_SAVE_HOOK_SECONDS:-(\d+)/u.exec(readFileSync(START, "utf8"))?.[1], String(SAVE_HOOK_SECONDS));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("bayma's server, as a pool's pod starts, opens none of the image's binaries before its binding is given, wherever its nodes serve the image", async (t) => {
  const { root, env: podEnv } = pod("exit 0");
  try {
    const attach = join(root, "attach");
    // bayma's node, first on its PATH, which says whether it was read by its access time: set long before it was written,
    // which a read moves on where the files' mount keeps access times at all.
    const bin = join(root, "bin");
    mkdirSync(bin);
    const node = join(bin, "node");
    writeFileSync(node, "#!/bin/sh\n", { mode: 0o755 });
    const unread = () => utimesSync(node, new Date("2000-01-01T00:00:00Z"), new Date());
    const read = () => statSync(node).atimeMs > Date.parse("2001-01-01T00:00:00Z");
    unread();
    readFileSync(node);
    if (!read()) return void t.skip(`${root} keeps no access times`);
    unread();
    rmSync(join(attach, "token"));
    // As a pool's pod was told to open them where its nodes served the image through passthrough.
    const server = spawn("sh", [START, "true", "mcp-http"], { env: { ...podEnv, PATH: `${bin}:${podEnv["PATH"]}`, ALASIO_WARM: "1" } });
    const ended = new Promise<number | null>((resolve) => server.once("exit", resolve));
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    const early = read();
    writeFileSync(join(attach, "token"), "binding");
    assert.equal(await ended, 0);
    assert.equal(early, false, "bayma's node was read before the pod's binding was given");
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
