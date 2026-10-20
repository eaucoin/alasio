/**
 * A node agent's workspaces on directories of a test's own, the node's mount table a
 * stand-in's: what its commands mount and unmount, in order; which of its sandboxes'
 * containers run; its pods' directories, as kubelet makes them; and the workspaces whose quota
 * the node's mount does not hold yet, for as long as `quotaHeldWithinMs` is waited for one.
 */
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

import { Effect } from "effect";

import type { HostCommands } from "../../src/node-agent/host.ts";
import { makeJuicefs } from "../../src/node-agent/juicefs.ts";
import { RunscError } from "../../src/node-agent/runsc.ts";
import { makeWorkspaces } from "../../src/node-agent/workspaces.ts";

export function workspacesNode({ quotaHeldWithinMs = 1_000 }: { readonly quotaHeldWithinMs?: number } = {}) {
  const root = mkdtempSync(join(tmpdir(), "alasio-workspaces-"));
  const workspacesRoot = join(root, "workspaces");
  const attachRoot = join(root, "attach");
  const mounts: string[] = [];
  // The mounts the agent's own namespace holds, as one started after an attach copied them.
  const ownMounts: string[] = [];
  // The mount points an unmount does not take, as one that did not.
  const stuck = new Set<string>();
  const ran: string[] = [];
  // Of each mount made, whether a token was beside it already.
  const tokenAtMount: boolean[] = [];
  const running = new Set<string>();
  // Of each container that runs, the annotations containerd gives its spec; what runsc ran in them, in order; and whether a hook fails.
  const annotations = new Map<string, Readonly<Record<string, string>>>();
  const execs: string[] = [];
  const hooks = { fail: false };
  const host: HostCommands = {
    run: (command) =>
      Effect.sync(() => {
        ran.push(command.join(" "));
        const [name, ...args] = command;
        if (name === "cat") return { exitCode: 0, stdout: mounts.map((point, n) => `${n + 30} 1 0:50 / ${point.replaceAll(" ", "\\040")} rw - fuse.juicefs JuiceFS:workspaces rw`).join("\n"), stderr: "" };
        if (name === "test") return { exitCode: existsSync(args[1] ?? "") ? 0 : 1, stdout: "", stderr: "" };
        if (name === "mount") {
          mounts.push(args[2] ?? "");
          tokenAtMount.push(existsSync(join(args[2] ?? "", "..", "token")));
        }
        if (name === "umount" && !stuck.has(args[1] ?? "")) mounts.splice(mounts.indexOf(args[1] ?? ""), 1);
        return { exitCode: 0, stdout: "", stderr: "" };
      }),
  };
  // JuiceFS's command, as a stand-in: each call's arguments and password logged, each directory's quota kept in a file of its own.
  const juicefs = join(root, "juicefs");
  const quotaDir = join(root, "quotas");
  mkdirSync(quotaDir);
  writeFileSync(juicefs, [
    "#!/bin/sh",
    `echo "$* META_PASSWORD=$META_PASSWORD" >> ${join(root, "juicefs.log")}`,
    'if [ "$1" = clone ]; then cp -a "$3" "$4"; exit; fi',
    'path="$5"; file="' + quotaDir + '/$(echo "$path" | tr / _)"',
    'if [ "$2" = delete ]; then rm -f "$file"; exit; fi',
    'if [ "$2" = set ]; then echo "$7" > "$file"; fi',
    // A check without repair finds the quota off as many times as the test says, as JuiceFS says it.
    `if [ "$2" = check ] && [ -z "$6" ] && [ "$(cat ${join(root, "inconsistent")} 2>/dev/null || echo 0)" -gt 0 ]; then`,
    `  echo $(( $(cat ${join(root, "inconsistent")}) - 1 )) > ${join(root, "inconsistent")}`,
    '  echo "<FATAL>: quota of $path is inconsistent, please repair it with --repair flag [main@main.go:31]" >&2; exit 1',
    "fi",
    'if [ ! -f "$file" ]; then echo "no quota of $path" >&2; exit 1; fi',
    'echo "+--+"; echo "| $path | $(cat "$file").0 GiB | 12 MiB | 1% | unlimited | 3 | |"; echo "+--+"',
  ].join("\n"));
  chmodSync(juicefs, 0o755);
  // Workspace storage mounted at the root, unless a test says it is not.
  const storage = { mounted: true };
  // The pods whose attached workspace is unreachable, `<namespace>/<pod>` each, as a test says.
  const unreachable = new Set<string>();
  // The workspaces whose quota the node's mount does not hold, by name, as a test says; and each look at one, `<directory> <root>`.
  const unheld = new Set<string>();
  const looks: string[] = [];
  const workspaces = makeWorkspaces({
    host,
    mounted: () => Promise.resolve(storage.mounted),
    reaches: (point) => Promise.resolve(![...unreachable].some((pod) => point === join(attachRoot, pod, "ws"))),
    holds: (directory, at) => {
      looks.push(`${directory} ${at}`);
      return Promise.resolve(!unheld.has(basename(directory)));
    },
    quotaHeldWithinMs,
    juicefs: makeJuicefs({ metaUrl: "redis://:the%20password@valkey.alasio.svc:6379/1", juicefs }),
    runsc: {
      container: (id) =>
        running.has(id) ? Effect.succeed({ release: "r", annotations: { ...annotations.get(id) }, tracksWrites: false, bundle: "" }) : Effect.fail(new RunscError({ release: "", command: "state", reason: "not found" })),
      exec: (release, id, argv) =>
        Effect.suspend(() => {
          execs.push(`${release} ${id} ${argv.join(" ")}`);
          return hooks.fail ? Effect.fail(new RunscError({ release, command: "exec", reason: "the hook exited 1" })) : Effect.void;
        }),
    },
    workspacesRoot,
    attachRoot,
    agentUid: process.getuid?.() ?? 1000,
    quotaSettleMs: 0,
    own: {
      under: (directory) => Promise.resolve(ownMounts.filter((point) => point === directory || point.startsWith(`${directory}/`))),
      unmount: (point) => Promise.resolve(void ownMounts.splice(ownMounts.indexOf(point), 1)),
    },
  });
  /** Has the next `count` checks of a quota, without repair, find it off. */
  const inconsistentChecks = (count: number) => writeFileSync(join(root, "inconsistent"), `${count}\n`);
  const files = (workspace: string) => {
    const directory = join(workspacesRoot, "main", workspace, "files");
    mkdirSync(join(directory, "workspace"), { recursive: true });
    mkdirSync(join(directory, "home"), { recursive: true });
    return directory;
  };
  /** The directory of `name` of `namespace`, as kubelet makes it as a pod of a pool starts. */
  const pod = (name: string, namespace = "main") => mkdirSync(join(attachRoot, namespace, name), { recursive: true });
  /** What JuiceFS's command was run with, a call a line. */
  const juicefsCalls = () => (existsSync(join(root, "juicefs.log")) ? readFileSync(join(root, "juicefs.log"), "utf8").trim().split("\n") : []);
  return { root, workspacesRoot, attachRoot, mounts, stuck, ran, tokenAtMount, running, annotations, execs, hooks, storage, unreachable, unheld, looks, workspaces, files, pod, juicefsCalls, inconsistentChecks, ownMounts, done: () => rmSync(root, { recursive: true, force: true }) };
}

