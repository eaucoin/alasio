/**
 * A node agent's workspaces on directories of a test's own, the node's mount table a
 * stand-in's: what its commands mount and unmount, in order; which of its sandboxes'
 * containers run; its pods' directories, as kubelet makes them; and the workspaces whose quota
 * the node's mount does not hold yet, for as long as `quotaHeldWithinMs` is waited for one; and a
 * sweep's grace for a directory whose pod is gone, `orphanGraceMs`, none unless given. Its
 * unmounts wait while a test holds them (`unmounts.held`), and one of a point the mount table
 * does not have fails as util-linux's umount does.
 */
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

import { Effect, Exit, Scope } from "effect";

import type { HostCommands } from "../../src/node-agent/host.ts";
import { makeJuicefs } from "../../src/node-agent/juicefs.ts";
import { RunscError } from "../../src/node-agent/runsc.ts";
import { makeWorkspaces } from "../../src/node-agent/workspaces.ts";

export function workspacesNode({ quotaHeldWithinMs = 1_000, orphanGraceMs = 0 }: { readonly quotaHeldWithinMs?: number; readonly orphanGraceMs?: number } = {}) {
  const root = mkdtempSync(join(tmpdir(), "alasio-workspaces-"));
  const workspacesRoot = join(root, "workspaces");
  const attachRoot = join(root, "attach");
  const mounts: string[] = [];
  // The mounts the agent's own namespace holds, as one started after an attach copied them.
  const ownMounts: string[] = [];
  // The mount points an unmount does not take, as one that did not; and those it fails for, as one busy.
  const stuck = new Set<string>();
  const busy = new Set<string>();
  const ran: string[] = [];
  // Of each mount made, whether a token was beside it already.
  const tokenAtMount: boolean[] = [];
  const running = new Set<string>();
  // Of each container that runs, the annotations containerd gives its spec; what runsc ran in them, in order; and whether a hook fails.
  const annotations = new Map<string, Readonly<Record<string, string>>>();
  const execs: string[] = [];
  const hooks = { fail: false };
  // Whether runsc's list of its containers fails, as a test says.
  const runscFails = { list: false };
  // What each unmount waits on before it is done, as a test holds them: none unless one does.
  const unmounts: { held: Promise<void> | null } = { held: null };
  const host: HostCommands = {
    run: (command) =>
      Effect.sync(() => ran.push(command.join(" "))).pipe(
        Effect.andThen(() => {
          const { held } = unmounts;
          return command[0] === "umount" && held !== null ? Effect.promise(() => held) : Effect.void;
        }),
        Effect.map(() => {
          const [name, ...args] = command;
          if (name === "cat") return { exitCode: 0, stdout: mounts.map((point, n) => `${n + 30} 1 0:50 / ${point.replaceAll(" ", "\\040")} rw - fuse.juicefs JuiceFS:workspaces rw`).join("\n"), stderr: "" };
          if (name === "test") return { exitCode: existsSync(args[1] ?? "") ? 0 : 1, stdout: "", stderr: "" };
          if (name === "mount") {
            mounts.push(args[2] ?? "");
            tokenAtMount.push(existsSync(join(args[2] ?? "", "..", "token")));
          }
          if (name === "umount") {
            const at = mounts.indexOf(args[1] ?? "");
            // As util-linux's umount says of a point the mount table does not have.
            if (at < 0) return { exitCode: 32, stdout: "", stderr: `umount: ${args[1] ?? ""}: not mounted.\n` };
            if (busy.has(args[1] ?? "")) return { exitCode: 32, stdout: "", stderr: `umount: ${args[1] ?? ""}: target is busy.\n` };
            if (!stuck.has(args[1] ?? "")) mounts.splice(at, 1);
          }
          return { exitCode: 0, stdout: "", stderr: "" };
        }),
      ),
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
  // The scope the workspaces' locks live in, closed as the test is done.
  const scope = Scope.makeUnsafe();
  const workspaces = Effect.runSync(makeWorkspaces({
    host,
    mounted: () => Promise.resolve(storage.mounted),
    reaches: (point) => Promise.resolve(![...unreachable].some((pod) => point === join(attachRoot, pod, "ws"))),
    holds: (directory, at) => {
      looks.push(`${directory} ${at}`);
      return Promise.resolve(!unheld.has(basename(directory)));
    },
    quotaHeldWithinMs,
    orphanGraceMs,
    juicefs: makeJuicefs({ metaUrl: "redis://:the%20password@valkey.alasio.svc:6379/1", juicefs }),
    runsc: {
      container: (id) =>
        running.has(id) ? Effect.succeed({ release: "r", annotations: { ...annotations.get(id) }, tracksWrites: false, bundle: "" }) : Effect.fail(new RunscError({ release: "", command: "state", reason: "not found" })),
      // Each that has annotations, whether it runs or not, as runsc lists what it keeps.
      containers: Effect.suspend(() =>
        runscFails.list ? Effect.fail(new RunscError({ release: "the node's", command: "list", reason: "runsc could not list its containers" })) : Effect.succeed([...annotations].map(([id, of]) => ({ id, bundle: "", annotations: { ...of } })))
      ),
      // A process of each that runs, as runsc runs one for a container's sandbox.
      processes: (containers) => Effect.sync(() => containers.filter(({ id }) => running.has(id)).map(({ id }, n) => ({ containerId: id, pid: 1000 + n, startTime: "1" }))),
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
  }).pipe(Scope.provide(scope)));
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
  return { root, workspacesRoot, attachRoot, mounts, stuck, busy, ran, tokenAtMount, running, annotations, execs, hooks, runscFails, storage, unreachable, unheld, looks, workspaces, files, pod, juicefsCalls, inconsistentChecks, ownMounts, unmounts, done: () => {
    Effect.runSync(Scope.close(scope, Exit.void));
    rmSync(root, { recursive: true, force: true });
  } };
}

