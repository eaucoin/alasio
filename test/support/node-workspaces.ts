/**
 * A node agent's workspaces on directories of a test's own, the node's mount table a
 * stand-in's: what its commands mount and unmount, in order; which of its sandboxes'
 * containers run; and its pods' directories, as kubelet makes them.
 */
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Effect } from "effect";

import type { HostCommands } from "../../src/node-agent/host.ts";
import { makeJuicefs } from "../../src/node-agent/juicefs.ts";
import { RunscError } from "../../src/node-agent/runsc.ts";
import { makeWorkspaces } from "../../src/node-agent/workspaces.ts";

export function workspacesNode() {
  const root = mkdtempSync(join(tmpdir(), "alasio-workspaces-"));
  const workspacesRoot = join(root, "workspaces");
  const attachRoot = join(root, "attach");
  const mounts: string[] = [];
  const ran: string[] = [];
  const running = new Set<string>();
  const host: HostCommands = {
    run: (command) =>
      Effect.sync(() => {
        ran.push(command.join(" "));
        const [name, ...args] = command;
        if (name === "cat") return { exitCode: 0, stdout: mounts.map((point, n) => `${n + 30} 1 0:50 / ${point.replaceAll(" ", "\\040")} rw - fuse.juicefs JuiceFS:workspaces rw`).join("\n"), stderr: "" };
        if (name === "test") return { exitCode: existsSync(args[1] ?? "") ? 0 : 1, stdout: "", stderr: "" };
        if (name === "mount") mounts.push(args[2] ?? "");
        if (name === "umount") mounts.splice(mounts.indexOf(args[1] ?? ""), 1);
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
    'if [ ! -f "$file" ]; then echo "no quota of $path" >&2; exit 1; fi',
    'echo "+--+"; echo "| $path | $(cat "$file").0 GiB | 12 MiB | 1% | unlimited | 3 | |"; echo "+--+"',
  ].join("\n"));
  chmodSync(juicefs, 0o755);
  // Workspace storage mounted at the root, unless a test says it is not.
  const storage = { mounted: true };
  const workspaces = makeWorkspaces({
    host,
    mounted: () => Promise.resolve(storage.mounted),
    juicefs: makeJuicefs({ metaUrl: "redis://:the%20password@valkey.alasio.svc:6379/1", juicefs }),
    runsc: { container: (id) => (running.has(id) ? Effect.succeed({ release: "r", annotations: {}, tracksWrites: false, bundle: "" }) : Effect.fail(new RunscError({ release: "", command: "state", reason: "not found" }))) },
    workspacesRoot,
    attachRoot,
    agentUid: process.getuid?.() ?? 1000,
  });
  const files = (workspace: string) => {
    const directory = join(workspacesRoot, "main", workspace, "files");
    mkdirSync(join(directory, "workspace"), { recursive: true });
    mkdirSync(join(directory, "home"), { recursive: true });
    return directory;
  };
  /** `name`'s directory, as kubelet makes it as a pod of a pool starts. */
  const pod = (name: string) => mkdirSync(join(attachRoot, name), { recursive: true });
  /** What JuiceFS's command was run with, a call a line. */
  const juicefsCalls = () => (existsSync(join(root, "juicefs.log")) ? readFileSync(join(root, "juicefs.log"), "utf8").trim().split("\n") : []);
  return { root, workspacesRoot, attachRoot, mounts, ran, running, storage, workspaces, files, pod, juicefsCalls, done: () => rmSync(root, { recursive: true, force: true }) };
}

