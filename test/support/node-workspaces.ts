/**
 * A node agent's workspaces on directories of a test's own, the node's mount table a
 * stand-in's: what its commands mount and unmount, in order; and which of its sandboxes'
 * containers run.
 */
import { existsSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Effect } from "effect";

import type { HostCommands } from "../../src/node-agent/host.ts";
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
  const workspaces = makeWorkspaces({
    host,
    runsc: { container: (id) => (running.has(id) ? Effect.succeed({ release: "r", annotations: {} }) : Effect.fail(new RunscError({ release: "", command: "state", reason: "not found" }))) },
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
  return { root, workspacesRoot, attachRoot, mounts, ran, running, workspaces, files, done: () => rmSync(root, { recursive: true, force: true }) };
}

