/**
 * What a node agent does with workspaces: a workspace's files, on the node's mount of
 * workspace storage (WORKSPACES_ROOT, `<environment>/<workspace>/files`, holding
 * `workspace/` and `home/`), attached into the directory of a pod of a sandbox pool on its
 * node (`ATTACH_ROOT/<pod>`, which the pod mounts at /attach, its own by `subPathExpr`), at
 * `ws/`, with its binding's token beside it at `token`, and what its bayma is to run with at
 * `env` (variables, each `NAME='value'`, as a shell reads them); detached once its sandbox can no
 * longer act; and a file of one read, or one received written into it, for what alasio does
 * with a session's files outside its sandbox.
 *
 * A pod's directory is made by kubelet as its pod starts, by its mount of its own: an attach
 * into one that is not there, or is no directory, as no pod of a pool on this node has one, is
 * refused, and none is made. It serves one binding in its life, as its pod does: an attach into
 * one that holds a mount at or under it is refused, so a workspace is never mounted over another,
 * which the pod would go on reaching beneath it. A detach is refused while any container of
 * the pod's sandbox runs: under a sandbox that acts, a lazy unmount would leave it holding
 * descriptors into the workspace it no longer shows. Once its sandbox has ended (its
 * checkpoint ended it, or its containers stopped), each mount under its directory is
 * unmounted lazily, as the sandbox's gofer keeps them busy to the last, then the directory is
 * removed, so its pod can be deleted: kubelet deletes no pod whose directory holds a mount.
 *
 * A file read or written is reached by its path from the workspace's root alone, walked one
 * name at a time as ./volume.ts walks one: one any part of whose path is a symlink, or that
 * is not a regular file, is refused, and so is one not reached in time.
 */
import { chown, lstat, mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { Effect, Schema } from "effect";

import { isValidVolumeId } from "../sandbox/names.ts";
import { type HostCommandError, type HostCommands, onNode } from "./host.ts";
import { ATTACH_ROOT, WORKSPACES_ROOT } from "./names.ts";
import type { Runsc } from "./runsc.ts";
import { readUnder, writeUnder } from "./volume.ts";

/** The user and group a sandbox's agent runs as, whose its files are. */
export const AGENT_UID = 1000;

/** An environment's name, as it names its namespaces: main, or a branch's. */
const ENVIRONMENT = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/u;
/** A pod's name, as a label, which `subPathExpr` gives its directory. */
const POD = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/u;

/** What the agent was asked of a workspace and refused: why, and the status it answers with. */
export class WorkspaceRefused extends Schema.TaggedError<WorkspaceRefused>()("WorkspaceRefused", {
  status: Schema.Number,
  reason: Schema.String,
}) {
  override get message(): string {
    return this.reason;
  }
}

const refused = (status: number, reason: string) => new WorkspaceRefused({ status, reason });

/** A workspace, by its environment and its id. */
export interface WorkspaceRef {
  readonly environment: string;
  readonly workspace: string;
}

/** A variable's name, as a shell takes one. */
const VARIABLE = /^[A-Z_][A-Z0-9_]*$/u;

/** `env` as a shell reads it, each value quoted whole: `NAME='value'`, a quote in it closed, escaped, and opened again. */
export const envFile = (env: Readonly<Record<string, string>>): string =>
  Object.entries(env).map(([name, value]) => `${name}='${value.split("'").join("'\\''")}'\n`).join("");

/** A mount point as mountinfo writes it, its octal escapes (`\040` for a space) undone. */
const unescapeMountPoint = (field: string): string => field.replace(/\\([0-7]{3})/gu, (_escape, octal: string) => String.fromCharCode(Number.parseInt(octal, 8)));

/** The mount points of the node's mount table, from its mountinfo (proc(5)): each line's fifth field. */
export function mountPoints(mountinfo: string): string[] {
  return mountinfo.split("\n").flatMap((line) => {
    const point = line.split(" ")[4];
    return point ? [unescapeMountPoint(point)] : [];
  });
}

/** What a workspace's agent does with workspaces; see makeWorkspaces. */
export interface Workspaces {
  /** Attaches `ref`'s files into `pod`'s directory, its binding's `token` and the variables `env` beside them. */
  readonly attach: (ref: WorkspaceRef, pod: string, token: string, env: Readonly<Record<string, string>>) => Effect.Effect<void, WorkspaceRefused | HostCommandError>;
  /** Detaches what is attached into `pod`'s directory, once none of `containerIds`, its sandbox's containers by their runtime's ids, runs. */
  readonly detach: (pod: string, containerIds: readonly string[]) => Effect.Effect<void, WorkspaceRefused | HostCommandError>;
  /** The file at `path` of `ref`'s files, `workspace/...` or `home/...`, of `maxBytes` at most. */
  readonly read: (ref: WorkspaceRef, path: string, maxBytes: number) => Effect.Effect<Buffer, WorkspaceRefused>;
  /** Writes `bytes` as the file at `path` of `ref`'s files, its agent's, its directories made. */
  readonly write: (ref: WorkspaceRef, path: string, bytes: Uint8Array) => Effect.Effect<void, WorkspaceRefused>;
}

/** How long a file of a workspace may take to read or write: the workspace's storage may not answer. */
const FILE_TIMEOUT = "30 seconds";

/** `path`, of a workspace's files, as the names under them, or null where it is not one of `workspace/` or `home/`. */
function namesOf(path: string): string[] | null {
  const names = path.split("/");
  return names.length >= 2 && (names[0] === "workspace" || names[0] === "home") ? names : null;
}

/** `io` done, a failure of it refused, as `status` says by what it said, and one that does not end in time too. */
const confined = <A>(path: string, io: () => Promise<A>): Effect.Effect<A, WorkspaceRefused> =>
  Effect.tryPromise({
    try: io,
    catch: (error) => {
      const said = error instanceof Error ? error.message : String(error);
      return refused(/there is none/u.test(said) ? 404 : /more than/u.test(said) ? 413 : /names no file/u.test(said) ? 400 : 403, `${path}: ${said}`);
    },
  }).pipe(Effect.timeoutOrElse({ duration: FILE_TIMEOUT, orElse: () => Effect.fail(refused(504, `${path} was not reached within ${FILE_TIMEOUT}`)) }));

/** What makeWorkspaces is given: the node's commands (mounts are made in its mount namespace), its runsc, and, but for tests, where its roots are and whose files are. */
export interface WorkspacesOptions {
  readonly host: HostCommands;
  readonly runsc: Pick<Runsc, "container">;
  readonly workspacesRoot?: string;
  readonly attachRoot?: string;
  readonly agentUid?: number;
}

/** The agent's workspaces, as the module says. */
export function makeWorkspaces({ host, runsc, workspacesRoot = WORKSPACES_ROOT, attachRoot = ATTACH_ROOT, agentUid = AGENT_UID }: WorkspacesOptions): Workspaces {
  /** Where `ref`'s files are on the node, its names checked. */
  const filesOf = ({ environment, workspace }: WorkspaceRef): Effect.Effect<string, WorkspaceRefused> =>
    !ENVIRONMENT.test(environment) || !isValidVolumeId(workspace)
      ? Effect.fail(refused(400, `no workspace is named ${environment}/${workspace}`))
      : Effect.succeed(join(workspacesRoot, environment, workspace, "files"));

  /** Where `pod`'s directory is on the node, its name checked. */
  const podDirectory = (pod: string): Effect.Effect<string, WorkspaceRefused> =>
    POD.test(pod) ? Effect.succeed(join(attachRoot, pod)) : Effect.fail(refused(400, `${pod} is no pod's name`));

  /** The node's mount points at or under `directory`, deepest first. */
  const mountsUnder = (directory: string) =>
    onNode(host, ["cat", "/proc/self/mountinfo"]).pipe(
      Effect.map(({ stdout }) => mountPoints(stdout).filter((point) => point === directory || point.startsWith(`${directory}/`)).toSorted((a, b) => b.length - a.length)),
    );

  return {
    attach: (ref, pod, token, env) =>
      Effect.gen(function*() {
        const files = yield* filesOf(ref);
        const unnamed = Object.keys(env).find((name) => !VARIABLE.test(name));
        if (unnamed !== undefined) return yield* refused(400, `${unnamed} is no variable's name`);
        const directory = yield* podDirectory(pod);
        const made = yield* Effect.promise(() => lstat(directory).then((stats) => stats.isDirectory(), () => false));
        if (!made) return yield* refused(404, `no pod of a pool on this node is named ${pod}: kubelet has made it no directory`);
        const held = yield* mountsUnder(directory);
        if (held.length > 0) return yield* refused(409, `${pod}'s directory holds ${held.join(", ")}: a pod serves one binding, and nothing is mounted over another`);
        const found = yield* host.run(["test", "-d", files]);
        if (found.exitCode !== 0) return yield* refused(404, `there is no workspace ${ref.environment}/${ref.workspace}`);
        const ws = join(directory, "ws");
        yield* Effect.promise(async () => {
          await mkdir(ws);
          await chown(directory, agentUid, agentUid);
          await chown(ws, agentUid, agentUid);
          // Beside the workspace, not in it: its sandbox reads it, and reaches no other's.
          await writeFile(join(directory, "env"), envFile(env), { mode: 0o400 });
          await chown(join(directory, "env"), agentUid, agentUid);
          // Written last, as its bayma starts once it is there.
          await writeFile(join(directory, "token"), token, { mode: 0o400 });
          await chown(join(directory, "token"), agentUid, agentUid);
        });
        yield* onNode(host, ["mount", "--bind", files, ws]);
      }),

    detach: (pod, containerIds) =>
      Effect.gen(function*() {
        const directory = yield* podDirectory(pod);
        const running = yield* Effect.forEach(containerIds, (id) => runsc.container(id).pipe(Effect.as(true), Effect.orElseSucceed(() => false)));
        if (running.some(Boolean)) return yield* refused(409, `${pod}'s sandbox still runs: what is attached into it is detached once it can no longer act`);
        for (const point of yield* mountsUnder(directory)) yield* onNode(host, ["umount", "--lazy", point]);
        yield* Effect.promise(() => rm(directory, { recursive: true, force: true }));
      }),

    read: (ref, path, maxBytes) =>
      Effect.gen(function*() {
        const names = namesOf(path);
        if (!names) return yield* refused(400, `${path} is not a path of a workspace's files`);
        const root = yield* filesOf(ref);
        return yield* confined(path, () => readUnder({ root, names }, maxBytes));
      }),

    write: (ref, path, bytes) =>
      Effect.gen(function*() {
        const names = namesOf(path);
        if (!names) return yield* refused(400, `${path} is not a path of a workspace's files`);
        const root = yield* filesOf(ref);
        yield* confined(path, () => writeUnder({ root, names }, bytes, { uid: agentUid, gid: agentUid }));
      }),
  };
}
