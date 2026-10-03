import { execFile } from "node:child_process";
import { mkdir, readdir, realpath, stat } from "node:fs/promises";
import { basename, isAbsolute, join, relative, resolve, sep } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

/** Directory names a Telegram-created workspace may use. */
const WORKSPACE_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;

/** Cap on candidate folders rendered as buttons; the rest stay reachable by name. */
export const MAX_LISTED_WORKSPACES = 16;

/** A folder under the workspace root that can be mounted, git repositories first. */
export interface WorkspaceCandidate {
  readonly name: string;
  readonly path: string;
  readonly git: boolean;
}

export class WorkspaceError extends Error {}

function hasErrorCode(error: unknown, code: string): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === code;
}

function isInside(root: string, candidate: string): boolean {
  const rel = relative(root, candidate);
  return rel === "" || (!rel.startsWith(`..${sep}`) && rel !== ".." && !isAbsolute(rel));
}

/**
 * Canonicalize a workspace root once so every later check compares realpaths.
 */
export async function resolveWorkspaceRoot(root: string): Promise<string> {
  const canonical = await realpath(root);
  const info = await stat(canonical);
  if (!info.isDirectory()) {
    throw new WorkspaceError(`Workspace root ${root} is not a directory`);
  }
  return canonical;
}

/**
 * Resolve an operator-supplied folder to a canonical path that lives under the
 * workspace root. Symlinks are followed before the containment check so a link
 * pointing outside the root is rejected, and only existing directories qualify.
 */
export async function resolveWorkspacePath(
  { root, candidate }: { readonly root: string; readonly candidate: string | null | undefined },
): Promise<string> {
  const raw = String(candidate ?? "").trim();
  if (!raw) {
    throw new WorkspaceError("Folder path is empty.");
  }
  const canonicalRoot = await resolveWorkspaceRoot(root);
  const requested = isAbsolute(raw) ? resolve(raw) : resolve(canonicalRoot, raw);
  let canonical: string;
  try {
    canonical = await realpath(requested);
  } catch (error) {
    if (hasErrorCode(error, "ENOENT")) {
      throw new WorkspaceError(`Folder ${raw} does not exist under ${canonicalRoot}.`);
    }
    throw error;
  }
  if (!isInside(canonicalRoot, canonical)) {
    throw new WorkspaceError(`Folder ${raw} is outside the workspace root ${canonicalRoot}.`);
  }
  const info = await stat(canonical);
  if (!info.isDirectory()) {
    throw new WorkspaceError(`${raw} is not a directory.`);
  }
  return canonical;
}

async function isGitRepository(path: string): Promise<boolean> {
  try {
    return (await stat(join(path, ".git"))).isDirectory();
  } catch {
    return false;
  }
}

/**
 * Top-level folders under the root, git repositories first, hidden folders
 * skipped. Symlinked entries are listed only when they resolve inside the root.
 */
export async function listWorkspaceCandidates(root: string): Promise<WorkspaceCandidate[]> {
  const canonicalRoot = await resolveWorkspaceRoot(root);
  const entries = await readdir(canonicalRoot, { withFileTypes: true });
  const candidates: WorkspaceCandidate[] = [];
  for (const entry of entries) {
    if (entry.name.startsWith(".")) {
      continue;
    }
    const path = join(canonicalRoot, entry.name);
    let canonical: string;
    try {
      canonical = await realpath(path);
      if (!(await stat(canonical)).isDirectory() || !isInside(canonicalRoot, canonical)) {
        continue;
      }
    } catch {
      continue;
    }
    candidates.push({ name: entry.name, path: canonical, git: await isGitRepository(canonical) });
  }
  candidates.sort((a, b) => Number(b.git) - Number(a.git) || a.name.localeCompare(b.name));
  return candidates;
}

/**
 * Create a fresh git-initialized folder directly under the root.
 */
export async function createWorkspace(
  { root, name }: { readonly root: string; readonly name: string | null | undefined },
): Promise<string> {
  const trimmed = String(name ?? "").trim();
  if (!WORKSPACE_NAME_PATTERN.test(trimmed) || trimmed === "." || trimmed === "..") {
    throw new WorkspaceError("Folder names may only use letters, digits, dot, dash and underscore, and must start with a letter or digit.");
  }
  const canonicalRoot = await resolveWorkspaceRoot(root);
  const path = join(canonicalRoot, trimmed);
  try {
    await mkdir(path);
  } catch (error) {
    if (hasErrorCode(error, "EEXIST")) {
      throw new WorkspaceError(`Folder ${trimmed} already exists. Use /workspace ${trimmed} to mount it.`);
    }
    throw error;
  }
  await execFileAsync("git", ["init", "-q", "-b", "main"], { cwd: path });
  return path;
}

export function workspaceLabel(path: string): string {
  if (typeof path === "string" && path.startsWith("sessionfs:")) {
    return `empty workspace ${path.slice("sessionfs:".length)}`;
  }
  return basename(path) || path;
}
