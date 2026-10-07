/**
 * What a branch environment is called, and what it is known by, as alasio and its command
 * line both say it: its namespaces, the port its parent forks its sessions on, and its
 * token to ask with (./fork.ts). Free of anything but Node's own, so the command line
 * takes it without the server.
 */
import { createHmac } from "node:crypto";

/** The port an alasio forks its sessions for its branch environments on. */
export const BRANCH_FORK_PORT = 4319;

/** The namespace of the branch environment `branch`: its alasio, its compute and its lake, and their Secrets. */
export const branchNamespace = (branch: string): string => `alasio-branch-${branch}`;

/** The namespace of the branch environment `branch`'s sessions. */
export const branchSessionsNamespace = (branch: string): string => `${branchNamespace(branch)}-sessions`;

/** The token the branch `branch` forks its parent's sessions with: `key`'s HMAC of its name. */
export function forkToken(key: string, branch: string): string {
  return createHmac("sha256", key).update(`alasio-branch-fork:${branch}`).digest("base64url");
}
