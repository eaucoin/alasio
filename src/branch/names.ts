/**
 * What a branch environment is called, and what it is known by, as alasio and its command
 * line both say it: its namespaces and the label they and its objects carry, the topic and
 * group its parent is asked to fork its sessions on, and its token to ask with (./fork.ts).
 * Free of anything but Node's own, so the command line takes it without the server.
 */
import { createHmac } from "node:crypto";

/** The label of a branch environment's objects and namespaces, whose value is its name. */
export const BRANCH_LABEL = "alasio.dev/branch";

/**
 * The label of a branch environment's sessions' namespace whose value is the environment it
 * was branched from, its parent's, main's: the one the sandbox gateway forks sessions into it
 * for (src/gateway/sessions.ts).
 */
export const BRANCH_PARENT_LABEL = "alasio.dev/branch-parent";

/** The name of main's environment, as its sessions are told apart from a branch's. */
export const MAIN_ENVIRONMENT_NAME = "main";

/** The topic of the log the environment `environment`'s branches ask it on to fork its sessions for them: a branch writes it, its alasio reads it. */
export const forksTopic = (environment: string): string => `forks.${environment}`;

/** The consumer group the environment `environment`'s alasio reads its branches' fork requests in. */
export const forksGroup = (environment: string): string => `alasio-forks.${environment}`;

/** The namespace of the branch environment `branch`: its alasio, its compute and its lake, and their Secrets. */
export const branchNamespace = (branch: string): string => `alasio-branch-${branch}`;

/** What ends the name of a branch environment's sessions' namespace, after its own's. */
export const SESSIONS_SUFFIX = "-sessions";

/** The namespace of the branch environment `branch`'s sessions. */
export const branchSessionsNamespace = (branch: string): string => `${branchNamespace(branch)}${SESSIONS_SUFFIX}`;

/** The token the branch `branch` forks its parent's sessions with: `key`'s HMAC of its name. */
export function forkToken(key: string, branch: string): string {
  return createHmac("sha256", key).update(`alasio-branch-fork:${branch}`).digest("base64url");
}
