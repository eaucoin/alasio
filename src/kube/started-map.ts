/**
 * Where alasio keeps the time of its last start (./started.ts), which `alasio status` reads
 * while alasio runs no replica (cli/src/commands/status.ts), and whose ConfigMap alasio's
 * manifests make, empty, alasio's Role letting it read and patch that one alone
 * (cli/src/manifests/alasio.ts). In a module of its own, with no dependency, so the command
 * line takes it as it is.
 */

/** The ConfigMap alasio keeps the time of its last start in, of its namespace. */
export const STARTED_CONFIG_MAP = "alasio-started";

/** Its key holding the time, as an ISO 8601 timestamp. */
export const STARTED_KEY = "startedAt";
