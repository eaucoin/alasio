/**
 * Commands run on the node itself, as its own processes run them: in the namespaces of
 * its PID 1, which the agent sees as it runs in the node's PID namespace, its mounts (so
 * the node's own binaries) and its network (so its packet filter and connection
 * tracking), entered with nsenter. nsenter is util-linux's, which every Ubuntu image has,
 * alasio's (../../Dockerfile) among them; what it runs is the node's, iptables and
 * conntrack among them, so the image needs neither.
 */
import { execFile } from "node:child_process";

import { Effect, Schema } from "effect";

/** k3s's binaries, of the release it runs: what it bundles of a userland, iptables' and conntrack's among them, the rest in `aux/`. */
const K3S_BIN = "/var/lib/rancher/k3s/data/current/bin";

/**
 * The PATH commands run with on the node, as k3s orders it for what it runs, kube-router
 * among it (cmd/k3s/main.go): its binaries, the node's own, then its `aux/`, so a node's
 * own iptables is the one kube-router programs where the node has one. A node in Docker
 * (cli/src/cluster/docker.ts) runs k3s's image, which holds k3s's binaries in /bin and
 * its `aux/` in /bin/aux (iptables-save among them), last on the image's PATH, as here.
 */
export const NODE_PATH = `${K3S_BIN}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:${K3S_BIN}/aux:/bin/aux`;

/** How a command run on the node exited, and what it printed. */
export interface HostRun {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}

/** A command could not be run on the node, or failed there: which, and why. */
export class HostCommandError extends Schema.TaggedError<HostCommandError>()("HostCommandError", {
  command: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `${this.command} on the node failed: ${this.reason}`;
  }
}

/** Commands run on the node; see makeHostCommands. */
export interface HostCommands {
  /** Runs `command` on the node: how it exited, whatever its status; fails when it could not be run. */
  readonly run: (command: readonly string[]) => Effect.Effect<HostRun, HostCommandError>;
}

/** The image's nsenter. */
export const NSENTER = "/usr/bin/nsenter";

/** The most a command run on the node may print: a node's whole filter table, with room. */
const MAX_OUTPUT_BYTES = 64 * 1024 * 1024;

/** Commands run on the node, as the module says, entering it with `nsenter`. */
export function makeHostCommands(nsenter = NSENTER): HostCommands {
  return {
    run: (command) =>
      Effect.tryPromise({
        try: (signal) =>
          new Promise<HostRun>((resolve, reject) => {
            execFile(nsenter, ["--target", "1", "--mount", "--net", "--", ...command], { env: { PATH: NODE_PATH }, signal, maxBuffer: MAX_OUTPUT_BYTES }, (error, stdout, stderr) => {
              // A status of its own is the command's, or nsenter's when it could not run it (126, 127).
              if (error === null || typeof error.code === "number") resolve({ exitCode: typeof error?.code === "number" ? error.code : 0, stdout, stderr });
              else reject(error);
            });
          }),
        catch: (cause) => new HostCommandError({ command: command.join(" "), reason: cause instanceof Error ? cause.message : String(cause) }),
      }),
  };
}

/** What `command` printed on the node, failing with what it said unless its exit is `succeeded`'s, a status of 0 unless told. */
export const onNode = (
  host: HostCommands,
  command: readonly string[],
  succeeded: (ran: HostRun) => boolean = ({ exitCode }) => exitCode === 0,
): Effect.Effect<HostRun, HostCommandError> =>
  host.run(command).pipe(
    Effect.filterOrFail(succeeded, (ran) => new HostCommandError({ command: command.join(" "), reason: ran.stderr.trim() || `it exited ${ran.exitCode}` })),
  );
