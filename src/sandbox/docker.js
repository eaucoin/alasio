/**
 * The one place session-filesystem code touches Docker, so the rest of the sandbox
 * modules take a small, faked-in-tests interface rather than child_process directly.
 * alasio already drives Docker (its Neon stack, bayma); this adds the session hosts.
 */
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export function createDocker(run = execFileAsync) {
  return {
    /**
     * Run a docker command to completion; returns `{ stdout, stderr }`, throws on non-zero.
     * `encoding: "buffer"` returns the output as bytes (a file read out of a container).
     */
    async cli(args, { input, encoding, maxBuffer = 16 * 1024 * 1024 } = {}) {
      const child = run("docker", args, { maxBuffer, ...(encoding ? { encoding } : {}) });
      if (input !== undefined && child.child?.stdin) {
        child.child.stdin.end(input);
      }
      return child;
    },
    /** True if a container of this name is running. */
    async isRunning(name) {
      const { stdout } = await execFileAsync("docker", ["ps", "--quiet", "--filter", `name=^${name}$`, "--filter", "status=running"]);
      return stdout.trim() !== "";
    },
    /** Spawn a long-lived docker process the caller owns (returns a ChildProcess). */
    spawn(args, options = {}) {
      return spawn("docker", args, options);
    },
  };
}
