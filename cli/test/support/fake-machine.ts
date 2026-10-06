/**
 * A machine for the tests of what alasio does to it: its files under a directory of the
 * test's, which stands for its root, its inotify limits there as high as a cluster here
 * needs unless the test lowers them; and the commands alasio runs on it as root and the
 * processes it kills, recorded, each command doing what the test says it does.
 */
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

import { Effect, Layer } from "effect";

import { INOTIFY_MINIMUMS, RootCommandFailed, RootSystem } from "../../src/cluster/machine.ts";

/** A command run as root, as it was asked for. */
export interface RanCommand {
  readonly command: string;
  readonly args: readonly string[];
  readonly env: Readonly<Record<string, string>>;
}

export class FakeMachine {
  /** The directory that stands for its root. */
  readonly root: string;
  readonly ran: RanCommand[] = [];
  /** The executables of the processes each kill was for. */
  readonly killed: (readonly string[])[] = [];
  /** What a command does; it fails with the reason this returns, unless it returns nothing. */
  onRun: (ran: RanCommand) => string | void = () => {};

  constructor(root: string) {
    this.root = root;
    for (const [name, minimum] of Object.entries(INOTIFY_MINIMUMS)) this.sysctl(name, minimum);
  }

  /** Where `path` of the machine is, under its root. */
  at(path: string): string {
    return join(this.root, path);
  }

  /** The file at `path` of the machine, or null when there is none. */
  read(path: string): string | null {
    try {
      return readFileSync(this.at(path), "utf8");
    } catch {
      return null;
    }
  }

  /** Writes `content` to `path` of the machine, making its directory. */
  write(path: string, content: string | Buffer): void {
    mkdirSync(dirname(this.at(path)), { recursive: true });
    writeFileSync(this.at(path), content);
  }

  /** Sets the kernel's setting `name` (a sysctl name), as /proc/sys has it. */
  sysctl(name: string, value: number): void {
    this.write(`/proc/sys/${name.replaceAll(".", "/")}`, `${value}\n`);
  }

  /** The commands run, each as its command and arguments. */
  commands(): string[] {
    return this.ran.map(({ command, args }) => [command, ...args].join(" "));
  }

  get layer(): Layer.Layer<RootSystem> {
    return Layer.succeed(
      RootSystem,
      RootSystem.of({
        run: (command, args, env = {}) =>
          Effect.suspend(() => {
            const ran = { command, args, env };
            this.ran.push(ran);
            const failure = this.onRun(ran);
            return failure ? Effect.fail(new RootCommandFailed({ command: [command, ...args].join(" "), reason: failure })) : Effect.void;
          }),
        kill: (executables) =>
          Effect.sync(() => {
            this.killed.push(executables);
            return 0;
          }),
      }),
    );
  }
}
