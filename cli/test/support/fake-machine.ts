/**
 * A machine for the tests of what alasio does to it: its files under a directory of the
 * test's, which stands for its root, its inotify limits there as high as a cluster here
 * needs unless the test lowers them; the commands alasio runs on it as root and the
 * processes it kills, recorded; systemd, with k3s as its install script makes it a
 * service; and a firewall, ufw or firewalld, when the test enables one. The commands
 * alasio runs do to k3s and the firewall what they would: starting k3s writes its
 * kubeconfig and its node's password, uninstalling it removes what its uninstall script
 * does, and the firewall's rules are added, removed and said; unless the test says what a
 * command does.
 */
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

import { Effect, Layer } from "effect";

import { Systemd, type UnitState } from "../../src/cluster/host.ts";
import { type Answer, INOTIFY_MINIMUMS, RootCommandFailed, RootSystem } from "../../src/cluster/machine.ts";

/** A command run as root, as it was asked for. */
export interface RanCommand {
  readonly command: string;
  readonly args: readonly string[];
  readonly env: Readonly<Record<string, string>>;
}

/** What k3s's install script makes, and its uninstall script removes, beside what alasio wrote. */
const K3S_FILES = ["/usr/local/bin/k3s-killall.sh", "/usr/local/bin/k3s-uninstall.sh", "/etc/systemd/system/k3s.service"] as const;

export class FakeMachine {
  /** The directory that stands for its root. */
  readonly root: string;
  readonly ran: RanCommand[] = [];
  /** The executables of the processes each kill was for. */
  readonly killed: (readonly string[])[] = [];
  /** k3s's service, as systemd says it is. */
  unit: UnitState = { loaded: false, active: "inactive", enabled: false };
  /** firewalld's service. */
  firewalld: UnitState = { loaded: false, active: "inactive", enabled: false };
  /** The networks ufw lets in, by a rule of its own, when it is enabled. */
  readonly ufwRules = new Set<string>();
  /** The sources of firewalld's trusted zone, permanent, and how many times it was reloaded. */
  readonly trusted = new Set<string>();
  reloads = 0;
  /** The kubeconfig k3s writes as it starts. */
  kubeconfig = "";
  /** What happens as k3s starts, beside its kubeconfig written. */
  onStart: () => void = () => {};
  /** What a command does; it fails with the reason this returns, unless it returns nothing. */
  onRun: (ran: RanCommand) => string | void = (ran) => this.act(ran);

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

  /** Whether there is anything at `path` of the machine. */
  has(path: string): boolean {
    return existsSync(this.at(path));
  }

  /** Writes `content` to `path` of the machine, making its directory. */
  write(path: string, content: string | Buffer): void {
    mkdirSync(dirname(this.at(path)), { recursive: true });
    writeFileSync(this.at(path), content);
  }

  /** Enables ufw, as its config says, letting `networks` in already. */
  enableUfw(networks: readonly string[] = []): void {
    this.write("/etc/ufw/ufw.conf", "# /etc/ufw/ufw.conf\nENABLED=yes\nLOGLEVEL=low\n");
    for (const network of networks) this.ufwRules.add(network);
  }

  /** Starts firewalld, its trusted zone's sources `networks` already. */
  enableFirewalld(networks: readonly string[] = []): void {
    this.firewalld = { loaded: true, active: "active", enabled: true };
    for (const network of networks) this.trusted.add(network);
  }

  /** Sets the kernel's setting `name` (a sysctl name), as /proc/sys has it. */
  sysctl(name: string, value: number): void {
    this.write(`/proc/sys/${name.replaceAll(".", "/")}`, `${value}\n`);
  }

  /** The commands run, each as its command and arguments. */
  commands(): string[] {
    return this.ran.map(({ command, args }) => [command, ...args].join(" "));
  }

  /** k3s started: its service active, and its kubeconfig and its node's password written. */
  private start(): void {
    this.unit = { ...this.unit, active: "active" };
    this.write("/etc/rancher/k3s/k3s.yaml", this.kubeconfig);
    this.write("/etc/rancher/node/password", "node password\n");
    this.onStart();
  }

  /** What `ran` does to k3s and the firewall, as k3s's scripts, systemd, ufw and firewalld would. */
  act({ command, args, env }: RanCommand): void {
    const [verb, ...rest] = args;
    const source = /^--(add|remove)-source=(.+)$/u.exec(args.at(-1) ?? "");
    if (command === "ufw" && verb === "allow") {
      this.ufwRules.add(args[2] ?? "");
    } else if (command === "ufw" && verb === "delete") {
      this.ufwRules.delete(args[3] ?? "");
    } else if (command === "firewall-cmd" && verb === "--reload") {
      this.reloads += 1;
    } else if (command === "firewall-cmd" && source) {
      if (source[1] === "add") this.trusted.add(source[2] ?? "");
      else this.trusted.delete(source[2] ?? "");
    } else if (command === "sh" && env["INSTALL_K3S_EXEC"]) {
      for (const path of K3S_FILES) this.write(path, "#!/bin/sh\n");
      this.unit = { loaded: true, active: "inactive", enabled: true };
      this.start();
    } else if (command === "systemctl" && verb === "restart") {
      this.start();
    } else if (command === "systemctl" && verb === "enable") {
      this.unit = { ...this.unit, enabled: true };
      if (rest.includes("--now")) this.start();
    } else if (command === "systemctl" && (verb === "disable" || verb === "stop")) {
      this.unit = { ...this.unit, active: "inactive", enabled: verb === "disable" ? false : this.unit.enabled };
    } else if (command === "/usr/local/bin/k3s-uninstall.sh") {
      for (const path of [...K3S_FILES, "/usr/local/bin/k3s", "/etc/rancher/k3s", "/var/lib/rancher/k3s", "/var/lib/kubelet"]) rmSync(this.at(path), { recursive: true, force: true });
      this.unit = { loaded: false, active: "inactive", enabled: false };
    }
  }

  /** What `command` answers: ufw's rules, or whether firewalld's trusted zone has a source. */
  private answer(command: string, args: readonly string[]): Answer {
    if (command === "ufw" && args.join(" ") === "show added") {
      return { exitCode: 0, stdout: ["Added user rules (see 'ufw status' for running firewall):", ...[...this.ufwRules].map((network) => `ufw allow from ${network}`), ""].join("\n") };
    }
    const query = /^--query-source=(.+)$/u.exec(args.at(-1) ?? "");
    if (command === "firewall-cmd" && query) return { exitCode: this.trusted.has(query[1] ?? "") ? 0 : 1, stdout: "" };
    return { exitCode: 1, stdout: "" };
  }

  get systemd(): Layer.Layer<Systemd> {
    return Layer.succeed(Systemd, Systemd.of({ unit: (name) => Effect.sync(() => (name === "firewalld" ? this.firewalld : this.unit)) }));
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
        ask: (command, args) =>
          Effect.sync(() => {
            this.ran.push({ command, args, env: {} });
            return this.answer(command, args);
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
