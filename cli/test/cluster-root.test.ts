/**
 * What alasio does as root (cli/src/cluster/root.ts): through sudo, as `alasio as-root`,
 * given the steps on its stdin, after saying what they are, and without asking for a
 * password where there is no terminal to ask at; and `alasio as-root` itself, which does
 * the steps it reads. sudo is a script of the test's, first on PATH, that keeps what it is
 * asked and does nothing.
 */
import assert from "node:assert/strict";
import { chmodSync, existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import { NodeServices } from "@effect/platform-node";
import { Effect, Layer, Logger, Schema, Stdio } from "effect";

import { Machine, RaiseInotify, SYSCTL_FILE } from "../src/cluster/machine.ts";
import { Root, RootSteps } from "../src/cluster/root.ts";
import { runAlasio } from "./support/cli.ts";
import { FakeMachine } from "./support/fake-machine.ts";

const STEPS = [RaiseInotify.make({ limits: [{ name: "fs.inotify.max_user_instances", minimum: 1024 }] })];

/** A sudo of the test's on PATH, which exits with `answer` to `sudo -n true` and with `exitCode` otherwise; what it was asked, and given. */
function fakeSudo(t: TestContext, { answer = 0, exitCode = 0 } = {}): { readonly asked: () => string[]; readonly given: () => string } {
  const directory = mkdtempSync(join(tmpdir(), "alasio-sudo-"));
  const log = join(directory, "asked");
  const stdin = join(directory, "given");
  writeFileSync(join(directory, "sudo"), [
    "#!/bin/sh",
    `printf '%s\\n' "$*" >>'${log}'`,
    `if [ "$*" = "-n true" ]; then exit ${answer}; fi`,
    `cat >'${stdin}'`,
    `exit ${exitCode}`,
  ].join("\n"));
  chmodSync(join(directory, "sudo"), 0o755);
  const path = process.env["PATH"];
  process.env["PATH"] = `${directory}:${path}`;
  t.after(() => {
    process.env["PATH"] = path;
    rmSync(directory, { recursive: true, force: true });
  });
  return {
    asked: () => (existsSync(log) ? readFileSync(log, "utf8").trim().split("\n") : []),
    given: () => readFileSync(stdin, "utf8"),
  };
}

/** Root's run of `steps`, through sudo, with stdin a terminal or not: its failure's message, if it failed, and what it said. */
async function throughSudo(t: TestContext, steps: typeof STEPS, terminal: boolean): Promise<{ readonly failed: string | null; readonly said: readonly string[] }> {
  const machine = new FakeMachine(mkdtempSync(join(tmpdir(), "alasio-machine-")));
  t.after(() => rmSync(machine.root, { recursive: true, force: true }));
  const said: string[] = [];
  const failed = await Effect.runPromise(
    Effect.flatMap(Root, (root) => root.run(steps)).pipe(
      Effect.match({ onFailure: (error) => error.message, onSuccess: () => null }),
      Effect.provide(
        Root.layer.pipe(
          Layer.provide(Layer.mergeAll(
            NodeServices.layer,
            Layer.succeed(Machine, { platform: "linux", arch: "x64", root: machine.root }),
            machine.layer,
            Stdio.layerTest({ stdinIsTerminal: Effect.succeed(terminal) }),
          )),
        ),
      ),
      Effect.provide(Logger.layer([Logger.make(({ message }) => void said.push(String(message)))])),
    ),
  );
  return { failed, said };
}

const asRoot = (sudoArgs: readonly string[]) => [...sudoArgs, "--", process.execPath, ...process.execArgv, process.argv[1], "as-root"].join(" ");

const skip = process.getuid?.() === 0 && "a process of root's does the steps itself";

test("without a terminal, the steps run through sudo that asks for no password, saying first what they are", { skip }, async (t) => {
  const sudo = fakeSudo(t);
  const { failed, said } = await throughSudo(t, STEPS, false);
  assert.equal(failed, null);
  assert.deepEqual(sudo.asked(), ["-n true", asRoot(["-n"])]);
  assert.deepEqual(Schema.decodeUnknownSync(RootSteps)(sudo.given()), STEPS);
  assert.deepEqual(said, [`alasio changes this machine as root, through sudo:\n  raise fs.inotify.max_user_instances to 1024, now and for every boot, in ${SYSCTL_FILE}`]);
});

test("at a terminal, sudo may ask for a password", { skip }, async (t) => {
  const sudo = fakeSudo(t, { answer: 1 });
  assert.equal((await throughSudo(t, STEPS, true)).failed, null);
  assert.deepEqual(sudo.asked(), [asRoot([])]);
});

test("without a terminal, a sudo that would ask for a password is not run, and what alasio must do as root is said", { skip }, async (t) => {
  const sudo = fakeSudo(t, { answer: 1 });
  const { failed } = await throughSudo(t, STEPS, false);
  assert.equal(failed, [
    "alasio must change this machine as root, and sudo cannot ask for a password without a terminal:",
    `  raise fs.inotify.max_user_instances to 1024, now and for every boot, in ${SYSCTL_FILE}`,
    "Run it at a terminal, or as root, or where sudo asks for no password.",
  ].join("\n"));
  assert.deepEqual(sudo.asked(), ["-n true"]);
});

test("steps that fail through sudo fail the command, which says they said why", { skip }, async (t) => {
  fakeSudo(t, { exitCode: 3 });
  assert.equal((await throughSudo(t, STEPS, false)).failed, "what alasio did as root, through sudo, exited with 3; it said why above");
});

test("no steps ask nothing of sudo", { skip }, async (t) => {
  const sudo = fakeSudo(t);
  assert.equal((await throughSudo(t, [], false)).failed, null);
  assert.deepEqual(sudo.asked(), []);
});

test("as-root does the steps it reads on stdin", async (t) => {
  const home = mkdtempSync(join(tmpdir(), "alasio-as-root-"));
  t.after(() => rmSync(home, { recursive: true, force: true }));
  const machine = new FakeMachine(join(home, "machine"));
  machine.sysctl("fs.inotify.max_user_instances", 128);
  const run = await runAlasio(["as-root"], { env: { HOME: home }, machine, stdin: Schema.encodeSync(RootSteps)(STEPS) });
  assert.ok(run.exit._tag === "Success");
  assert.equal(machine.read("/proc/sys/fs/inotify/max_user_instances"), "1024\n");
  assert.match(machine.read(SYSCTL_FILE) ?? "", /\nfs\.inotify\.max_user_instances = 1024\n$/u);
});
