/**
 * alasio's npm package, `alasio`, as npm packs it from cli/ once it is built: checked to
 * hold the compiled command line and nothing else, installed into a prefix of its own,
 * and run from there, as `npx alasio` runs it. CI's package job runs it whole:
 *
 *   node tooling/cli-package.ts
 *
 * The end-to-end run (test/e2e) installs the package as this does, and runs alasio from it.
 */
import { execFile } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { VERSION } from "../cli/src/release.ts";

const run = promisify(execFile);
const root = fileURLToPath(new URL("..", import.meta.url));
/** npm's answers can be long: a pack's lists every file. */
const MAX_BUFFER = 16 * 1024 * 1024;

/** What the package must hold: its command, and the CRD it applies, which is data, not code. */
export const REQUIRED = ["dist/cli/src/main.js", "dist/cli/src/manifests/sandboxes.agents.x-k8s.io.json"] as const;

/** A packed package: its tarball, and the paths it holds, relative to the package. */
export interface PackedCli {
  readonly tarball: string;
  readonly files: readonly string[];
}

/** What `npm pack --json` says of each package it packed, as far as this reads it. */
interface PackReport {
  readonly filename: string;
  readonly files: readonly { readonly path: string }[];
}

/** What is wrong with a package of `files`: a file it must not hold (sources, tests, anything not built), or one it lacks. */
export function packageProblems(files: readonly string[]): string[] {
  const stray = files.filter((file) => file !== "package.json" && !(file.startsWith("dist/") && /\.(js|js\.map|json)$/u.test(file)));
  const tests = files.filter((file) => /(^|\/)test\//u.test(file));
  const missing = REQUIRED.filter((file) => !files.includes(file));
  return [
    ...stray.map((file) => `${file} is not of the built command line`),
    ...tests.map((file) => `${file} is a test's`),
    ...missing.map((file) => `${file} is missing`),
  ];
}

/** Builds the command line and packs its package into `destination`. */
export async function packCli(destination: string): Promise<PackedCli> {
  await run("npm", ["run", "build", "--workspace", "cli"], { cwd: root, maxBuffer: MAX_BUFFER });
  const { stdout } = await run("npm", ["pack", "--workspace", "cli", "--pack-destination", destination, "--json"], { cwd: root, maxBuffer: MAX_BUFFER });
  // npm answers a pack with a report of each package it packed, here the one.
  const [report] = JSON.parse(stdout) as readonly PackReport[];
  if (!report) throw new Error("npm packed nothing");
  return { tarball: join(destination, report.filename), files: report.files.map(({ path }) => path) };
}

/** Installs the package of `tarball` into `prefix`, as npm installs a command: the path of its `alasio`. */
export async function installCli(tarball: string, prefix: string): Promise<string> {
  await run("npm", ["install", "--global", "--prefix", prefix, "--no-audit", "--no-fund", tarball], { maxBuffer: MAX_BUFFER });
  return join(prefix, "bin", "alasio");
}

if (import.meta.main) {
  const work = mkdtempSync(join(tmpdir(), "alasio-package-"));
  try {
    const { tarball, files } = await packCli(work);
    const problems = packageProblems(files);
    if (problems.length > 0) throw new Error(`the package is not as it should be:\n${problems.map((problem) => `  ${problem}`).join("\n")}`);
    console.log(`${tarball}: ${files.length} files, the built command line alone`);
    const alasio = await installCli(tarball, join(work, "prefix"));
    const { stdout: version } = await run(alasio, ["--version"]);
    if (!version.includes(VERSION)) throw new Error(`alasio --version answered ${version.trim()}, not ${VERSION}`);
    const { stdout: help } = await run(alasio, ["--help"]);
    const commands = ["init", "up", "status", "logs", "restart", "upgrade", "login", "lake", "down", "uninstall"];
    const unlisted = commands.filter((command) => !new RegExp(`^\\s+${command}\\b`, "mu").test(help));
    if (unlisted.length > 0) throw new Error(`alasio --help lists no ${unlisted.join(", ")}:\n${help}`);
    console.log(`installed, alasio --version answers ${version.trim()}, and alasio --help lists its ${commands.length} commands`);
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
}
