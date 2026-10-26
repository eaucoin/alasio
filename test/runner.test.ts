import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { promisify } from "node:util";

// The test runner as package.json's scripts invoke it, as CI runs them: a suite that cannot be
// built fails the run, as Node's runner alone does not (../tooling/suite-failures.ts); and a
// test or hook that never ends fails by its name, within the script's bound of each, the run
// exiting though what it waited on holds the process open, never left to hang until CI's job
// limit cancels it, saying nothing.

const root = join(import.meta.dirname, "..");
const scripts = (JSON.parse(readFileSync(join(root, "package.json"), "utf8")) as { readonly scripts: Readonly<Record<string, string>> }).scripts;

/** The script `name` run on `file` in place of the files it names, `flags` after its own: how it exited, and what it said. */
async function ran(name: string, file: string, flags: readonly string[] = []): Promise<{ readonly code: number; readonly said: string }> {
  const script = scripts[name] ?? "";
  const words = script.split(" ").filter((word) => !word.endsWith(".ts") || word.includes("tooling/"));
  assert.equal(words[0], "node", script);
  try {
    // As a run of its own, not one within this test's, which the runner says by its environment.
    const { NODE_TEST_CONTEXT: _within, ...env } = process.env;
    const { stdout, stderr } = await promisify(execFile)(process.execPath, [...words.slice(1), ...flags, file], { cwd: root, env });
    return { code: 0, said: `${stdout}${stderr}` };
  } catch (error) {
    const { code, stdout, stderr } = error as { code: number; stdout: string; stderr: string };
    return { code, said: `${stdout}${stderr}` };
  }
}

for (const name of ["test", "test:e2e", "test:gateway-memory"]) {
  test(`${name}, given a file whose suite throws as it is built, or whose suite's body rejects, fails, saying which; given one whose suites build, passes`, async () => {
    for (const fixture of ["throws", "rejects"]) {
      const { code, said } = await ran(name, `test/fixtures/suites/${fixture}.fixture.ts`);
      assert.notEqual(code, 0, said);
      assert.match(said, /the suite "a suite (that cannot be built|whose body rejects)" of [^\n]+ failed, which fails the run: (asked before there was anything to ask|rejected as it was built)/u);
    }
    const { code, said } = await ran(name, "test/fixtures/suites/builds.fixture.ts");
    assert.equal(code, 0, said);
  });
}

/** What the script `name` bounds each test and hook by, in milliseconds, its --test-timeout: none where it sets none. */
const boundOf = (name: string): number | null => Number(/(?:^| )--test-timeout=(\d+)(?: |$)/u.exec(scripts[name] ?? "")?.[1] ?? Number.NaN) || null;

for (const [name, bound] of [["test", 300_000], ["test:gateway-memory", 300_000], ["test:e2e", 1_800_000]] as const) {
  test(`${name} bounds each test and hook by ${bound / 60_000} minutes, so one that never ends fails by its name, and the run exits though what it waited on holds the process open`, { timeout: 60_000 }, async () => {
    // Above the slowest test of its run, which sets a bound of its own where it needs longer: of the unit's, a minute and a
    // quarter on a runner; of the end-to-end's, the gate's two hours, its own.
    assert.equal(boundOf(name), bound);
    assert.match(scripts[name] ?? "", / --test-force-exit /u);
    // Its bound made two seconds, the last of its flags of it the runner takes.
    const { code, said } = await ran(name, "test/fixtures/suites/hangs.fixture.ts", ["--test-timeout=2000"]);
    assert.notEqual(code, 0, said);
    assert.match(said, /✖ a test that never ends \([\d.]+ms\)\n\s*'test timed out after 2000ms'/u);
    assert.match(said, /✖ a suite whose hook never ends \([\d.]+ms\)\n\s*'test timed out after 2000ms'/u);
  });
}
