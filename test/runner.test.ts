import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { promisify } from "node:util";

// The test runner as package.json's scripts invoke it, as CI runs them: a suite that cannot be
// built fails the run, as Node's runner alone does not (../tooling/suite-failures.ts).

const root = join(import.meta.dirname, "..");
const scripts = (JSON.parse(readFileSync(join(root, "package.json"), "utf8")) as { readonly scripts: Readonly<Record<string, string>> }).scripts;

/** The script `name` run on `file` in place of the files it names: how it exited, and what it said. */
async function ran(name: string, file: string): Promise<{ readonly code: number; readonly said: string }> {
  const script = scripts[name] ?? "";
  const words = script.split(" ").filter((word) => !word.endsWith(".ts") || word.includes("tooling/"));
  assert.equal(words[0], "node", script);
  try {
    // As a run of its own, not one within this test's, which the runner says by its environment.
    const { NODE_TEST_CONTEXT: _within, ...env } = process.env;
    const { stdout, stderr } = await promisify(execFile)(process.execPath, [...words.slice(1), file], { cwd: root, env });
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
