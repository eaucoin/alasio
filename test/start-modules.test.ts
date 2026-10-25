/**
 * What alasio loads as it starts: src/main.ts's imports, and all they import, each module as a
 * load hook sees it, in a process of its own.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

const ROOT = fileURLToPath(new URL("..", import.meta.url));

/** The URL of each module src/main.ts's imports load, but main's own run, which they are imported for. */
function loadedAtStart(): string[] {
  const specifiers = readFileSync(`${ROOT}/src/main.ts`, "utf8").split("\n").flatMap((line) => {
    const from = /^import (?!type )[^"]* from "([^"]+)";$/u.exec(line)?.[1];
    if (from === undefined) return [];
    // Relative to src/, where main.ts is; the script below is run from the repository's root.
    return [from.startsWith("../") ? `./${from.slice(3)}` : from.startsWith("./") ? `./src/${from.slice(2)}` : from];
  });
  const script = [
    'import { registerHooks } from "node:module";',
    "const loaded = [];",
    "registerHooks({ load(url, context, next) { loaded.push(url); return next(url, context); } });",
    `for (const specifier of ${JSON.stringify(specifiers)}) await import(specifier);`,
    "console.log(JSON.stringify(loaded));",
  ].join("\n");
  const said = execFileSync(process.execPath, ["--input-type=module", "-e", script], { cwd: ROOT, encoding: "utf8" });
  return JSON.parse(said.trim().split("\n").at(-1) ?? "[]") as string[];
}

test("alasio's start loads no Redis client, as nothing of alasio's uses one: @effect/platform-node is imported by its modules, not its index, which exports its Redis layer", () => {
  const loaded = loadedAtStart();
  // What it loads at all, so that the test fails were the import of main's modules to load nothing.
  assert.ok(loaded.some((url) => url.endsWith("/src/alasio.ts")), `alasio's start loaded ${loaded.length} modules, none of src/alasio.ts`);
  assert.deepEqual(loaded.filter((url) => /\/node_modules\/(?:@redis\/|redis\/)/u.test(url)).slice(0, 5), []);
});
