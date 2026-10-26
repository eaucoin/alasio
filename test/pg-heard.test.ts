/**
 * That every pg client and pool alasio's code makes hears its connection's loss: each is made by
 * heardClient or heardPool (src/persistence/sql.ts; the lake's package, built apart, by its own
 * heardClient, neon/lake/src/lake.ts), as pg tells a connection's 'error' listeners of it as it
 * is lost, its backend terminated as by a compute's restart, and with none listening Node takes
 * it as the process's to die of. A pg Client, Pool or GrantedPool made anywhere else in src/,
 * neon/ or webhook/ is refused, but as the pool heardPool is given to make.
 */
import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

const ROOT = fileURLToPath(new URL("..", import.meta.url));

/** Where the helpers are, each by its file, and the functions there that may make what they hear. */
const HELPERS: Readonly<Record<string, readonly string[]>> = {
  "src/persistence/sql.ts": ["heardClient", "heardPool"],
  "neon/lake/src/lake.ts": ["heardClient"],
};

/** A construction of a pg client or pool, of pg's own or alasio's GrantedPool. */
const CONSTRUCTION = /\bnew\s+(?:pg\.)?(?:Client|Pool|GrantedPool)\s*\(/gu;

/** The TypeScript files of `directory`, its tests' and its packages' dependencies left out. */
function sources(directory: string): string[] {
  return readdirSync(directory).flatMap((name) => {
    const path = join(directory, name);
    if (name === "node_modules" || name === "test" || name.startsWith(".")) return [];
    if (statSync(path).isDirectory()) return sources(path);
    return /\.(?:m|c)?ts$/u.test(name) && !/\.test\.ts$/u.test(name) ? [path] : [];
  });
}

/**
 * The constructions of `text`, the file `file`, that hear nothing: each not within the body of
 * one of the helpers its file defines, nor on a line that gives it to heardPool or heardClient.
 * Pure, for its own test.
 */
function unheard(file: string, text: string): string[] {
  const lines = text.split("\n");
  const helpers = HELPERS[file] ?? [];
  return lines.flatMap((line, n) => {
    const found = [...line.matchAll(CONSTRUCTION)];
    return found.flatMap((match) => {
      if (/\bheard(?:Pool|Client)\s*\(/u.test(line.slice(0, match.index))) return [];
      // Within a helper's body: its declaration above, and no top-level closing brace between.
      for (let above = n; above >= 0; above -= 1) {
        const declared = /^export function (\w+)\b/u.exec(lines[above] ?? "");
        if (declared) {
          if (helpers.includes(declared[1] ?? "")) return [];
          break;
        }
        if (above < n && /^\}/u.test(lines[above] ?? "")) break;
      }
      return [`${file}:${n + 1}: ${line.trim()}`];
    });
  });
}

test("every pg client and pool of src/, neon/ and webhook/ is made by heardClient or heardPool, and the helpers are where the rule says", () => {
  const files = ["src", "neon", "webhook"].flatMap((directory) => sources(join(ROOT, directory)));
  assert.ok(files.length > 100, `only ${files.length} files were read`);
  const refused = files.flatMap((path) => unheard(relative(ROOT, path), readFileSync(path, "utf8")));
  assert.deepEqual(refused, [], "a pg client or pool made but by heardClient or heardPool, which hears no loss of its connection");
  for (const [file, helpers] of Object.entries(HELPERS)) {
    const text = readFileSync(join(ROOT, file), "utf8");
    for (const helper of helpers) assert.match(text, new RegExp(`^export function ${helper}\\b`, "mu"), `${file} defines no ${helper}`);
  }
});

test("the rule refuses a client or pool made bare, by pg's names or alasio's, anywhere but in a helper's body or given to heardPool", () => {
  const bare = [
    "const client = new pg.Client({ connectionString });",
    "const pool = new Pool({ max: 1 });",
    "  const granted = new GrantedPool(config);",
  ];
  for (const line of bare) assert.equal(unheard("src/x.ts", `import pg from "pg";\n${line}\n`).length, 1, line);
  assert.deepEqual(unheard("src/x.ts", "const pool = heardPool(config, { make: (config) => new GrantedPool(config) });\n"), []);
  // A helper's own, in its file alone; and not past its body.
  const helper = "export function heardClient(config) {\n  const client = new pg.Client(config);\n  return client;\n}\nconst after = new pg.Client({});\n";
  assert.equal(unheard("src/persistence/sql.ts", helper).length, 1);
  assert.equal(unheard("src/other.ts", helper).length, 2);
});
