/**
 * Generates the Codex app-server protocol's types from the Codex alasio pins
 * (node_modules/.bin/codex), so they always describe the app-server alasio runs:
 *
 *   node tooling/codex-protocol-types.ts
 *
 * Codex writes its protocol as TypeScript whose imports name no extension; they are
 * kept here, under .types/codex, as declarations that import each other by the names
 * Node's module resolution expects. npm run typecheck runs this first.
 */
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const target = join(root, ".types", "codex");

/** Every file under `directory`, as paths relative to it. */
function files(directory: string): string[] {
  return readdirSync(directory, { recursive: true, withFileTypes: true })
    .filter((entry) => entry.isFile())
    .map((entry) => relative(directory, join(entry.parentPath, entry.name)));
}

const generated = mkdtempSync(join(tmpdir(), "codex-protocol-"));
try {
  execFileSync(join(root, "node_modules", ".bin", "codex"), ["app-server", "generate-ts", "--out", generated], { stdio: "inherit" });
  rmSync(target, { recursive: true, force: true });
  for (const file of files(generated).filter((name) => name.endsWith(".ts"))) {
    // A specifier names a module or a directory of them, whose index.ts it means. One
    // naming neither would leave its types silently unknown, so it stops the run.
    const resolve = (specifier: string): string => {
      const named = join(generated, dirname(file), specifier);
      if (existsSync(join(named, "index.ts"))) return `${specifier}/index.js`;
      if (existsSync(`${named}.ts`)) return `${specifier}.js`;
      throw new Error(`${file} imports ${specifier}, which Codex did not generate`);
    };
    const source = readFileSync(join(generated, file), "utf8")
      .replace(/(from\s+")(\.{1,2}\/[^"]+)(")/gu, (_, start: string, specifier: string, end: string) => `${start}${resolve(specifier)}${end}`);
    const declaration = join(target, file.replace(/\.ts$/u, ".d.ts"));
    mkdirSync(dirname(declaration), { recursive: true });
    writeFileSync(declaration, source);
  }
} finally {
  rmSync(generated, { recursive: true, force: true });
}
