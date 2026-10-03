// @ts-nocheck
/**
 * Shell commands embedded in code sent to a REPL tool such as bayma `exec`.
 *
 * Claude's Bash tool is disabled, so shell work arrives as source code: Bun
 * shell templates (`$\`...\``), `Bun.spawn`/`spawnSync`/`execSync` style calls,
 * or a bare command line typed into a shell-language session. Restart
 * provenance and the database guardrail match command strings, so they are
 * applied to each command this recovers plus the raw code itself. This is
 * best effort: a command assembled at runtime is not visible here.
 */
const BUN_SHELL_TEMPLATE = /\$`((?:\\[\s\S]|\$\{[^}]*\}|[^`\\])*)`/g;
const SPAWN_ARRAY = /\b(?:spawn|spawnSync|execFile|execFileSync)\s*\(\s*\[([^\]]*)\]/g;
const EXEC_STRING = /\b(?:exec|execSync|spawn|spawnSync)\s*\(\s*(["'`])((?:\\[\s\S]|(?!\1)[^\\])*)\1/g;
const STRING_LITERAL = /(["'`])((?:\\[\s\S]|(?!\1)[^\\])*)\1/g;

function stripInterpolations(text) {
  return text.replace(/\$\{[^}]*\}/g, "X").trim();
}

export function extractShellCommands(code) {
  const source = typeof code === "string" ? code : "";
  if (!source.trim()) {
    return [];
  }
  const commands = new Set([source.trim()]);
  for (const match of source.matchAll(BUN_SHELL_TEMPLATE)) {
    commands.add(stripInterpolations(match[1]));
  }
  for (const match of source.matchAll(SPAWN_ARRAY)) {
    const parts = [...match[1].matchAll(STRING_LITERAL)].map((part) => part[2]);
    if (parts.length > 0) {
      commands.add(parts.join(" "));
    }
  }
  for (const match of source.matchAll(EXEC_STRING)) {
    commands.add(stripInterpolations(match[2]));
  }
  for (const line of source.split("\n")) {
    if (line.trim()) {
      commands.add(line.trim());
    }
  }
  return [...commands].filter(Boolean);
}
