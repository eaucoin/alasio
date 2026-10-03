// @ts-nocheck
/**
 * Reads single string settings from the operator's `$CODEX_HOME/config.toml`, where alasio
 * must combine rather than override them (developer instructions). Only root-table string
 * keys are read, in each of TOML's four string forms; anything else reads as unset.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";

const BASIC_ESCAPES = { b: "\b", t: "\t", n: "\n", f: "\f", r: "\r", '"': '"', "\\": "\\", e: "\x1b" };

function unescapeBasic(value, multiline) {
  let out = value;
  // In a multi-line basic string, a backslash ending a line trims the break and leading space.
  if (multiline) out = out.replace(/\\[ \t]*\r?\n\s*/g, "");
  return out.replace(/\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|.)/g, (whole, esc) => {
    if (esc[0] === "u" || esc[0] === "U") return String.fromCodePoint(Number.parseInt(esc.slice(1), 16));
    return BASIC_ESCAPES[esc] ?? whole;
  });
}

/** The root-table string value of `key` in TOML `text`, or null. */
export function tomlRootString(text, key) {
  const root = String(text).split(/^\s*\[/m)[0];
  const keyPattern = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const at = new RegExp(`^[ \\t]*${keyPattern}[ \\t]*=[ \\t]*`, "m").exec(root);
  if (!at) return null;
  const rest = root.slice(at.index + at[0].length);
  const forms = [
    { open: '"""', basic: true, multiline: true },
    { open: "'''", basic: false, multiline: true },
    { open: '"', basic: true, multiline: false },
    { open: "'", basic: false, multiline: false },
  ];
  for (const form of forms) {
    if (!rest.startsWith(form.open)) continue;
    let body = rest.slice(form.open.length);
    if (form.multiline) body = body.replace(/^\r?\n/, "");
    let end = -1;
    for (let i = 0; i < body.length; i += 1) {
      if (form.basic && body[i] === "\\") {
        i += 1;
        continue;
      }
      if (!form.multiline && body[i] === "\n") return null;
      if (body.startsWith(form.open, i)) {
        end = i;
        break;
      }
    }
    if (end < 0) return null;
    const raw = body.slice(0, end);
    return form.basic ? unescapeBasic(raw, form.multiline) : raw;
  }
  return null;
}

/** The operator's own `developer_instructions` in their Codex config, or null. */
export function operatorDeveloperInstructions(codexHomeDir) {
  try {
    return tomlRootString(readFileSync(join(codexHomeDir, "config.toml"), "utf8"), "developer_instructions");
  } catch {
    return null;
  }
}
