const SHELL_WRAPPER_PATTERN = /^(?:\/usr\/bin\/env\s+)?(?:\/bin\/|\/usr\/bin\/)?(?:bash|sh)\s+-lc\s+(?:(['"])([\s\S]*)\1|(\S+))$/i;

/** A command line's executable, by basename and lower-cased, and its arguments; `exe` is "" when there is none. */
export interface ResolvedCommand {
  readonly exe: string;
  readonly args: readonly string[];
}

export function unwrapShellCommand(command: string): string {
  const trimmed = command.trim();
  const match = SHELL_WRAPPER_PATTERN.exec(trimmed);
  const innerCommand = match?.[2] ?? match?.[3];
  return innerCommand?.trim() || trimmed;
}

export function unwrapShellCommandOnce(command: string): string | null {
  const trimmed = command.trim();
  const match = SHELL_WRAPPER_PATTERN.exec(trimmed);
  const innerCommand = match?.[2] ?? match?.[3];
  return innerCommand?.trim() ?? null;
}

export function tokenizeShellCommand(command: string): string[] {
  const tokens: string[] = [];
  let current = "";
  let quote: string | null = null;
  let escaped = false;
  for (const ch of command) {
    if (escaped) {
      current += ch;
      escaped = false;
      continue;
    }
    if (ch === "\\") {
      escaped = true;
      continue;
    }
    if (quote) {
      if (ch === quote) {
        quote = null;
      } else {
        current += ch;
      }
      continue;
    }
    if (ch === "'" || ch === '"') {
      quote = ch;
      continue;
    }
    if (/\s/.test(ch)) {
      if (current) {
        tokens.push(current);
        current = "";
      }
      continue;
    }
    current += ch;
  }
  if (current) {
    tokens.push(current);
  }
  return tokens;
}

function commandBasename(token: string): string {
  const parts = token.split("/");
  return (parts[parts.length - 1] || token).trim();
}

function isEnvAssignmentToken(token: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_]*=.*/.test(token);
}

export function resolveCommandTokens(tokens: readonly string[]): ResolvedCommand {
  // Every read through `token` below is guarded by `index < tokens.length`.
  const token = (at: number): string => tokens[at] ?? "";
  let index = 0;
  while (index < tokens.length && isEnvAssignmentToken(token(index))) {
    index += 1;
  }
  if (index < tokens.length && commandBasename(token(index)).toLowerCase() === "env") {
    index += 1;
    while (index < tokens.length && token(index).startsWith("-")) {
      if (token(index) === "--") {
        index += 1;
        break;
      }
      index += 1;
    }
    while (index < tokens.length && isEnvAssignmentToken(token(index))) {
      index += 1;
    }
  }
  if (index >= tokens.length) {
    return { exe: "", args: [] };
  }
  return {
    exe: commandBasename(token(index)).toLowerCase(),
    args: tokens.slice(index + 1),
  };
}
