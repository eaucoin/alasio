// @ts-nocheck
const SHELL_WRAPPER_PATTERN = /^(?:\/usr\/bin\/env\s+)?(?:\/bin\/|\/usr\/bin\/)?(?:bash|sh)\s+-lc\s+(?:(['"])([\s\S]*)\1|(\S+))$/i;

export function unwrapShellCommand(command) {
  const trimmed = command.trim();
  const match = SHELL_WRAPPER_PATTERN.exec(trimmed);
  const innerCommand = match?.[2] ?? match?.[3];
  return innerCommand?.trim() || trimmed;
}

export function unwrapShellCommandOnce(command) {
  const trimmed = command.trim();
  const match = SHELL_WRAPPER_PATTERN.exec(trimmed);
  const innerCommand = match?.[2] ?? match?.[3];
  return innerCommand?.trim() ?? null;
}

export function tokenizeShellCommand(command) {
  const tokens = [];
  let current = "";
  let quote = null;
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

function commandBasename(token) {
  const parts = token.split("/");
  return (parts[parts.length - 1] || token).trim();
}

function isEnvAssignmentToken(token) {
  return /^[A-Za-z_][A-Za-z0-9_]*=.*/.test(token);
}

export function resolveCommandTokens(tokens) {
  let index = 0;
  while (index < tokens.length && isEnvAssignmentToken(tokens[index])) {
    index += 1;
  }
  if (index < tokens.length && commandBasename(tokens[index]).toLowerCase() === "env") {
    index += 1;
    while (index < tokens.length && tokens[index].startsWith("-")) {
      if (tokens[index] === "--") {
        index += 1;
        break;
      }
      index += 1;
    }
    while (index < tokens.length && isEnvAssignmentToken(tokens[index])) {
      index += 1;
    }
  }
  if (index >= tokens.length) {
    return { exe: "", args: [] };
  }
  return {
    exe: commandBasename(tokens[index]).toLowerCase(),
    args: tokens.slice(index + 1),
  };
}
