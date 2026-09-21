import { resolveCommandTokens, tokenizeShellCommand, unwrapShellCommand } from "./shell-command.js";

const SUPABASE_QUERY_ENDPOINT_PATTERN = /(?:\/v1\/projects\/[^/]+\/database\/query(?:\b|[?&/])|api\.supabase\.com\/v1\/projects\/[^/]+\/database\/query(?:\b|[?&/]))/i;
const SUPABASE_TOKEN_FILE_PATTERN = /(?:^|\/)\.(?:supabase|config\/supabase)\/access-token$/i;

export const MAX_DB_GUARDRAIL_RECOVERY_ATTEMPTS = 1;

function isDirectSupabaseCredentialProbe(normalizedCommand, exe, tokens) {
  if (exe === "printenv" && tokens[1] === "SUPABASE_ACCESS_TOKEN") {
    return true;
  }
  if (exe === "env" && /SUPABASE_ACCESS_TOKEN/.test(normalizedCommand)) {
    return true;
  }
  if (exe === "echo" && tokens.some((token) => token === "$SUPABASE_ACCESS_TOKEN" || token === "${SUPABASE_ACCESS_TOKEN}")) {
    return true;
  }
  if (exe === "cat" && tokens.slice(1).some((token) => SUPABASE_TOKEN_FILE_PATTERN.test(token))) {
    return true;
  }
  if (["node", "bun", "python", "python3", "perl", "ruby"].includes(exe) &&
    (/SUPABASE_ACCESS_TOKEN/.test(normalizedCommand) || SUPABASE_TOKEN_FILE_PATTERN.test(normalizedCommand))) {
    return true;
  }
  return false;
}

export function isBlockedDbCommand(command) {
  const normalizedCommand = unwrapShellCommand(command);
  const tokens = tokenizeShellCommand(normalizedCommand);
  if (tokens.length === 0) {
    return false;
  }
  const { exe, args } = resolveCommandTokens(tokens);
  if (!exe) {
    return false;
  }
  if (isDirectSupabaseCredentialProbe(normalizedCommand, exe, tokens)) {
    return true;
  }
  if (exe === "psql") {
    return true;
  }
  if (exe === "supabase") {
    if (args[0] === "db") {
      return true;
    }
    if (args[0] === "migration" && args[1] === "repair") {
      return true;
    }
  }
  if (exe === "drizzle-kit" && args[0] === "pull") {
    return true;
  }
  if (exe === "bun") {
    const runIndex = args.indexOf("run");
    const scriptName = runIndex >= 0 ? args[runIndex + 1] : "";
    if (scriptName === "schema:pull" || scriptName === "db:pull") {
      return true;
    }
  }
  if ((exe === "curl" || exe === "wget") && args.some((token) => SUPABASE_QUERY_ENDPOINT_PATTERN.test(token))) {
    return true;
  }
  if ((exe === "bunx" || exe === "npx") && args[0] === "drizzle-kit" && args[1] === "pull") {
    return true;
  }
  if (exe === "npm" && args[0] === "exec" && args[1] === "drizzle-kit" && args[2] === "pull") {
    return true;
  }
  if (exe === "pnpm" && (args[0] === "dlx" || args[0] === "exec") && args[1] === "drizzle-kit" && args[2] === "pull") {
    return true;
  }
  return false;
}

export function buildDbGuardrailSyntheticText(command) {
  return "[SYSTEM GUARDRAIL EVENT]\n\n" +
    "Your attempted tool command was blocked by the monorepo DB guardrail.\n\n" +
    `Blocked command: \`${command}\`\n\n` +
    "Policy:\n" +
    "- Local DB access is forbidden in monorepo.\n" +
    "- Local schema pull and local schema rebaseline are forbidden in monorepo.\n" +
    "- Use the readonly query CLI for inspection.\n" +
    "- Use CI workflows for schema reconciliation.\n\n" +
    "Instruction:\n" +
    "- Do not retry the blocked command.\n" +
    "- Continue the task using compliant paths only.\n" +
    "- If the blocked command was central to the plan, briefly explain the constraint to the user and propose the compliant alternative.";
}

export function buildDbGuardrailFallbackText(command) {
  return "I hit the monorepo DB guardrail while trying to continue. " +
    `The blocked command was \`${command}\`. ` +
    "Local DB access and local schema pull are forbidden here, so I need to continue with the readonly query CLI for inspection and CI workflows for schema reconciliation.";
}
