/**
 * Harness identifiers shared by persistence, operator controls, and runtime selection.
 *
 * A harness is the agent runtime alasio drives for a Telegram conversation. Each
 * conversation has at most one active harness at a time; the other harness keeps
 * its own parked session pointer so switching never mounts a foreign session.
 *
 * A conversation starts with no harness mounted. Until the operator chooses one
 * through /service, alasio only ever answers with the service picker. The
 * `null` harness is therefore a real state, not a fallback, and callers must
 * not coerce it to a default.
 */
import { createHmac } from "node:crypto";

export const CODEX_HARNESS = "codex";
export const CLAUDE_HARNESS = "claude";
export const HARNESS_NAMES = Object.freeze([CODEX_HARNESS, CLAUDE_HARNESS] as const);

/** The port alasio relays its Codex login to its harness pools' pods on (../codex/login-relay.ts). */
export const CODEX_LOGIN_PORT = 4320;

/** The variable the relay of the Codex login is given the key it checks the turns' tokens by in, in hexadecimal (poolLoginKey). */
export const CODEX_LOGIN_KEY_VARIABLE = "ALASIO_CODEX_LOGIN_KEY";

/**
 * The key alasio's pool login tokens, which the relay of the Codex login checks
 * (../codex/login-relay.ts), are signed with: its environment's own key of the gateway's caller
 * key's, made apart, so a token of one is never the other, and the relay, holding it alone, signs
 * none of the environment's sessions' tokens.
 */
export const poolLoginKey = (environmentKey: Buffer): Buffer => createHmac("sha256", environmentKey).update("codex-login").digest();

/** The name of a harness alasio can drive. */
export type HarnessName = (typeof HARNESS_NAMES)[number];

const DISPLAY_NAMES: Readonly<Record<HarnessName, string>> = Object.freeze({
  [CODEX_HARNESS]: "Codex",
  [CLAUDE_HARNESS]: "Claude",
});

export function isHarnessName(candidate: unknown): candidate is HarnessName {
  const names: readonly string[] = HARNESS_NAMES;
  return typeof candidate === "string" && names.includes(candidate);
}

export function normalizeHarnessName(candidate: unknown, fallback?: HarnessName): HarnessName;
export function normalizeHarnessName(candidate: unknown, fallback: HarnessName | null): HarnessName | null;
export function normalizeHarnessName(candidate: unknown, fallback: HarnessName | null = CODEX_HARNESS): HarnessName | null {
  if (typeof candidate !== "string") {
    return fallback;
  }
  const lowered = candidate.trim().toLowerCase();
  if (lowered === "claude" || lowered === "claude-code" || lowered === "claude_code" || lowered === "claudecode" || lowered === "claude code") {
    return CLAUDE_HARNESS;
  }
  if (lowered === "codex") {
    return CODEX_HARNESS;
  }
  return fallback;
}

export function harnessDisplayName(harness: HarnessName | null): string {
  return (harness && DISPLAY_NAMES[harness]) ?? DISPLAY_NAMES[CODEX_HARNESS];
}

/**
 * Harness pre-mounted on newly created conversations.
 *
 * Neutral by default: returns null unless ALASIO_DEFAULT_HARNESS opts into a
 * harness, in which case new conversations skip the picker.
 */
export function getDefaultHarness(env: NodeJS.ProcessEnv = process.env): HarnessName | null {
  return normalizeHarnessName(env["ALASIO_DEFAULT_HARNESS"], null);
}
