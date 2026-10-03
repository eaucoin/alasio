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
export const CODEX_HARNESS = "codex";
export const CLAUDE_HARNESS = "claude";
export const HARNESS_NAMES = Object.freeze([CODEX_HARNESS, CLAUDE_HARNESS] as const);

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
