/**
 * Waiting on what a test observes, instead of sleeping: `eventually` polls until what it
 * looks for is there, and fails with what it saw when it is not there in time.
 */
import { setTimeout as delay } from "node:timers/promises";

export interface EventuallyOptions {
  readonly timeoutMs?: number;
  /** What the failure says was seen instead. */
  readonly seen?: () => unknown;
}

/** What `probe` returns once it returns something other than undefined, within `timeoutMs`. */
export async function eventually<T>(what: string, probe: () => T | undefined, { timeoutMs = 5_000, seen }: EventuallyOptions = {}): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const found = probe();
    if (found !== undefined) return found;
    if (Date.now() >= deadline) {
      throw new Error(`timed out after ${timeoutMs} ms waiting for ${what}${seen ? `; saw ${JSON.stringify(seen(), null, 2)}` : ""}`);
    }
    await delay(5);
  }
}
