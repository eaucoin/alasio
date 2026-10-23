/**
 * Waiting on what a test observes, instead of sleeping: `eventually` polls until what it
 * looks for is there, and fails with what it saw when it is not there in time.
 */
import { setTimeout as delay } from "node:timers/promises";

/**
 * How long a test waits on what it observes by default: an outer bound for what never comes,
 * which its failure names, not a threshold. What a machine under the full suite's load does
 * may take many times what it takes alone, as a child alasio's first request of a pod's
 * app-server does; a wait ends as soon as what it waits on is there, so a long bound costs
 * nothing but the failure's own time.
 */
export const WAIT_MS = 30_000;

export interface EventuallyOptions {
  readonly timeoutMs?: number;
  /** What the failure says was seen instead. */
  readonly seen?: () => unknown;
}

/** What `probe` returns, or resolves to, once that is something other than undefined, within `timeoutMs`. */
export async function eventually<T>(
  what: string,
  probe: () => T | undefined | Promise<T | undefined>,
  { timeoutMs = WAIT_MS, seen }: EventuallyOptions = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const found = await probe();
    if (found !== undefined) return found;
    if (Date.now() >= deadline) {
      throw new Error(`timed out after ${timeoutMs} ms waiting for ${what}${seen ? `; saw ${JSON.stringify(seen(), null, 2)}` : ""}`);
    }
    await delay(5);
  }
}
