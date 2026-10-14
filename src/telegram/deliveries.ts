/**
 * How Telegram's deliveries of a bot's updates to the webhook intake fare, as the bot's
 * getWebhookInfo says, which is where alone Telegram says a delivery failed: alike for
 * alasio, which watches its own bot's (./delivery-watch.ts), and for alasio status, which
 * says each environment's. Free of anything but the language's own, so the command line
 * takes it without the server.
 */

/** What a bot's getWebhookInfo says of its deliveries: how many updates wait, and Telegram's last failed delivery, when and why. */
export interface Deliveries {
  readonly pending_update_count: number;
  readonly last_error_date?: number | undefined;
  readonly last_error_message?: string | undefined;
}

/** Deliveries that fail: what getWebhookInfo says of them, and the fix Telegram's error points to, null where it points to none. */
export interface FailingDeliveries {
  readonly description: string;
  readonly fix: string | null;
}

/** How long a failed delivery is a failure now, as Telegram keeps its last error after it delivers again. */
const LATELY_SECONDS = 600;

/**
 * Telegram's errors that say it could not connect to the intake at all, as where the
 * networks it delivers from are not among install.webhook.sources, which the intake's
 * NetworkPolicy and a LoadBalancer admit alone: the connection is dropped, and times out,
 * or refused.
 */
const UNREACHABLE = /^(Connection timed out|Connection refused|No route to host|Network is unreachable)\b/u;

/** Telegram's error that says the name it is told to deliver to resolves to no address. */
const UNRESOLVED = /^Failed to resolve host\b/u;

/** The fix Telegram's error `error` points to, null where it points to none. */
function fixOf(error: string): string | null {
  if (UNREACHABLE.test(error)) {
    return "Telegram cannot connect to it, as where its networks have changed from those install.webhook.sources lists: list Telegram's (core.telegram.org/bots/webhooks) there, and alasio up admits them";
  }
  if (UNRESOLVED.test(error)) return "Telegram cannot resolve install.webhook.publicAddress: give the name an address, or set the address itself, and alasio up registers each bot with it";
  return null;
}

/**
 * Whether `deliveries` fail at `now` (milliseconds): updates wait, and Telegram's last
 * delivery failed lately; null where they do not, as where nothing waits, every update
 * delivered since.
 */
export function failingDeliveries(deliveries: Deliveries, now: number): FailingDeliveries | null {
  const { pending_update_count: pending, last_error_date: date, last_error_message: error = "no reason given" } = deliveries;
  if (pending === 0 || date === undefined || now / 1000 - date >= LATELY_SECONDS) return null;
  const at = new Date(date * 1000).toISOString().replace(/\.\d{3}Z$/u, "Z");
  return { description: `${pending} update${pending === 1 ? "" : "s"} waiting; Telegram's last delivery failed at ${at}: ${error}`, fix: fixOf(error) };
}
