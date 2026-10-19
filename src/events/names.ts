/**
 * What an environment's events (./events.ts) are known by: their topic in the log, and the
 * group its alasio reads it in. Free of anything but itself, so the command line, which makes
 * the topic and its principal's access, takes it without the events.
 */

/** The events topic of the environment `environment`: main's, or its branch's. */
export const eventsTopic = (environment: string): string => `events.${environment}`;

/** The consumer group the environment `environment`'s alasio reads its events topic in, apart from its Telegram updates' group, so neither holds the other back. */
export const eventsGroup = (environment: string): string => `alasio-events.${environment}`;
