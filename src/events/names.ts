/**
 * What an environment's events (./events.ts) are known by: their topic in the log. Free of
 * anything but itself, so the command line, which makes the topic, takes it without the events.
 */

/** The events topic of the environment `environment`: main's, or its branch's. */
export const eventsTopic = (environment: string): string => `events.${environment}`;
