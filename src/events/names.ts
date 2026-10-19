/**
 * What an environment's events (./events.ts) are known by: their topic in the log, the group
 * its alasio reads it in, and the variables that say where a program tells of them. Free of
 * anything but itself, so the command line, which makes the topic and its principal's access,
 * takes it without the events.
 */

/** What every environment's events topic is named with first. */
export const EVENTS_TOPIC_PREFIX = "events.";

/** The events topic of the environment `environment`: main's, or its branch's. */
export const eventsTopic = (environment: string): string => `${EVENTS_TOPIC_PREFIX}${environment}`;

/** The consumer group the environment `environment`'s alasio reads its events topic in, apart from its Telegram updates' group, so neither holds the other back. */
export const eventsGroup = (environment: string): string => `alasio-events.${environment}`;

/** How long an environment's events topic keeps what it is told: a week, past any wait of its alasio's to read it. */
export const EVENTS_RETENTION_MS = 7 * 86_400_000;

/**
 * How long an alasio keeps the record of each event it handled (../persistence/store.ts
 * withEventHandled): a day past what its topic keeps, as the log deletes a segment, closed at
 * the hour, only once all of it is older than the topic's retention. A group the log forgot
 * the offsets of, idle as long, reads its topic again from where it reads one told of none (a
 * branch's from its first record kept, ./consume.ts), and finds each event it handled still
 * recorded, so handles none again.
 */
export const EVENTS_HANDLED_KEPT_MS = EVENTS_RETENTION_MS + 86_400_000;

/** The variables of where a program tells of events (./events.ts Events.fromEnvironment): the log's brokers, its principal's name, and the file of its password. */
export const EVENTS_BROKERS_VARIABLE = "EVENTS_BROKERS";
export const EVENTS_USERNAME_VARIABLE = "EVENTS_USERNAME";
export const EVENTS_PASSWORD_FILE_VARIABLE = "EVENTS_PASSWORD_FILE";
