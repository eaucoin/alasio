/**
 * Telegram's deliveries of the bot's updates to the webhook intake, watched by alasio,
 * which holds the bot's token and runs throughout, where its command line, whose alasio
 * status says the same, runs only when asked: Telegram fails a delivery silently, saying
 * so in getWebhookInfo alone. Where they fail (./deliveries.ts), alasio logs why, with the
 * fix Telegram's error points to, and alasio.telegram.deliveries.failing says so, which an
 * alert rule pages on (neon/grafana/provisioning/alerting/rules.yaml).
 */
import type { ObservableResult } from "@opentelemetry/api";
import { Clock, Effect, Schedule, type Scope } from "effect";

import { meter } from "../telemetry/index.ts";
import { TelegramClient } from "./client.ts";
import { failingDeliveries } from "./deliveries.ts";

/**
 * How often Telegram is asked: a failure is seen within five minutes, and twice within the
 * ten its last error counts as one now, so a failure that lasts is never lost between two
 * asks, at a call Telegram's limits never notice. The first ask is as long after alasio
 * starts, as alasio up, which registers its bot, has just said how its deliveries fare.
 */
const CHECK_EVERY = "5 minutes";

const failing = meter.createObservableGauge("alasio.telegram.deliveries.failing", {
  description: "1 while Telegram's deliveries of the bot's updates to the webhook intake fail: updates wait, and its last delivery failed lately; 0 while they do not",
  unit: "1",
});

/** Asks Telegram how the bot's deliveries fare every CHECK_EVERY, for as long as the scope is open. */
export const watchDeliveries: Effect.Effect<void, never, TelegramClient | Scope.Scope> = Effect.gen(function*() {
  const client = yield* TelegramClient;
  // Whether the last ask found them failing, which the gauge says as metrics are exported; nothing before the first.
  let found: boolean | null = null;
  yield* Effect.acquireRelease(
    Effect.sync(() => {
      const observe = (result: ObservableResult) => {
        if (found !== null) result.observe(found ? 1 : 0);
      };
      failing.addCallback(observe);
      return observe;
    }),
    (observe) => Effect.sync(() => failing.removeCallback(observe)),
  );
  // An ask that fails says nothing of the deliveries, which the gauge keeps saying as the last ask found them.
  const check = Effect.gen(function*() {
    const deliveries = failingDeliveries(yield* client.call("getWebhookInfo"), yield* Clock.currentTimeMillis);
    if (deliveries) {
      yield* Effect.logWarning(`Telegram's deliveries to the webhook intake fail: ${deliveries.description}${deliveries.fix ? `; ${deliveries.fix}` : ""}`);
    } else if (found) {
      yield* Effect.logInfo("Telegram delivers to the webhook intake again");
    }
    found = deliveries !== null;
  }).pipe(
    Effect.catch((error) => Effect.logWarning(`Telegram could not be asked how its deliveries fare: ${error.message}`)),
    Effect.catchDefect((defect) => Effect.logWarning(`Telegram could not be asked how its deliveries fare: ${String(defect)}`)),
  );
  yield* check.pipe(Effect.schedule(Schedule.spaced(CHECK_EVERY)), Effect.forkScoped);
});
