/**
 * alasio's watch of Telegram's deliveries to the webhook intake
 * (src/telegram/delivery-watch.ts), the bot's getWebhookInfo answered by a stand-in: asked
 * every five minutes, it logs deliveries that fail, with the fix Telegram's error points
 * to, and says in its gauge whether they do.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { Effect, Layer, Logger } from "effect";
import { TestClock } from "effect/testing";

// First: alasio's modules create their metrics as they load.
import { collected } from "./support/metrics.ts";
import { watchDeliveries } from "../src/telegram/delivery-watch.ts";
import { botApiError, botApiLayer } from "./support/bot-api.ts";
import { eventually } from "./support/wait.ts";

const GAUGE = "alasio.telegram.deliveries.failing";

test("deliveries that fail are logged as Telegram is asked every five minutes, with the fix its error points to, and the gauge says so until they deliver again", async () => {
  const logged: string[] = [];
  const delivering = { url: "https://203.0.113.7/telegram/main/route", has_custom_certificate: true, pending_update_count: 0 };
  let answer: () => object = () => delivering;
  let asked = 0;
  const telegram = botApiLayer(async (call) => {
    assert.equal(call.method, "getWebhookInfo");
    asked += 1;
    return answer();
  });
  /** Five minutes on, once Telegram has been asked again and what it said is logged, `lines` lines in all. */
  const fiveMinutesOn = (lines: number) =>
    Effect.gen(function*() {
      const before = asked;
      yield* TestClock.adjust("5 minutes");
      yield* Effect.promise(() => eventually("Telegram asked, and its answer logged", () => (asked > before && logged.length === lines ? true : undefined), { seen: () => logged }));
    });
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    yield* watchDeliveries;
    // Not asked as alasio starts, so the gauge says nothing yet.
    assert.equal(asked, 0);
    assert.deepEqual(yield* Effect.promise(() => collected(GAUGE)), []);

    // Updates wait, and Telegram could not connect a minute before it is asked.
    answer = () => ({ ...delivering, pending_update_count: 3, last_error_date: 240, last_error_message: "Connection timed out" });
    yield* fiveMinutesOn(1);
    assert.equal(
      logged[0],
      "Telegram's deliveries to the webhook intake fail: 3 updates waiting; Telegram's last delivery failed at 1970-01-01T00:04:00Z: Connection timed out; " +
        "Telegram cannot connect to it, as where its networks have changed from those install.webhook.sources lists: list Telegram's (core.telegram.org/bots/webhooks) there, and alasio up admits them",
    );
    assert.deepEqual(yield* Effect.promise(() => collected(GAUGE)), [{ attributes: "", value: 1 }]);

    // Every update delivered since, though Telegram still says its last error.
    answer = () => ({ ...delivering, last_error_date: 240, last_error_message: "Connection timed out" });
    yield* fiveMinutesOn(2);
    assert.equal(logged[1], "Telegram delivers to the webhook intake again");
    assert.deepEqual(yield* Effect.promise(() => collected(GAUGE)), [{ attributes: "", value: 0 }]);

    // An ask Telegram refuses says nothing of the deliveries: the gauge says what the last one found.
    answer = () => {
      throw botApiError("Internal Server Error", 500);
    };
    yield* fiveMinutesOn(3);
    assert.match(logged[2] ?? "", /^Telegram could not be asked how its deliveries fare: Telegram getWebhookInfo failed: HTTP 500 /u);
    assert.deepEqual(yield* Effect.promise(() => collected(GAUGE)), [{ attributes: "", value: 0 }]);
  })).pipe(Effect.provide(Layer.mergeAll(telegram, TestClock.layer(), Logger.layer([Logger.make(({ message }) => void logged.push(String(message)))])))));
});
