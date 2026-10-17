/**
 * The outbox in tests: what is due delivered, however its passes share it out.
 */
import { Effect } from "effect";

import type { StoreError } from "../../src/persistence/sql.ts";
import type { Store } from "../../src/persistence/store.ts";
import type { Outbox } from "../../src/telegram/outbox.ts";

/**
 * Delivers what is due until nothing is: a pass passes over a chat another pass is
 * delivering, as the outbox's own runs beside the test's, so it is asked again until the
 * store has nothing due.
 */
export const deliverAll = (outbox: Outbox["Service"], store: Store["Service"]): Effect.Effect<void, StoreError> =>
  Effect.gen(function*() {
    for (;;) {
      yield* outbox.deliverDue;
      if ((yield* store.getDueOutbox(1)).length === 0) return;
      yield* Effect.sleep("10 millis");
    }
  });
