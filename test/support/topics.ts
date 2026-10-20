/**
 * A topic of the log held in memory, as alasio's tests stand one in for its groups' members
 * (src/log/topic.ts): its records in the order written, one group reading them, told of each
 * committed, a member that joins again reading on from the last told.
 */
import { setTimeout as sleep } from "node:timers/promises";

import { Effect } from "effect";

import type { TopicFeed, TopicRecord } from "../../src/log/topic.ts";

/** A topic held in memory, and its one group. */
export interface MemoryTopic {
  /** Writes `value` after what was written before. */
  readonly write: (value: string) => void;
  /** Joins the group: its records from the last told. */
  readonly join: Effect.Effect<TopicFeed>;
  /** How many of its records the group has been told of. */
  readonly committed: () => number;
  /** Every record written, in order. */
  readonly written: () => readonly string[];
}

/** The topic `name`, holding `values` to begin with. */
export function memoryTopic(name: string, values: readonly string[] = []): MemoryTopic {
  const records = [...values];
  let committed = 0;
  return {
    write: (value) => void records.push(value),
    join: Effect.sync(() => {
      let next = committed;
      let closed = false;
      return {
        async next(): Promise<TopicRecord> {
          for (;;) {
            if (closed) throw new Error("the member was closed");
            if (next < records.length) {
              const at = next++;
              return { value: Buffer.from(records[at] ?? ""), position: `${name}/0/${at}`, commit: async () => void (committed = Math.max(committed, at + 1)) };
            }
            await sleep(20);
          }
        },
        async close() {
          closed = true;
        },
      };
    }),
    committed: () => committed,
    written: () => [...records],
  };
}
