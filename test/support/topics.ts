/**
 * A topic of the log held in memory, as alasio's tests stand one in for its groups' members
 * (src/log/topic.ts): its records in the order written, one group reading them, told of each
 * committed, a member that joins again reading on from the last told; a group told of none
 * reading from the first record (join), or, as the log's, from its first or the next written,
 * as it is told to (joinTopic).
 */
import { setTimeout as sleep } from "node:timers/promises";

import { Effect } from "effect";

import type { JoinTopic, TopicFeed, TopicRecord } from "../../src/log/topic.ts";

/** A topic held in memory, and its one group. */
export interface MemoryTopic {
  /** Writes `value` after what was written before. */
  readonly write: (value: string) => void;
  /** Joins the group: its records from the last told, or from the first. */
  readonly join: Effect.Effect<TopicFeed>;
  /** Joins the group as the log does: its records from the last told, or, of a group told of none, from the first (`earliest`) or the next written (`latest`). */
  readonly joinTopic: JoinTopic;
  /** How many of its records the group has been told of. */
  readonly committed: () => number;
  /** Every record written, in order. */
  readonly written: () => readonly string[];
}

/** The topic `name`, holding `values` to begin with. */
export function memoryTopic(name: string, values: readonly string[] = []): MemoryTopic {
  const records = [...values];
  let committed = 0;
  let told = false;
  const feedFrom = (from: number): TopicFeed => {
    let next = from;
    let closed = false;
    return {
      async next(): Promise<TopicRecord> {
        for (;;) {
          if (closed) throw new Error("the member was closed");
          if (next < records.length) {
            const at = next++;
            return {
              value: Buffer.from(records[at] ?? ""),
              position: `${name}/0/${at}`,
              commit: async () => {
                committed = Math.max(committed, at + 1);
                told = true;
              },
            };
          }
          await sleep(20);
        }
      },
      async close() {
        closed = true;
      },
    };
  };
  return {
    write: (value) => void records.push(value),
    join: Effect.sync(() => feedFrom(committed)),
    joinTopic: async (_log, untold) => feedFrom(told ? committed : untold === "earliest" ? 0 : records.length),
    committed: () => committed,
    written: () => [...records],
  };
}
