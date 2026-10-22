/**
 * A topic of the log held in memory, as alasio's tests stand one in for its groups' members
 * (src/log/topic.ts): its records in the order written, one group reading them, told of each
 * committed, a member that joins again reading on from the last told; a group told of none
 * reading from the first record (join), or, as the log's, from its first or the next written,
 * as it is told to (joinTopic).
 */
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
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

/**
 * The group of `topic`, served at the loopback to a member in another process (remoteJoin), as
 * the log serves its members: a join, a record at a time, each told committed by the member;
 * a member whose process goes is closed with its connection, and what it read and never told
 * committed read again by the next to join.
 */
export async function serveTopic(topic: MemoryTopic): Promise<{ readonly url: string; readonly close: () => Promise<void> }> {
  /** The members joined, and the records each read and has yet to tell committed, by their positions. */
  const members = new Map<string, { readonly feed: TopicFeed; readonly read: Map<string, TopicRecord> }>();
  const server = createServer((incoming, outgoing) => {
    const url = new URL(incoming.url ?? "/", "http://topic");
    const member = members.get(url.searchParams.get("member") ?? "");
    const answer = (status: number, body?: object) => void outgoing.writeHead(status, { "content-type": "application/json" }).end(body ? JSON.stringify(body) : undefined);
    incoming.resume();
    void (async () => {
      if (url.pathname === "/join") {
        const id = randomUUID();
        members.set(id, { feed: await Effect.runPromise(topic.join), read: new Map() });
        return answer(200, { member: id });
      }
      if (!member) return answer(404, { error: "no such member" });
      if (url.pathname === "/next") {
        // A member whose process went, as its connection does, is closed: what it read is read again by the next.
        outgoing.on("close", () => {
          if (!outgoing.writableFinished) void member.feed.close();
        });
        const record = await member.feed.next().catch(() => null);
        if (!record) return answer(410, { error: "the member was closed" });
        member.read.set(record.position, record);
        return answer(200, { value: record.value?.toString("utf8") ?? null, position: record.position });
      }
      if (url.pathname === "/commit") {
        const position = url.searchParams.get("position") ?? "";
        await member.read.get(position)?.commit();
        member.read.delete(position);
        return answer(204);
      }
      if (url.pathname === "/close") {
        await member.feed.close();
        members.delete(url.searchParams.get("member") ?? "");
        return answer(204);
      }
      return answer(404, { error: "no such call" });
    })();
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    close: async () => {
      await Promise.all([...members.values()].map(({ feed }) => feed.close()));
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
    },
  };
}

/** Joins the group a topic served at `url` (serveTopic) is read in, as a member of another process. */
export async function remoteJoin(url: string): Promise<TopicFeed> {
  const call = async (path: string, member?: string): Promise<Response> => {
    const response = await fetch(`${url}${path}${member === undefined ? "" : `${path.includes("?") ? "&" : "?"}member=${member}`}`, { method: "POST" });
    if (!response.ok) throw new Error(`the topic answered ${path} with ${response.status}: ${await response.text()}`);
    return response;
  };
  const { member } = (await (await call("/join")).json()) as { readonly member: string };
  return {
    next: async () => {
      const { value, position } = (await (await call("/next", member)).json()) as { readonly value: string | null; readonly position: string };
      return { value: value === null ? undefined : Buffer.from(value), position, commit: async () => void (await call(`/commit?position=${encodeURIComponent(position)}`, member)) };
    },
    close: async () => void (await call("/close", member)),
  };
}
