/**
 * neon-control's record, in the storage controller's Postgres, in a schema of its own
 * (`neon_control`), so that it outlives any replica of neon-control and is shared by all
 * of them: a row of each branch, its name and its record as ./branches.ts has it; a row
 * of each compute woken or put to sleep, and when; where the tenant's shards are, as the
 * storage controller last said, which every compute's spec names; and of each compute,
 * where its spec last said they were.
 *
 * What changes branches runs under one lock, a session's advisory lock on the database,
 * so two replicas never change them at once, nor repair a timeline whose deletion one of
 * them has begun: `changing` holds it for the whole of its work, and each `save` within
 * it is written at once, so a creation asked again after a failure, or of another
 * replica, finds what the first recorded. A compute is woken or put to sleep under a lock
 * of its own (`computeLocked`), so a wake never finds the pod a sleep is stopping, and the
 * computes are given a new layout under another (`tryLayoutLocked`). One replica leads,
 * running what must run once (./service.ts's loops): the one holding another advisory
 * lock, on a connection of its own, for as long as that connection lives.
 *
 * Each lock is held on a connection of its own, made for it, and what only reads takes
 * none, on a pool of its own: so however many wait for a lock, reads go on. A lock is
 * waited for a minute, unless the record is given another patience, and then given up
 * (RecordBusy). Every connection has TCP keepalives on both ends, so a replica that dies,
 * or whose network does, loses its locks within a minute, not when the kernel's two
 * hours run out.
 *
 * The storage controller's own tables, in its `public` schema, are never touched.
 */
import pg from "pg";

import type { Branch, ShardLayout } from "./branches.ts";

/** The advisory lock key of changes to the branches: `neon_ctl`, its bytes read as a number. */
const CHANGE_LOCK = 0x6e65_6f6e_5f63_746cn;

/** The advisory lock key of the replica that leads (./service.ts): `neon_ldr`, likewise. */
const LEADER_LOCK = 0x6e65_6f6e_5f6c_6472n;

/** The advisory lock key of giving computes the shards' layout (./service.ts's /notify-attach): `neon_lay`, likewise. */
const LAYOUT_LOCK = 0x6e65_6f6e_5f6c_6179n;

/** The first of the two keys of each compute's advisory lock, the second its id's hash: `neon`, its bytes read as a number. */
const COMPUTE_LOCKS = 0x6e65_6f6e;

/** What every connection is made with: a bound on connecting, and keepalives, the client's and the server's. */
const CONNECTION = {
  connectionTimeoutMillis: 10_000,
  keepAlive: true,
  keepAliveInitialDelayMillis: 10_000,
  options: "-c tcp_keepalives_idle=30 -c tcp_keepalives_interval=10 -c tcp_keepalives_count=3",
} satisfies pg.ClientConfig;

/** Which lock: of changes to the branches, or of a compute. */
export type RecordLock = "changes" | "compute";

/** A lock another replica held for longer than the record's patience: what waited for it may be asked again. */
export class RecordBusy extends Error {
  readonly lock: RecordLock;

  constructor(lock: RecordLock, what: string) {
    super(`${what} is held by another replica of neon-control; ask again`);
    this.lock = lock;
  }
}

/** Every branch, as recorded, in the order each was first recorded, main first. */
async function recorded(on: pg.Pool | pg.Client): Promise<Branch[]> {
  const { rows } = await on.query<{ branch: Branch }>("select branch from neon_control.branches order by branch->>'createdAt', name");
  return rows.map((row) => row.branch);
}

/** What changes the branches is given: them, as recorded when it took the lock, and how to record their next state. */
export interface Changing {
  branches: Branch[];
  save(next: Branch[]): Promise<void>;
}

/** A compute, as recorded: when it was last woken, and last put to sleep, in milliseconds since the epoch, or null for never. */
export interface ComputeTimes {
  id: string;
  wokenAt: number | null;
  suspendedAt: number | null;
}

/** The record, in the database `url` reaches. */
export class BranchRecord {
  readonly #url: string;
  readonly #pool: pg.Pool;
  readonly #busy: (lock: RecordLock) => void;
  readonly #patienceMs: number;
  /** The connection holding the leader's lock, while this replica leads. */
  #leading: pg.Client | null = null;

  /** `busy` hears of each lock given up (RecordBusy), after `patienceMs`. */
  constructor(url: string, { busy = () => {}, patienceMs = 60_000 }: { busy?: (lock: RecordLock) => void; patienceMs?: number } = {}) {
    this.#url = url;
    this.#busy = busy;
    this.#patienceMs = patienceMs;
    this.#pool = new pg.Pool({ connectionString: url, max: 4, ...CONNECTION });
    // An idle connection lost is replaced on the next checkout.
    this.#pool.on("error", () => {});
  }

  /** Makes the schema and its tables, idempotently, under the lock, so two replicas starting at once do not race. */
  ensureSchema(): Promise<void> {
    return this.#holding(CHANGE_LOCK, "changes", "the lock of changes", async (client) => {
      await client.query("create schema if not exists neon_control");
      await client.query("create table if not exists neon_control.branches (name text primary key, branch jsonb not null)");
      await client.query("create table if not exists neon_control.computes (id text primary key, woken_at timestamptz, suspended_at timestamptz)");
      await client.query("create table if not exists neon_control.layout (tenant_id text primary key, layout jsonb not null)");
      await client.query("create table if not exists neon_control.compute_layouts (id text primary key, layout jsonb not null)");
    });
  }

  /** Every branch, as recorded now. */
  branches(): Promise<Branch[]> {
    return recorded(this.#pool);
  }

  /** Runs `work` holding the lock of changes, with the branches as recorded once it holds it. */
  changing<T>(work: (changing: Changing) => Promise<T>): Promise<T> {
    return this.#holding(CHANGE_LOCK, "changes", "the lock of changes", (client) => this.#change(client, work));
  }

  /** Runs `work` as `changing` does, if no other holds the lock now; null, without running it, if one does. */
  async tryChanging<T>(work: (changing: Changing) => Promise<T>): Promise<{ value: T } | null> {
    return this.#trying(CHANGE_LOCK, async (client) => this.#change(client, work));
  }

  /** Runs `work` holding the lock of the compute `id`, waiting for it as long as `patienceMs`, the record's patience unless given. */
  computeLocked<T>(id: string, work: () => Promise<T>, patienceMs = this.#patienceMs): Promise<T> {
    return this.#holding([COMPUTE_LOCKS, id], "compute", `the lock of ${id}`, () => work(), patienceMs);
  }

  /** Runs `work` as `computeLocked` does, if no other holds the compute's lock now; null, without running it, if one does. */
  tryComputeLocked<T>(id: string, work: () => Promise<T>): Promise<{ value: T } | null> {
    return this.#trying([COMPUTE_LOCKS, id], () => work());
  }

  /**
   * Runs `work` holding the lock of the layout, if no other holds it now; null, without
   * running it, if one does: so one replica at a time records the layout notified and gives
   * it to the computes, and a notification taken by another meanwhile is asked again.
   */
  tryLayoutLocked<T>(work: () => Promise<T>): Promise<{ value: T } | null> {
    return this.#trying(LAYOUT_LOCK, () => work());
  }

  /** Records the compute `id` woken, or asked to be, now. */
  async woke(id: string): Promise<void> {
    await this.#pool.query(
      "insert into neon_control.computes (id, woken_at) values ($1, now()) on conflict (id) do update set woken_at = excluded.woken_at",
      [id],
    );
  }

  /** Records the compute `id` put to sleep now. */
  async suspended(id: string): Promise<void> {
    await this.#pool.query(
      "insert into neon_control.computes (id, suspended_at) values ($1, now()) on conflict (id) do update set suspended_at = excluded.suspended_at",
      [id],
    );
  }

  /** Where the shards of the tenant `tenantId` are, as recorded; null before the first record of it. */
  async layout(tenantId: string): Promise<ShardLayout | null> {
    const { rows } = await this.#pool.query<{ layout: ShardLayout }>("select layout from neon_control.layout where tenant_id = $1", [tenantId]);
    return rows[0]?.layout ?? null;
  }

  /** Records the shards of the tenant `tenantId` placed as `layout` says. */
  async saveLayout(tenantId: string, layout: ShardLayout): Promise<void> {
    await this.#pool.query(
      "insert into neon_control.layout (tenant_id, layout) values ($1, $2) on conflict (tenant_id) do update set layout = excluded.layout",
      [tenantId, JSON.stringify(layout)],
    );
  }

  /** Records the shards of the tenant `tenantId` placed as `layout` says, where nothing of it is recorded yet. */
  async seedLayout(tenantId: string, layout: ShardLayout): Promise<void> {
    await this.#pool.query(
      "insert into neon_control.layout (tenant_id, layout) values ($1, $2) on conflict (tenant_id) do nothing",
      [tenantId, JSON.stringify(layout)],
    );
  }

  /** Where the compute `id`'s spec, fetched or given anew, last said the shards were. */
  async gaveLayout(id: string, layout: ShardLayout): Promise<void> {
    await this.#pool.query(
      "insert into neon_control.compute_layouts (id, layout) values ($1, $2) on conflict (id) do update set layout = excluded.layout",
      [id, JSON.stringify(layout)],
    );
  }

  /** Each compute's last given layout (gaveLayout), by its id. */
  async givenLayouts(): Promise<Map<string, ShardLayout>> {
    const { rows } = await this.#pool.query<{ id: string; layout: ShardLayout }>("select id, layout from neon_control.compute_layouts");
    return new Map(rows.map(({ id, layout }) => [id, layout]));
  }

  /** Every compute recorded woken or put to sleep. */
  async computes(): Promise<ComputeTimes[]> {
    const { rows } = await this.#pool.query<{ id: string; woken_at: Date | null; suspended_at: Date | null }>(
      "select id, woken_at, suspended_at from neon_control.computes order by id",
    );
    return rows.map(({ id, woken_at, suspended_at }) => ({ id, wokenAt: woken_at?.getTime() ?? null, suspendedAt: suspended_at?.getTime() ?? null }));
  }

  /**
   * Whether this replica leads: it does once it has taken the leader's lock, when no other
   * holds it, and from then on while the connection holding it answers, which is asked
   * each time.
   */
  async leads(): Promise<boolean> {
    if (this.#leading) {
      const holding = this.#leading;
      const answers = await holding.query("select 1").then(() => true, () => false);
      if (answers) return true;
      // Its lock went with it, if the server noticed; if not, its keepalives let it go.
      this.#leading = null;
      await holding.end().catch(() => {});
    }
    // Each of its queries bounded, as a connection whose peer is gone may not answer at all.
    const client = this.#client({ query_timeout: 10_000 });
    // A lost connection lets the lock go: another replica may lead, or this one again.
    const lost = () => {
      if (this.#leading === client) this.#leading = null;
    };
    client.on("error", lost);
    client.on("end", lost);
    try {
      await client.connect();
      const { rows } = await client.query<{ held: boolean }>("select pg_try_advisory_lock($1) as held", [String(LEADER_LOCK)]);
      if (rows[0]?.held) {
        this.#leading = client;
        return true;
      }
    } catch (error) {
      await client.end().catch(() => {});
      throw error;
    }
    await client.end();
    return false;
  }

  async end(): Promise<void> {
    await this.#leading?.end();
    await this.#pool.end();
  }

  /** A connection of its own, for a lock. */
  #client(config: pg.ClientConfig = {}): pg.Client {
    const client = new pg.Client({ connectionString: this.#url, ...CONNECTION, ...config });
    // Its loss fails what it runs; an error between queries has nothing to fail.
    client.on("error", () => {});
    return client;
  }

  /**
   * Runs `work` on a connection of its own holding the advisory lock `key` (one key, or
   * two: a class and a name hashed), waited for as long as the record's patience
   * (RecordBusy after that). The lock ends with the connection.
   */
  async #holding<T>(key: bigint | readonly [number, string], lock: RecordLock, what: string, work: (client: pg.Client) => Promise<T>, patienceMs = this.#patienceMs): Promise<T> {
    const client = this.#client();
    await client.connect();
    try {
      // At least a millisecond: lock_timeout 0 waits for ever.
      await client.query(`set lock_timeout = ${Math.max(1, Math.round(patienceMs))}`);
      await client.query(lockQuery("pg_advisory_lock", key), lockParameters(key)).catch((error: unknown) => {
        // Postgres's lock_not_available: lock_timeout ran out.
        if ((error as { code?: string }).code !== "55P03") throw error;
        this.#busy(lock);
        throw new RecordBusy(lock, what);
      });
      return await work(client);
    } finally {
      await client.end().catch(() => {});
    }
  }

  /** Runs `work` as #holding does, if the lock `key` is free now; null if it is not. */
  async #trying<T>(key: bigint | readonly [number, string], work: (client: pg.Client) => Promise<T>): Promise<{ value: T } | null> {
    const client = this.#client();
    await client.connect();
    try {
      const { rows } = await client.query<{ held: boolean }>(`${lockQuery("pg_try_advisory_lock", key)} as held`, lockParameters(key));
      if (!rows[0]?.held) return null;
      return { value: await work(client) };
    } finally {
      await client.end().catch(() => {});
    }
  }

  /** Runs `work` with the branches as recorded on `client`, which holds the lock of changes. */
  async #change<T>(client: pg.Client, work: (changing: Changing) => Promise<T>): Promise<T> {
    const changing: Changing = {
      branches: await recorded(client),
      save: async (next) => {
        await save(client, changing.branches, next);
        changing.branches = next;
      },
    };
    return work(changing);
  }
}

/** The query taking the advisory lock `key` with the function `lock`. */
const lockQuery = (lock: string, key: bigint | readonly [number, string]): string =>
  typeof key === "bigint" ? `select ${lock}($1)` : `select ${lock}($1, hashtext($2))`;

const lockParameters = (key: bigint | readonly [number, string]): string[] => (typeof key === "bigint" ? [String(key)] : [String(key[0]), key[1]]);

/** Writes what differs between `previous` and `next`, in one transaction. */
async function save(client: pg.Client, previous: readonly Branch[], next: readonly Branch[]): Promise<void> {
  const kept = new Set(next.map(({ name }) => name));
  await client.query("begin");
  try {
    for (const { name } of previous) if (!kept.has(name)) await client.query("delete from neon_control.branches where name = $1", [name]);
    for (const branch of next) {
      const before = previous.find(({ name }) => name === branch.name);
      if (before && JSON.stringify(before) === JSON.stringify(branch)) continue;
      await client.query(
        "insert into neon_control.branches (name, branch) values ($1, $2) on conflict (name) do update set branch = excluded.branch",
        [branch.name, JSON.stringify(branch)],
      );
    }
    await client.query("commit");
  } catch (error) {
    await client.query("rollback");
    throw error;
  }
}
