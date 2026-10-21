/**
 * The harness host's pool of connections to alasio's Neon, each connection carrying its pod's
 * grant (../harness/grant.ts) in its setting (GRANT_SETTING), which the state's policies read
 * (../persistence/harness-grants.ts): set as the connection is made, before it is handed on.
 *
 * A grant given anew (grant) is every connection's from then on: one made with another, idle
 * or handed back since, is ended as it would be handed on, and another made in its place, so
 * no connection handed on carries a grant it was given before. One held as it was given goes
 * on with what it carries until it is handed back, as its work began under that grant.
 * Before any grant is given, a connection carries none, and reaches nothing.
 */
import pg, { type PoolClient } from "pg";

import { GRANT_SETTING } from "../harness/grant.ts";

/** What a connection handed on whose grant is not the pool's is ended with. */
class GrantReplaced extends Error {
  constructor() {
    super("its grant was replaced");
  }
}

/** A pool's grants: the one each connection is to carry, null until one is given, and the one each carries, as it was set. */
interface Grants {
  grant: string | null;
  readonly carried: WeakMap<object, string | null>;
}

/** Sets the pool's grant on `client`, a connection just made, recording what it carries. */
async function carry(grants: Grants, client: pg.ClientBase): Promise<void> {
  const { grant } = grants;
  await client.query("select set_config($1, $2, false)", [GRANT_SETTING, grant ?? ""]);
  grants.carried.set(client, grant);
}

/** A pool of connections, each carrying the grant the pool was last given, as the module says. */
export class GrantedPool extends pg.Pool {
  readonly #grants: Grants;

  constructor(config: pg.PoolConfig) {
    const grants: Grants = { grant: null, carried: new WeakMap() };
    super({ ...config, onConnect: (client) => carry(grants, client) });
    this.#grants = grants;
  }

  /** Gives the pool `grant`, which every connection handed on from now on carries. */
  grant(grant: string): void {
    this.#grants.grant = grant;
  }

  /** A connection carrying the pool's grant: one carrying another, made before it was given, ended and another taken. */
  async #carrying(): Promise<PoolClient> {
    for (;;) {
      const client = await super.connect();
      if (this.#grants.carried.get(client) === this.#grants.grant) return client;
      client.release(new GrantReplaced());
    }
  }

  override connect(): Promise<PoolClient>;
  override connect(callback: (err: Error | undefined, client: PoolClient | undefined, done: (release?: unknown) => void) => void): void;
  override connect(callback?: (err: Error | undefined, client: PoolClient | undefined, done: (release?: unknown) => void) => void): Promise<PoolClient> | void {
    const carrying = this.#carrying();
    if (!callback) return carrying;
    carrying.then(
      (client) => callback(undefined, client, (release) => client.release(release as Error | boolean | undefined)),
      (error: unknown) => callback(error instanceof Error ? error : new Error(String(error)), undefined, () => {}),
    );
  }
}
