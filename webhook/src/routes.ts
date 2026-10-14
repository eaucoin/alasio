/**
 * The intake's routes, one an environment, as alasio's command line writes them in the
 * routes Secret (cli/src/webhook.ts), a key an environment: the path Telegram is told to
 * deliver its bot's updates to, unguessable; the secret token it is told to send with
 * each; and the users the environment allows. Read from the Secret's files as the kubelet
 * keeps them, and read again as they change, so an environment made or deleted is served
 * or not within the kubelet's sync, without a restart.
 */
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

/** An environment's route. */
export interface Route {
  readonly environment: string;
  readonly path: string;
  readonly secretToken: string;
  readonly allowedUserIds: ReadonlySet<string>;
  /** The log's topic its updates are written to. */
  readonly topic: string;
}

/** An environment's name, as a branch's is, or main. */
const ENVIRONMENT = /^[a-z0-9]([-a-z0-9]{0,28}[a-z0-9])?$/u;
/** A route's path: the environment's, and 128 random bits or more in base64url. */
const PATH = /^\/telegram\/[a-z0-9-]+\/[\w-]{22,}$/u;
/** A secret token as Telegram takes one: 1 to 256 of A-Z, a-z, 0-9, _ and -. */
const SECRET_TOKEN = /^[\w-]{32,256}$/u;

/** The route of the environment `environment`, from what its key holds; an error says what is wrong with it. */
export function parseRoute(environment: string, text: string): Route {
  if (!ENVIRONMENT.test(environment)) throw new Error(`${environment} is no environment's name`);
  const value: unknown = JSON.parse(text);
  const { path, secretToken, allowedUserIds } = (typeof value === "object" && value !== null ? value : {}) as Record<string, unknown>;
  if (typeof path !== "string" || !PATH.test(path) || !path.startsWith(`/telegram/${environment}/`)) throw new Error(`the route of ${environment} has no path of its own`);
  if (typeof secretToken !== "string" || !SECRET_TOKEN.test(secretToken)) throw new Error(`the route of ${environment} has no secret token of 32 characters or more`);
  if (!Array.isArray(allowedUserIds) || allowedUserIds.length === 0 || !allowedUserIds.every((id) => typeof id === "string" && /^[1-9][0-9]{0,19}$/u.test(id))) {
    throw new Error(`the route of ${environment} allows no user`);
  }
  return { environment, path, secretToken, allowedUserIds: new Set(allowedUserIds), topic: `telegram.${environment}` };
}

/**
 * The routes in `directory`, a file an environment, by their paths. A file that is not a
 * route is left out, and said by `refused`; the kubelet's own (its `..data` and dated
 * directories) are not routes.
 */
export function readRoutes(directory: string, refused: (environment: string, why: string) => void): ReadonlyMap<string, Route> {
  const routes = new Map<string, Route>();
  for (const entry of readdirSync(directory)) {
    if (entry.startsWith(".")) continue;
    try {
      const route = parseRoute(entry, readFileSync(join(directory, entry), "utf8"));
      routes.set(route.path, route);
    } catch (error) {
      refused(entry, error instanceof Error ? error.message : String(error));
    }
  }
  return routes;
}
