/**
 * Telegram's deliveries to the webhook intake (./manifests/webhook.ts), as alasio's
 * command line arranges them, it alone: alasio itself never tells Telegram anything of
 * them. Each environment, main and each branch's, has a route in the routes Secret, which
 * the intake reads (webhook/src/routes.ts): an unguessable path, a secret token Telegram
 * sends with each delivery, and the users the environment allows, which the intake lets
 * through alone. Its bot is registered once the intake serves its route: Telegram is told
 * the route's URL on the intake's public address, the secret token, the updates alasio
 * acts on, and the intake's certificate, uploaded, which Telegram trusts alone; and its
 * menu is set to alasio's commands. alasio up
 * registers main's again each time, with the certificate the stack's setup keeps or has
 * just renewed (neon/control/certificate.ts); a branch environment's bot is registered as
 * the branch is made, and told to deliver nowhere as it is deleted.
 */
import { randomBytes } from "node:crypto";
import { duplexPair } from "node:stream";

import type { V1Pod, V1Secret } from "@kubernetes/client-node";
import { Duration, Effect, Redacted, Schema } from "effect";

import { FIELD_MANAGER, hasStatus, kind, KubeApi, type KubeApiError } from "./kube/api.ts";
import { selectorOf, type WaitOptions } from "./kube/rollout.ts";
import { NAMESPACE, selectorLabels } from "./manifests/common.ts";
import type { InstallConfig } from "./manifests/config.ts";
import { ROUTES_SECRET, TLS_SECRET, WEBHOOK_COMPONENT, WEBHOOK_HEALTH_PORT } from "./manifests/webhook.ts";
import type { TelegramBot } from "./secrets.ts";
import { TelegramBotApi, type WebhookError, type WebhookInfo } from "./telegram.ts";
import { failingDeliveries } from "../../src/telegram/deliveries.ts";

const SECRET = kind("Secret");
const POD = kind("Pod");

/** An environment's route, as the routes Secret keeps it under the environment's name. */
export interface Route {
  readonly path: string;
  readonly secretToken: string;
  readonly allowedUserIds: readonly string[];
}

/** The webhook intake did not serve an environment's route in time, or its certificate is not there to upload. */
export class WebhookNotServing extends Schema.TaggedError<WebhookNotServing>()("WebhookNotServing", {
  message: Schema.String,
}) {}

/** The data of the Secret `found`, decoded. */
const dataOf = (found: V1Secret | null): Record<string, string> =>
  Object.fromEntries(Object.entries(found?.data ?? {}).map(([key, value]) => [key, Buffer.from(value, "base64").toString("utf8")]));

/** The routes Secret, as the API addresses it. */
const ROUTES = { ...SECRET, namespace: NAMESPACE, name: ROUTES_SECRET };

/** The routes Secret, holding no route: each is patched in under its environment's key alone (setRoute). */
const routesSecret: V1Secret = {
  apiVersion: "v1",
  kind: "Secret",
  metadata: { name: ROUTES_SECRET, namespace: NAMESPACE, labels: { "app.kubernetes.io/managed-by": "alasio", "app.kubernetes.io/part-of": "alasio" } },
  type: "Opaque",
};

/**
 * Sets `environment`'s route in the routes Secret, or takes it out where `route` is null,
 * by a merge patch of its key alone: environments made and deleted at once each keep the
 * others' routes, which a write of the whole Secret, from a read of it, would take out or
 * put back. Where there is no Secret yet, it is applied first, holding no route: of
 * environments that find none at once, the last to apply it takes out none the others
 * have patched in.
 */
const setRoute = Effect.fnUntraced(function*(environment: string, route: Route | null): Effect.fn.Return<void, KubeApiError, KubeApi> {
  const kube = yield* KubeApi;
  const patch = kube.patch(ROUTES, { data: { [environment]: route && Buffer.from(JSON.stringify(route), "utf8").toString("base64") } }, FIELD_MANAGER);
  yield* patch.pipe(Effect.catchIf(hasStatus(404), () => Effect.andThen(kube.apply(routesSecret), patch)));
});

/** The routes the Secret holds, by environment; one that does not parse is none. */
const readRoutes: Effect.Effect<Record<string, Route>, KubeApiError, KubeApi> = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const routes: Record<string, Route> = {};
  for (const [environment, text] of Object.entries(dataOf(yield* kube.get<V1Secret>(ROUTES)))) {
    try {
      routes[environment] = JSON.parse(text) as Route;
    } catch {
      // Made anew below, where it is asked for.
    }
  }
  return routes;
});

/**
 * The route of `environment`, allowing `allowedUserIds`: the one it has, its users as
 * given now, or a new one, its path and secret token random.
 */
export const ensureRoute = Effect.fnUntraced(function*(environment: string, allowedUserIds: readonly string[]): Effect.fn.Return<Route, KubeApiError, KubeApi> {
  const current = (yield* readRoutes)[environment];
  const route: Route = {
    path: current?.path ?? `/telegram/${environment}/${randomBytes(16).toString("base64url")}`,
    secretToken: current?.secretToken ?? randomBytes(32).toString("base64url"),
    allowedUserIds: [...allowedUserIds],
  };
  yield* setRoute(environment, route);
  return route;
});

/** Takes the route of `environment` out of the routes Secret. */
export const removeRoute = Effect.fnUntraced(function*(environment: string): Effect.fn.Return<void, KubeApiError, KubeApi> {
  if ((yield* readRoutes)[environment]) yield* setRoute(environment, null);
});

/** The environments the intake's pod `pod` routes, as its health port says; none where it does not answer. */
const routedBy = (pod: string): Effect.Effect<readonly string[], KubeApiError, KubeApi> =>
  Effect.gen(function*() {
    const kube = yield* KubeApi;
    const [here, there] = duplexPair();
    // What came back once the forward ends, as the intake closes it after its answer, or it fails.
    const answered = new Promise<string>((resolve) => {
      const chunks: Buffer[] = [];
      const ended = () => resolve(Buffer.concat(chunks).toString("utf8"));
      here.on("data", (chunk: Buffer) => chunks.push(chunk));
      here.on("end", ended);
      here.on("close", ended);
      here.on("error", ended);
    });
    here.write("GET /environments HTTP/1.0\r\nHost: webhook\r\n\r\n");
    yield* kube.portForward({ namespace: NAMESPACE, pod }, WEBHOOK_HEALTH_PORT, there).pipe(Effect.timeout("10 seconds"), Effect.ignore);
    here.destroy();
    const text = yield* Effect.promise(() => answered);
    try {
      const routes = JSON.parse(text.slice(text.indexOf("\r\n\r\n") + 4)) as { environment: string }[];
      return routes.map(({ environment }) => environment);
    } catch {
      return [];
    }
  });

/** Waits until every running pod of the intake routes `environment`, as the kubelet hands them the routes Secret it was written in. */
export const awaitRouted = Effect.fnUntraced(function*(environment: string, { timeout }: WaitOptions): Effect.fn.Return<void, KubeApiError | WebhookNotServing, KubeApi> {
  const kube = yield* KubeApi;
  const deadline = Date.now() + Duration.toMillis(Duration.fromInputUnsafe(timeout));
  for (;;) {
    const pods = (yield* kube.list<V1Pod>(POD, { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels(WEBHOOK_COMPONENT)) }))
      .filter(({ status, metadata }) => status?.phase === "Running" && !metadata?.deletionTimestamp);
    const routed = yield* Effect.forEach(pods, ({ metadata }) => routedBy(metadata?.name ?? ""));
    if (pods.length > 0 && routed.every((environments) => environments.includes(environment))) return;
    if (Date.now() > deadline) {
      return yield* new WebhookNotServing({ message: `the webhook intake does not route ${environment} yet: its pods are not running, or have not been handed its route` });
    }
    yield* Effect.sleep("2 seconds");
  }
});

/** The URL Telegram delivers `route`'s updates to, on the intake's public address and port. */
export const routeUrl = ({ webhook }: InstallConfig, route: Route): string => `https://${webhook.publicAddress}${webhook.port === 443 ? "" : `:${webhook.port}`}${route.path}`;

/** The certificate the intake serves, which Telegram is given, as the stack's setup keeps it. */
const certificate: Effect.Effect<string, KubeApiError | WebhookNotServing, KubeApi> = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const cert = dataOf(yield* kube.get<V1Secret>({ ...SECRET, namespace: NAMESPACE, name: TLS_SECRET }))["tls.crt"];
  if (!cert) return yield* new WebhookNotServing({ message: `${TLS_SECRET} holds no certificate: the stack's setup makes it as alasio up runs` });
  return cert;
});

/** Has Telegram deliver `bot`'s updates to `route`, once the intake serves it, with the intake's certificate, and its menu offer alasio's commands. */
export const registerBot = Effect.fnUntraced(function*(
  config: InstallConfig,
  environment: string,
  bot: TelegramBot,
  route: Route,
  options: WaitOptions,
): Effect.fn.Return<void, KubeApiError | WebhookNotServing | WebhookError, KubeApi | TelegramBotApi> {
  yield* awaitRouted(environment, options);
  const telegram = yield* TelegramBotApi;
  const address = config.webhook.publicAddress;
  yield* telegram.setWebhook(bot.token, {
    url: routeUrl(config, route),
    certificate: yield* certificate,
    secretToken: Redacted.make(route.secretToken),
    ipAddress: /^(\d{1,3}\.){3}\d{1,3}$/u.test(address) ? address : undefined,
  });
  // Here, as each bot is registered, rather than as alasio starts, which would ask it of every start.
  yield* telegram.setMenu(bot.token);
  yield* Effect.logInfo(`Telegram delivers ${bot.username ? `@${bot.username}` : "the bot"}'s updates to the webhook intake, for ${environment}`);
});

/** The fix of deliveries that fail where nothing points to another. */
const REGISTER_AGAIN = "alasio up registers each bot again";

/**
 * How a bot's deliveries fare, as its getWebhookInfo says: whether to `url`, how many
 * wait, and where they fail (../../src/telegram/deliveries.ts) Telegram's last error and
 * when; and their fix, null where they do not fail.
 */
export function describeDeliveries(info: WebhookInfo, url: string, now = Date.now()): { readonly line: string; readonly fix: string | null } {
  if (info.url !== url) return { line: info.url ? `delivered elsewhere, to ${info.url}: alasio up registers it again` : "not delivered: alasio up registers it", fix: REGISTER_AGAIN };
  const failing = failingDeliveries(info, now);
  if (failing) return { line: failing.description, fix: failing.fix ?? REGISTER_AGAIN };
  return { line: `delivered, ${info.pending_update_count} update${info.pending_update_count === 1 ? "" : "s"} waiting`, fix: null };
}

/** A bot of an environment's, as status asks Telegram of it: the environment, and its bot. */
export interface EnvironmentBot {
  readonly environment: string;
  readonly bot: TelegramBot;
}

/** How each of `bots`' deliveries fare, as Telegram says: a line of each, and the fixes of those that fail, each once. */
export const describeBots = Effect.fnUntraced(function*(config: InstallConfig, bots: readonly EnvironmentBot[]): Effect.fn.Return<{ readonly lines: readonly string[]; readonly fixes: readonly string[] }, KubeApiError, KubeApi | TelegramBotApi> {
  const telegram = yield* TelegramBotApi;
  const routes = yield* readRoutes;
  const described = yield* Effect.forEach(bots, ({ environment, bot }) => {
    const name = `${environment}'s bot${bot.username ? ` @${bot.username}` : ""}`;
    const route = routes[environment];
    if (!route) return Effect.succeed({ line: `${name}: no route: alasio up gives it one`, fix: REGISTER_AGAIN });
    return telegram.getWebhookInfo(bot.token).pipe(
      Effect.map((info) => describeDeliveries(info, routeUrl(config, route))),
      Effect.catch((error) => Effect.succeed({ line: `not asked: ${error.message}`, fix: REGISTER_AGAIN })),
      Effect.map(({ line, fix }) => ({ line: `${name}: ${line}`, fix })),
    );
  });
  return {
    lines: ["Telegram's deliveries:", ...described.map(({ line }) => `  ${line}`)],
    fixes: [...new Set(described.flatMap(({ fix }) => (fix === null ? [] : [fix])))],
  };
});
