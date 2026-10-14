/**
 * alasio's webhook intake: the one thing of alasio's the internet reaches. Telegram
 * delivers each environment's bot's updates to it, over HTTPS, at the environment's route
 * (./routes.ts), and it writes those alasio would act on to the environment's topic in the
 * log, answering once the log has them (./intake.ts). It holds no bot token: its only
 * credentials are each route's secret token, and its principal on the log, which may only
 * write the Telegram topics.
 *
 * It serves HTTPS on WEBHOOK_PORT with the certificate in WEBHOOK_TLS (`tls.crt`,
 * `tls.key`), the self-signed one alasio's command line uploads to Telegram with each
 * registration, read again as it changes; and, on HEALTH_PORT, plain HTTP inside the
 * cluster alone: /healthz, /readyz (the log has taken a record or been reached), /environments (the environments it routes, which the command line waits for
 * before it registers one) and /metrics (Prometheus, for the stack's collector), with the
 * certificate's expiry, which an alert watches.
 *
 * Environment: WEBHOOK_PORT, HEALTH_PORT, WEBHOOK_TLS, WEBHOOK_ROUTES (the routes Secret's
 * directory), LOG_BROKERS, LOG_USERNAME, LOG_PASSWORD.
 */
import { X509Certificate } from "node:crypto";
import { readFileSync } from "node:fs";
import { createServer as createHttpServer } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import { join } from "node:path";

import { ProduceAcks, Producer } from "@platformatic/kafka";

import { intakeHandler, type Outcome, rateLimiter } from "./intake.ts";
import { readRoutes, type Route } from "./routes.ts";

/** How often the routes and the certificate are read again: within the kubelet's own sync of a Secret's files. */
const RELOAD_MS = 10_000;
/** Each source address's rate: Telegram's own deliveries, a few connections of the bot's at a time, stay far under it. */
const RATE = { perSecond: 20, burst: 40 };

const required = (key: string): string => {
  const value = process.env[key]?.trim();
  if (!value) throw new Error(`${key} must be set`);
  return value;
};

function log(message: string, fields: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({ time: new Date().toISOString(), message, ...fields }));
}

const tlsDirectory = required("WEBHOOK_TLS");
const routesDirectory = required("WEBHOOK_ROUTES");

const counts = new Map<string, number>();
const counted = (outcome: Outcome, environment: string | null, why?: string): void => {
  const key = `outcome="${outcome}",environment="${environment ?? ""}"`;
  counts.set(key, (counts.get(key) ?? 0) + 1);
  if (why) log("an update was not written to the log", { environment, why });
};

let routes: ReadonlyMap<string, Route> = new Map();
let refusedRoutes = "";
const loadRoutes = (): void => {
  const refusals: string[] = [];
  routes = readRoutes(routesDirectory, (environment, why) => refusals.push(`${environment}: ${why}`));
  const said = refusals.join("; ");
  if (said !== refusedRoutes) log("routes refused", { refusals });
  refusedRoutes = said;
};

let tls = { cert: "", key: "" };
let expiresAt = 0;
const loadTls = (): boolean => {
  const read = { cert: readFileSync(join(tlsDirectory, "tls.crt"), "utf8"), key: readFileSync(join(tlsDirectory, "tls.key"), "utf8") };
  if (read.cert === tls.cert && read.key === tls.key) return false;
  tls = read;
  expiresAt = Date.parse(new X509Certificate(read.cert).validTo) / 1000;
  return true;
};

loadRoutes();
loadTls();

const producer = new Producer({
  clientId: "webhook",
  bootstrapBrokers: required("LOG_BROKERS").split(","),
  sasl: { mechanism: "SCRAM-SHA-512", username: required("LOG_USERNAME"), password: required("LOG_PASSWORD") },
  idempotent: true,
  acks: ProduceAcks.ALL,
});
let logReached = false;
const produce = async (topic: string, key: string, value: Buffer): Promise<void> => {
  await producer.send({ messages: [{ topic, key: Buffer.from(key), value }] });
  logReached = true;
};

const server = createHttpsServer(
  { ...tls, minVersion: "TLSv1.2", requestTimeout: 15_000, headersTimeout: 10_000, keepAliveTimeout: 30_000 },
  intakeHandler({ routes: () => routes, produce, allow: rateLimiter(RATE.perSecond, RATE.burst), counted }),
);
server.listen(Number(required("WEBHOOK_PORT")), "0.0.0.0", () => log("serving Telegram's deliveries", { environments: [...routes.values()].map(({ environment }) => environment) }));

const reload = setInterval(() => {
  try {
    loadRoutes();
    if (loadTls()) {
      server.setSecureContext(tls);
      log("certificate read again", { expiresAt: new Date(expiresAt * 1000).toISOString() });
    }
  } catch (error) {
    log("routes or certificate could not be read again", { error: error instanceof Error ? error.message : String(error) });
  }
}, RELOAD_MS);

// Whether the log is reached, before any update was: its brokers' metadata, as the producer asks it.
const probeLog = (): void => {
  producer.metadata({ topics: [] }).then(() => {
    logReached = true;
  }, () => {});
};
probeLog();

const health = createHttpServer((request, response) => {
  if (request.method !== "GET") return void response.writeHead(405).end();
  if (request.url === "/healthz") return void response.writeHead(200, { "content-type": "text/plain" }).end("serving\n");
  if (request.url === "/readyz") {
    if (!logReached) probeLog();
    return void response.writeHead(logReached ? 200 : 503, { "content-type": "text/plain" }).end(logReached ? "ready\n" : "the log is not reached\n");
  }
  if (request.url === "/environments") {
    return void response.writeHead(200, { "content-type": "application/json" }).end(`${JSON.stringify([...routes.values()].map(({ environment, path }) => ({ environment, path })))}\n`);
  }
  if (request.url === "/metrics") {
    const lines = [
      "# HELP webhook_requests_total Requests the intake answered, by outcome and environment",
      "# TYPE webhook_requests_total counter",
      ...[...counts].map(([labels, count]) => `webhook_requests_total{${labels}} ${count}`),
      "# HELP webhook_routes The environments the intake routes",
      "# TYPE webhook_routes gauge",
      `webhook_routes ${routes.size}`,
      "# HELP webhook_certificate_expiry_timestamp_seconds When the certificate Telegram trusts expires",
      "# TYPE webhook_certificate_expiry_timestamp_seconds gauge",
      `webhook_certificate_expiry_timestamp_seconds ${expiresAt}`,
      "",
    ];
    return void response.writeHead(200, { "content-type": "text/plain; version=0.0.4" }).end(lines.join("\n"));
  }
  response.writeHead(404).end();
});
health.listen(Number(required("HEALTH_PORT")), "0.0.0.0");

// A request being answered is finished first; the log is left once it has.
process.on("SIGTERM", () => {
  log("stopping");
  clearInterval(reload);
  health.close();
  server.close(() => {
    producer.close().finally(() => process.exit(0));
  });
  setTimeout(() => process.exit(0), 20_000).unref();
});
