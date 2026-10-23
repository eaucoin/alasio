/**
 * alasio's configuration as its Deployment gives it (src/config.ts loadAlasioConfig): its own
 * pod's name, which its conversations' claims are held as (src/codex/worker-claims.ts), read of
 * ALASIO_POD, never its hostname, and alasio not started without it.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { loadAlasioConfig } from "../src/config.ts";

/** What alasio's Deployment gives alasio of what it requires, its pod's name `pod`, where given. */
const given = (pod?: string): NodeJS.ProcessEnv => ({
  TELEGRAM_BOT_TOKEN: "123:the-bot",
  ALASIO_LOG_BROKERS: "alasio-log:9092",
  ALASIO_LOG_USERNAME: "alasio-main",
  ALASIO_LOG_PASSWORD_FILE: "/run/alasio/log/password",
  ALASIO_TELEGRAM_TOPIC: "telegram.main",
  ALASIO_TELEGRAM_GROUP: "alasio-main",
  ALASIO_GATEWAY_URL: "http://alasio-sandbox-gateway.alasio.svc.cluster.local:8080",
  ALASIO_GATEWAY_ENVIRONMENT: "main",
  ALASIO_GATEWAY_CALLER_KEY: "the-caller-key",
  ALASIO_CODEX_LOGIN_URL: "http://alasio-codex-login.alasio.svc.cluster.local:4320/v1",
  ...(pod === undefined ? {} : { ALASIO_POD: pod }),
});

test("alasio's pod is the one its Deployment names, which it holds its conversations' claims as, and alasio starts in none without it", () => {
  assert.equal(loadAlasioConfig(given("alasio-7c99d-x2kq4")).pod, "alasio-7c99d-x2kq4");
  for (const pod of [undefined, "", " "]) assert.throws(() => loadAlasioConfig(given(pod)), /ALASIO_POD/u, JSON.stringify(pod));
});
