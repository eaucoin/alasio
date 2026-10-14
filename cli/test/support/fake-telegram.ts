/**
 * Telegram's Bot API in memory, as far as alasio's command line asks it: getMe, which
 * answers with the bot of a token it knows, and refuses any other as Telegram does; and
 * setWebhook, deleteWebhook and getWebhookInfo, which keep where each bot's updates are
 * delivered, as it was last told.
 */
import { once } from "node:events";
import { createServer, type IncomingMessage } from "node:http";
import type { AddressInfo } from "node:net";

/** Where a bot's updates are delivered, as setWebhook told it. */
export interface FakeWebhook {
  readonly url: string;
  readonly secretToken: string;
  readonly certificate: string;
  readonly allowedUpdates: readonly string[];
  readonly ipAddress: string | undefined;
}

export interface FakeTelegram {
  /** Where the API is, as TELEGRAM_API_ROOT says. */
  readonly root: string;
  /** The tokens of the bots it knows, and their usernames. */
  readonly bots: Map<string, string>;
  /** Where each bot's updates are delivered, by its token; none where it is not told, or told to deliver nowhere. */
  readonly webhooks: Map<string, FakeWebhook>;
  /** What getWebhookInfo says of a bot's last failed delivery, by its token, as the test sets it. */
  readonly lastErrors: Map<string, { readonly date: number; readonly message: string }>;
  /** How many of a bot's updates getWebhookInfo says wait, by its token, as the test sets it; none where it is not set. */
  readonly pending: Map<string, number>;
  /** The paths it was asked for, which hold the tokens. */
  readonly requests: string[];
  readonly close: () => Promise<void>;
}

/** The form `request` sent, multipart or none. */
async function formOf(request: IncomingMessage): Promise<FormData> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(chunk as Buffer);
  if (request.method !== "POST") return new FormData();
  return new Request("http://fake", { method: "POST", headers: { "content-type": request.headers["content-type"] ?? "" }, body: Buffer.concat(chunks) }).formData();
}

export async function serveFakeTelegram(): Promise<FakeTelegram> {
  const bots = new Map<string, string>();
  const webhooks = new Map<string, FakeWebhook>();
  const lastErrors = new Map<string, { readonly date: number; readonly message: string }>();
  const pending = new Map<string, number>();
  const requests: string[] = [];
  const server = createServer((request, response) => {
    requests.push(request.url ?? "");
    const answer = (status: number, body: unknown) => {
      response.writeHead(status, { "Content-Type": "application/json" });
      response.end(JSON.stringify(body));
    };
    const [, encoded, method] = /^\/bot([^/]+)\/(\w+)$/u.exec(request.url ?? "") ?? [];
    const token = encoded === undefined ? undefined : decodeURIComponent(encoded);
    const username = token === undefined ? undefined : bots.get(token);
    if (token === undefined || username === undefined) return void answer(401, { ok: false, error_code: 401, description: "Unauthorized" });
    void formOf(request).then(async (form) => {
      if (method === "getMe") return answer(200, { ok: true, result: { id: 1, is_bot: true, username } });
      if (method === "setWebhook") {
        const certificate = form.get("certificate");
        webhooks.set(token, {
          url: String(form.get("url") ?? ""),
          secretToken: String(form.get("secret_token") ?? ""),
          certificate: certificate instanceof Blob ? await certificate.text() : "",
          allowedUpdates: JSON.parse(String(form.get("allowed_updates") ?? "[]")) as string[],
          ipAddress: form.has("ip_address") ? String(form.get("ip_address")) : undefined,
        });
        return answer(200, { ok: true, result: true, description: "Webhook was set" });
      }
      if (method === "deleteWebhook") {
        webhooks.delete(token);
        return answer(200, { ok: true, result: true, description: "Webhook was deleted" });
      }
      if (method === "getWebhookInfo") {
        const lastError = lastErrors.get(token);
        return answer(200, {
          ok: true,
          result: {
            url: webhooks.get(token)?.url ?? "",
            has_custom_certificate: webhooks.has(token),
            pending_update_count: pending.get(token) ?? 0,
            ...(lastError ? { last_error_date: lastError.date, last_error_message: lastError.message } : {}),
          },
        });
      }
      return answer(404, { ok: false, error_code: 404, description: "Not Found" });
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return {
    root: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    bots,
    webhooks,
    lastErrors,
    pending,
    requests,
    close: async () => {
      server.closeAllConnections();
      server.close();
      await once(server, "close");
    },
  };
}
