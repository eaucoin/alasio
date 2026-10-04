/**
 * Telegram's Bot API in memory, as far as alasio's command line asks it: getMe, which
 * answers with the bot of a token it knows, and refuses any other as Telegram does.
 */
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";

export interface FakeTelegram {
  /** Where the API is, as TELEGRAM_API_ROOT says. */
  readonly root: string;
  /** The tokens of the bots it knows, and their usernames. */
  readonly bots: Map<string, string>;
  /** The paths it was asked for, which hold the tokens. */
  readonly requests: string[];
  readonly close: () => Promise<void>;
}

export async function serveFakeTelegram(): Promise<FakeTelegram> {
  const bots = new Map<string, string>();
  const requests: string[] = [];
  const server = createServer((request, response) => {
    requests.push(request.url ?? "");
    const token = /^\/bot(.+)\/getMe$/u.exec(request.url ?? "")?.[1];
    const username = token === undefined ? undefined : bots.get(decodeURIComponent(token));
    response.writeHead(username === undefined ? 401 : 200, { "Content-Type": "application/json" });
    response.end(JSON.stringify(username === undefined ? { ok: false, error_code: 401, description: "Unauthorized" } : { ok: true, result: { id: 1, is_bot: true, username } }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return {
    root: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    bots,
    requests,
    close: async () => {
      server.closeAllConnections();
      server.close();
      await once(server, "close");
    },
  };
}
