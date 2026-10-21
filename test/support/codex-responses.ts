/**
 * A stand-in for the Responses API that the real Codex app-server calls in tests, so its turns
 * run with no login and no model, and a Codex home whose one model provider it is.
 */
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";

/** An event the Responses API streams: its type, and what it carries. */
interface ResponsesEvent {
  readonly type: string;
  readonly [field: string]: unknown;
}

/** The stand-in for the Responses API: its URL, the request bodies it was sent, and how to stop it. */
export interface ResponsesStandIn {
  readonly url: string;
  readonly requests: readonly string[];
  close(): Promise<void>;
}

/** Server-sent events as the Responses API streams them. */
function sse(events: readonly ResponsesEvent[]): string {
  return events.map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join("");
}

/** A stand-in for the Responses API that answers the nth request "answer n", and keeps what each asked. */
export async function startResponses(): Promise<ResponsesStandIn> {
  const requests: string[] = [];
  const server = createServer((request, response) => {
    const body: Buffer[] = [];
    request.on("data", (chunk: Buffer) => body.push(chunk));
    request.on("end", () => {
      if (request.method !== "POST" || !request.url?.endsWith("/responses")) {
        response.writeHead(404).end();
        return;
      }
      const raw = Buffer.concat(body);
      const text = request.headers["content-encoding"] === "zstd" ? zstdDecompressSync(raw).toString() : raw.toString();
      requests.push(text);
      const id = `resp_${requests.length}`;
      response.writeHead(200, { "content-type": "text/event-stream" });
      response.end(sse([
        { type: "response.created", response: { id } },
        {
          type: "response.output_item.done",
          item: { type: "message", role: "assistant", id: `msg_${requests.length}`, content: [{ type: "output_text", text: `answer ${requests.length}` }] },
        },
        {
          type: "response.completed",
          response: { id, usage: { input_tokens: 0, input_tokens_details: null, output_tokens: 0, output_tokens_details: null, total_tokens: 0 } },
        },
      ]));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address !== null && typeof address === "object", "a TCP server's address");
  return { url: `http://127.0.0.1:${address.port}/v1`, requests, close: () => new Promise((resolve) => server.close(() => resolve())) };
}

/** A Codex home of its own, whose only provider is the stand-in. */
export function makeCodexHome(root: string, name: string, providerUrl: string): string {
  const home = join(root, name);
  mkdirSync(home, { recursive: true });
  writeFileSync(join(home, "config.toml"), `
model_provider = "stand_in"

[model_providers.stand_in]
name = "stand-in"
base_url = "${providerUrl}"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
`);
  return home;
}

