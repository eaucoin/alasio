/**
 * The binder's reverse proxy: a request passed to the pod its key is bound to, as it comes
 * and as it is answered, streamed both ways with nothing buffered and no time limit of its
 * own, as MCP's Streamable HTTP holds a POST open for as long as a call runs and a GET open
 * for its events. The caller's credentials stay here: the pod is sent the binding's token in
 * their place. A caller that goes away ends the request to the pod, and a pod that goes away
 * ends the caller's.
 */
import { type IncomingHttpHeaders, type IncomingMessage, type OutgoingHttpHeaders, request, type ServerResponse } from "node:http";

/** Where a key's requests go: its pod's address and port, and the binding's token the pod takes them with. */
export interface Route {
  readonly address: string;
  readonly port: number;
  readonly token: string;
}

/**
 * What a proxied request came to: answered by the pod (its status), or not reached (why), and
 * whether its connection to the pod was made first (`connected`), as one reused is, so the pod
 * may have had some of it, or none, so it had nothing of it; and the error's code.
 */
export type Proxied =
  | { readonly reached: true; readonly status: number }
  | { readonly reached: false; readonly error: string; readonly connected: boolean; readonly code: string | undefined };

// Hop-by-hop headers (RFC 9110 7.6.1), each connection's own, and the caller's credentials.
const HOP_BY_HOP = new Set(["connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "authorization"]);

/** `headers` without those of one hop alone, nor those the Connection header names. */
function endToEnd(headers: IncomingHttpHeaders): OutgoingHttpHeaders {
  const named = new Set(String(headers.connection ?? "").toLowerCase().split(",").map((name) => name.trim()).filter(Boolean));
  return Object.fromEntries(Object.entries(headers).filter(([name]) => !HOP_BY_HOP.has(name) && !named.has(name)));
}

/**
 * Passes `incoming` to `route` and its answer to `outgoing`, as the module says: its body as
 * it comes, or `body`, where the caller read it first, so it may be passed again. Where the
 * pod is not reached, nothing is answered: the caller decides what to answer (as binding the
 * key anew), from what it came to.
 */
export function proxy(incoming: IncomingMessage, outgoing: ServerResponse, route: Route, body?: Uint8Array): Promise<Proxied> {
  return new Promise((resolve) => {
    const upstream = request({
      host: route.address,
      port: route.port,
      method: incoming.method,
      path: incoming.url,
      headers: { ...endToEnd(incoming.headers), authorization: `Bearer ${route.token}`, "x-forwarded-for": incoming.socket.remoteAddress ?? "" },
    });
    let answered = false;
    // Whether its connection was made: at once, where it reuses one, as Node's agent keeps them.
    let connected = false;
    upstream.on("socket", (socket) => {
      if (!socket.connecting) connected = true;
      else socket.once("connect", () => void (connected = true));
    });
    upstream.on("response", (response) => {
      answered = true;
      outgoing.writeHead(response.statusCode ?? 502, endToEnd(response.headers));
      outgoing.flushHeaders();
      response.pipe(outgoing);
      response.on("error", () => outgoing.destroy());
      response.on("end", () => resolve({ reached: true, status: response.statusCode ?? 502 }));
      // The pod gone mid-answer: the caller's request ends with it.
      response.on("aborted", () => {
        outgoing.destroy();
        resolve({ reached: true, status: response.statusCode ?? 502 });
      });
    });
    upstream.on("error", (error) => {
      if (answered) outgoing.destroy();
      else resolve({ reached: false, error: error.message, connected, code: (error as NodeJS.ErrnoException).code });
    });
    // The caller gone: so is its request to the pod.
    outgoing.on("close", () => {
      if (!outgoing.writableFinished) upstream.destroy();
    });
    if (body === undefined) incoming.pipe(upstream);
    else upstream.end(body);
  });
}
