/**
 * How a request is passed to the sandbox gateway, as its loopback passes a harness's call to its
 * bayma (../mcp/bayma-proxy.ts) and as alasio's own client of it asks it (./gateway-client.ts):
 * one policy, as both meet the gateway's restarts alike.
 *
 * The gateway not reached at all, its connection never made, as while it starts again, or not
 * made within CONNECT_WITHIN_MS, as one whose SYN nothing answers, a request is passed again
 * every UNREACHED_EVERY_MS, for GATEWAY_RESTART_MS from when it came, where its body is whole:
 * nothing of it reached the gateway, so passing it again does nothing twice. One the gateway
 * answers 503 saying it passed it to no pod (../binder/names.ts NOT_PASSED_HEADER), as it does
 * stopping, or as it answers a bearer held to a binding it has not heard of yet, is passed again
 * after its Retry-After, for RETRY_WITHIN_MS. Any other answer is the request's, as is a failure
 * once its connection was made, as a pod given it may have run it; and one whose body streams,
 * which could not be passed again, is passed once. A gateway whose name resolves to nothing is
 * one not reached too: as the gateway's Service may be while it is made anew, it is asked again,
 * and fails at the bound.
 */
import { type ClientRequest, type IncomingMessage, type OutgoingHttpHeaders, request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import type { Readable } from "node:stream";

import { NOT_PASSED_HEADER } from "../binder/names.ts";

/**
 * How long a request whose connection to the gateway was never made is passed again for, from
 * when it came: past a restart of the gateway's, which the end-to-end test measures (the gateway
 * reached by none of its pods 12 to 25 s, on k3s in Docker and on a host alike), with room; and
 * within a harness's own wait for a call (Claude Code's 60 s), so that one given up is answered
 * the loopback's 502, its call never passed on, before its harness times it out unsure.
 */
export const GATEWAY_RESTART_MS = 45_000;

/** How often a request is passed again while its connection to the gateway is never made. */
const UNREACHED_EVERY_MS = 1_000;

/**
 * How long a connection to the gateway is waited for, at most, before it is taken as never made:
 * one whose SYN nothing answers, as one routed to a pod going away may be, would otherwise wait
 * out the kernel's own retries, minutes, past GATEWAY_RESTART_MS and the harness's wait both. A
 * connection to a gateway that runs is made within milliseconds.
 */
export const CONNECT_WITHIN_MS = 5_000;

/** How long a request answered 503 as passed to no pod, with a Retry-After, is passed again for, from when it came. */
const RETRY_WITHIN_MS = 30_000;

/** How long a request is passed again for while the gateway is not reached, and how long each connection is waited for: GATEWAY_RESTART_MS and CONNECT_WITHIN_MS but for tests. */
export interface GatewayPassing {
  readonly unreachedForMs?: number | undefined;
  readonly connectWithinMs?: number | undefined;
}

/** Where a request goes, as each pass of it asks: its URL, and the headers it goes with, a bearer got as it goes among them. */
export interface GatewayTarget {
  readonly url: URL;
  readonly headers: OutgoingHttpHeaders;
}

/** A request to the gateway as passToGateway passes it. */
export interface GatewayRequest extends GatewayPassing {
  readonly method: string;
  /** Where each pass goes, asked as it goes; its failure the request's. */
  readonly target: () => Promise<GatewayTarget>;
  /** Its body, whole, which may be passed again; or streamed, passed once as it comes. */
  readonly body: Buffer | Readable;
  /** Its answer, the gateway's, not to be passed again: read by the caller from here. */
  readonly answered: (answer: IncomingMessage) => void;
  /** Its failure, the gateway not reached within its bound, or gone once its connection was made. */
  readonly failed: (error: Error) => void;
}

/** Passes `request` to the gateway as the module says: what stops it, its pass under way cut, and none after. */
export function passToGateway({ method, target, body, answered, failed, unreachedForMs = GATEWAY_RESTART_MS, connectWithinMs = CONNECT_WITHIN_MS }: GatewayRequest): () => void {
  const until = Date.now() + RETRY_WITHIN_MS;
  const unreachedUntil = Date.now() + unreachedForMs;
  const whole = Buffer.isBuffer(body);
  let current: ClientRequest | null = null;
  let stopped = false;
  const pass = (): void =>
    void target().then(({ url, headers }) => {
      if (stopped) return;
      const send = url.protocol === "https:" ? httpsRequest : httpRequest;
      // A connection of its own, never one kept from a request before: so whether it was made says
      // whether the gateway may have it, as one kept may have been closed as the request went.
      const sent = send(url, { method, headers, agent: false }, (answer) => {
        const after = Number(answer.headers["retry-after"] ?? 1);
        if (answer.statusCode === 503 && answer.headers[NOT_PASSED_HEADER] !== undefined && whole && Number.isFinite(after) && Date.now() + after * 1000 < until) {
          answer.resume();
          return void setTimeout(() => !stopped && pass(), after * 1000);
        }
        answered(answer);
      });
      current = sent;
      /** Whether its connection to the gateway was made: one that was may have passed it on. */
      let connected = false;
      sent.on("socket", (socket) => {
        if (!socket.connecting) return void (connected = true);
        const waited = Math.max(0, Math.min(connectWithinMs, unreachedUntil - Date.now()));
        const unmade = setTimeout(() => sent.destroy(new Error(`its connection to the gateway was not made within ${waited} ms`)), waited);
        socket.once("connect", () => {
          clearTimeout(unmade);
          connected = true;
        });
        socket.once("close", () => clearTimeout(unmade));
      });
      sent.on("error", (error) => {
        if (stopped) return;
        if (connected || !whole || Date.now() + UNREACHED_EVERY_MS >= unreachedUntil) return failed(error);
        setTimeout(() => !stopped && pass(), UNREACHED_EVERY_MS);
      });
      if (whole) sent.end(body);
      else body.pipe(sent);
    }, (error: unknown) => !stopped && failed(error instanceof Error ? error : new Error(String(error))));
  pass();
  return () => {
    stopped = true;
    current?.destroy();
  };
}
