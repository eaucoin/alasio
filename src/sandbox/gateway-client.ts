/**
 * alasio's client of the sandbox gateway (../gateway/sessions.ts, ../binder/server.ts), which
 * holds its environment's session workspaces: each reached by a caller's token alasio mints
 * itself, of the workspace's key (`sessionfs:<volume id>`), naming its environment and its
 * internet, as the gateway places it, signed by its environment's own key of the gateway's
 * caller key (../binder/names.ts environmentKey), which the gateway checks by the environment
 * the token names. alasio holds no binding: the gateway binds the workspace to a pod of its
 * pool as it is reached, and lets it go once it is idle.
 *
 * - `endpoint`: bayma's, the gateway's /mcp, which it passes to the bound pod, binding it first,
 *   with a caller's token good for TOKEN_TTL_MS.
 * - `bind`: binds the workspace ahead of any request (POST /bind), asked again every few seconds
 *   while the gateway's pool has no pod for it (503), until BIND_WAIT_MS.
 * - `release`: lets its binding go (POST /release), saved whole first where it is snapshotted.
 * - `create`, `remove`, `quota`, `list`, `readFile`, `writeFile`, `fork`: the workspace itself,
 *   as the gateway answers them, through its node agents (POST and DELETE /workspace,
 *   GET /workspace/quota, GET /workspace/all, GET and PUT /workspace/files, POST /workspace/forks).
 *
 * What the gateway refuses is a GatewayError with its status and what it said.
 */
import { Clock, Duration, Effect, Schedule, Schema } from "effect";

import { mintToken } from "../binder/tokens.ts";
import type { BaymaEndpoint } from "../kube/sandboxes.ts";
import { sessionKey } from "./names.ts";

/** A session's internet, as its pool is chosen by it. */
export type NetMode = "none" | "full";

/** How long a caller's token alasio mints is good for: past any turn, as a turn's bayma is reached by one minted as it starts. */
export const TOKEN_TTL_MS = 6 * 60 * 60_000;
/** How long a bind asked ahead is asked again for, while the gateway's pool has no pod for it. */
export const BIND_WAIT_MS = 5 * 60_000;
/** How long a call of the gateway's may take, in ms, but a bind's and a release's, which wait for a pod and a save. */
const CALL_TIMEOUT_MS = 60_000;
/** How long a bind or a release may take, in ms: a restore's, or a save's, with room. */
const LONG_CALL_TIMEOUT_MS = 10 * 60_000;

/** What the gateway refused, or what failed reaching it. */
export class GatewayError extends Schema.TaggedError<GatewayError>()("GatewayError", {
  call: Schema.String,
  status: Schema.optional(Schema.Number),
  reason: Schema.String,
}) {
  override get message(): string {
    return `the sandbox gateway did not ${this.call}${this.status === undefined ? "" : ` (${this.status})`}: ${this.reason}`;
  }
}

/** A bind refused as its pool had no pod for it in time, to be asked again after what it said. */
class NotYet extends Schema.TaggedError<NotYet>()("NotYet", { reason: Schema.String }) {}

/** A workspace's quota, as the gateway reads it back. */
export interface Quota {
  readonly capacityBytes: number;
  readonly usedBytes: number;
}

/** The gateway's session workspaces, as the module says. */
export interface GatewayWorkspaces {
  readonly endpoint: (volumeId: string, mode: NetMode) => Effect.Effect<BaymaEndpoint>;
  readonly bind: (volumeId: string, mode: NetMode) => Effect.Effect<void, GatewayError>;
  readonly release: (volumeId: string, mode: NetMode) => Effect.Effect<void, GatewayError>;
  /** Makes the workspace, of the gateway's quota; again, its quota set anew. */
  readonly create: (volumeId: string) => Effect.Effect<void, GatewayError>;
  /** Deletes the workspace, its snapshots with it, as it does one there is not; refused, 409, while it is bound. */
  readonly remove: (volumeId: string) => Effect.Effect<void, GatewayError>;
  /** Its quota, or null where it has none, as it has none where it is not there. */
  readonly quota: (volumeId: string) => Effect.Effect<Quota | null, GatewayError>;
  /** The names of the environment's workspaces there are. */
  readonly list: Effect.Effect<readonly string[], GatewayError>;
  /** Its file at `path`, `workspace/...` or `home/...`, of `maxBytes` at most. */
  readonly readFile: (volumeId: string, path: string, maxBytes: number) => Effect.Effect<Uint8Array, GatewayError>;
  /** Writes `bytes` into its file at `path`, `workspace/...` or `home/...`, its directories made. */
  readonly writeFile: (volumeId: string, path: string, bytes: Uint8Array) => Effect.Effect<void, GatewayError>;
  /** Makes `into`, a workspace of `environment`, a clone of it, given its snapshot where it was released into one. */
  readonly fork: (volumeId: string, into: { readonly environment: string; readonly workspace: string }) => Effect.Effect<void, GatewayError>;
}

/** What makeGatewayWorkspaces is given: the gateway's address, alasio's environment's name, and its own key of the gateway's caller key. */
export interface GatewayWorkspacesOptions {
  readonly url: string;
  readonly environment: string;
  readonly callerKey: Buffer;
}

const QuotaAnswer = Schema.Struct({ capacityBytes: Schema.Number, usedBytes: Schema.Number });
const AllAnswer = Schema.Struct({ workspaces: Schema.Array(Schema.String) });

/** The gateway's session workspaces, as the module says, by alasio's environment's key. */
export function makeGatewayWorkspaces({ url, environment, callerKey }: GatewayWorkspacesOptions): GatewayWorkspaces {
  const base = url.replace(/\/+$/u, "");
  /** A caller's token of `volumeId`'s key, good for TOKEN_TTL_MS from now, its internet `mode`. */
  const tokenOf = (volumeId: string, mode: NetMode) =>
    Effect.map(Clock.currentTimeMillis, (now) => mintToken(callerKey, { key: sessionKey(volumeId), expires: now + TOKEN_TTL_MS, options: { environment, mode } }));

  /** Asks the gateway `method path` of `volumeId`'s, with `body`: its status and its body. */
  const ask = (what: string, volumeId: string, mode: NetMode, method: string, path: string, { body, timeoutMs = CALL_TIMEOUT_MS }: { readonly body?: Uint8Array | string; readonly timeoutMs?: number } = {}) =>
    Effect.gen(function*() {
      const token = yield* tokenOf(volumeId, mode);
      return yield* Effect.tryPromise({
        try: async (signal) => {
          const response = await fetch(`${base}${path}`, {
            method,
            headers: { authorization: `Bearer ${token}`, ...(typeof body === "string" ? { "content-type": "application/json" } : {}) },
            ...(body === undefined ? {} : { body: typeof body === "string" ? body : Buffer.from(body) }),
            signal,
          });
          const bytes = new Uint8Array(await response.arrayBuffer());
          return { status: response.status, bytes };
        },
        catch: (error) => new GatewayError({ call: what, reason: error instanceof Error ? error.message : String(error) }),
      }).pipe(Effect.timeoutOrElse({ duration: Duration.millis(timeoutMs), orElse: () => Effect.fail(new GatewayError({ call: what, reason: `it did not answer within ${timeoutMs / 1000} s` })) }));
    });

  /** What the gateway said of a refusal: its `error`, where it said one as JSON, else its text. */
  const said = (bytes: Uint8Array): string => {
    const text = Buffer.from(bytes).toString("utf8");
    try {
      const parsed = JSON.parse(text) as { readonly error?: unknown };
      return typeof parsed.error === "string" ? parsed.error : text;
    } catch {
      return text;
    }
  };
  /** `answer`, a failure where its status is not `ok`. */
  const expect = (what: string, ok: readonly number[]) => (answer: { readonly status: number; readonly bytes: Uint8Array }) =>
    ok.includes(answer.status) ? Effect.succeed(answer) : Effect.fail(new GatewayError({ call: what, status: answer.status, reason: said(answer.bytes) }));
  const json = <A, I>(what: string, schema: Schema.Codec<A, I>) => (bytes: Uint8Array) =>
    Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(Buffer.from(bytes).toString("utf8")).pipe(
      Effect.mapError((error) => new GatewayError({ call: what, reason: `its answer is not one: ${error.message}` })),
    );

  return {
    endpoint: (volumeId, mode) => Effect.map(tokenOf(volumeId, mode), (token) => ({ url: `${base}/mcp`, headers: { Authorization: `Bearer ${token}` } })),
    bind: (volumeId, mode) => {
      const what = `bind ${volumeId}`;
      return ask(what, volumeId, mode, "POST", "/bind", { timeoutMs: LONG_CALL_TIMEOUT_MS }).pipe(
        Effect.flatMap((answer): Effect.Effect<void, GatewayError | NotYet> => {
          if (answer.status === 204) return Effect.void;
          if (answer.status === 503) return Effect.fail(new NotYet({ reason: said(answer.bytes) }));
          return Effect.fail(new GatewayError({ call: what, status: answer.status, reason: said(answer.bytes) }));
        }),
        Effect.retry({
          while: (error) => error._tag === "NotYet",
          schedule: Schedule.max([Schedule.spaced("2 seconds"), Schedule.during(BIND_WAIT_MS)]),
        }),
        Effect.catchTag("NotYet", (error) => Effect.fail(new GatewayError({ call: what, status: 503, reason: error.reason }))),
      );
    },
    release: (volumeId, mode) => {
      const what = `release ${volumeId}`;
      return ask(what, volumeId, mode, "POST", "/release", { timeoutMs: LONG_CALL_TIMEOUT_MS }).pipe(Effect.flatMap(expect(what, [204])), Effect.asVoid);
    },
    create: (volumeId) => {
      const what = `make the workspace ${volumeId}`;
      return ask(what, volumeId, "none", "POST", "/workspace").pipe(Effect.flatMap(expect(what, [204])), Effect.asVoid);
    },
    remove: (volumeId) => {
      const what = `delete the workspace ${volumeId}`;
      return ask(what, volumeId, "none", "DELETE", "/workspace").pipe(Effect.flatMap(expect(what, [204])), Effect.asVoid);
    },
    quota: (volumeId) => {
      const what = `read the quota of ${volumeId}`;
      return ask(what, volumeId, "none", "GET", "/workspace/quota").pipe(
        Effect.flatMap(expect(what, [200, 404])),
        Effect.flatMap(({ status, bytes }) => (status === 404 ? Effect.succeed(null) : json(what, QuotaAnswer)(bytes))),
      );
    },
    // Of any workspace's key: the environment's own, listed whole.
    list: Effect.suspend(() => {
      const what = "list the workspaces";
      return ask(what, "list", "none", "GET", "/workspace/all").pipe(
        Effect.flatMap(expect(what, [200])),
        Effect.flatMap(({ bytes }) => json(what, AllAnswer)(bytes)),
        Effect.map(({ workspaces }) => workspaces),
      );
    }),
    readFile: (volumeId, path, maxBytes) => {
      const what = `read ${path} of ${volumeId}`;
      return ask(what, volumeId, "none", "GET", `/workspace/files?${new URLSearchParams({ path, max: String(maxBytes) })}`).pipe(
        Effect.flatMap(expect(what, [200])),
        Effect.map(({ bytes }) => bytes),
      );
    },
    writeFile: (volumeId, path, bytes) => {
      const what = `write ${path} of ${volumeId}`;
      return ask(what, volumeId, "none", "PUT", `/workspace/files?${new URLSearchParams({ path })}`, { body: bytes }).pipe(Effect.flatMap(expect(what, [204])), Effect.asVoid);
    },
    fork: (volumeId, into) => {
      const what = `fork ${volumeId} into ${into.environment}'s ${into.workspace}`;
      return ask(what, volumeId, "none", "POST", "/workspace/forks", { body: JSON.stringify(into) }).pipe(Effect.flatMap(expect(what, [204])), Effect.asVoid);
    },
  };
}
