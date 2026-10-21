/**
 * The harness host as alasio reaches it where its conversations' harnesses run in pools of
 * their own: each conversation's `alasio harness`, bound by the sandbox gateway to a pod of its
 * harness's pool (../gateway/harnesses.ts), reached through the gateway's front
 * (./remote-host.ts), each call bearing a caller's token of the conversation's harness's key
 * (harness:<conversation>), minted as the call is made, of its environment, its harness and
 * its workspace's kind as its mount says now, so a token is never kept past its time, nor past
 * a conversation's switching its harness or its workspace.
 *
 * What only reads what a binding has (its status, its background work, its turn steered or
 * stopped) binds nothing: of a conversation bound nowhere, nothing runs, and a hold of the
 * store's keeps it as alasio's process would say. A reservation binds it ahead (POST /bind), as
 * a turn is about to start, asked again while the front cannot bind it yet, as a pod slow to
 * serve after its node started again is, what it waits on told the reservation's caller, who
 * says it in the turn's status message; then holds it at its host. A turn and a fresh session
 * reach the pod bound then, given the binding's grant (./grant.ts), kept as the conversation's
 * as it is bound, which the pod reaches the conversation's rows by and no other's. Each turn
 * is given its workspace's bayma as alasio brings it up, as
 * the host there has no key to (TurnStart.bayma): a session's, brought up as it is in alasio's
 * process, a branch's inherited session forked first; a folder instance's, bound, what a long
 * bind of it waits on said in the turn's status, its chat told of a bind that failed. Its
 * bearer stops at no time, but is held to the binding of the conversation's harness the bind
 * ahead answers with, good for as long as that is held (../binder/server.ts): the pod's
 * harness, and the work it leaves going on, reach bayma for as long as the pod is bound, with no
 * turn of alasio's to renew it, and alasio asleep.
 *
 * Each Codex turn is given a token for alasio's relay of its Codex login (../codex/login-relay.ts),
 * good for CODEX_LOGIN_TTL_MS, renewed every `renewMs` for as long as the turn runs
 * (HarnessHost.renewCodexLogin), and, a folder conversation's, the operator's Codex
 * configuration, as only alasio holds the login; and alasio, the one refresher of the login,
 * keeps it fresh before the turn and as it renews its token (../codex/login-refresh.ts), by its
 * own app-server. alasio's own pod is held busy for as long as it drives a turn (../kube/busy.ts),
 * as where the turn runs in its process, since it is alasio that takes the turn's answer to its
 * chat. The operator's panels are alasio's (./panels.ts): a fork of a session, and a thread's
 * goals, of the conversation's pod, bound ahead for them (forkSession, goals).
 */
import { join } from "node:path";

import { Clock, Effect, Layer, Option, Result, Schedule, Schema } from "effect";

import { type HeldTo, mintToken } from "../binder/tokens.ts";
import { CodexAppServer } from "../codex/app-server/client.ts";
import { codexConfiguration } from "../codex/configuration.ts";
import { buildCodexEnv } from "../codex/env.ts";
import { makeLoginFreshness } from "../codex/login-refresh.ts";
import { poolLoginKey } from "../codex/login-relay.ts";
import { harnessKey } from "../gateway/names.ts";
import { BusyPods } from "../kube/busy.ts";
import { HostBayma } from "../mcp/bayma.ts";
import { Store } from "../persistence/store.ts";
import { BIND_WAIT_MS, TOKEN_TTL_MS } from "../sandbox/gateway-client.ts";
import { SessionSandboxes } from "../sandbox/index.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import { type ConversationHarness, HarnessHost, type TurnBayma, type TurnCodex, type TurnStart } from "./host.ts";
import { HarnessHostError } from "./index.ts";
import { grantDigest, grantKey, harnessGrant } from "./grant.ts";
import { CODEX_HARNESS, type HarnessName } from "./names.ts";
import { remoteHost } from "./remote-host.ts";

/** How often a harness the front cannot bind yet is asked to be bound again. */
const BIND_RETRY = "2 seconds";
/** What a turn says it waits on while its conversation's harness is being bound. */
export const HARNESS_BINDING = "its harness, whose pod is being readied";
/** How long a Codex turn's token for alasio's relay of its login is good for: minutes, so one taken from a pod is of little use. */
const CODEX_LOGIN_TTL_MS = 10 * 60_000;
/** How often a running Codex turn's token is renewed, by default: well within its time. */
const CODEX_LOGIN_RENEW_MS = 3 * 60_000;
/** How often a call of one binding is made again of the binding the conversation's harness was bound to anew instead, at most: its pod gone once, and once more as it was. */
const MOVED_RETRIES = 2;

/** What the front says as it refuses a bind: why, and whether it holds the key, bound again only as its caller asks. */
const BindRefusal = Schema.Struct({ error: Schema.String, reason: Schema.optional(Schema.String), held: Schema.optional(Schema.Boolean) });

/** What the front answers a bind with: the binding, its Lease's uid, and the namespace that is in. */
const Bound = Schema.Struct({ binding: Schema.String, namespace: Schema.String });

/** The front cannot bind the conversation's harness yet, saying why. */
class BindNotYet extends Schema.TaggedError<BindNotYet>()("BindNotYet", { reason: Schema.String }) {}

/**
 * The front alasio reaches its pools' hosts through: its address, and alasio's environment's
 * name and its own key of the gateway's caller key; where its Codex login is relayed to its
 * pools' pods (../codex/login-relay.ts), its Codex home, whose login it keeps fresh and whose
 * configuration a folder conversation's Codex is given, and how often a running Codex turn's
 * token is renewed (CODEX_LOGIN_RENEW_MS unless a test says).
 */
export interface PooledHostOptions {
  readonly url: string;
  readonly environment: string;
  readonly callerKey: Buffer;
  readonly codexLogin: { readonly url: string; readonly home: string; readonly renewMs?: number };
  /** Tells the lake's loader of what a pod committed, as each turn of its ends, the pod telling it nothing itself; nothing where none is given. */
  readonly written?: (() => void) | undefined;
}

/** The host of each conversation, bound in its harness's pool, as the module says. */
export const pooledHarnessHost = ({ url, environment, callerKey, codexLogin, written = () => {} }: PooledHostOptions): Layer.Layer<HarnessHost, never, Store | CodexAppServer> =>
  Layer.effect(
    HarnessHost,
    Effect.gen(function*() {
      const store = yield* Store;
      const appServer = yield* CodexAppServer;
      const sandboxes = Option.getOrNull(yield* Effect.serviceOption(SessionSandboxes));
      const hostBayma = Option.getOrNull(yield* Effect.serviceOption(HostBayma));
      const busyPods = Option.getOrNull(yield* Effect.serviceOption(BusyPods));

      /** A caller's token of the conversation's harness's key, placed by `harness` and `workingDirectory`'s kind, good for TOKEN_TTL_MS from now. */
      const tokenOf = (conversationId: string, harness: HarnessName, workingDirectory: string) =>
        Effect.map(Clock.currentTimeMillis, (now) =>
          mintToken(callerKey, {
            key: harnessKey(conversationId),
            expires: now + TOKEN_TTL_MS,
            options: { environment, harness, workspace: parseWorkspace(workingDirectory)?.kind === "sessionfs" ? "session" : "folder" },
          }));
      /** The binding of each conversation's harness its reservation bound ahead, while it is reserved: what its turn's bayma is held to. */
      const reserved = new Map<string, HeldTo>();
      /** A conversation bound nowhere: nothing runs in it, since ever, and a hold of the store's keeps it as it would anywhere. */
      const unbound = (conversationId: string) =>
        Effect.map(store.isConversationHeld(conversationId), (held) => ({ busy: held, running: false, cliInitiated: false, held, idleSince: 0 }));
      /** The conversation's host, its calls bearing `token`, and, where the caller bound one, for that binding alone, giving its grant. */
      const hostBy = (token: Effect.Effect<string, HarnessHostError>, bound?: { readonly binding: string; readonly grant: string }) => remoteHost({ url, token, unbound, bound });
      const grantsKey = grantKey(callerKey);
      /** The grant of the binding `held` of the conversation's harness, kept as the conversation's, as the module says. */
      const grantOf = (conversationId: string, held: HeldTo): string => harnessGrant(grantsKey, conversationId, held.binding);
      const granted = (conversationId: string, held: HeldTo): Effect.Effect<string, HarnessHostError> => {
        const grant = grantOf(conversationId, held);
        return Effect.as(store.issueHarnessGrant(conversationId, held.binding, grantDigest(grant)), grant).pipe(
          Effect.mapError((error) => new HarnessHostError({ reason: `its harness's grant could not be kept: ${error.message}` })),
        );
      };
      /**
       * `call`, of the binding `first` of the conversation's harness alone, its grant kept as the
       * conversation's first; made again of the binding the front says it is bound to instead,
       * where it was bound anew meanwhile, as a pod gone is, that one's grant kept and given in
       * its place, MOVED_RETRIES times at most, so no pod is ever given another binding's grant:
       * what it answered, and the binding it answered of.
       */
      const onBinding = <A, E, R>(
        conversationId: string,
        first: HeldTo,
        call: (held: HeldTo, bound: { readonly binding: string; readonly grant: string }) => Effect.Effect<A, E | HarnessHostError, R>,
      ): Effect.Effect<readonly [A, HeldTo], E | HarnessHostError, R> =>
        Effect.gen(function*() {
          let held = first;
          for (let moves = 0; ; moves += 1) {
            const grant = yield* granted(conversationId, held);
            const answered = yield* Effect.result(call(held, { binding: held.binding, grant }));
            if (Result.isSuccess(answered)) return [answered.success, held] as const;
            const moved = answered.failure instanceof HarnessHostError ? answered.failure.moved : undefined;
            if (!moved || moves >= MOVED_RETRIES) return yield* Effect.fail(answered.failure);
            yield* Effect.logInfo(`${conversationId}'s harness was bound anew, from ${held.binding} to ${moved.binding}: its call is made again of that one`);
            held = { ...held, ...moved };
          }
        });
      /**
       * `call` of the conversation's host, as its mount places it now, given its token's making;
       * or, where it has no harness in a workspace, so that nothing is bound for it, what `none`
       * says.
       */
      const onHost = <A, E, R>(conversationId: string, call: (host: HarnessHost["Service"], token: Effect.Effect<string>) => Effect.Effect<A, E, R>, none: Effect.Effect<A, E, R>) =>
        Effect.flatMap(store.getMount(conversationId), ({ harness, workingDirectory }) => {
          if (!harness || !workingDirectory) return none;
          const token = tokenOf(conversationId, harness, workingDirectory);
          return call(hostBy(token), token);
        });

      /** What a long bind of the turn `start`'s waits on, said in its status, as the harness in alasio's process says it. */
      const waitingOf = (start: TurnStart) => (what: string | null) =>
        store.turnPersistence(start.conversationId, start.epoch).setWaiting(what).pipe(
          Effect.catch((error) => Effect.logWarning(`could not say thread=${start.conversationId} waits on ${what ?? "nothing"}: ${error.message}`)),
        );

      /**
       * The conversation's harness bound ahead (POST /bind) with the caller's token `token` makes,
       * asked again every BIND_RETRY while the front cannot bind it yet (503), as its pool has no
       * pod serving for it in time, until BIND_WAIT_MS, what it waits on told `waiting` meanwhile,
       * and nothing once bound: its binding, which its turns' bayma is held to; one the front
       * holds, its binds having failed one after another, is not asked again, as it is bound again
       * only as its caller asks.
       */
      const boundAhead = (conversationId: string, token: Effect.Effect<string>, waiting: (what: string | null) => Effect.Effect<void>): Effect.Effect<HeldTo, HarnessHostError> => {
        let waited = false;
        const once = Effect.flatMap(token, (bearer) =>
          Effect.tryPromise({
            try: async (signal) => {
              const answer = await fetch(`${url}/bind`, { method: "POST", headers: { authorization: `Bearer ${bearer}` }, signal });
              return { status: answer.status, said: await answer.text() };
            },
            catch: (cause) => new HarnessHostError({ reason: cause instanceof Error ? cause.message : String(cause) }),
          })).pipe(
            Effect.flatMap(({ status, said }): Effect.Effect<HeldTo, HarnessHostError | BindNotYet> => {
              if (status === 200) {
                return Schema.decodeUnknownEffect(Schema.fromJsonString(Bound))(said).pipe(
                  Effect.map(({ binding, namespace }) => ({ key: harnessKey(conversationId), binding, namespace })),
                  Effect.mapError((error) => new HarnessHostError({ reason: `the sandbox gateway's bind answered no binding: ${error.message}` })),
                );
              }
              const refusal = Option.getOrNull(Schema.decodeUnknownOption(Schema.fromJsonString(BindRefusal))(said));
              const why = `the sandbox gateway answered ${status}: ${refusal ? `${refusal.error}${refusal.reason ? ` (${refusal.reason})` : ""}` : said}`;
              if (status === 503 && refusal?.held !== true) {
                return Effect.andThen(waited ? Effect.void : Effect.andThen(Effect.sync(() => void (waited = true)), waiting(HARNESS_BINDING)), Effect.fail(new BindNotYet({ reason: why })));
              }
              return Effect.fail(new HarnessHostError({ reason: why }));
            }),
          );
        return once.pipe(
          Effect.retry({ while: (error) => error._tag === "BindNotYet", schedule: Schedule.max([Schedule.spaced(BIND_RETRY), Schedule.during(BIND_WAIT_MS)]) }),
          Effect.catchTag("BindNotYet", (error) => Effect.fail(new HarnessHostError({ reason: error.reason }))),
          Effect.ensuring(Effect.suspend(() => (waited ? waiting(null) : Effect.void))),
        );
      };

      /**
       * `call` of the conversation's host as `of` places it, its harness bound ahead first and the
       * call for that binding alone, giving its grant (onBinding): what it answered.
       */
      const onBound = <A, E>(of: ConversationHarness, call: (host: HarnessHost["Service"]) => Effect.Effect<A, E | HarnessHostError>): Effect.Effect<A, E | HarnessHostError> =>
        Effect.gen(function*() {
          const token = tokenOf(of.conversationId, of.harness, of.workingDirectory);
          const held = yield* boundAhead(of.conversationId, token, () => Effect.void);
          const [answered] = yield* onBinding(of.conversationId, held, (_held, bound) => call(hostBy(token, bound)));
          return answered;
        });

      /** The bayma of the turn `start`'s workspace, brought up for it, its bearer held to the binding `held` of the conversation's harness, as the module says. */
      const baymaOf = (start: TurnStart, held: HeldTo): Effect.Effect<TurnBayma> =>
        Effect.gen(function*() {
          const workspace = parseWorkspace(start.workingDirectory);
          if (workspace?.kind === "sessionfs") {
            if (!sandboxes) return { endpoint: null, notice: "This deployment offers no session filesystems, so this turn runs without bayma's tools." };
            return yield* sandboxes.ensureSession(workspace.volumeId, held).pipe(
              Effect.map(({ bayma }): TurnBayma => ({ endpoint: bayma, notice: null })),
              Effect.catch((error) => Effect.succeed<TurnBayma>({ endpoint: null, notice: `Session ${workspace.volumeId}: its bayma did not start (${error.message}). This turn runs without bayma's tools.` })),
            );
          }
          if (!hostBayma) return { endpoint: null, notice: "This deployment offers no folder workspaces' bayma, so this turn runs without bayma's REPL tools." };
          const outcome = yield* hostBayma.ensure({ harness: start.harness, threadKey: start.conversationId, workingDirectory: start.workingDirectory, chatId: start.chatId, waiting: waitingOf(start), held });
          return outcome.server ? { endpoint: { url: outcome.server.url, headers: outcome.server.headers }, notice: null } : { endpoint: null, notice: outcome.notice };
        });

      const loginKey = poolLoginKey(callerKey);
      // Refreshed by alasio's own app-server, as Codex refreshes it, where its access token is about to stop; said, and the turn run on what there is, where it cannot be.
      const loginFresh = (yield* makeLoginFreshness(join(codexLogin.home, "auth.json"), appServer.refreshLogin({ env: buildCodexEnv(), cwd: codexLogin.home }))).pipe(
        Effect.catch((error) => Effect.logWarning(`the Codex login could not be refreshed: ${error.message}`)),
      );
      /** A token of the conversation's Codex turn for alasio's relay of its login, good for CODEX_LOGIN_TTL_MS, the login kept fresh first. */
      const loginOf = (conversationId: string): Effect.Effect<TurnCodex["login"]> =>
        Effect.andThen(loginFresh, Effect.map(Clock.currentTimeMillis, (now) => ({ url: codexLogin.url, token: mintToken(loginKey, { key: harnessKey(conversationId), expires: now + CODEX_LOGIN_TTL_MS }) })));
      /** What a Codex turn of `start`'s is given of its login, a token of the turn's for alasio's relay, and, a folder conversation's, the operator's Codex configuration; none for another harness's. */
      const codexOf = (start: TurnStart): Effect.Effect<TurnCodex | null> =>
        Effect.gen(function*() {
          if (start.harness !== CODEX_HARNESS) return null;
          const login = yield* loginOf(start.conversationId);
          const home = parseWorkspace(start.workingDirectory)?.kind === "sessionfs" ? null : yield* codexConfiguration(codexLogin.home);
          return { login, home };
        });
      /** The token of the Codex turn `start`'s renewed on `host` every renewMs, for as long as it runs, as the module says. */
      const renewing = (host: HarnessHost["Service"], start: TurnStart): Effect.Effect<void> =>
        Effect.flatMap(loginOf(start.conversationId), (login) => host.renewCodexLogin(start.conversationId, login)).pipe(
          Effect.catch((error) => Effect.logWarning(`thread=${start.conversationId}'s Codex login could not be renewed: ${error.message}`)),
          Effect.delay(codexLogin.renewMs ?? CODEX_LOGIN_RENEW_MS),
          Effect.forever,
        );

      return HarnessHost.of({
        // The binding it bound ahead kept for as long as it is reserved, which its turn's bayma is
        // held to: by a reservation the host made alone, and let go of by that one alone, so a
        // reservation refused, as another holds the conversation, leaves the other's as it is.
        reserve: (conversationId, waiting = () => Effect.void) =>
          onHost(conversationId, (_host, token) =>
            Effect.gen(function*() {
              const [made, held] = yield* onBinding(conversationId, yield* boundAhead(conversationId, token, waiting), (_held, bound) => hostBy(token, bound).reserve(conversationId));
              if (made) {
                yield* Effect.acquireRelease(Effect.sync(() => void reserved.set(conversationId, held)), () =>
                  Effect.sync(() => {
                    if (reserved.get(conversationId) === held) reserved.delete(conversationId);
                  }));
              }
              return made;
            }), Effect.succeed(false)),
        startTurn: (start) =>
          Effect.gen(function*() {
            if (busyPods) yield* busyPods.hold;
            const reservedTo = reserved.get(start.conversationId);
            if (!reservedTo) return yield* new HarnessHostError({ reason: `a turn of ${start.conversationId} was started with its harness reserved for none` });
            const codex = yield* codexOf(start);
            const token = tokenOf(start.conversationId, start.harness, start.workingDirectory);
            // Of the binding reserved, or the one its harness is bound to anew, its pod gone: its bayma held to that one, and its grant given.
            const [result] = yield* onBinding(start.conversationId, reservedTo, (held, bound) =>
              Effect.gen(function*() {
                const bayma = yield* baymaOf(start, held);
                const host = hostBy(token, bound);
                if (codex) yield* Effect.forkScoped(renewing(host, start));
                return yield* host.startTurn({ ...start, bayma, codex });
              }).pipe(Effect.scoped)).pipe(
                // What the turn committed, the lake's to load once it ends, however it ends.
                Effect.ensuring(Effect.sync(written)),
              );
            return result;
          }).pipe(Effect.scoped),
        // Of the binding the conversation's harness has now, binding none: a turn of one let go of, its pod gone, is known nowhere.
        attach: (turn) => onHost(turn.conversationId, (host) => host.attach(turn), Effect.succeedNone),
        steer: (conversationId, prompt, fileIds) => onHost(conversationId, (host) => host.steer(conversationId, prompt, fileIds), Effect.succeedNone),
        interrupt: (conversationId, reason) =>
          onHost(conversationId, (host) => host.interrupt(conversationId, reason), Effect.succeed(false)).pipe(
            Effect.catchTag("StoreError", (error) => Effect.fail(new HarnessHostError({ reason: error.message }))),
          ),
        status: (conversationId) => onHost(conversationId, (host) => host.status(conversationId), unbound(conversationId)),
        backgroundWork: (conversationId) => onHost(conversationId, (host) => host.backgroundWork(conversationId), Effect.succeed(false)),
        drain: (conversationId) => onHost(conversationId, (host) => host.drain(conversationId), Effect.succeed(true)),
        renewCodexLogin: (conversationId, login) =>
          onHost(conversationId, (host) => host.renewCodexLogin(conversationId, login), Effect.void).pipe(
            Effect.catchTag("StoreError", (error) => Effect.fail(new HarnessHostError({ reason: error.message }))),
          ),
        // Bound ahead first, as a reservation is, so the pod is given its grant, as what its harness makes of a fresh session it may keep at once.
        startFreshSession: (params) => onBound(params, (host) => host.startFreshSession(params)),
        // Of the conversation's pod, bound for it, as only its harness holds its sessions' app-server state.
        forkSession: (params) => onBound(params, (host) => host.forkSession(params)),
        goals: (of) => ({
          read: (params) => onBound(of, (host) => host.goals(of).read(params)),
          set: (params) => onBound(of, (host) => host.goals(of).set(params)),
          clear: (params) => onBound(of, (host) => host.goals(of).clear(params)),
          waitForTurnId: (threadId, timeoutMs) => onBound(of, (host) => host.goals(of).waitForTurnId(threadId, timeoutMs)),
        }),
      });
    }),
  );
