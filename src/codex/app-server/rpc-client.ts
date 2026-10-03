import type { ClientNotification, RequestId, ServerRequest } from "../../../.types/codex/index.js";
import type { Logger } from "../../shared/log.ts";
import { rpcCall, type TraceCarrier, traceCarrier } from "../../telemetry/index.ts";
import type { CodexEnv } from "../env.ts";
import { type AppServerProcess, elapsedMs, type SpawnAppServer, startAppServerProcess } from "./process.ts";
import {
  type AppServerMethod,
  type AppServerNotification,
  type AppServerParams,
  type AppServerResult,
  serverRequestResponse,
} from "./protocol.ts";

const REQUEST_TIMEOUT_MS = 5 * 60 * 1000;
const START_TIMEOUT_MS = 20 * 1000;

/** Where the app-server process runs: its environment and working directory. */
export interface AppServerScope {
  readonly env: CodexEnv;
  readonly cwd: string;
}

export interface AppServerRpcClientOptions {
  readonly log: Logger;
  readonly onNotification: (message: AppServerNotification) => void;
  readonly onFailure: (error: Error) => void;
  readonly spawnProcess?: SpawnAppServer | undefined;
}

/** A JSON-RPC message the app-server writes, before it is known which kind it is. */
interface IncomingMessage {
  readonly id?: RequestId | null;
  readonly method?: string;
  readonly params?: unknown;
  readonly result?: unknown;
  readonly error?: { readonly message?: string };
}

/** A JSON-RPC message alasio writes: a request, a notification, or an answer to the app-server's request. */
interface OutgoingMessage {
  readonly id?: RequestId;
  readonly method?: string;
  readonly params?: unknown;
  readonly result?: unknown;
  readonly trace?: TraceCarrier;
}

/** A request awaiting the app-server's response. */
interface PendingRequest {
  readonly resolve: (result: unknown) => void;
  readonly reject: (error: Error) => void;
  readonly timer: NodeJS.Timeout;
}

export class AppServerRpcClient {
  private readonly log: Logger;
  private readonly onNotification: (message: AppServerNotification) => void;
  private readonly onFailure: (error: Error) => void;
  private readonly spawnProcess: SpawnAppServer;
  process: AppServerProcess | null;
  private nextId: number;
  private readonly pending: Map<RequestId, PendingRequest>;
  private initialized: boolean;
  private startPromise: Promise<void> | null;

  constructor({ log, onNotification, onFailure, spawnProcess = startAppServerProcess }: AppServerRpcClientOptions) {
    this.log = log;
    this.onNotification = onNotification;
    this.onFailure = onFailure;
    // How the app-server process is created. The default spawns the local codex
    // binary; the session-filesystem client injects one that runs it in its own Codex
    // home's directory (see codex/sessionfs.ts).
    this.spawnProcess = spawnProcess;
    this.process = null;
    this.nextId = 1;
    this.pending = new Map();
    this.initialized = false;
    this.startPromise = null;
  }

  async start({ env, cwd }: AppServerScope): Promise<void> {
    if (this.process?.child && !this.process.child.killed && this.initialized) {
      return;
    }
    if (this.startPromise) {
      return this.startPromise;
    }
    this.startPromise = this.startInner({ env, cwd }).finally(() => {
      this.startPromise = null;
    });
    return this.startPromise;
  }

  private async startInner({ env, cwd }: AppServerScope) {
    this.stop();
    const startedAt = process.hrtime.bigint();
    // A process stopped or replaced may still report its exit, late: only
    // the current one's events count.
    const started = this.spawnProcess({
      cwd,
      env,
      onLine: (line) => this.process === started && this.handleLine(line),
      onExit: (code, signal) => this.process === started && this.handleExit(code, signal),
      onError: (error) => this.process === started && this.handleSpawnError(error),
    });
    this.process = started;
    const init = await this.request("initialize", {
      clientInfo: {
        name: "alasio_telegram",
        title: "Alasio Telegram",
        version: "1.0.0",
      },
      capabilities: {
        experimentalApi: true,
      },
    }, START_TIMEOUT_MS);
    this.notify("initialized", null);
    this.initialized = true;
    this.log.info(`initialized ms=${elapsedMs(startedAt).toFixed(1)} user_agent=${JSON.stringify(init.userAgent)}`);
  }

  stop(): void {
    if (this.process) {
      this.process.stop();
      this.process = null;
    }
    this.initialized = false;
    this.failPending(new Error("Codex app-server stopped"));
  }

  private handleExit(code: number | null, signal: NodeJS.Signals | null) {
    this.log.warn(`app-server exited code=${code} signal=${signal}`);
    this.failPending(new Error(`Codex app-server exited code=${code} signal=${signal}`));
    this.initialized = false;
    this.process = null;
  }

  private handleSpawnError(error: Error) {
    this.log.error(`app-server spawn failed: ${error instanceof Error ? error.message : String(error)}`);
    this.failPending(error);
    this.initialized = false;
    this.process = null;
  }

  private failPending(error: Error) {
    for (const entry of this.pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(error);
    }
    this.pending.clear();
    this.onFailure(error);
  }

  handleLine(line: string): void {
    if (!line.trim()) {
      return;
    }
    // The app-server writes JSON-RPC messages, one per line.
    let message: IncomingMessage;
    try {
      message = JSON.parse(line);
    } catch {
      this.log.warn(`Ignoring non-JSON app-server line: ${line.slice(0, 200)}`);
      return;
    }
    if (message.id != null && this.pending.has(message.id)) {
      this.resolvePending(message.id, message);
      return;
    }
    if (message.id != null && message.method) {
      // A message with an id and a method is a request of the app-server's, in its protocol.
      this.respondToServerRequest(message as ServerRequest);
      return;
    }
    if (message.method) {
      // A message with a method and no id is a notification, in the app-server's protocol.
      this.onNotification(message as AppServerNotification);
    }
  }

  private resolvePending(id: RequestId, message: IncomingMessage) {
    // handleLine resolves only an id it has pending.
    const entry = this.pending.get(id)!;
    this.pending.delete(id);
    clearTimeout(entry.timer);
    if (message.error) {
      entry.reject(new Error(message.error.message ?? JSON.stringify(message.error)));
    } else {
      entry.resolve(message.result);
    }
  }

  private respondToServerRequest(message: ServerRequest) {
    const result = serverRequestResponse(message.method);
    this.write({ id: message.id, result });
  }

  private write(payload: OutgoingMessage) {
    if (!this.process?.child?.stdin) {
      throw new Error("Codex app-server is not running");
    }
    this.process.child.stdin.write(`${JSON.stringify(payload)}\n`);
  }

  /**
   * A request, as a client span named by its method. The request carries the span's
   * trace context, so what the app-server does for it joins alasio's trace.
   */
  request<M extends AppServerMethod>(method: M, params: AppServerParams<M>, timeoutMs = REQUEST_TIMEOUT_MS): Promise<AppServerResult<M>> {
    return rpcCall({ system: "jsonrpc", service: "codex", method }, (span) => {
      const id = this.nextId;
      this.nextId += 1;
      span.setAttribute("rpc.jsonrpc.request_id", String(id));
      const trace = traceCarrier();
      this.write({ id, method, params, ...(trace ? { trace } : {}) });
      return new Promise<AppServerResult<M>>((resolve, reject) => {
        const timer = setTimeout(() => {
          this.pending.delete(id);
          reject(new Error(`Codex app-server request timed out: ${method}`));
        }, timeoutMs);
        // The app-server answers a request with its method's result.
        this.pending.set(id, { resolve: (result) => resolve(result as AppServerResult<M>), reject, timer });
      });
    });
  }

  notify(method: ClientNotification["method"], params: null): void {
    this.write({ method, params });
  }
}
