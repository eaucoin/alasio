/**
 * Codex as alasio's tests see it: the app-server alasio spawns is
 * test/support/fake-codex-app-server.ts, which relays its JSON-RPC to the test, and the
 * test answers each request and sends each notification itself. Everything is typed with
 * the protocol of the Codex alasio pins (.types/codex).
 */
import { chmodSync, writeFileSync } from "node:fs";
import { type Server, type Socket, createServer } from "node:net";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

import type { ClientNotification, ClientRequest, InitializeResponse, RequestId, ServerNotification, v2 } from "../../.types/codex/index.js";
import { initializeResponse } from "./codex-protocol.ts";
import type { FakeCodexCommand, FakeCodexReport } from "./fake-codex-app-server.ts";
import { eventually } from "./wait.ts";

/** What the app-server answers each request alasio makes of it with. */
export interface CodexResults {
  initialize: InitializeResponse;
  "thread/loaded/list": v2.ThreadLoadedListResponse;
  "thread/resume": v2.ThreadResumeResponse;
  "thread/start": v2.ThreadStartResponse;
  "thread/fork": v2.ThreadForkResponse;
  "thread/list": v2.ThreadListResponse;
  "thread/turns/list": v2.ThreadTurnsListResponse;
  "model/list": v2.ModelListResponse;
  "turn/start": v2.TurnStartResponse;
  "turn/steer": v2.TurnSteerResponse;
  "turn/interrupt": v2.TurnInterruptResponse;
  "thread/goal/get": v2.ThreadGoalGetResponse;
  "thread/goal/set": v2.ThreadGoalSetResponse;
  "thread/goal/clear": v2.ThreadGoalClearResponse;
}

export type CodexMethod = keyof CodexResults;

/** A request alasio made of the app-server. */
export interface CodexRequest<M extends CodexMethod = CodexMethod> {
  readonly method: M;
  readonly id: RequestId;
  readonly params: Extract<ClientRequest, { readonly method: M }>["params"];
}

/** Something alasio wrote to an app-server: a request, a notification, or an answer to one of the app-server's requests. */
type Written = ClientRequest | ClientNotification | { readonly id: RequestId; readonly result: unknown };

/** An app-server process alasio started: how, and whether it has exited. */
export interface CodexProcess {
  readonly argv: readonly string[];
  readonly cwd: string;
  readonly pid: number;
  exited: boolean;
}

/** A request as received: which process it came to, and whether a test or a standing answer has taken it. */
interface Received {
  readonly request: ClientRequest;
  readonly process: number;
  taken: boolean;
}

/** A standing answer: every request of its method is answered with what it returns. */
type Responder<M extends CodexMethod> = (request: CodexRequest<M>) => CodexResults[M];

const STAND_IN = fileURLToPath(new URL("./fake-codex-app-server.ts", import.meta.url));

/** `value` quoted for sh. */
function shellQuote(value: string): string {
  return `'${value.replaceAll("'", `'\\''`)}'`;
}

function isRequest(message: Written): message is ClientRequest {
  return "method" in message && "id" in message;
}

export class FakeCodexAppServer {
  /** The binary alasio runs as Codex: a script that runs the stand-in, connected to this test. */
  readonly bin: string;
  readonly processes: CodexProcess[] = [];
  /** What alasio notified the app-server of, in order. */
  readonly notifications: ClientNotification[] = [];
  private readonly server: Server;
  private readonly sockets: Socket[] = [];
  private readonly received: Received[] = [];
  private readonly responders = new Map<string, (request: ClientRequest) => unknown>();

  private constructor(server: Server, bin: string) {
    this.server = server;
    this.bin = bin;
    this.answerEvery("initialize", () => initializeResponse());
  }

  /** A stand-in whose socket and binary are in `directory`. */
  static async start(directory: string): Promise<FakeCodexAppServer> {
    const socketPath = join(directory, "codex.sock");
    const bin = join(directory, "codex");
    writeFileSync(bin, `#!/bin/sh\nexec ${shellQuote(process.execPath)} ${shellQuote(STAND_IN)} ${shellQuote(socketPath)} "$@"\n`);
    chmodSync(bin, 0o755);
    const server = createServer();
    const codex = new FakeCodexAppServer(server, bin);
    server.on("connection", (socket) => codex.accept(socket));
    await new Promise<void>((resolve) => server.listen(socketPath, resolve));
    return codex;
  }

  async close(): Promise<void> {
    for (const socket of this.sockets) socket.destroy();
    await new Promise((resolve) => this.server.close(resolve));
  }

  private accept(socket: Socket): void {
    let index = -1;
    createInterface({ input: socket, crlfDelay: Infinity }).on("line", (line) => {
      // The stand-in reports in these frames, one per line.
      const report = JSON.parse(line) as FakeCodexReport;
      if ("spawned" in report) {
        index = this.processes.push({ ...report.spawned, exited: false }) - 1;
        this.sockets[index] = socket;
        return;
      }
      // alasio writes the app-server JSON-RPC in its protocol.
      this.receive(index, JSON.parse(report.stdin) as Written);
    });
    socket.on("close", () => {
      const spawned = this.processes[index];
      if (spawned) spawned.exited = true;
    });
  }

  private receive(process: number, message: Written): void {
    if (!isRequest(message)) {
      if ("method" in message) this.notifications.push(message);
      return;
    }
    const received: Received = { request: message, process, taken: false };
    this.received.push(received);
    const responder = this.responders.get(message.method);
    if (responder) {
      received.taken = true;
      this.write(process, { id: message.id, result: responder(message) });
    }
  }

  private write(process: number, message: object): void {
    const command: FakeCodexCommand = { stdout: JSON.stringify(message) };
    this.sockets[process]?.write(`${JSON.stringify(command)}\n`);
  }

  /** The app-server process alasio started last. */
  private get current(): number {
    return this.processes.length - 1;
  }

  /** Answers every request of `method`, from now on, with what `respond` returns. */
  answerEvery<M extends CodexMethod>(method: M, respond: Responder<M>): void {
    // Requests are filed under their method, so a responder of M is only given M's.
    this.responders.set(method, respond as (request: ClientRequest) => unknown);
  }

  /** The methods of every request alasio made, in order. */
  methods(): string[] {
    return this.received.map(({ request }) => request.method);
  }

  /** Every request of `method` alasio made, in order. */
  requests<M extends CodexMethod>(method: M): CodexRequest<M>[] {
    // Picked by their method.
    return this.received.filter(({ request }) => request.method === method).map(({ request }) => request as CodexRequest<M>);
  }

  /** The methods of the requests no test has taken and no standing answer answered, in order. */
  unanswered(): string[] {
    return this.received.filter(({ taken }) => !taken).map(({ request }) => request.method);
  }

  /** The first request of `method` not yet taken, once alasio has made it; the test answers it. */
  async next<M extends CodexMethod>(method: M, { timeoutMs = 5_000 }: { readonly timeoutMs?: number } = {}): Promise<CodexRequest<M>> {
    const received = await eventually(`alasio to request ${method} of the app-server`, () => this.received.find((candidate) => !candidate.taken && candidate.request.method === method), {
      timeoutMs,
      seen: () => this.received.map(({ request, taken }) => `${request.method}${taken ? "" : " (unanswered)"}`),
    });
    received.taken = true;
    // Found by its method just above.
    return received.request as CodexRequest<M>;
  }

  /** Answers `request` with `result`. */
  answer<M extends CodexMethod>(request: CodexRequest<M>, result: CodexResults[M]): void {
    this.write(this.processOf(request), { id: request.id, result });
  }

  /** Sends alasio a notification from the current app-server process. */
  notify(...notifications: readonly ServerNotification[]): void {
    for (const notification of notifications) this.write(this.current, notification);
  }

  /** Has the current app-server process exit with `code`, as a crash does. */
  exit(code: number): void {
    const command: FakeCodexCommand = { exit: code };
    this.sockets[this.current]?.write(`${JSON.stringify(command)}\n`);
  }

  /** Resolves once every app-server process alasio started has exited. */
  async allExited({ timeoutMs = 5_000 }: { readonly timeoutMs?: number } = {}): Promise<void> {
    await eventually("every app-server process to exit", () => (this.processes.every(({ exited }) => exited) ? true : undefined), { timeoutMs });
  }

  private processOf(request: object): number {
    const received = this.received.find((candidate) => candidate.request === request);
    if (!received) throw new Error(`alasio made no request ${JSON.stringify(request)}`);
    return received.process;
  }
}
