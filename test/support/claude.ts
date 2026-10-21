/**
 * Claude Code as alasio's tests see it. Faking the CLI process is not worth it, so the
 * stand-in is at the narrowest seam alasio has, the query it starts through the Agent SDK
 * (ClaudeQueryFactory): in alasio's process, `bridgedQueryFactory` starts each query as a
 * connection to the test's socket, relaying the prompts alasio pushes and the interrupts it
 * makes; in the test, FakeClaude hands each query to the test, which emits what Claude
 * Code would.
 */
import { type Server, type Socket, connect, createServer } from "node:net";
import { createInterface } from "node:readline";

import type { Options, SDKMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";

import type { ClaudeQueryFactory } from "../../src/harness/claude/runtime.ts";
import { type StampedPrompt, stamped } from "./claude-sdk.ts";
import { eventually } from "./wait.ts";

/** The options of a query a test reads: those that survive JSON. */
type SeenOption = "cwd" | "model" | "effort" | "resume" | "sessionId" | "mcpServers" | "disallowedTools" | "permissionMode" | "persistSession";

/** What a test reads of the options a query was started with. */
export type SeenOptions = { readonly [K in SeenOption]?: Options[K] | undefined };

/** What alasio's side of a query tells the test. */
type QueryReport =
  | { readonly started: SeenOptions }
  | { readonly read: true }
  | { readonly prompt: SDKUserMessage }
  | { readonly interrupt: true };

/** What the test tells alasio's side of a query: a message Claude Code writes. */
interface QueryCommand {
  readonly message: SDKMessage;
}

function frame(message: QueryReport | QueryCommand): string {
  return `${JSON.stringify(message)}\n`;
}

/**
 * alasio's side: each query alasio starts connects to the socket at `socketPath` and is
 * driven from there.
 */
export function bridgedQueryFactory(socketPath: string): ClaudeQueryFactory {
  return ({ prompt, options }) => {
    const socket = connect(socketPath);
    const report = (message: QueryReport) => socket.write(frame(message));
    const { cwd, model, effort, resume, sessionId, mcpServers, disallowedTools, permissionMode, persistSession } = options;
    report({ started: { cwd, model, effort, resume, sessionId, mcpServers, disallowedTools, permissionMode, persistSession } });
    void (async () => {
      for await (const message of prompt) report({ prompt: message });
    })();
    const pending: SDKMessage[] = [];
    let ended = false;
    let wake: (() => void) | null = null;
    const finish = () => {
      ended = true;
      wake?.();
    };
    createInterface({ input: socket, crlfDelay: Infinity }).on("line", (line) => {
      // The test sends these commands, one per line.
      const command = JSON.parse(line) as QueryCommand;
      pending.push(command.message);
      wake?.();
    });
    socket.on("close", finish);
    async function* messages(): AsyncGenerator<SDKMessage, void> {
      for (;;) {
        const message = pending.shift();
        if (message) {
          yield message;
          // alasio asks for the next message once it has handled this one.
          report({ read: true });
          continue;
        }
        if (ended) return;
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
        wake = null;
      }
    }
    return Object.assign(messages(), {
      interrupt: async (): Promise<undefined> => {
        report({ interrupt: true });
        return undefined;
      },
      close: (): void => {
        socket.end();
      },
    });
  };
}

/** One Claude Code process alasio started, as the test drives it. */
export class FakeClaudeQuery {
  readonly options: SeenOptions;
  /** How many times alasio interrupted a turn. */
  interrupts = 0;
  private emitted = 0;
  private read = 0;
  private readonly socket: Socket;
  private readonly prompts: SDKUserMessage[] = [];

  constructor(socket: Socket, options: SeenOptions) {
    this.socket = socket;
    this.options = options;
  }

  /** Files a report from alasio's side. */
  receive(report: QueryReport): void {
    if ("prompt" in report) this.prompts.push(report.prompt);
    else if ("read" in report) this.read += 1;
    else if ("interrupt" in report) this.interrupts += 1;
  }

  /** The next prompt alasio pushes into the process, once it has. */
  async nextPrompt(): Promise<StampedPrompt> {
    const prompt = await eventually("alasio to push a prompt into Claude Code", () => this.prompts.shift());
    return stamped(prompt);
  }

  /** Claude Code writes `messages`. */
  emit(...messages: readonly SDKMessage[]): void {
    for (const message of messages) this.socket.write(frame({ message }));
    this.emitted += messages.length;
  }

  /** Resolves once alasio has read and handled every message Claude Code wrote. */
  async handled(): Promise<void> {
    await eventually("alasio to handle what Claude Code wrote", () => (this.read >= this.emitted ? true : undefined), { seen: () => ({ emitted: this.emitted, read: this.read }) });
  }
}

/** Every Claude Code query alasio starts, in order: the test's side of the bridge. */
export class FakeClaude {
  readonly socketPath: string;
  readonly queries: FakeClaudeQuery[] = [];
  private readonly server: Server;
  private readonly sockets = new Set<Socket>();

  private constructor(server: Server, socketPath: string) {
    this.server = server;
    this.socketPath = socketPath;
  }

  /** Serves the bridge at `socketPath`. */
  static async start(socketPath: string): Promise<FakeClaude> {
    const server = createServer();
    const claude = new FakeClaude(server, socketPath);
    server.on("connection", (socket) => claude.accept(socket));
    await new Promise<void>((resolve) => server.listen(socketPath, resolve));
    return claude;
  }

  async close(): Promise<void> {
    for (const socket of this.sockets) socket.destroy();
    await new Promise((resolve) => this.server.close(resolve));
  }

  private accept(socket: Socket): void {
    this.sockets.add(socket);
    socket.on("close", () => this.sockets.delete(socket));
    let query: FakeClaudeQuery | null = null;
    createInterface({ input: socket, crlfDelay: Infinity }).on("line", (line) => {
      // alasio's side reports in these frames, one per line.
      const report = JSON.parse(line) as QueryReport;
      if ("started" in report) {
        query = new FakeClaudeQuery(socket, report.started);
        this.queries.push(query);
        return;
      }
      query?.receive(report);
    });
  }

  /** The `index`th query alasio started, once it has. */
  async query(index: number): Promise<FakeClaudeQuery> {
    return await eventually(`alasio to start Claude Code query ${index}`, () => this.queries[index], { seen: () => this.queries.length });
  }
}
