#!/usr/bin/env node
/**
 * A stand-in for Claude Code, for the end-to-end run, which has no Claude login: the
 * run's install has alasio run it as Claude Code (ALASIO_CLAUDE_BIN), which the Agent SDK
 * starts as it starts the CLI, from the image ./harness-stand-ins.Dockerfile builds. It
 * speaks the CLI's stream-json on stdio as the CLI does, as far as alasio's turns go: the
 * SDK's control requests are answered, `initialize` with what the CLI says of itself and
 * the rest with nothing; and each prompt is answered with ANSWER, after the session's
 * init: at once, or, for a prompt `wait N`, N seconds after it comes, as a turn whose
 * tools run that long, unless it is interrupted first; for a prompt `background N`, at once,
 * a background task of its own told live (background_tasks_changed), which N seconds on reports
 * BACKGROUND_REPORT as a turn of its own, as the CLI's background agent's does, its prompt none,
 * and is told ended; for a prompt `options`, beside how it was started (`options: <JSON>`), its
 * tools, allowed and disallowed, its setting sources and its MCP servers; for a prompt
 * `memory`, beside the operator's memory and skills, as its user settings source reads them from
 * its config directory (`memory: <CLAUDE.md's first line>; skills: <names>`); for a prompt
 * `edit <name> <text>`, once it wrote `text` to the file `name` of its working directory, as its
 * Write tool would (`edited <name>`); and for a prompt `read <name>`, beside that file's text
 * (`read <name>: <text>`). Each answer's result names the prompt it answers, as the CLI's does. The session's transcript is written as the CLI
 * writes it, under `$CLAUDE_CONFIG_DIR/projects/<the working directory's key>/`, and
 * mirrored to the SDK as the CLI mirrors it (transcript_mirror), so alasio's session store,
 * and the lake, have it. A session it is given (`--session-id`) or resumes (`--resume`)
 * goes on in its transcript.
 *
 *   claude-stand-in.ts [the CLI's flags]...
 */
import { randomUUID } from "node:crypto";
import { appendFileSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";

/** What every turn answers. */
export const ANSWER = "Answered by the end-to-end run's stand-in for Claude Code.";

/** What a background task reports as a turn of its own, once its time is over. */
export const BACKGROUND_REPORT = "Reported by a background task of the end-to-end run's stand-in for Claude Code.";

/** The model it says it is, unless the CLI is given one. */
const MODEL = "claude-stand-in";

/** A project's key, as the CLI names a working directory's folder of transcripts. */
export const projectKey = (cwd: string): string => cwd.replace(/[^a-zA-Z0-9]/gu, "-");

/** The value of `flag` among the CLI's arguments, `--flag=value` as the SDK gives it or `--flag value`, if it is given. */
function flagOf(args: readonly string[], flag: string): string | undefined {
  const joined = args.find((arg) => arg.startsWith(`${flag}=`));
  if (joined !== undefined) return joined.slice(flag.length + 1);
  const index = args.indexOf(flag);
  return index === -1 ? undefined : args[index + 1];
}

/** A prompt as the SDK writes one: its uuid, and its text, or its content's text blocks. */
interface Prompt {
  readonly type: "user";
  readonly uuid?: string;
  readonly message: { readonly content: string | readonly { readonly type: string; readonly text?: string }[] };
}

/** A control request of the SDK's: its id, and what it asks. */
interface ControlRequest {
  readonly type: "control_request";
  readonly request_id: string;
  readonly request: { readonly subtype: string };
}

const textOf = ({ message: { content } }: Prompt): string =>
  typeof content === "string" ? content : content.flatMap(({ type, text }) => (type === "text" && text ? [text] : [])).join("\n");

/** What its config directory is, as the CLI resolves it: CLAUDE_CONFIG_DIR, else ~/.claude. */
const configDir = (): string => process.env["CLAUDE_CONFIG_DIR"] ?? join(homedir(), ".claude");

/** What answering `text` finds besides its answer, in the working directory `cwd`, the CLI started with `args`, as the module says; null for a prompt that finds nothing. */
function found(text: string, cwd: string, args: readonly string[]): string | null {
  if (text === "options") {
    const listed = (flag: string) => flagOf(args, flag)?.split(",").filter(Boolean) ?? null;
    const mcp = flagOf(args, "--mcp-config");
    return `options: ${JSON.stringify({
      tools: listed("--tools"),
      allowedTools: listed("--allowedTools"),
      disallowedTools: listed("--disallowedTools"),
      settingSources: listed("--setting-sources"),
      strictMcpConfig: args.includes("--strict-mcp-config"),
      mcpServers: mcp ? Object.keys((JSON.parse(mcp) as { readonly mcpServers?: object }).mcpServers ?? {}) : [],
    })}`;
  }
  if (text === "memory") {
    const read = (path: string) => {
      try {
        return readFileSync(path, "utf8");
      } catch {
        return null;
      }
    };
    const listed = (path: string) => {
      try {
        return readdirSync(path).sort();
      } catch {
        return [];
      }
    };
    return `memory: ${read(join(configDir(), "CLAUDE.md"))?.split("\n")[0] ?? "none"}; skills: ${listed(join(configDir(), "skills")).join(",") || "none"}`;
  }
  const edit = /^edit (\S+) (.+)$/su.exec(text);
  if (edit?.[1] && edit[2] !== undefined) {
    writeFileSync(join(cwd, edit[1]), edit[2]);
    return `edited ${edit[1]}`;
  }
  const read = /^read (\S+)$/u.exec(text)?.[1];
  if (read) return `read ${read}: ${readFileSync(join(cwd, read), "utf8")}`;
  return null;
}

/** Runs the CLI stand-in, given the CLI's arguments, until the SDK closes its input or stops it. */
function serve(args: readonly string[]): void {
  const sessionId = flagOf(args, "--resume") ?? flagOf(args, "--session-id") ?? randomUUID();
  const model = flagOf(args, "--model") ?? MODEL;
  const cwd = process.cwd();
  const transcript = join(configDir(), "projects", projectKey(cwd), `${sessionId}.jsonl`);
  const write = (message: object) => process.stdout.write(`${JSON.stringify(message)}\n`);
  /** The last entry of the transcript, which the next names as its parent. */
  let parent: string | null = null;
  const record = (type: "user" | "assistant", message: object, uuid: string = randomUUID()) => {
    const entry = { type, uuid, parentUuid: parent, sessionId, cwd, timestamp: new Date().toISOString(), message };
    parent = uuid;
    mkdirSync(join(transcript, ".."), { recursive: true });
    appendFileSync(transcript, `${JSON.stringify(entry)}\n`);
    write({ type: "transcript_mirror", filePath: transcript, entries: [entry] });
  };

  let initialized = false;
  /** The prompt being answered, and what answers it once its wait is over. */
  let running: { readonly uuid: string; readonly timer: NodeJS.Timeout } | null = null;
  /** The assistant's message saying `text`, written to the transcript and to the SDK. */
  const say = (text: string) => {
    const message = { id: `msg_${randomUUID()}`, type: "message", role: "assistant", model, content: [{ type: "text", text }], stop_reason: "end_turn", stop_sequence: null, usage: { input_tokens: 1, output_tokens: 1 } };
    record("assistant", message);
    write({ type: "assistant", message, parent_tool_use_id: null, session_id: sessionId, uuid: randomUUID() });
  };
  const result = (uuid: string, answer: string | null, started: number) => writeResult([uuid], answer, started);
  /** A turn's result, of the prompts `uuids` it answers, none for a turn of its own. */
  const writeResult = (uuids: readonly string[], answer: string | null, started: number) =>
    write({
      type: "result",
      subtype: answer === null ? "error_during_execution" : "success",
      is_error: answer === null,
      duration_ms: Date.now() - started,
      duration_api_ms: 0,
      num_turns: 1,
      ...(answer === null ? {} : { result: answer }),
      session_id: sessionId,
      total_cost_usd: 0,
      usage: { input_tokens: 1, output_tokens: 1, cache_read_input_tokens: 0, cache_creation_input_tokens: 0 },
      modelUsage: {},
      permission_denials: [],
      user_message_uuids: uuids,
      uuid: randomUUID(),
    });

  const answerPrompt = (prompt: Prompt) => {
    const uuid = prompt.uuid ?? randomUUID();
    const started = Date.now();
    if (!initialized) {
      initialized = true;
      write({ type: "system", subtype: "init", session_id: sessionId, cwd, model, tools: [], mcp_servers: [], permissionMode: "bypassPermissions", apiKeySource: "none", slash_commands: [], output_style: "default", uuid: randomUUID() });
    }
    const text = textOf(prompt);
    record("user", { role: "user", content: text }, uuid);
    const seconds = Number(/^wait (\d+)$/u.exec(text.trim())?.[1] ?? 0);
    const said = [ANSWER, ...[found(text.trim(), cwd, args)].filter((extra) => extra !== null)].join("\n\n");
    const answer = () => {
      running = null;
      say(said);
      result(uuid, said, started);
    };
    const background = Number(/^background (\d+)$/u.exec(text.trim())?.[1] ?? Number.NaN);
    if (!Number.isNaN(background)) {
      const task = { task_id: randomUUID(), task_type: "local_agent", description: "the end-to-end run's background task" };
      write({ type: "system", subtype: "background_tasks_changed", tasks: [task], uuid: randomUUID(), session_id: sessionId });
      setTimeout(() => {
        // A turn of its own, as the CLI reports a background agent's: no prompt of the SDK's answered.
        say(BACKGROUND_REPORT);
        write({ type: "system", subtype: "background_tasks_changed", tasks: [], uuid: randomUUID(), session_id: sessionId });
        writeResult([], BACKGROUND_REPORT, Date.now());
      }, background * 1000);
    }
    if (seconds === 0) answer();
    else running = { uuid, timer: setTimeout(answer, seconds * 1000) };
  };

  const control = ({ request_id, request: { subtype } }: ControlRequest) => {
    let response: object = {};
    if (subtype === "initialize") {
      response = { commands: [], agents: [], output_style: "default", available_output_styles: ["default"], models: [{ value: model, displayName: model, description: "The end-to-end run's stand-in" }], account: {} };
    } else if (subtype === "interrupt" && running) {
      clearTimeout(running.timer);
      result(running.uuid, null, Date.now());
      running = null;
    } else if (subtype === "mcp_status") {
      response = { mcpServers: [] };
    }
    write({ type: "control_response", response: { subtype: "success", request_id, response } });
  };

  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
  lines.on("line", (line) => {
    if (!line.trim()) return;
    // The SDK writes a message a line: its prompts, its control requests, and its answers to the CLI's.
    const message = JSON.parse(line) as Prompt | ControlRequest | { readonly type: string };
    if (message.type === "user") answerPrompt(message as Prompt);
    else if (message.type === "control_request") control(message as ControlRequest);
  });
  lines.on("close", () => process.exit(0));
  process.on("SIGTERM", () => process.exit(0));
}

if (import.meta.main) serve(process.argv.slice(2));
