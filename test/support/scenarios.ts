/**
 * The steps alasio's app tests share, as the operator and the stand-ins take them: mount a
 * service and a folder, start and finish a Codex turn. Each waits for what alasio does
 * in response, and returns what the test asserts on.
 */
import type { TestContext } from "node:test";

import type { v2 } from "../../.types/codex/index.js";
import type { CodexRequest, FakeCodexAppServer } from "./codex.ts";
import { agentMessage, codexThread, codexTurn, itemCompleted, threadStartResponse, turnCompleted, turnStarted } from "./codex-protocol.ts";
import { type AlasioOptions, type RunningAlasio, startAlasio } from "./alasio.ts";
import type { Shown } from "./telegram.ts";

/** alasio for one test, taken down after it whatever happens. */
export async function alasioFor(t: TestContext, options: AlasioOptions = {}): Promise<RunningAlasio> {
  const alasio = await startAlasio(options);
  t.after(() => alasio.close());
  return alasio;
}

/** Mounts `service` and the folder `folder` with /service and /workspace, as the operator does. */
export async function mount(alasio: RunningAlasio, service: "codex" | "claude", folder: string): Promise<void> {
  const { telegram } = alasio;
  const mark = telegram.mark();
  telegram.say(`/service ${service}`);
  await telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Workspace"), { mark });
  telegram.say(`/workspace ${folder}`);
  await telegram.waitFor("sendMessage", (call) => call.params.text.includes(`Mounted ${folder} (`), { mark });
}

/** Answers alasio's thread/start with a new thread `threadId` in `cwd`; the request. */
export async function answerThreadStart(codex: FakeCodexAppServer, threadId: string, cwd: string): Promise<CodexRequest<"thread/start">> {
  const request = await codex.next("thread/start");
  codex.answer(request, threadStartResponse(codexThread(threadId, { cwd })));
  return request;
}

/** Answers alasio's thread/loaded/list with `threadIds`. */
export async function answerLoadedThreads(codex: FakeCodexAppServer, threadIds: readonly string[]): Promise<void> {
  codex.answer(await codex.next("thread/loaded/list"), { data: [...threadIds], nextCursor: null });
}

/** Answers alasio's turn/start with the turn `turnId`, and reports it started; the request. */
export async function answerTurnStart(codex: FakeCodexAppServer, threadId: string, turnId: string): Promise<CodexRequest<"turn/start">> {
  const request = await codex.next("turn/start");
  codex.answer(request, { turn: codexTurn(turnId) });
  codex.notify(turnStarted(threadId, codexTurn(turnId)));
  return request;
}

/** Codex finishes the turn: `items`, then the turn completed with `status`. */
export function finishTurn(codex: FakeCodexAppServer, threadId: string, turnId: string, items: readonly v2.ThreadItem[], status: v2.TurnStatus = "completed"): void {
  codex.notify(
    ...items.map((item) => itemCompleted(threadId, turnId, item)),
    turnCompleted(threadId, codexTurn(turnId, { status })),
  );
}

/** Codex answers the turn with `answer` as its final answer. */
export function answerTurn(codex: FakeCodexAppServer, threadId: string, turnId: string, answer: string): void {
  finishTurn(codex, threadId, turnId, [agentMessage(`${turnId}-answer`, answer, "final_answer")]);
}

/** The text of the text input of a turn/start or turn/steer. */
export function inputText(request: CodexRequest<"turn/start"> | CodexRequest<"turn/steer">): string {
  return request.params.input.map((input) => (input.type === "text" ? input.text : `<${input.type}>`)).join("");
}

/** The status message's line while a turn of `service` runs, its start time left out. */
export const WORKING = (service: string): string => `${service} is working · started <tg-time unix="…" format="r">just now</tg-time>`;

/**
 * `entries` in a fixed order of their own, for comparing what alasio shows from work it
 * runs concurrently, whose calls may reach Telegram in either order.
 */
export function inAnyOrder(entries: readonly Shown[]): Shown[] {
  const key = (entry: Shown) => `${entry.method}\n${entry.text}`;
  return entries.toSorted((a, b) => (key(a) < key(b) ? -1 : key(a) > key(b) ? 1 : 0));
}

/** What alasio showed, with what depends on the clock left out: a status line's start, and how long a turn worked. */
export function timeless(entries: readonly Shown[]): Shown[] {
  return entries.map((entry) => ({
    ...entry,
    text: entry.text
      .replace(/<tg-time unix="\d+"/u, '<tg-time unix="…"')
      .replace(/ worked for (?:less than 1 second|\d+ seconds?)\.$/u, " worked for a moment."),
  }));
}
