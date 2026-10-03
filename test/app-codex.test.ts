/**
 * alasio with Codex, from the outside: what the operator says and presses in Telegram,
 * what alasio asks of the Codex app-server, and what the operator sees come back.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { agentMessage, codexThread, codexTurn, commandExecution, threadResumeResponse, turnCompleted, turnError } from "./support/codex-protocol.ts";
import type { RunningAlasio } from "./support/alasio.ts";
import {
  WORKING,
  answerLoadedThreads,
  answerThreadStart,
  answerTurn,
  answerTurnStart,
  finishTurn,
  inAnyOrder,
  inputText,
  mount,
  alasioFor,
  timeless,
} from "./support/scenarios.ts";
import { OPERATOR_ID } from "./support/telegram.ts";

/** What alasio adds to every Codex thread's developer instructions. */
const REPLY_INSTRUCTIONS = "Your replies reach the operator in Telegram through alasio. To show the operator an image or video file, embed it on a line of its own with Markdown image syntax and a local path, `![short caption](path)`, absolute or relative to the working directory: alasio sends the file itself in its place, several embeds in one paragraph as a collage. Naming a path any other way, such as in backticks, only refers to the file. Photos up to 10 MB and videos up to 50 MB, at most 10 per reply.";

/** The concurrency panel's buttons. */
const CONCURRENT_BUTTONS = [["Steer", "Queue"], ["Swerve", "Discard"]];

/** Mounts Codex on `alpha` and starts a turn of `prompt`, which runs as turn-1 on thread-1 until the test ends it. */
async function runningTurn(alasio: RunningAlasio, prompt = "Fix the build"): Promise<void> {
  await mount(alasio, "codex", "alpha");
  alasio.telegram.say(prompt);
  await answerThreadStart(alasio.codex, "thread-1", alasio.folder("alpha"));
  await answerTurnStart(alasio.codex, "thread-1", "turn-1");
  await alasio.telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Codex is working"));
}

/** The operator sends `text` while a turn runs, and gets the concurrency panel. */
async function concurrentPrompt(alasio: RunningAlasio, text: string): Promise<void> {
  const mark = alasio.telegram.mark();
  alasio.telegram.say(text);
  const [panel] = await alasio.telegram.waitForShown(1, { mark });
  assert.deepEqual(panel, {
    method: "sendMessage",
    text: `Codex is currently working. What should I do with this message?\n\n${text}`,
    buttons: CONCURRENT_BUTTONS,
  });
}

test("a message before any service is mounted gets the service picker and is not queued; Codex, then a folder, mount, and the next prompt runs", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  const alpha = alasio.folder("alpha");

  telegram.say("Fix the build");
  assert.deepEqual(await telegram.waitForShown(1), [{
    method: "sendMessage",
    text: [
      "Service",
      "",
      "Active: none",
      "Status: idle",
      "",
      "Mounted sessions",
      "  Codex: no mounted session",
      "  Claude: no mounted session",
      "",
      "Nothing runs until a service is chosen. Each service keeps its own sessions and uses its own local login.",
      "",
      "No service is mounted. Choose Codex or Claude to start; your message was not queued.",
    ].join("\n"),
    buttons: [["Use Codex", "Use Claude"], ["Close"]],
  }]);

  let mark = telegram.mark();
  telegram.press("Use Codex");
  assert.deepEqual(await telegram.waitForShown(3, { mark }), [
    { method: "answerCallbackQuery", text: "Mounted Codex. Send a message to start." },
    {
      method: "editMessageText",
      text: [
        "Service",
        "",
        "Active: Codex",
        "Status: idle",
        "",
        "Mounted sessions",
        "* Codex: no mounted session",
        "  Claude: no mounted session",
        "",
        "Sessions belong to one service. Switching parks the current session and resumes the other service's own session.",
        "",
        "Mounted Codex. Send a message to start.",
      ].join("\n"),
      buttons: [["Use Claude"], ["Close"]],
    },
    {
      method: "sendMessage",
      text: [
        "Workspace",
        "",
        "Folder: none",
        "Status: idle",
        `Root: ${alasio.workspaceRoot}`,
        "",
        "Sessions belong to one service and one folder. Switching folders parks the current sessions and restores the ones from the chosen folder.",
        "",
        "Type /workspace <name> to mount a folder under the root, or /workspace new <name> to create a git-initialized one.",
        "",
        "No folder is mounted. Choose a folder to work in, or create one; your message was not queued.",
      ].join("\n"),
      buttons: [["· alpha"], ["New folder…", "Refresh", "Close"]],
    },
  ]);

  mark = telegram.mark();
  telegram.press("· alpha");
  assert.deepEqual(await telegram.waitForShown(2, { mark }), [
    { method: "answerCallbackQuery", text: `Mounted alpha (${alpha}). Send a message to start.` },
    {
      method: "editMessageText",
      text: [
        "Workspace",
        "",
        `Folder: ${alpha}`,
        "Status: idle",
        `Root: ${alasio.workspaceRoot}`,
        "",
        "Sessions belong to one service and one folder. Switching folders parks the current sessions and restores the ones from the chosen folder.",
        "",
        "Type /workspace <name> to mount a folder under the root, or /workspace new <name> to create a git-initialized one.",
        "",
        `Mounted alpha (${alpha}). Send a message to start.`,
      ].join("\n"),
      buttons: [["* · alpha"], ["New folder…", "Refresh", "Close"]],
    },
  ]);
  // The first message was not queued: mounting ran nothing, and the next prompt is the first turn.
  assert.equal(codex.processes.length, 0);

  telegram.say("Now fix it");
  await answerThreadStart(codex, "thread-1", alpha);
  const turnStart = await answerTurnStart(codex, "thread-1", "turn-1");
  assert.equal(inputText(turnStart), "Now fix it");
  answerTurn(codex, "thread-1", "turn-1", "Fixed.");
  await telegram.waitFor("sendRichMessage");
  assert.deepEqual(codex.methods(), ["initialize", "thread/start", "turn/start"]);
});

test("a Codex turn: a status message, sent then edited, and the final answer alone as the reply; what alasio asks of the app-server", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  const alpha = alasio.folder("alpha");
  await mount(alasio, "codex", "alpha");
  const mark = telegram.mark();

  telegram.say("Fix the build");
  const threadStart = await answerThreadStart(codex, "thread-1", alpha);
  assert.deepEqual(threadStart.params, {
    cwd: alpha,
    model: "gpt-5.6-sol",
    approvalPolicy: "never",
    sandbox: "danger-full-access",
    config: {
      project_doc_max_bytes: 32768,
      developer_instructions: REPLY_INSTRUCTIONS,
      mcp_servers: { bayma: { url: "http://bayma.test:7290/mcp", http_headers: { Authorization: "Bearer folder-bayma" }, startup_timeout_sec: 60 } },
      model_reasoning_effort: "high",
    },
  });
  const turnStart = await answerTurnStart(codex, "thread-1", "turn-1");
  assert.deepEqual(turnStart.params, {
    threadId: "thread-1",
    input: [{ type: "text", text: "Fix the build", text_elements: [] }],
    cwd: alpha,
    model: "gpt-5.6-sol",
    effort: "high",
    approvalPolicy: "never",
    sandboxPolicy: { type: "dangerFullAccess" },
  });
  const status = await telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Codex is working"), { mark });
  assert.equal(status.params.parse_mode, "HTML");
  assert.equal(status.params.chat_id, String(OPERATOR_ID));

  finishTurn(codex, "thread-1", "turn-1", [
    agentMessage("m1", "Looking at the failing step.", "commentary"),
    commandExecution("c1", "npm test"),
    agentMessage("m2", "Fixed: the **lockfile** was stale.", "final_answer"),
  ]);
  const reply = await telegram.waitFor("sendRichMessage", () => true, { mark });
  assert.equal(reply.params.chat_id, String(OPERATOR_ID));
  assert.deepEqual(timeless(telegram.shown(mark)), [
    { method: "sendMessage", text: WORKING("Codex") },
    { method: "editMessageText", text: "Codex worked for a moment." },
    { method: "sendRichMessage", text: "Fixed: the **lockfile** was stale." },
  ]);
  const [edit] = telegram.callsTo("editMessageText", mark);
  assert.equal(edit?.params.message_id, status.messageId);
  // One app-server, started in the folder, as Codex's app-server over stdio.
  assert.deepEqual(codex.processes.map(({ argv, cwd }) => ({ argv, cwd })), [{ argv: ["app-server", "--disable", "plugins", "--listen", "stdio://"], cwd: alpha }]);
  assert.deepEqual(codex.notifications, [{ method: "initialized", params: null }]);
  assert.deepEqual(codex.unanswered(), []);
});

test("Steer sends the concurrent message into the running turn as turn/steer", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await runningTurn(alasio);
  await concurrentPrompt(alasio, "Also update the docs");

  const mark = telegram.mark();
  telegram.press("Steer");
  const steer = await codex.next("turn/steer");
  assert.deepEqual(steer.params, { threadId: "thread-1", expectedTurnId: "turn-1", input: [{ type: "text", text: "Also update the docs", text_elements: [] }] });
  codex.answer(steer, { turnId: "turn-1" });
  assert.deepEqual(await telegram.waitForShown(2, { mark }), [
    { method: "answerCallbackQuery", text: "Steered." },
    { method: "editMessageText", text: "Sent as guidance to the active Codex turn." },
  ]);

  answerTurn(codex, "thread-1", "turn-1", "Fixed, docs too.");
  await telegram.waitFor("sendRichMessage", (call) => call.params.rich_message.markdown === "Fixed, docs too.");
  // The steered message was taken by the turn: the next turn is the operator's next prompt.
  telegram.say("Thanks");
  await answerLoadedThreads(codex, ["thread-1"]);
  assert.equal(inputText(await answerTurnStart(codex, "thread-1", "turn-2")), "Thanks");
});

test("Queue runs the concurrent message as a turn of its own once the running turn ends", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await runningTurn(alasio);
  await concurrentPrompt(alasio, "Also update the docs");

  let mark = telegram.mark();
  telegram.press("Queue");
  assert.deepEqual(await telegram.waitForShown(2, { mark }), [
    { method: "answerCallbackQuery", text: "Queued." },
    { method: "editMessageText", text: "Queued. Codex will process this after the current task." },
  ]);
  assert.deepEqual(codex.requests("turn/start").length, 1);

  mark = telegram.mark();
  answerTurn(codex, "thread-1", "turn-1", "Fixed.");
  await answerLoadedThreads(codex, ["thread-1"]);
  const queued = await answerTurnStart(codex, "thread-1", "turn-2");
  assert.equal(queued.params.threadId, "thread-1");
  assert.equal(inputText(queued), "Also update the docs");
  answerTurn(codex, "thread-1", "turn-2", "Docs updated.");
  await telegram.waitFor("sendRichMessage", (call) => call.params.rich_message.markdown === "Docs updated.", { mark });
  assert.deepEqual(timeless(telegram.shown(mark)).filter(({ method }) => method === "sendRichMessage"), [
    { method: "sendRichMessage", text: "Fixed." },
    { method: "sendRichMessage", text: "Docs updated." },
  ]);
});

test("Swerve interrupts the running turn and runs the concurrent message next", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await runningTurn(alasio);
  await concurrentPrompt(alasio, "Stop that, do this instead");

  const mark = telegram.mark();
  telegram.press("Swerve");
  const interrupt = await codex.next("turn/interrupt");
  assert.deepEqual(interrupt.params, { threadId: "thread-1", turnId: "turn-1" });
  codex.answer(interrupt, {});
  codex.notify(turnCompleted("thread-1", codexTurn("turn-1", { status: "interrupted" })));
  await answerLoadedThreads(codex, ["thread-1"]);
  const swerved = await answerTurnStart(codex, "thread-1", "turn-2");
  assert.equal(inputText(swerved), "Stop that, do this instead");
  answerTurn(codex, "thread-1", "turn-2", "Done instead.");
  await telegram.waitFor("sendRichMessage", () => true, { mark });

  const shown = timeless(telegram.shown(mark));
  // The press is answered as the turn is interrupted, and the next turn's status follows.
  assert.deepEqual(inAnyOrder(shown.filter(({ method }) => method !== "sendMessage" && method !== "sendRichMessage")), inAnyOrder([
    { method: "answerCallbackQuery", text: "Swerving." },
    { method: "editMessageText", text: "Swerving Codex to this message." },
    { method: "editMessageText", text: "Codex interrupted." },
    { method: "editMessageText", text: "Codex worked for a moment." },
  ]));
  assert.deepEqual(shown.filter(({ method }) => method === "sendMessage" || method === "sendRichMessage"), [
    { method: "sendMessage", text: WORKING("Codex") },
    { method: "sendRichMessage", text: "Done instead." },
  ]);
});

test("Discard drops the concurrent message: the running turn finishes and nothing else runs", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await runningTurn(alasio);
  await concurrentPrompt(alasio, "Never mind this");

  const mark = telegram.mark();
  telegram.press("Discard");
  assert.deepEqual(await telegram.waitForShown(2, { mark }), [
    { method: "answerCallbackQuery", text: "Discarded." },
    { method: "editMessageText", text: "Discarded." },
  ]);
  answerTurn(codex, "thread-1", "turn-1", "Fixed.");
  await telegram.waitFor("sendRichMessage", () => true, { mark });

  telegram.say("Next");
  await answerLoadedThreads(codex, ["thread-1"]);
  assert.equal(inputText(await answerTurnStart(codex, "thread-1", "turn-2")), "Next");
});

test("/stop interrupts the running turn, which ends as stopped, and the next prompt runs", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await runningTurn(alasio);

  let mark = telegram.mark();
  telegram.say("/stop");
  const interrupt = await codex.next("turn/interrupt");
  assert.deepEqual(interrupt.params, { threadId: "thread-1", turnId: "turn-1" });
  codex.answer(interrupt, {});
  codex.notify(turnCompleted("thread-1", codexTurn("turn-1", { status: "interrupted" })));
  const shown = await telegram.waitForShown(3, { mark });
  assert.deepEqual(shown[0], { method: "sendMessage", text: "Stopping Codex..." });
  assert.deepEqual(inAnyOrder(shown.slice(1)), inAnyOrder([
    { method: "editMessageText", text: "Codex interrupted." },
    { method: "editMessageText", text: "Codex stopped." },
  ]));
  const stopped = telegram.callsTo("editMessageText", mark).find((call) => call.params.text === "Codex stopped.");
  assert.equal(stopped?.params.message_id, telegram.callsTo("sendMessage", mark)[0]?.messageId);

  mark = telegram.mark();
  telegram.say("Try again");
  await answerLoadedThreads(codex, ["thread-1"]);
  assert.equal(inputText(await answerTurnStart(codex, "thread-1", "turn-2")), "Try again");
  answerTurn(codex, "thread-1", "turn-2", "Done.");
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  // The stopped turn delivered nothing.
  assert.deepEqual(telegram.callsTo("sendRichMessage").map((call) => call.params.rich_message.markdown), ["Done."]);
});

test("/stop with no turn running says so", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  await mount(alasio, "codex", "alpha");
  const mark = alasio.telegram.mark();
  alasio.telegram.say("/stop");
  assert.deepEqual(await alasio.telegram.waitForShown(1, { mark }), [{ method: "sendMessage", text: "No active query to stop." }]);
});

test("an error Codex does not retry ends the turn as not completed, and its message is not shown", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await runningTurn(alasio);
  const mark = telegram.mark();
  codex.notify(
    turnError("thread-1", "turn-1", "Rate limit reached", false),
    turnCompleted("thread-1", codexTurn("turn-1", { status: "failed", error: { message: "Rate limit reached", codexErrorInfo: null, additionalDetails: null, misalignment: null } })),
  );
  // characterizes current behaviour: the error is kept in the turn's response but never
  // reaches the operator, who sees only that Codex did not complete; only final-answer
  // text is ever delivered.
  assert.deepEqual(await telegram.waitForShown(1, { mark }), [{ method: "editMessageText", text: "Codex did not complete." }]);

  telegram.say("Again");
  await answerLoadedThreads(codex, ["thread-1"]);
  await answerTurnStart(codex, "thread-1", "turn-2");
  answerTurn(codex, "thread-1", "turn-2", "Done.");
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  assert.deepEqual(telegram.callsTo("sendRichMessage", mark).map((call) => call.params.rich_message.markdown), ["Done."]);
});

test("an error Codex is retrying is not reported: the turn goes on to its answer", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await runningTurn(alasio);
  const mark = telegram.mark();
  codex.notify(turnError("thread-1", "turn-1", "stream disconnected, retrying", true));
  answerTurn(codex, "thread-1", "turn-1", "Fixed after a retry.");
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  assert.deepEqual(timeless(telegram.shown(mark)), [
    { method: "editMessageText", text: "Codex worked for a moment." },
    { method: "sendRichMessage", text: "Fixed after a retry." },
  ]);
});

test("the app-server exiting mid-turn fails the turn visibly, and the next prompt starts a fresh app-server that resumes the thread", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  const alpha = alasio.folder("alpha");
  await runningTurn(alasio);

  let mark = telegram.mark();
  codex.exit(1);
  assert.deepEqual(await telegram.waitForShown(1, { mark }), [{ method: "editMessageText", text: "Codex did not complete." }]);
  await codex.allExited();

  mark = telegram.mark();
  telegram.say("Try again");
  await answerLoadedThreads(codex, []);
  const resume = await codex.next("thread/resume");
  assert.deepEqual(resume.params, {
    threadId: "thread-1",
    excludeTurns: true,
    cwd: alpha,
    model: "gpt-5.6-sol",
    approvalPolicy: "never",
    sandbox: "danger-full-access",
    config: {
      project_doc_max_bytes: 32768,
      developer_instructions: REPLY_INSTRUCTIONS,
      mcp_servers: { bayma: { url: "http://bayma.test:7290/mcp", http_headers: { Authorization: "Bearer folder-bayma" }, startup_timeout_sec: 60 } },
      model_reasoning_effort: "high",
    },
  });
  codex.answer(resume, threadResumeResponse(codexThread("thread-1", { cwd: alpha })));
  assert.equal(inputText(await answerTurnStart(codex, "thread-1", "turn-2")), "Try again");
  answerTurn(codex, "thread-1", "turn-2", "Done.");
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  assert.equal(codex.processes.length, 2);
  assert.deepEqual(codex.methods(), ["initialize", "thread/start", "turn/start", "initialize", "thread/loaded/list", "thread/resume", "turn/start"]);
});
