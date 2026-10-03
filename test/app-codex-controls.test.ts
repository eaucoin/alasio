/**
 * The operator's controls over Codex, from the outside: /goal, /model, /service and
 * /workspace, and /sessions, as the operator uses them in Telegram and as alasio carries
 * them to the Codex app-server.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import type { v2 } from "../.types/codex/index.js";
import {
  agentMessage,
  codexModel,
  codexThread,
  codexTurn,
  threadGoal,
  threadResumeResponse,
  threadStartResponse,
  turnStarted,
  userMessage,
} from "./support/codex-protocol.ts";
import {
  WORKING,
  answerLoadedThreads,
  answerThreadStart,
  answerTurn,
  answerTurnStart,
  inputText,
  mount,
  alasioFor,
  timeless,
} from "./support/scenarios.ts";

test("/goal <objective> starts a session, sets the goal, and the goal's own turn runs to its reply; /goal shows its panel, and Pause pauses it", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await mount(alasio, "codex", "alpha");

  let mark = telegram.mark();
  telegram.say("/goal Ship the release");
  await answerThreadStart(codex, "thread-g", alasio.folder("alpha"));
  const get = await codex.next("thread/goal/get");
  assert.deepEqual(get.params, { threadId: "thread-g" });
  codex.answer(get, { goal: null });
  const set = await codex.next("thread/goal/set");
  assert.deepEqual(set.params, { threadId: "thread-g", objective: "Ship the release", status: "active" });
  const goal = threadGoal("thread-g", "Ship the release");
  codex.answer(set, { goal });
  // Codex runs the goal's turn itself; alasio follows it rather than starting one.
  codex.notify({ method: "thread/goal/updated", params: { threadId: "thread-g", turnId: "turn-g", goal } }, turnStarted("thread-g", codexTurn("turn-g")));
  await telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Codex is working"), { mark });
  answerTurn(codex, "thread-g", "turn-g", "Release shipped.");
  await telegram.waitFor("sendRichMessage", () => true, { mark });
  assert.deepEqual(timeless(telegram.shown(mark)), [
    { method: "sendMessage", text: WORKING("Codex") },
    { method: "editMessageText", text: "Codex worked for a moment." },
    { method: "sendRichMessage", text: "Release shipped." },
  ]);
  assert.deepEqual(codex.methods(), ["initialize", "thread/start", "thread/goal/get", "thread/goal/set"]);

  mark = telegram.mark();
  telegram.say("/goal");
  codex.answer(await codex.next("thread/goal/get"), { goal: { ...goal, tokensUsed: 12_345, timeUsedSeconds: 125 } });
  assert.deepEqual(await telegram.waitForShown(1, { mark }), [{
    method: "sendMessage",
    text: "Goal\n\nShip the release\n\nStatus: active\nTurn: idle\nTime: 2m\nTokens: 12.3K",
    buttons: [["Pause", "Clear"], ["Close"]],
  }]);

  mark = telegram.mark();
  telegram.press("Pause");
  const pause = await codex.next("thread/goal/set");
  assert.deepEqual(pause.params, { threadId: "thread-g", status: "paused" });
  codex.answer(pause, { goal: { ...goal, status: "paused" } });
  assert.deepEqual(await telegram.waitForShown(2, { mark }), [
    { method: "answerCallbackQuery", text: "Working..." },
    { method: "editMessageText", text: "Goal\n\nShip the release\n\nStatus: paused\nTurn: idle\nTokens: 0", buttons: [["Resume", "Clear"], ["Close"]] },
  ]);
});

test("/model lists the app-server's models; the chosen model and effort are what the next turn/start carries, until reset", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  await mount(alasio, "codex", "alpha");
  const efforts = (...levels: string[]): v2.ReasoningEffortOption[] => levels.map((reasoningEffort) => ({ reasoningEffort, description: "" }));
  codex.answerEvery("model/list", () => ({
    data: [
      codexModel("gpt-5.6-sol", { displayName: "GPT-5.6 Sol", supportedReasoningEfforts: efforts("medium", "high"), defaultReasoningEffort: "high", isDefault: true }),
      codexModel("gpt-5.6-mini", { displayName: "GPT-5.6 Mini", supportedReasoningEfforts: efforts("low", "medium"), defaultReasoningEffort: "medium" }),
      codexModel("gpt-internal", { displayName: "Internal", hidden: true }),
    ],
    nextCursor: null,
  }));

  let mark = telegram.mark();
  telegram.say("/model");
  assert.deepEqual(await telegram.waitForShown(1, { mark }), [{
    method: "sendMessage",
    text: "Model for Codex\n\nCurrent: gpt-5.6-sol at high effort (default)\n\nChoose a model, then an effort. It applies from the next turn.",
    buttons: [["GPT-5.6 Sol", "GPT-5.6 Mini"], ["Close"]],
  }]);
  assert.deepEqual(codex.requests("model/list").map(({ params }) => params), [{ includeHidden: false, limit: 100 }]);

  mark = telegram.mark();
  telegram.press("GPT-5.6 Mini");
  assert.deepEqual(await telegram.waitForShown(2, { mark }), [
    { method: "answerCallbackQuery", text: "Now choose an effort." },
    { method: "editMessageText", text: "GPT-5.6 Mini (gpt-5.6-mini)\n\nChoose an effort.", buttons: [["low", "medium (default)"], ["Close"]] },
  ]);
  mark = telegram.mark();
  telegram.press("low");
  assert.deepEqual(await telegram.waitForShown(2, { mark }), [
    { method: "answerCallbackQuery", text: "Model set." },
    { method: "editMessageText", text: "Codex will use GPT-5.6 Mini (gpt-5.6-mini) at low effort from the next turn." },
  ]);

  telegram.say("Fix the build");
  const threadStart = await answerThreadStart(codex, "thread-1", alasio.folder("alpha"));
  // The thread is loaded on alasio's pinned model; the turn runs on the chosen one.
  assert.equal(threadStart.params.model, "gpt-5.6-sol");
  const turnStart = await answerTurnStart(codex, "thread-1", "turn-1");
  assert.deepEqual([turnStart.params.model, turnStart.params.effort], ["gpt-5.6-mini", "low"]);
  answerTurn(codex, "thread-1", "turn-1", "Fixed.");
  await telegram.waitFor("sendRichMessage");

  mark = telegram.mark();
  telegram.say("/model");
  assert.deepEqual(await telegram.waitForShown(1, { mark }), [{
    method: "sendMessage",
    text: "Model for Codex\n\nCurrent: gpt-5.6-mini at low effort\n\nChoose a model, then an effort. It applies from the next turn.",
    buttons: [["GPT-5.6 Sol", "GPT-5.6 Mini"], ["Use default", "Close"]],
  }]);
  mark = telegram.mark();
  telegram.press("Use default");
  assert.deepEqual(await telegram.waitForShown(2, { mark }), [
    { method: "answerCallbackQuery", text: "Reset." },
    { method: "editMessageText", text: "Codex is back on its default model from the next turn." },
  ]);
  telegram.say("Again");
  await answerLoadedThreads(codex, ["thread-1"]);
  const next = await answerTurnStart(codex, "thread-1", "turn-2");
  assert.deepEqual([next.params.model, next.params.effort], ["gpt-5.6-sol", "high"]);
});

test("/service and /workspace are refused while a turn runs", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha", "beta"] });
  const { telegram, codex } = alasio;
  const alpha = alasio.folder("alpha");
  await mount(alasio, "codex", "alpha");
  telegram.say("Fix the build");
  await answerThreadStart(codex, "thread-a", alpha);
  await answerTurnStart(codex, "thread-a", "turn-1");
  await telegram.waitFor("sendMessage", (call) => call.params.text.startsWith("Codex is working"));
  const blocker = "Codex is currently working. Stop the active turn before switching services.";

  let mark = telegram.mark();
  telegram.say("/service claude");
  assert.deepEqual(await telegram.waitForShown(1, { mark }), [{
    method: "sendMessage",
    text: [
      "Service",
      "",
      "Active: Codex",
      "Status: working",
      "",
      "Mounted sessions",
      "* Codex: session thread-a",
      "  Claude: no mounted session",
      "",
      "Sessions belong to one service. Switching parks the current session and resumes the other service's own session.",
      "",
      blocker,
    ].join("\n"),
    buttons: [["Use Claude"], ["Close"]],
  }]);

  mark = telegram.mark();
  telegram.say("/workspace beta");
  assert.deepEqual(await telegram.waitForShown(1, { mark }), [{
    method: "sendMessage",
    text: [
      "Workspace",
      "",
      `Folder: ${alpha}`,
      "Status: working",
      `Root: ${alasio.workspaceRoot}`,
      "",
      "Sessions belong to one service and one folder. Switching folders parks the current sessions and restores the ones from the chosen folder.",
      "",
      "Type /workspace <name> to mount a folder under the root, or /workspace new <name> to create a git-initialized one.",
      "",
      blocker,
    ].join("\n"),
    buttons: [["* · alpha", "· beta"], ["New folder…", "Refresh", "Close"]],
  }]);

  // Still Codex in alpha: the turn finishes there, and so does the next.
  answerTurn(codex, "thread-a", "turn-1", "Fixed.");
  await telegram.waitFor("sendRichMessage");
  telegram.say("And the docs");
  await answerLoadedThreads(codex, ["thread-a"]);
  const next = await answerTurnStart(codex, "thread-a", "turn-2");
  assert.deepEqual([next.params.threadId, next.params.cwd], ["thread-a", alpha]);
});

test("switching folders parks the session under the folder it leaves and restores the one parked under the folder it mounts; so does switching services", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha", "beta"] });
  const { telegram, codex } = alasio;
  const alpha = alasio.folder("alpha");
  const beta = alasio.folder("beta");
  await mount(alasio, "codex", "alpha");
  telegram.say("Work in alpha");
  await answerThreadStart(codex, "thread-a", alpha);
  await answerTurnStart(codex, "thread-a", "turn-1");
  answerTurn(codex, "thread-a", "turn-1", "Done in alpha.");
  await telegram.waitFor("sendRichMessage");

  let mark = telegram.mark();
  telegram.say("/workspace beta");
  await telegram.waitFor("sendMessage", (call) => call.params.text.endsWith(`Switched to beta (${beta}).`), { mark });
  telegram.say("Work in beta");
  const betaThread = await answerThreadStart(codex, "thread-b", beta);
  assert.equal(betaThread.params.cwd, beta);
  await answerTurnStart(codex, "thread-b", "turn-2");
  answerTurn(codex, "thread-b", "turn-2", "Done in beta.");
  await telegram.waitFor("sendRichMessage", () => true, { mark });

  mark = telegram.mark();
  telegram.say("/workspace alpha");
  await telegram.waitFor("sendMessage", (call) => call.params.text.endsWith(`Switched to alpha (${alpha}).`), { mark });
  telegram.say("Back in alpha");
  await answerLoadedThreads(codex, ["thread-a", "thread-b"]);
  const restored = await answerTurnStart(codex, "thread-a", "turn-3");
  assert.deepEqual([restored.params.threadId, restored.params.cwd, inputText(restored)], ["thread-a", alpha, "Back in alpha"]);
  answerTurn(codex, "thread-a", "turn-3", "Done again.");
  await telegram.waitFor("sendRichMessage", () => true, { mark });

  mark = telegram.mark();
  telegram.say("/service claude");
  assert.deepEqual(await telegram.waitForShown(1, { mark }), [{
    method: "sendMessage",
    text: [
      "Service",
      "",
      "Active: Claude",
      "Status: idle",
      "",
      "Mounted sessions",
      "  Codex: session thread-a",
      "* Claude: no mounted session",
      "",
      "Sessions belong to one service. Switching parks the current session and resumes the other service's own session.",
      "",
      "Switched to Claude.",
    ].join("\n"),
    buttons: [["Use Codex"], ["Close"]],
  }]);
  mark = telegram.mark();
  telegram.say("/service codex");
  await telegram.waitFor("sendMessage", (call) => call.params.text.endsWith("Switched to Codex."), { mark });
  telegram.say("Codex again");
  await answerLoadedThreads(codex, ["thread-a", "thread-b"]);
  const back = await answerTurnStart(codex, "thread-a", "turn-4");
  assert.equal(back.params.threadId, "thread-a");
  assert.deepEqual(codex.requests("thread/start").map(({ params }) => params.cwd), [alpha, beta]);
});

test("/sessions lists the folder's threads from thread/list; a listed session is previewed and mounted, and the next prompt resumes it; New Session starts one", async (t) => {
  const alasio = await alasioFor(t, { folders: ["alpha"] });
  const { telegram, codex } = alasio;
  const alpha = alasio.folder("alpha");
  await mount(alasio, "codex", "alpha");
  codex.answerEvery("thread/list", () => ({
    data: [
      codexThread("thread-x", { name: "Refactor parser", updatedAt: 1_790_769_600, cwd: alpha }),
      codexThread("thread-y", { preview: "Add tests for the lexer", updatedAt: 1_790_683_200, cwd: alpha }),
    ],
    nextCursor: null,
    backwardsCursor: null,
  }));
  codex.answerEvery("thread/turns/list", ({ params }) => ({
    data: params.threadId === "thread-x"
      ? [codexTurn("turn-x1", { items: [userMessage("u1", [{ type: "text", text: "Refactor it", text_elements: [] }]), agentMessage("a1", "Parser refactored.", "final_answer")] })]
      : [],
    nextCursor: null,
    backwardsCursor: null,
  }));

  let mark = telegram.mark();
  telegram.say("/sessions");
  assert.deepEqual(await telegram.waitForShown(1, { mark }), [{
    method: "sendMessage",
    text: [
      "Sessions (Codex)",
      "",
      "Mounted",
      "No mounted Codex session.",
      "",
      "Recent sessions (page 1/1)",
      "1. 2026-09-30 - Refactor parser",
      "2. 2026-09-29 - Add tests for the lexer",
    ].join("\n"),
    buttons: [["Current Session", "New Session"], ["1. 2026-09-30 - Refactor parser"], ["2. 2026-09-29 - Add tests for the lexer"], ["Close"]],
  }]);
  assert.deepEqual(codex.requests("thread/list").map(({ params }) => params), [
    { cwd: alpha, sortKey: "updated_at", sourceKinds: ["cli", "vscode", "exec"], limit: 100 },
    { cwd: alpha, sortKey: "updated_at", sourceKinds: ["cli", "vscode", "exec"], limit: 100 },
  ]);

  mark = telegram.mark();
  telegram.press("1. 2026-09-30 - Refactor parser");
  assert.deepEqual(await telegram.waitForShown(2, { mark }), [
    { method: "answerCallbackQuery", text: "Working..." },
    {
      method: "editMessageText",
      text: "Session\n\nSession: thread-x\nMounted: no\n\nParser refactored.",
      buttons: [["Mount This Session"], ["Rewind", "Back"], ["Close"]],
    },
  ]);
  mark = telegram.mark();
  telegram.press("Mount This Session");
  assert.deepEqual(await telegram.waitForShown(2, { mark }), [
    { method: "answerCallbackQuery", text: "Working..." },
    {
      method: "editMessageText",
      text: "Current Session (Codex)\n\nSession: thread-x\nStatus: idle\n\nParser refactored.",
      buttons: [["Rewind", "Sessions"], ["New Session"], ["Close"]],
    },
  ]);

  telegram.say("Continue the refactor");
  await answerLoadedThreads(codex, []);
  const resume = await codex.next("thread/resume");
  assert.equal(resume.params.threadId, "thread-x");
  codex.answer(resume, threadResumeResponse(codexThread("thread-x", { cwd: alpha })));
  const turnStart = await answerTurnStart(codex, "thread-x", "turn-x2");
  assert.deepEqual([turnStart.params.threadId, inputText(turnStart)], ["thread-x", "Continue the refactor"]);
  answerTurn(codex, "thread-x", "turn-x2", "Continued.");
  await telegram.waitFor("sendRichMessage");

  mark = telegram.mark();
  telegram.press("New Session");
  const fresh = await codex.next("thread/start");
  codex.answer(fresh, threadStartResponse(codexThread("thread-new", { cwd: alpha })));
  assert.deepEqual(await telegram.waitForShown(2, { mark }), [
    { method: "answerCallbackQuery", text: "Working..." },
    {
      method: "editMessageText",
      text: "Current Session (Codex)\n\nSession: thread-n\nStatus: idle\n\nNo assistant message found yet.",
      buttons: [["Rewind", "Sessions"], ["New Session"], ["Close"]],
    },
  ]);
  telegram.say("Start over");
  await answerLoadedThreads(codex, ["thread-x", "thread-new"]);
  assert.equal((await answerTurnStart(codex, "thread-new", "turn-n1")).params.threadId, "thread-new");
});
