import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Effect, Layer } from "effect";

import { tomlRootString, operatorDeveloperInstructions } from "../src/codex/config-toml.ts";
import { ReplyMedia } from "../src/codex/reply-media.ts";
import { buildCodexThreadConfig } from "../src/codex/thread-config.ts";
import { buildClaudeQueryOptions } from "../src/harness/claude/runtime.ts";
import { REPLY_INSTRUCTIONS, withReplyInstructions } from "../src/harness/reply-instructions.ts";
import { SqliteStore, Store } from "../src/persistence/store.ts";
import type { SessionFilesystems } from "../src/sandbox/index.ts";
import type { MediaAttachment } from "../src/telegram/client.ts";
import { Outbox } from "../src/telegram/outbox.ts";
import { findMediaEmbeds, mediaIdsIn, placeMedia, sniffMedia, withoutMediaLines } from "../src/telegram/rich-media.ts";
import { toRichMarkdown } from "../src/telegram/rich-markdown.ts";
import { sessionFsWorkspace } from "../src/workspace/kind.ts";
import { type BotCall, botApiClient, botApiError, botApiLayer, paramsOf } from "./support/bot-api.ts";

const PNG = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 0, 0, 0, 0, 0]);
const JPEG = Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
const MP4 = Buffer.concat([Buffer.from([0, 0, 0, 0x20]), Buffer.from("ftypisom"), Buffer.alloc(4)]);
const GIF = Buffer.concat([Buffer.from("GIF89a"), Buffer.alloc(10)]);

function withDir<T>(fn: (dir: string) => T | Promise<T>): Promise<T> {
  const dir = mkdtempSync(join(tmpdir(), "alasio-reply-media-"));
  return Promise.resolve(fn(dir)).finally(() => rmSync(dir, { recursive: true, force: true }));
}

test("embeds are image syntax with a local path, outside code", () => {
  const md = [
    "Here ![a render](out/render.png) and ![](~/shot.jpg).",
    "Not these: `![x](in-code.png)`, ![remote](https://example.com/x.png), ![tg](tg://photo?id=m1).",
    "```md",
    "![fenced](fenced.png)",
    "```",
    "Just a path: `/tmp/named.png`",
  ].join("\n");
  assert.deepEqual(findMediaEmbeds(md), [
    { caption: "a render", path: "out/render.png" },
    { caption: "", path: "~/shot.jpg" },
  ]);
});

test("media go below the block that shows them, one block each or a collage", () => {
  const md = [
    "Intro.",
    "",
    "- **Film:** ![seesaw](a.mp4), 6 seconds",
    "- another item",
    "",
    "![](b.png) ![Grid two](c.png)",
    "",
    "Missing ![gone](nope.png) and again ![seesaw](a.mp4).",
  ].join("\n");
  const placed = placeMedia(md, [
    { id: "m1", kind: "video", caption: "seesaw" },
    { id: "m2", kind: "photo", caption: "" },
    { id: "m3", kind: "photo", caption: "Grid two" },
    { note: "file not found" },
    { duplicateOf: "m1" },
  ]);
  assert.equal(placed, [
    "Intro.",
    "",
    "- **Film:** seesaw, 6 seconds",
    "- another item",
    "",
    '![](tg://video?id=m1 "seesaw")',
    "",
    "<tg-collage>",
    "![](tg://photo?id=m2)",
    '![](tg://photo?id=m3 "Grid two")',
    "</tg-collage>",
    "",
    "Missing gone *(not attached: file not found)* and again seesaw.",
  ].join("\n"));
  assert.deepEqual(mediaIdsIn(placed), ["m1", "m2", "m3"]);
  // The escaper leaves alasio's media lines alone and still escapes the prose around them.
  assert.equal(toRichMarkdown(`${placed}\n\nCosts $5`).split("\n").filter((l) => /tg:\/\/|tg-collage/.test(l)).length, 5);
  assert.match(toRichMarkdown("Costs $5"), /\\\$5/);
  assert.doesNotMatch(withoutMediaLines(placed), /tg:\/\/|tg-collage/);
});

test("a file is identified by its bytes, not its name", () => {
  assert.deepEqual(sniffMedia(PNG), { kind: "photo", ext: "png" });
  assert.deepEqual(sniffMedia(JPEG), { kind: "photo", ext: "jpg" });
  assert.deepEqual(sniffMedia(MP4), { kind: "video", ext: "mp4" });
  assert.deepEqual(sniffMedia(GIF), { kind: "video", ext: "gif", animation: true });
  assert.equal(sniffMedia(Buffer.from("just some text here")), null);
});

test("in a folder workspace, files resolve against it and are copied until delivery", () => withDir(async (dir) => {
  const workspace = join(dir, "ws");
  mkdirSync(join(workspace, "out"), { recursive: true });
  writeFileSync(join(workspace, "out", "render.png"), PNG);
  writeFileSync(join(workspace, "notes.txt"), "not media at all");
  writeFileSync(join(workspace, "huge.png"), Buffer.concat([PNG, Buffer.alloc(10 * 1024 * 1024)]));
  const media = new ReplyMedia({ stateDir: join(dir, "state"), workspaceForChat: () => workspace });
  const { text, options } = await media.prepare({
    chatId: 1,
    key: "resp-1",
    text: "![render](out/render.png)\n\n![n](notes.txt) ![h](huge.png) ![m](missing.png) ![again](out/render.png)",
  });
  assert.equal(options.format, "rich");
  assert.equal(options.media?.length, 1);
  const [item] = options.media ?? [];
  assert.ok(item);
  assert.deepEqual({ id: item.id, kind: item.kind }, { id: "m1", kind: "photo" });
  assert.equal(options.mediaDir, join(dir, "state", "reply-media", "resp-1"));
  assert.deepEqual(readFileSync(item.file), PNG); // a copy, not the original
  assert.equal(statSync(item.file).mode & 0o777, 0o600);
  assert.match(text, /^!\[\]\(tg:\/\/photo\?id=m1 "render"\)$/m);
  assert.match(text, /n \*\(not attached: not an image or video\)\*/);
  assert.match(text, /h \*\(not attached: 10\.0 MB, over Telegram's 10 MB photo limit\)\*/);
  assert.match(text, /m \*\(not attached: file not found\)\*/);
  assert.match(text, / again$/m); // a repeat keeps its caption, not a second copy

  const plain = await media.prepare({ chatId: 1, key: "resp-2", text: "No media, `just/a.png` named." });
  assert.deepEqual(plain, { text: "No media, `just/a.png` named.", options: { format: "rich" } });
}));

test("a reply carries at most ten media", () => withDir(async (dir) => {
  for (let i = 0; i < 12; i += 1) writeFileSync(join(dir, `${i}.png`), PNG);
  const media = new ReplyMedia({ stateDir: join(dir, "state"), workspaceForChat: () => dir });
  const embeds = Array.from({ length: 12 }, (_, i) => `![${i}](${i}.png)`).join("\n\n");
  const { text, options } = await media.prepare({ chatId: 1, key: "k", text: embeds });
  assert.equal(options.media?.length, 10);
  assert.equal((text.match(/not attached: more than 10 in one reply/g) ?? []).length, 2);
}));

test("in a session filesystem, files are read through the sandbox, never from the host", () => withDir(async (dir) => {
  const reads: { volumeId: string; path: string; maxBytes: number }[] = [];
  const sandbox: Pick<SessionFilesystems, "readFile"> = {
    readFile: async (volumeId, path, maxBytes) => {
      reads.push({ volumeId, path, maxBytes });
      return path === "/workspace/out.mp4" ? { bytes: MP4 } : { note: "file not found" };
    },
  };
  const media = new ReplyMedia({ stateDir: dir, workspaceForChat: () => sessionFsWorkspace("fs-abc123"), sandbox });
  const { text, options } = await media.prepare({
    chatId: 1,
    key: "k",
    text: "![film](/workspace/out.mp4) and ![host file](/etc/hostname) and ![home](~/pic.png)",
  });
  assert.deepEqual(reads.map((r) => r.path), ["/workspace/out.mp4", "/etc/hostname", "/home/agent/pic.png"]);
  assert.ok(reads.every((r) => r.volumeId === "fs-abc123"));
  assert.equal(options.media?.length, 1);
  assert.equal(options.media[0]?.kind, "video");
  assert.match(text, /host file \*\(not attached: file not found\)\*/); // the sandbox has no such file
}));

test("a rich part uploads its media with it; when rejected, text then media go the classic way", () => withDir(async (dir) => {
  const photo = join(dir, "m1.png");
  const video = join(dir, "m2.mp4");
  const gif = join(dir, "m3.gif");
  writeFileSync(photo, PNG);
  writeFileSync(video, MP4);
  writeFileSync(gif, GIF);
  const media: MediaAttachment[] = [
    { id: "m1", kind: "photo", animation: false, file: photo },
    { id: "m2", kind: "video", animation: false, file: video },
    { id: "m3", kind: "video", animation: true, file: gif },
  ];
  const text = "Look:\n\n<tg-collage>\n![](tg://photo?id=m1)\n![](tg://video?id=m2)\n</tg-collage>\n\n![](tg://video?id=m3 \"loop\")";
  const calls: BotCall[] = [];
  let reject = false;
  const client = await botApiClient(async (call) => {
    calls.push(call);
    if (reject && call.files && call.method === "sendRichMessage") {
      throw botApiError("Bad Request", 400);
    }
    return call.method === "sendMediaGroup" ? [{ message_id: 1 }, { message_id: 2 }] : { message_id: calls.length };
  });

  await Effect.runPromise(client.sendMessage(7, text, { format: "rich", media, mediaDir: dir }));
  assert.equal(calls.length, 1);
  const [rich] = calls;
  const richParams = paramsOf(rich, "sendRichMessage");
  assert.deepEqual(richParams.rich_message.media?.map((m) => [m.id, m.media.type, m.media.media]), [
    ["m1", "photo", "attach://m1"],
    ["m2", "video", "attach://m2"],
    ["m3", "animation", "attach://m3"],
  ]);
  assert.deepEqual(rich?.files, ["m1", "m2", "m3"]);
  assert.equal("mediaDir" in richParams, false); // alasio's own options stay home
  assert.equal("media" in richParams, false);

  calls.length = 0;
  reject = true;
  await Effect.runPromise(client.sendMessage(7, text, { format: "rich", media, mediaDir: dir }));
  assert.deepEqual(calls.map((c) => c.method), ["sendRichMessage", "sendMessage", "sendAnimation", "sendMediaGroup"]);
  assert.doesNotMatch(paramsOf(calls[1], "sendMessage").text, /tg:\/\/|tg-collage/);
  const album = paramsOf(calls[3], "sendMediaGroup");
  assert.equal(album.disable_notification, true);
  assert.deepEqual(album.media.map((m) => m.type), ["photo", "video"]);
}));

test("a sent reply's media copies are deleted", () => withDir(async (dir) => {
  const mediaDir = join(dir, "reply-media", "k");
  mkdirSync(mediaDir, { recursive: true });
  writeFileSync(join(mediaDir, "m1.png"), PNG);
  const store = new SqliteStore(dir);
  try {
    const telegram = botApiLayer(async (call) => {
      assert.equal(call.method, "sendRichMessage");
      return { message_id: 1 };
    });
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const outbox = yield* Outbox;
      yield* outbox.enqueueText({ chatId: "7", text: "x", options: { format: "rich", mediaDir } });
      yield* outbox.deliverDue;
    })).pipe(Effect.provide(Outbox.layer.pipe(Layer.provide([telegram, Layer.succeed(Store, store)])))));
    assert.equal(store.getPendingOutboxCount(), 0);
    assert.equal(existsSync(mediaDir), false);
  } finally {
    store.close();
  }
}));

test("the operator's own Codex developer instructions are read in every TOML string form", () => {
  assert.equal(tomlRootString('developer_instructions = "Be \\"brief\\".\\nThanks"', "developer_instructions"), 'Be "brief".\nThanks');
  assert.equal(tomlRootString("developer_instructions = 'C:\\raw\\path'", "developer_instructions"), "C:\\raw\\path");
  assert.equal(tomlRootString('developer_instructions = """\nline one\nline \\\n   two"""', "developer_instructions"), "line one\nline two");
  assert.equal(tomlRootString("developer_instructions = '''\nkeep \\n literal'''", "developer_instructions"), "keep \\n literal");
  assert.equal(tomlRootString('model = "x"\n[profile.a]\ndeveloper_instructions = "not root"', "developer_instructions"), null);
  assert.equal(tomlRootString("developer_instructions = 42", "developer_instructions"), null);
  return withDir((dir) => {
    assert.equal(operatorDeveloperInstructions(dir), null); // no config at all
    writeFileSync(join(dir, "config.toml"), 'developer_instructions = "Mine first."\n');
    assert.equal(operatorDeveloperInstructions(dir), "Mine first.");
  });
});

test("both harnesses are told how to show media, Codex after the operator's own instructions", () => withDir((dir) => {
  assert.equal(withReplyInstructions(null), REPLY_INSTRUCTIONS);
  assert.equal(withReplyInstructions("Mine."), `Mine.\n\n${REPLY_INSTRUCTIONS}`);
  assert.match(REPLY_INSTRUCTIONS, /!\[short caption\]\(path\)/);

  writeFileSync(join(dir, "config.toml"), 'developer_instructions = "Operator rule."\n');
  const config = buildCodexThreadConfig({ codexEnv: { CODEX_HOME: dir, HOME: dir }, bayma: { url: "http://bayma.alasio-host.svc:7290/mcp", headers: {} } });
  assert.equal(config.developer_instructions, `Operator rule.\n\n${REPLY_INSTRUCTIONS}`);

  const claude = buildClaudeQueryOptions({ workingDirectory: dir, claudeEnv: {}, mcpServers: {}, controller: new AbortController(), hooks: {} });
  assert.deepEqual(claude.systemPrompt, { type: "preset", preset: "claude_code", append: REPLY_INSTRUCTIONS });
}));
