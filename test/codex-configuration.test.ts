/**
 * The operator's Codex configuration as a harness pool's pod's folder conversations' Codex is
 * given it (src/codex/configuration.ts): what of their Codex home configures Codex, by path, as
 * text; never their login, nor what Codex keeps of its own, nor what a link points to; each file
 * of its most, and all of them of theirs.
 */
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Effect } from "effect";

import { codexConfiguration, MAX_FILE_BYTES, MAX_TOTAL_BYTES } from "../src/codex/configuration.ts";

/** A Codex home of the test's own, made by `make`, given to `use`, then removed. */
async function inHome(make: (home: string) => void, use: (home: string) => Promise<void>): Promise<void> {
  const home = mkdtempSync(join(tmpdir(), "alasio-codex-configuration-"));
  try {
    make(home);
    await use(home);
  } finally {
    rmSync(home, { recursive: true, force: true });
  }
}

test("a Codex home's configuration is its config.toml, AGENTS.md, prompts and skills, by path, and none of its login or Codex's own", async () => {
  await inHome((home) => {
    writeFileSync(join(home, "config.toml"), 'model = "gpt-5"\n');
    writeFileSync(join(home, "AGENTS.md"), "Be brief.\n");
    mkdirSync(join(home, "prompts"));
    writeFileSync(join(home, "prompts", "review.md"), "Review it.\n");
    mkdirSync(join(home, "skills", "release"), { recursive: true });
    writeFileSync(join(home, "skills", "release", "SKILL.md"), "Release it.\n");
    writeFileSync(join(home, "auth.json"), '{"tokens":{}}');
    mkdirSync(join(home, "sessions"));
    writeFileSync(join(home, "sessions", "rollout.jsonl"), "{}\n");
  }, async (home) => {
    assert.deepEqual(await Effect.runPromise(codexConfiguration(home)), {
      "config.toml": 'model = "gpt-5"\n',
      "AGENTS.md": "Be brief.\n",
      "prompts/review.md": "Review it.\n",
      "skills/release/SKILL.md": "Release it.\n",
    });
  });
});

test("a link in the configuration is passed over, a file or a directory it points to read of none, the login among them", async () => {
  await inHome((home) => {
    writeFileSync(join(home, "auth.json"), '{"tokens":{}}');
    mkdirSync(join(home, "skills"));
    writeFileSync(join(home, "skills", "kept.md"), "Kept.\n");
    symlinkSync(join(home, "auth.json"), join(home, "skills", "login.md"));
    symlinkSync("/etc", join(home, "skills", "machine"));
    symlinkSync(join(home, "auth.json"), join(home, "AGENTS.md"));
  }, async (home) => {
    assert.deepEqual(await Effect.runPromise(codexConfiguration(home)), { "skills/kept.md": "Kept.\n" });
  });
});

test("a file over its most is left out, and so is each that would take them all over theirs", async () => {
  await inHome((home) => {
    writeFileSync(join(home, "config.toml"), "x".repeat(MAX_FILE_BYTES + 1));
    mkdirSync(join(home, "skills"));
    // Of a skill's files, in their names' order, those that fit in all.
    const each = MAX_FILE_BYTES;
    const fitting = Math.floor(MAX_TOTAL_BYTES / each);
    for (let n = 0; n <= fitting; n += 1) writeFileSync(join(home, "skills", `part-${String(n).padStart(2, "0")}.md`), "y".repeat(each));
  }, async (home) => {
    const given = await Effect.runPromise(codexConfiguration(home));
    assert.equal(given["config.toml"], undefined);
    const parts = Object.keys(given).toSorted();
    assert.equal(parts.length, Math.floor(MAX_TOTAL_BYTES / MAX_FILE_BYTES));
    assert.ok(Object.values(given).reduce((total, text) => total + text.length, 0) <= MAX_TOTAL_BYTES);
    assert.equal(parts.at(-1), `skills/part-${String(parts.length - 1).padStart(2, "0")}.md`);
  });
});
