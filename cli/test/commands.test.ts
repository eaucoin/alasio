import assert from "node:assert/strict";
import { test } from "node:test";

import { NodeServices } from "@effect/platform-node";
import { Effect } from "effect";
import { Command } from "effect/cli";

import { alasio } from "../src/commands.ts";

test("alasio answers --version with this package's version", async () => {
  const lines: string[] = [];
  const original = console.log;
  console.log = (line: unknown) => void lines.push(String(line));
  try {
    await Effect.runPromise(Command.runWith(alasio, { version: "9.9.9" })(["--version"]).pipe(Effect.provide(NodeServices.layer)));
  } finally {
    console.log = original;
  }
  assert.match(lines.join("\n"), /9\.9\.9/u);
});
