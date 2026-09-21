import assert from "node:assert/strict";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import {
  provisionBreadbutterRustCapability,
  resolveBreadbutterRustCapability,
  runBreadbutterRustCapability,
} from "../src/mcp/breadbutter-rust-capability.js";

async function makeCapabilityFixture() {
  const root = await mkdtemp(join(tmpdir(), "alasio-breadbutter-rust-capability-"));
  const skillRoot = join(root, ".agents/skills/monorepo-breadbutter");
  const crateRoot = join(skillRoot, "rust/monorepo-breadbutter");
  await mkdir(join(crateRoot, "src"), { recursive: true });
  const rustSources = [
    "git.rs",
    "hooks.rs",
    "lib.rs",
    "process.rs",
  ];
  await Promise.all([
    writeFile(join(skillRoot, "SKILL.md"), "# fixture\n", "utf8"),
    writeFile(join(skillRoot, "Cargo.toml"), '[package]\nname = "package-set"\n', "utf8"),
    writeFile(join(skillRoot, "Cargo.lock"), "version = 4\n", "utf8"),
    writeFile(
      join(skillRoot, "provision-rust-workbench.mjs"),
      'export async function provisionRustWorkbench() { return { marker: "first" }; }\n',
      "utf8",
    ),
    writeFile(join(skillRoot, "rust-toolchain.toml"), '[toolchain]\nchannel = "1.97.1"\n', "utf8"),
    writeFile(join(crateRoot, "Cargo.lock"), "version = 4\n", "utf8"),
    writeFile(join(crateRoot, "Cargo.toml"), '[package]\nname = "fixture"\n', "utf8"),
    ...rustSources.map((name) => writeFile(join(crateRoot, "src", name), "// fixture\n", "utf8")),
  ]);
  return {
    root,
    skillRoot,
    cleanup: async () => await rm(root, { recursive: true, force: true }),
  };
}

test("Rust capability resolves only for a complete unified Bayma REPL skill", async () => {
  const fixture = await makeCapabilityFixture();
  try {
    const serverConfig = { command: "bayma-repl", args: ["mcp-stdio"] };
    const capability = await resolveBreadbutterRustCapability({
      serverName: "bayma_repl",
      serverConfig,
      workingDirectory: fixture.root,
    });

    assert.equal(capability.repoRoot, await realpath(fixture.root));
    assert.equal(capability.skillRoot, fixture.skillRoot);
    assert.equal(capability.rustVersion, "1.97.1");
    assert.match(capability.fingerprint, /^[0-9a-f]{64}$/u);
    assert.match(capability.bridgeCode, /:dep qi_mono_breadbutter/u);
    assert.match(capability.code, /open_qi_mono_breadbutter/u);
    assert.doesNotMatch(capability.code, /packages::/u);
    assert.doesNotMatch(capability.code, /operations/u);

    await writeFile(join(fixture.skillRoot, "Cargo.lock"), "version = 4\n# changed\n", "utf8");
    const changed = await resolveBreadbutterRustCapability({
      serverName: "bayma_repl",
      serverConfig,
      workingDirectory: fixture.root,
    });
    assert.notEqual(changed.fingerprint, capability.fingerprint);

    assert.equal(
      await resolveBreadbutterRustCapability({
        serverName: "other_mcp",
        serverConfig: { command: "other-mcp", args: ["serve"] },
        workingDirectory: fixture.root,
      }),
      null,
    );

    await rm(join(fixture.skillRoot, "Cargo.lock"));
    await assert.rejects(
      resolveBreadbutterRustCapability({
        serverName: "bayma_repl",
        serverConfig,
        workingDirectory: fixture.root,
      }),
      /capability is incomplete/u,
    );
  } finally {
    await fixture.cleanup();
  }
});

test("Rust capability reloads a changed provisioner by capability fingerprint", async () => {
  const fixture = await makeCapabilityFixture();
  try {
    const input = {
      serverName: "bayma_repl",
      serverConfig: { command: "bayma-repl", args: ["mcp-stdio"] },
      workingDirectory: fixture.root,
    };
    const first = await resolveBreadbutterRustCapability(input);
    assert.equal((await provisionBreadbutterRustCapability(first)).marker, "first");

    await writeFile(
      join(fixture.skillRoot, "provision-rust-workbench.mjs"),
      'export async function provisionRustWorkbench() { return { marker: "second" }; }\n',
      "utf8",
    );
    const second = await resolveBreadbutterRustCapability(input);
    assert.notEqual(second.fingerprint, first.fingerprint);
    assert.equal((await provisionBreadbutterRustCapability(second)).marker, "second");
  } finally {
    await fixture.cleanup();
  }
});

test("Rust capability waits, verifies evidence, and closes its session", async () => {
  const fixture = await makeCapabilityFixture();
  try {
    const capability = await resolveBreadbutterRustCapability({
      serverName: "bayma_repl",
      serverConfig: { command: "bayma-repl", args: ["mcp-stdio"] },
      workingDirectory: fixture.root,
    });
    const calls = [];
    const evidence = {
      kind: "monorepo-breadbutter-rust",
      repo_root: capability.repoRoot,
      rust: "1.97.1",
    };
    const client = {
      async callTool(request) {
        calls.push(request);
        if (request.name === "session.create") {
          return { structuredContent: { session: { session_id: "sess_rust_probe" } } };
        }
        if (request.name === "exec") {
          const bridgeLoad = request.arguments.code === capability.bridgeCode;
          return {
            structuredContent: {
              done: false,
              exec_id: bridgeLoad
                ? "exec_bridge"
                : "exec_readiness",
              next_seq: 1,
            },
          };
        }
        if (request.name === "wait") {
          return {
            structuredContent: {
              done: true,
              status: "ok",
              stdout_text: request.arguments.exec_id === "exec_readiness"
                ? `${JSON.stringify(evidence)}\n`
                : "",
            },
          };
        }
        return { structuredContent: { session: { status: "closed" } } };
      },
    };

    assert.deepEqual(await runBreadbutterRustCapability(client, capability), evidence);
    assert.deepEqual(
      calls.map(({ name }) => name),
      [
        "session.create",
        "exec",
        "wait",
        "exec",
        "wait",
        "session.close",
      ],
    );
    assert.equal(calls[0].arguments.runtime, "rust");
    assert.equal(calls[0].arguments.cwd, capability.repoRoot);
  } finally {
    await fixture.cleanup();
  }
});

test("Rust capability closes its session after invalid evidence", async () => {
  const fixture = await makeCapabilityFixture();
  try {
    const capability = await resolveBreadbutterRustCapability({
      serverName: "bayma_repl",
      serverConfig: { command: "bayma-repl", args: ["mcp-stdio"] },
      workingDirectory: fixture.root,
    });
    const calls = [];
    let execCount = 0;
    const client = {
      async callTool(request) {
        calls.push(request.name);
        if (request.name === "session.create") {
          return { structuredContent: { session: { session_id: "sess_rust_probe" } } };
        }
        if (request.name === "exec") {
          execCount += 1;
          return {
            structuredContent: {
              done: true,
              status: "ok",
              stdout_text: execCount < 2 ? "" : "not-json\n",
            },
          };
        }
        return { structuredContent: { session: { status: "closed" } } };
      },
    };

    await assert.rejects(runBreadbutterRustCapability(client, capability), /invalid readiness JSON/u);
    assert.deepEqual(
      calls,
      ["session.create", "exec", "exec", "session.close"],
    );
  } finally {
    await fixture.cleanup();
  }
});
