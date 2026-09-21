import assert from "node:assert/strict";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import {
  resolveBreadbutterPythonCapability,
  runBreadbutterPythonCapability,
} from "../src/mcp/breadbutter-python-capability.js";

async function makeCapabilityFixture() {
  const root = await mkdtemp(join(tmpdir(), "alasio-breadbutter-capability-"));
  const skillRoot = join(root, ".agents/skills/monorepo-breadbutter");
  await mkdir(skillRoot, { recursive: true });
  await Promise.all([
    writeFile(join(skillRoot, "SKILL.md"), "# fixture\n", "utf8"),
    writeFile(
      join(skillRoot, "pyproject.toml"),
      '[project]\nrequires-python = ">=3.12,<3.13"\n',
      "utf8",
    ),
    writeFile(
      join(skillRoot, "qi_mono_breadbutter.py"),
      "# fixture\n",
      "utf8",
    ),
    writeFile(
      join(skillRoot, "uv.lock"),
      'version = 1\nrequires-python = "==3.12.*"\n',
      "utf8",
    ),
  ]);
  return {
    root,
    skillRoot,
    cleanup: async () => await rm(root, { recursive: true, force: true }),
  };
}

test("Breadbutter capability resolves only for a complete unified Bayma REPL skill", async () => {
  const fixture = await makeCapabilityFixture();
  try {
    const serverConfig = {
      command: "bayma-repl",
      args: ["mcp-stdio"],
    };
    const capability = await resolveBreadbutterPythonCapability({
      serverName: "bayma_repl",
      serverConfig,
      workingDirectory: fixture.root,
    });

    assert.equal(capability.repoRoot, await realpath(fixture.root));
    assert.equal(capability.skillRoot, fixture.skillRoot);
    assert.equal(capability.supportedMinor, "3.12");
    assert.equal(
      capability.expectedSitePackages,
      `${fixture.skillRoot}/.venv/lib/python3.12/site-packages`,
    );
    assert.match(capability.fingerprint, /^[0-9a-f]{64}$/u);
    assert.match(capability.code, /open_qi_mono_breadbutter/u);

    await writeFile(
      join(fixture.skillRoot, "qi_mono_breadbutter.py"),
      "# changed fixture\n",
      "utf8",
    );
    const changedCapability = await resolveBreadbutterPythonCapability({
      serverName: "bayma_repl",
      serverConfig,
      workingDirectory: fixture.root,
    });
    assert.notEqual(changedCapability.fingerprint, capability.fingerprint);

    await writeFile(
      join(fixture.skillRoot, "pyproject.toml"),
      "[project]\nrequires-python = '>=3.12,<3.13' # equivalent TOML\n",
      "utf8",
    );
    const restyledCapability = await resolveBreadbutterPythonCapability({
      serverName: "bayma_repl",
      serverConfig,
      workingDirectory: fixture.root,
    });
    assert.equal(restyledCapability.supportedMinor, "3.12");

    assert.equal(
      await resolveBreadbutterPythonCapability({
        serverName: "other_mcp",
        serverConfig: { command: "other-mcp", args: ["serve"] },
        workingDirectory: fixture.root,
      }),
      null,
    );

    await rm(join(fixture.skillRoot, "uv.lock"));
    await assert.rejects(
      resolveBreadbutterPythonCapability({
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

test("Breadbutter capability waits, verifies evidence, and closes its session", async () => {
  const fixture = await makeCapabilityFixture();
  try {
    const capability = await resolveBreadbutterPythonCapability({
      serverName: "bayma_repl",
      serverConfig: { command: "bayma-repl", args: ["mcp-stdio"] },
      workingDirectory: fixture.root,
    });
    const calls = [];
    const evidence = {
      kind: "monorepo-breadbutter-python",
      packages: 23,
      python: "3.12.13",
      repo_root: capability.repoRoot,
      site_packages: `${capability.skillRoot}/.venv/lib/python3.12/site-packages`,
    };
    const client = {
      async callTool(request) {
        calls.push(request);
        switch (request.name) {
          case "session.create":
            return { structuredContent: { session: { session_id: "sess_probe" } } };
          case "exec":
            return {
              structuredContent: {
                done: false,
                exec_id: "exec_probe",
                next_seq: 1,
              },
            };
          case "wait":
            return {
              structuredContent: {
                done: true,
                status: "ok",
                stdout_text: `${JSON.stringify(evidence)}\n`,
              },
            };
          case "session.close":
            return { structuredContent: { session: { status: "closed" } } };
          default:
            throw new Error(`unexpected tool ${request.name}`);
        }
      },
    };

    assert.deepEqual(
      await runBreadbutterPythonCapability(client, capability),
      evidence,
    );
    assert.deepEqual(
      calls.map(({ name }) => name),
      ["session.create", "exec", "wait", "session.close"],
    );
    assert.equal(calls[0].arguments.runtime, "python");
    assert.equal(calls[0].arguments.cwd, capability.repoRoot);
    assert.equal(calls[1].arguments.session_id, "sess_probe");
    assert.equal(calls[2].arguments.from_seq, 1);
    assert.equal(calls[3].arguments.session_id, "sess_probe");
  } finally {
    await fixture.cleanup();
  }
});

test("Breadbutter capability closes its session after invalid evidence", async () => {
  const fixture = await makeCapabilityFixture();
  try {
    const capability = await resolveBreadbutterPythonCapability({
      serverName: "bayma_repl",
      serverConfig: { command: "bayma-repl", args: ["mcp-stdio"] },
      workingDirectory: fixture.root,
    });
    const calls = [];
    const client = {
      async callTool(request) {
        calls.push(request.name);
        if (request.name === "session.create") {
          return { structuredContent: { session: { session_id: "sess_probe" } } };
        }
        if (request.name === "exec") {
          return {
            structuredContent: {
              done: true,
              status: "ok",
              stdout_text: "not-json\n",
            },
          };
        }
        return { structuredContent: { session: { status: "closed" } } };
      },
    };

    await assert.rejects(
      runBreadbutterPythonCapability(client, capability),
      /invalid readiness JSON/u,
    );
    assert.deepEqual(calls, ["session.create", "exec", "session.close"]);
  } finally {
    await fixture.cleanup();
  }
});
