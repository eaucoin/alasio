import { createHash } from "node:crypto";
import { readFile, realpath } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { parse as parseToml } from "smol-toml";

import { invokesBaymaReplServer } from "./bayma-state.js";

const BREADBUTTER_RELATIVE_ROOT = ".agents/skills/monorepo-breadbutter";
const CAPABILITY_FILES = [
  "SKILL.md",
  "Cargo.toml",
  "Cargo.lock",
  "provision-rust-workbench.mjs",
  "rust-toolchain.toml",
  "rust/monorepo-breadbutter/Cargo.lock",
  "rust/monorepo-breadbutter/Cargo.toml",
  "rust/monorepo-breadbutter/src/git.rs",
  "rust/monorepo-breadbutter/src/hooks.rs",
  "rust/monorepo-breadbutter/src/lib.rs",
  "rust/monorepo-breadbutter/src/process.rs",
];
const MAX_WAIT_POLLS = 300;

function buildBridgeCode() {
  return ':dep qi_mono_breadbutter = { package = "monorepo-breadbutter-rust-bridge", path = ".agents/skills/monorepo-breadbutter/rust/monorepo-breadbutter" }';
}

function buildCapabilityCode() {
  return [
    "use qi_mono_breadbutter::{GitHooks, NativeGit, MonorepoRustContext, open_qi_mono_breadbutter};",
    "let monorepo: MonorepoRustContext = open_qi_mono_breadbutter(std::env::current_dir().unwrap()).unwrap();",
    "let repo_root: std::path::PathBuf = monorepo.repo_root().to_path_buf();",
    'println!("{}", monorepo.readiness_json());',
  ].join("\n");
}

function structuredContent(result, label) {
  const structured = result?.structuredContent;
  if (!structured || typeof structured !== "object" || Array.isArray(structured)) {
    throw new Error(`${label} returned no structured content`);
  }
  return structured;
}

function parseReadinessEvidence(snapshot) {
  const stdout = typeof snapshot.stdout_text === "string" ? snapshot.stdout_text : "";
  const line = stdout.split(/\r?\n/u).findLast((candidate) => candidate.trim());
  if (!line) {
    throw new Error("Breadbutter Rust capability emitted no readiness evidence");
  }
  try {
    const evidence = JSON.parse(line);
    if (!evidence || typeof evidence !== "object" || Array.isArray(evidence)) {
      throw new Error("evidence must be an object");
    }
    return evidence;
  } catch (error) {
    throw new Error(
      `Breadbutter Rust capability emitted invalid readiness JSON: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

function parseRustVersion(toolchainSource) {
  let parsed;
  try {
    parsed = parseToml(toolchainSource);
  } catch (error) {
    throw new Error(
      `monorepo Breadbutter rust-toolchain.toml is invalid: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  const channel = parsed.toolchain?.channel;
  if (typeof channel !== "string" || !/^\d+\.\d+\.\d+$/u.test(channel)) {
    throw new Error("monorepo Breadbutter must pin one exact Rust toolchain");
  }
  return channel;
}

export async function resolveBreadbutterRustCapability({
  serverName,
  serverConfig,
  workingDirectory,
}) {
  if (!workingDirectory || !invokesBaymaReplServer(serverName, serverConfig)) {
    return null;
  }
  const repoRoot = await realpath(workingDirectory);
  const skillRoot = join(repoRoot, BREADBUTTER_RELATIVE_ROOT);
  let sources;
  try {
    sources = await Promise.all(CAPABILITY_FILES.map(async (relativePath) => ({
      relativePath,
      contents: await readFile(join(skillRoot, relativePath)),
    })));
  } catch (error) {
    if (error?.code === "ENOENT" && error?.path?.endsWith("SKILL.md")) {
      return null;
    }
    throw new Error(
      `monorepo Breadbutter Rust capability is incomplete under ${skillRoot}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  const toolchainSource = sources
    .find(({ relativePath }) => relativePath === "rust-toolchain.toml")
    .contents
    .toString("utf8");
  const rustVersion = parseRustVersion(toolchainSource);
  const fingerprint = createHash("sha256");
  for (const { relativePath, contents } of sources) {
    fingerprint.update(relativePath);
    fingerprint.update("\0");
    fingerprint.update(contents);
    fingerprint.update("\0");
  }
  return {
    bridgeCode: buildBridgeCode(),
    code: buildCapabilityCode(),
    fingerprint: fingerprint.digest("hex"),
    repoRoot,
    rustVersion,
    baymaCommand: serverConfig.command,
    skillRoot,
  };
}

async function runCapabilityCell(client, sessionId, code, label) {
  let snapshot = structuredContent(
    await client.callTool({
      name: "exec",
      arguments: {
        session_id: sessionId,
        code,
        yield_time_ms: 5_000,
      },
    }),
    `Breadbutter Rust ${label} exec`,
  );
  for (let poll = 0; snapshot.done !== true; poll += 1) {
    if (poll >= MAX_WAIT_POLLS) {
      throw new Error(
        `Breadbutter Rust ${label} did not settle after ${MAX_WAIT_POLLS} waits`,
      );
    }
    snapshot = structuredContent(
      await client.callTool({
        name: "wait",
        arguments: {
          session_id: sessionId,
          exec_id: snapshot.exec_id,
          from_seq: snapshot.next_seq,
          yield_time_ms: 1_000,
        },
      }),
      `Breadbutter Rust ${label} wait`,
    );
  }
  if (snapshot.status !== "ok") {
    const detail = snapshot.error_text || snapshot.stderr_text || "no error text";
    throw new Error(
      `Breadbutter Rust ${label} ended with ${snapshot.status}: ${detail}`,
    );
  }
  return snapshot;
}

export async function provisionBreadbutterRustCapability(capability) {
  const provisionerUrl = pathToFileURL(
    join(capability.skillRoot, "provision-rust-workbench.mjs"),
  );
  provisionerUrl.searchParams.set("fingerprint", capability.fingerprint);
  const provisioner = await import(provisionerUrl.href);
  if (typeof provisioner.provisionRustWorkbench !== "function") {
    throw new Error("Breadbutter Rust provisioner exports no provisioning function");
  }
  return await provisioner.provisionRustWorkbench({
    baymaCommand: capability.baymaCommand,
    skillRoot: capability.skillRoot,
  });
}

export async function runBreadbutterRustCapability(client, capability) {
  const created = structuredContent(
    await client.callTool({
      name: "session.create",
      arguments: {
        runtime: "rust",
        cwd: capability.repoRoot,
        title: "alasio-breadbutter-rust-preflight",
      },
    }),
    "Breadbutter Rust session.create",
  );
  const sessionId = created.session?.session_id;
  if (typeof sessionId !== "string" || !sessionId) {
    throw new Error("Breadbutter Rust session.create returned no session id");
  }
  try {
    await runCapabilityCell(client, sessionId, capability.bridgeCode, "policy bridge");
    const snapshot = await runCapabilityCell(client, sessionId, capability.code, "readiness");
    const evidence = parseReadinessEvidence(snapshot);
    if (
      evidence.kind !== "monorepo-breadbutter-rust"
      || evidence.repo_root !== capability.repoRoot
      || evidence.rust !== capability.rustVersion
    ) {
      throw new Error(
        `Breadbutter Rust capability returned inconsistent evidence: ${JSON.stringify(evidence)}`,
      );
    }
    return evidence;
  } finally {
    await client.callTool({
      name: "session.close",
      arguments: { session_id: sessionId },
    });
  }
}
