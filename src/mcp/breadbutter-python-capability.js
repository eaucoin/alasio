import { createHash } from "node:crypto";
import { readFile, realpath } from "node:fs/promises";
import { join } from "node:path";
import { parse as parseToml } from "smol-toml";

import { invokesBaymaReplServer } from "./bayma-state.js";

const BREADBUTTER_RELATIVE_ROOT = ".agents/skills/monorepo-breadbutter";
const CAPABILITY_FILES = [
  "SKILL.md",
  "pyproject.toml",
  "qi_mono_breadbutter.py",
  "uv.lock",
];
const MAX_WAIT_POLLS = 50;

function pythonString(value) {
  return JSON.stringify(String(value));
}

function parseSupportedMinor(pyproject) {
  let parsed;
  try {
    parsed = parseToml(pyproject);
  } catch (error) {
    throw new Error(
      `monorepo Breadbutter pyproject.toml is invalid: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  const requiresPython = parsed.project?.["requires-python"];
  const match = typeof requiresPython === "string"
    ? /^>=(\d+\.\d+),<(\d+\.\d+)$/u.exec(requiresPython)
    : null;
  if (!match) {
    throw new Error(
      "monorepo Breadbutter must declare one exact Python minor interval",
    );
  }
  const [major, minor] = match[1].split(".").map(Number);
  const [upperMajor, upperMinor] = match[2].split(".").map(Number);
  if (upperMajor !== major || upperMinor !== minor + 1) {
    throw new Error(
      `monorepo Breadbutter Python interval ${requiresPython} spans more than one minor`,
    );
  }
  return match[1];
}

function buildCapabilityCode(skillRoot) {
  return [
    "from pathlib import Path",
    "import json",
    "import sys",
    `skill_root = Path(${pythonString(skillRoot)})`,
    "if str(skill_root) not in sys.path:",
    "    sys.path.insert(0, str(skill_root))",
    "from qi_mono_breadbutter import open_qi_mono_breadbutter",
    "breadbutter_state = open_qi_mono_breadbutter(cwd=Path.cwd())",
    "globals().update(breadbutter_state)",
    "print(json.dumps({",
    '    "kind": "monorepo-breadbutter-python",',
    '    "packages": len(monorepo.versions),',
    '    "python": sys.version.split()[0],',
    '    "repo_root": str(repo_root),',
    '    "site_packages": str(breadbutter_site_packages),',
    "}, sort_keys=True))",
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
  const stdout = typeof snapshot.stdout_text === "string"
    ? snapshot.stdout_text
    : "";
  const line = stdout.split(/\r?\n/u).findLast((candidate) => candidate.trim());
  if (!line) {
    throw new Error("Breadbutter Python capability emitted no readiness evidence");
  }
  let evidence;
  try {
    evidence = JSON.parse(line);
  } catch (error) {
    throw new Error(
      `Breadbutter Python capability emitted invalid readiness JSON: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (!evidence || typeof evidence !== "object" || Array.isArray(evidence)) {
    throw new Error("Breadbutter Python capability evidence must be an object");
  }
  return evidence;
}

export async function resolveBreadbutterPythonCapability({
  serverName,
  serverConfig,
  workingDirectory,
}) {
  if (
    !workingDirectory
    || !invokesBaymaReplServer(serverName, serverConfig)
  ) {
    return null;
  }

  const repoRoot = await realpath(workingDirectory);
  const skillRoot = join(repoRoot, BREADBUTTER_RELATIVE_ROOT);
  let skillSource;
  try {
    skillSource = await readFile(join(skillRoot, "SKILL.md"));
  } catch (error) {
    if (error?.code === "ENOENT") {
      return null;
    }
    throw error;
  }
  let sources;
  try {
    const remainingSources = await Promise.all(
      CAPABILITY_FILES.slice(1).map(async (relativePath) => ({
        relativePath,
        contents: await readFile(join(skillRoot, relativePath)),
      })),
    );
    sources = [
      { relativePath: "SKILL.md", contents: skillSource },
      ...remainingSources,
    ];
  } catch (error) {
    throw new Error(
      `monorepo Breadbutter capability is incomplete under ${skillRoot}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  const pyproject = sources
    .find(({ relativePath }) => relativePath === "pyproject.toml")
    .contents.toString("utf8");
  const supportedMinor = parseSupportedMinor(pyproject);
  const expectedSitePackages = join(
    skillRoot,
    ".venv",
    "lib",
    `python${supportedMinor}`,
    "site-packages",
  );
  const fingerprint = createHash("sha256");
  for (const { relativePath, contents } of sources) {
    fingerprint.update(relativePath);
    fingerprint.update("\0");
    fingerprint.update(contents);
    fingerprint.update("\0");
  }

  return {
    code: buildCapabilityCode(skillRoot),
    fingerprint: fingerprint.digest("hex"),
    expectedSitePackages,
    repoRoot,
    skillRoot,
    supportedMinor,
  };
}

export async function runBreadbutterPythonCapability(client, capability) {
  const created = structuredContent(
    await client.callTool({
      name: "session.create",
      arguments: {
        runtime: "python",
        cwd: capability.repoRoot,
        title: "alasio-breadbutter-preflight",
      },
    }),
    "Breadbutter Python session.create",
  );
  const sessionId = created.session?.session_id;
  if (typeof sessionId !== "string" || !sessionId) {
    throw new Error("Breadbutter Python session.create returned no session id");
  }

  try {
    let snapshot = structuredContent(
      await client.callTool({
        name: "exec",
        arguments: {
          session_id: sessionId,
          code: capability.code,
          yield_time_ms: 5_000,
        },
      }),
      "Breadbutter Python exec",
    );
    for (let poll = 0; snapshot.done !== true; poll += 1) {
      if (poll >= MAX_WAIT_POLLS) {
        throw new Error(
          `Breadbutter Python capability did not settle after ${MAX_WAIT_POLLS} waits`,
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
        "Breadbutter Python wait",
      );
    }

    if (snapshot.status !== "ok") {
      const detail = snapshot.error_text || snapshot.stderr_text || "no error text";
      throw new Error(
        `Breadbutter Python capability ended with ${snapshot.status}: ${detail}`,
      );
    }
    const evidence = parseReadinessEvidence(snapshot);
    if (
      evidence.kind !== "monorepo-breadbutter-python"
      || evidence.repo_root !== capability.repoRoot
      || typeof evidence.packages !== "number"
      || evidence.packages < 1
      || typeof evidence.python !== "string"
      || !evidence.python.startsWith(`${capability.supportedMinor}.`)
      || typeof evidence.site_packages !== "string"
      || evidence.site_packages !== capability.expectedSitePackages
    ) {
      throw new Error(
        `Breadbutter Python capability returned inconsistent evidence: ${JSON.stringify(evidence)}`,
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
