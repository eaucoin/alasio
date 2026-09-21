import { join } from "node:path";

function sanitizePathToken(value) {
  return value.replace(/[^A-Za-z0-9_.-]+/g, "-").replace(/^-+|-+$/g, "") || "default";
}

function materializeBaymaArgs(args, env, threadKey, serverName) {
  const normalizedArgs = Array.isArray(args)
    ? args.map((value) => String(value))
    : [];
  const stateDir = join(
    env.TMPDIR || "/tmp",
    "alasio-bayma",
    `${process.pid}-${sanitizePathToken(threadKey)}`,
    sanitizePathToken(String(serverName)),
  );
  const stateDirIndex = normalizedArgs.indexOf("--state-dir");
  if (stateDirIndex >= 0) {
    if (stateDirIndex === normalizedArgs.length - 1) {
      normalizedArgs.push(stateDir);
    } else {
      normalizedArgs[stateDirIndex + 1] = stateDir;
    }
    return normalizedArgs;
  }
  return [...normalizedArgs, "--state-dir", stateDir];
}

function normalizedExecutableName(value) {
  if (typeof value !== "string") {
    return "";
  }
  return value
    .split("/")
    .pop()
    ?.toLowerCase()
    .replace(/\.(?:c?js|mjs|exe)$/u, "")
    .replace(/[._]/gu, "-") || "";
}

export function invokesBaymaServer(serverName, serverConfig) {
  const commandName = typeof serverConfig.command === "string"
    ? serverConfig.command.split("/").pop()?.toLowerCase() || ""
    : "";
  const normalizedServerName = String(serverName).toLowerCase().replace(/[._]/g, "-");
  const commandParts = [serverConfig.command, ...(Array.isArray(serverConfig.args) ? serverConfig.args : [])];
  return normalizedServerName === "bayma"
    || normalizedServerName.startsWith("bayma-")
    || commandName === "bayma"
    || commandParts
      .map((value) => typeof value === "string"
        ? value.split("/").pop()?.toLowerCase() || ""
        : "")
      .some((value) => value === "bayma" || value.startsWith("bayma-") || value.startsWith("bayma_"));
}

export function invokesBaymaReplServer(serverName, serverConfig) {
  const candidates = [
    String(serverName),
    serverConfig?.command,
    ...(Array.isArray(serverConfig?.args) ? serverConfig.args : []),
  ].map(normalizedExecutableName);
  return candidates.includes("bayma-repl");
}

export function materializeMcpServerConfig(serverName, serverConfig, env, threadKey) {
  if (!serverConfig || typeof serverConfig !== "object" || Array.isArray(serverConfig)) {
    return serverConfig;
  }
  const normalized = { ...serverConfig };
  if (invokesBaymaServer(serverName, normalized)) {
    normalized.args = materializeBaymaArgs(normalized.args, env, threadKey, serverName);
  }
  return normalized;
}
