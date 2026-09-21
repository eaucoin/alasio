import { existsSync, readdirSync, statSync } from "node:fs";
import { homedir } from "node:os";
import { basename, join } from "node:path";
import { parseJsonlFileHead } from "./jsonl.js";
import { getLastAssistantText } from "./summary.js";

export function getSessionsDir() {
  return join(homedir(), ".codex", "sessions");
}

function walkJsonlFiles(rootDir) {
  if (!existsSync(rootDir)) {
    return [];
  }
  const out = [];
  const stack = [rootDir];
  while (stack.length > 0) {
    const dir = stack.pop();
    let entries = [];
    try {
      entries = readdirSync(dir);
    } catch {
      continue;
    }
    for (const name of entries) {
      const fullPath = join(dir, name);
      try {
        const st = statSync(fullPath);
        if (st.isDirectory()) {
          stack.push(fullPath);
        } else if (st.isFile() && name.endsWith(".jsonl")) {
          out.push(fullPath);
        }
      } catch {
        // Ignore entries that disappear during traversal.
      }
    }
  }
  return out;
}

export function getSessionDateLabel(filePath) {
  for (const record of parseJsonlFileHead(filePath)) {
    if (record.type === "session_meta") {
      const timestamp = record.payload?.timestamp ?? record.timestamp ?? "";
      if (typeof timestamp === "string" && timestamp) {
        return timestamp.slice(0, 10);
      }
    }
  }
  try {
    return new Date(statSync(filePath).mtimeMs).toISOString().slice(0, 10);
  } catch {
    return "-";
  }
}

export function getSessionMetaId(filePath) {
  for (const record of parseJsonlFileHead(filePath)) {
    if (record.type === "session_meta") {
      const id = record.payload?.id;
      if (typeof id === "string" && id) {
        return id;
      }
    }
  }
  return null;
}

function hasAssistantTextResponse(filePath) {
  return Boolean(getLastAssistantText(filePath));
}

export function getValidSessionFiles() {
  const sessionsDir = getSessionsDir();
  if (!existsSync(sessionsDir)) {
    return [];
  }
  return walkJsonlFiles(sessionsDir)
    .map((filePath) => {
      try {
        return { filePath, mtime: statSync(filePath).mtimeMs };
      } catch {
        return { filePath, mtime: 0 };
      }
    })
    .sort((a, b) => b.mtime - a.mtime)
    .map((entry) => entry.filePath)
    .filter((filePath) => hasAssistantTextResponse(filePath));
}

export function findSessionFileById(sessionId) {
  const files = getValidSessionFiles();
  for (const file of files) {
    if (getSessionMetaId(file) === sessionId) {
      return file;
    }
  }
  return files.find((file) => basename(file, ".jsonl") === sessionId || file.includes(sessionId)) ?? null;
}
