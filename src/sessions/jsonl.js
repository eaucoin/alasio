import { closeSync, openSync, readSync, statSync } from "node:fs";

export const JSONL_HEAD_BYTES = 64 * 1024;
export const JSONL_TAIL_BYTES = 8 * 1024 * 1024;

export function parseJsonlContent(content) {
  const records = [];
  for (const line of content.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) {
      continue;
    }
    try {
      records.push(JSON.parse(trimmed));
    } catch {
      // Ignore malformed records in append-only Codex logs.
    }
  }
  return records;
}

export function readFileSlice(filePath, start, length) {
  const fd = openSync(filePath, "r");
  try {
    const buffer = Buffer.alloc(length);
    const bytesRead = readSync(fd, buffer, 0, length, start);
    return buffer.subarray(0, bytesRead).toString("utf-8");
  } finally {
    closeSync(fd);
  }
}

export function parseJsonlFileHead(filePath) {
  try {
    const stats = statSync(filePath);
    return parseJsonlContent(readFileSlice(filePath, 0, Math.min(stats.size, JSONL_HEAD_BYTES)));
  } catch {
    return [];
  }
}
