import { statSync } from "node:fs";
import { JSONL_TAIL_BYTES, readFileSlice } from "./jsonl.js";

function getAssistantText(record) {
  if (record.type === "event_msg" && record.payload?.type === "agent_message") {
    const text = record.payload?.message;
    return typeof text === "string" && text.trim() ? text.trim() : null;
  }
  if (record.type === "response_item"
    && record.payload?.type === "message"
    && record.payload?.role === "assistant") {
    const content = record.payload?.content;
    if (Array.isArray(content)) {
      const textItem = content.find((item) => item?.type === "output_text" && typeof item?.text === "string" && item.text.trim());
      return textItem ? textItem.text.trim() : null;
    }
  }
  return null;
}

function findLastAssistantTextInLines(lines) {
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const trimmed = lines[i].trim();
    if (!trimmed) {
      continue;
    }
    try {
      const text = getAssistantText(JSON.parse(trimmed));
      if (text) {
        return text;
      }
    } catch {
      // Ignore malformed or chunk-partial JSONL rows.
    }
  }
  return null;
}

export function getLastAssistantText(filePath) {
  try {
    const stats = statSync(filePath);
    let end = stats.size;
    while (end > 0) {
      const start = Math.max(0, end - JSONL_TAIL_BYTES);
      const content = readFileSlice(filePath, start, end - start);
      const lines = content.split("\n");
      if (start > 0) {
        lines.shift();
      }
      if (end < stats.size) {
        lines.pop();
      }
      const text = findLastAssistantTextInLines(lines);
      if (text) {
        return text;
      }
      end = start;
    }
  } catch {
    return null;
  }
  return null;
}
