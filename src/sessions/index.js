import { existsSync, readFileSync } from "node:fs";
import { basename } from "node:path";
import { SESSIONS_PER_PAGE } from "../shared/runtime-constants.js";
import { findSessionFileById, getSessionDateLabel, getSessionMetaId, getValidSessionFiles } from "./discovery.js";
import { getLastAssistantText } from "./summary.js";

function truncateSessionText(text, maxChars = 40, suffix = "...") {
    if (text.length <= maxChars) {
        return text;
    }
    if (maxChars <= suffix.length) {
        return suffix.slice(0, maxChars);
    }
    return `${text.slice(0, maxChars - suffix.length).trimEnd()}${suffix}`;
}

export function listSessions(page = 1) {
    const validFiles = getValidSessionFiles();
    const startIdx = (page - 1) * SESSIONS_PER_PAGE;
    const pageFiles = validFiles.slice(startIdx, startIdx + SESSIONS_PER_PAGE);
    return pageFiles.map((filePath) => {
        const uuid = getSessionMetaId(filePath) ?? basename(filePath, ".jsonl");
        const bestAssistantText = getLastAssistantText(filePath) ?? "";
        return {
            uuid,
            timestamp: getSessionDateLabel(filePath),
            label: truncateSessionText(bestAssistantText, 40, "..."),
        };
    });
}

export function getTotalSessionPages() {
    return Math.ceil(getValidSessionFiles().length / SESSIONS_PER_PAGE) || 1;
}

export function getSessionByNumber(num) {
    const validFiles = getValidSessionFiles();
    const idx = num - 1;
    if (idx < 0 || idx >= validFiles.length) {
        return null;
    }
    return getSessionMetaId(validFiles[idx]) ?? basename(validFiles[idx], ".jsonl");
}

export function getSessionLastMessage(sessionId) {
    const sessionFile = findSessionFileById(sessionId);
    if (!sessionFile || !existsSync(sessionFile)) {
        return null;
    }
    return getLastAssistantText(sessionFile);
}

export function listSessionMessages(sessionId) {
    const sessionFile = findSessionFileById(sessionId);
    if (!sessionFile || !existsSync(sessionFile)) {
        return [];
    }
    const userMessages = [];
    const lines = readFileSync(sessionFile, "utf-8").split("\n");
    for (let i = 0; i < lines.length; i += 1) {
        const trimmed = lines[i].trim();
        if (!trimmed) {
            continue;
        }
        try {
            const record = JSON.parse(trimmed);
            if (record.type === "event_msg" && record.payload?.type === "user_message") {
                const text = String(record.payload?.message ?? "").trim();
                if (text) {
                    userMessages.push({
                        timestamp: record.timestamp ?? "",
                        text,
                        uuid: `line_${i + 1}`,
                    });
                }
            }
        }
        catch {
            // Ignore malformed JSONL rows.
        }
    }
    return [...userMessages].reverse().map((message, index) => ({
        index: -(index + 1),
        timestamp: message.timestamp,
        text: message.text,
        uuid: message.uuid,
    }));
}

export function getTotalRewindPages(sessionId) {
    return Math.ceil(listSessionMessages(sessionId).length / SESSIONS_PER_PAGE) || 1;
}
