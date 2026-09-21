import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { findSessionFileById, getSessionsDir } from "./discovery.js";

export function createForkedSession(sessionId, beforeUuid) {
    const sessionFile = findSessionFileById(sessionId);
    if (!sessionFile || !existsSync(sessionFile)) {
        return null;
    }
    try {
        const linesToKeep = [];
        const newSessionId = randomUUID();
        const lines = readFileSync(sessionFile, "utf-8").split("\n");
        for (let i = 0; i < lines.length; i += 1) {
            const line = lines[i];
            const trimmed = line.trim();
            if (!trimmed) {
                continue;
            }
            try {
                const record = JSON.parse(trimmed);
                if (record.type === "event_msg" && record.payload?.type === "user_message" && `line_${i + 1}` === beforeUuid) {
                    break;
                }
                if (record.type === "session_meta" && record.payload && typeof record.payload === "object") {
                    record.payload.id = newSessionId;
                    record.payload.timestamp = new Date().toISOString();
                    linesToKeep.push(JSON.stringify(record));
                    continue;
                }
                linesToKeep.push(line);
            }
            catch {
                linesToKeep.push(line);
            }
        }
        if (linesToKeep.length === 0) {
            return null;
        }
        const now = new Date();
        const ts = now.toISOString().replace(/:/g, "-").replace(/\..+/, "");
        const newSessionDir = join(
            getSessionsDir(),
            `${now.getUTCFullYear()}`,
            `${String(now.getUTCMonth() + 1).padStart(2, "0")}`,
            `${String(now.getUTCDate()).padStart(2, "0")}`,
        );
        mkdirSync(newSessionDir, { recursive: true });
        writeFileSync(join(newSessionDir, `rollout-${ts}-${newSessionId}.jsonl`), `${linesToKeep.join("\n")}\n`, "utf-8");
        return newSessionId;
    }
    catch {
        return null;
    }
}
