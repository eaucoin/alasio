/** A command the operator typed into a conversation, as parseCommand reads it. */
export type OperatorCommand =
    | { readonly type: "stop" }
    | { readonly type: "session_panel" }
    | { readonly type: "model" }
    | { readonly type: "service"; readonly target: string }
    | { readonly type: "workspace"; readonly args: string }
    | { readonly type: "sessions_panel" }
    | { readonly type: "sessions_new" }
    | { readonly type: "goal"; readonly args: string }
    | { readonly type: "sessions"; readonly page: number }
    | { readonly type: "rewind_exec"; readonly index: number }
    | { readonly type: "rewind_list"; readonly page: number }
    | { readonly type: "resume"; readonly ref: string; readonly followUp: string };

export function parseCommand(text: string): OperatorCommand | null {
    const trimmed = text.trim();
    const normalized = trimmed.replace(/^\/([a-z][a-z0-9_]*)(?:@[a-z0-9_]+)?/i, "/$1");
    if (normalized.toLowerCase() === "!stop" || normalized.toLowerCase() === "/stop") {
        return { type: "stop" };
    }
    if (normalized.toLowerCase() === "/session") {
        return { type: "session_panel" };
    }
    if (normalized.toLowerCase() === "/model") {
        return { type: "model" };
    }
    const serviceMatch = /^\/service(?:\s+([a-z][a-z0-9_-]*))?\s*$/i.exec(normalized);
    if (serviceMatch) {
        return { type: "service", target: (serviceMatch[1] || "").toLowerCase() };
    }
    const workspaceMatch = /^\/workspace(?:\s+([\s\S]*))?$/i.exec(normalized);
    if (workspaceMatch) {
        return { type: "workspace", args: (workspaceMatch[1] || "").trim() };
    }
    if (normalized.toLowerCase() === "/sessions") {
        return { type: "sessions_panel" };
    }
    if (normalized.toLowerCase() === "!sessions new" || normalized.toLowerCase() === "/sessions new") {
        return { type: "sessions_new" };
    }
    const goalMatch = /^\/goal(?:\s+([\s\S]*))?$/i.exec(normalized);
    if (goalMatch) {
        return { type: "goal", args: (goalMatch[1] || "").trim() };
    }
    const sessionsMatch = /^!sessions(?:\s+page\s+(\d+))?$/i.exec(normalized) ?? /^\/sessions\s+page\s+(\d+)$/i.exec(normalized);
    if (sessionsMatch) {
        const page = sessionsMatch[1] ? Number.parseInt(sessionsMatch[1], 10) : 1;
        return { type: "sessions", page };
    }
    const rewindExecMatch = /^!rewind\s+(-\d+)$/i.exec(trimmed);
    if (rewindExecMatch) {
        // The group is not optional, so every match fills it and the `?? ""` never applies.
        return { type: "rewind_exec", index: Number.parseInt(rewindExecMatch[1] ?? "", 10) };
    }
    const rewindListMatch = /^!rewind(?:\s+page\s+(\d+))?$/i.exec(trimmed);
    if (rewindListMatch) {
        const page = rewindListMatch[1] ? Number.parseInt(rewindListMatch[1], 10) : 1;
        return { type: "rewind_list", page };
    }
    if (/^!resume/i.test(trimmed)) {
        const resumeMatch = /^!resume(?:\s+([a-f0-9-]{36}|\d+))?\s*(.*)/is.exec(trimmed);
        if (resumeMatch) {
            return {
                type: "resume",
                ref: resumeMatch[1] || "1",
                followUp: (resumeMatch[2] || "").trim(),
            };
        }
    }
    return null;
}
