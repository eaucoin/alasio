function appendBlock(blockSequence, persistence, pendingResponseId, block) {
    blockSequence.push(block);
    persistence.appendBlockToPending(pendingResponseId, block);
}

export { appendBlock };

export function isVisibleCodexItem(item) {
    switch (item.type) {
        case "agent_message":
            return Boolean(item.text?.trim());
        case "command_execution":
        case "mcp_tool_call":
        case "web_search":
            return true;
        case "file_change":
            return Array.isArray(item.changes) && item.changes.length > 0;
        case "error":
            return Boolean(item.message?.trim());
        default:
            return false;
    }
}

export function mapItemToBlocks(item, params) {
    const { blockSequence, persistence, pendingResponseId } = params;
    switch (item.type) {
        case "agent_message":
            if (item.text?.trim()) {
                appendBlock(blockSequence, persistence, pendingResponseId, {
                    type: "text",
                    content: item.text,
                    phase: item.phase ?? null,
                });
            }
            break;
        case "command_execution":
            appendBlock(blockSequence, persistence, pendingResponseId, {
                type: "tool",
                name: "Bash",
            });
            break;
        case "file_change":
            if (Array.isArray(item.changes)) {
                for (const change of item.changes) {
                    appendBlock(blockSequence, persistence, pendingResponseId, {
                        type: "tool",
                        name: change.kind === "delete" ? "Delete" : "Edit",
                    });
                }
            }
            break;
        case "mcp_tool_call": {
            const toolUseId = item.id;
            const toolName = `${item.server}.${item.tool}`;
            const args = item.arguments ?? {};
            const questions = args?.questions;
            if (Array.isArray(questions) && questions.length > 0) {
                appendBlock(blockSequence, persistence, pendingResponseId, {
                    type: "ask_user_question",
                    tool_use_id: toolUseId,
                    questions,
                    summary: `Questions: ${questions.map((q) => q.header ?? "Q").join(", ")}`,
                });
            }
            else {
                appendBlock(blockSequence, persistence, pendingResponseId, {
                    type: "tool",
                    name: toolName,
                });
            }
            break;
        }
        case "web_search":
            appendBlock(blockSequence, persistence, pendingResponseId, {
                type: "tool",
                name: "WebSearch",
            });
            break;
        case "reasoning":
        case "todo_list":
            break;
        case "error":
            if (item.message?.trim()) {
                appendBlock(blockSequence, persistence, pendingResponseId, {
                    type: "text",
                    content: `Error: ${item.message}`,
                });
            }
            break;
        default:
            break;
    }
}
