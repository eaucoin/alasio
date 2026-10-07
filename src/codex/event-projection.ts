import type { FileChangeItem as SdkFileChangeItem } from "@openai/codex-sdk";
import { Effect } from "effect";

import type { MessagePhase, v2 } from "../../.types/codex/index.js";
import type { StoreError } from "../persistence/sql.ts";
import type { Store } from "../persistence/store.ts";
import type { PassedThroughItem } from "./app-server/protocol.ts";

/*
 * Codex-shaped items: what a harness reports an agent did, in the shape of the Codex SDK's
 * thread items. Codex exec reports them so; Codex app-server items and Claude Agent SDK
 * messages are projected into them. Each carries what this module reads.
 */

/** Text from the agent: commentary while it works, or its final answer. */
export interface AgentMessageItem {
  readonly type: "agent_message";
  readonly id?: string;
  readonly text: string;
  readonly phase?: MessagePhase | null;
}

/** A shell command the agent ran. */
export interface CommandExecutionItem {
  readonly type: "command_execution";
  readonly id?: string;
  readonly command: string;
}

/** One file a file change touched; the kind is a string from Codex exec and an object from app-server. */
export interface FileChange {
  readonly path: string;
  readonly kind: SdkFileChangeItem["changes"][number]["kind"] | v2.PatchChangeKind;
}

/**
 * Whether a change deletes its file: Codex exec names the kind, the app-server's
 * protocol describes it as an object.
 */
function isDeletion(kind: FileChange["kind"]): boolean {
  return typeof kind === "string" ? kind === "delete" : kind.type === "delete";
}

/** Files the agent changed. */
export interface FileChangeItem {
  readonly type: "file_change";
  readonly id?: string;
  readonly changes: readonly FileChange[];
}

/** A call to an MCP tool, or to another tool reported as one. */
export interface McpToolCallItem {
  readonly type: "mcp_tool_call";
  readonly id: string;
  readonly server: string;
  readonly tool: string;
  readonly arguments?: unknown;
}

/** A web search the agent ran. */
export interface WebSearchItem {
  readonly type: "web_search";
  readonly id?: string;
  readonly query?: string;
}

/** A non-fatal error reported as an item. */
export interface ErrorItem {
  readonly type: "error";
  readonly message: string;
}

/** Items no response block shows, the app-server's with no Codex SDK counterpart among them. */
export interface UnshownItem {
  readonly type: "reasoning" | "todo_list" | "context_compaction" | PassedThroughItem["type"];
}

export type CodexItem =
  | AgentMessageItem
  | CommandExecutionItem
  | FileChangeItem
  | McpToolCallItem
  | WebSearchItem
  | ErrorItem
  | UnshownItem;

/** A question an agent asks the user through a tool, as the tool's arguments carry it. */
export interface AskedQuestion {
  readonly header?: string;
}

const ERROR_PREFIX = "Error: ";

/** A block reporting what stopped a turn, as every harness records it. */
export function errorBlock(message: string): ResponseBlock {
  return { type: "text", content: `${ERROR_PREFIX}${message}` };
}

/** The messages of the error blocks among a turn's blocks, in order. */
export function errorsIn(blocks: readonly ResponseBlock[]): string[] {
  return blocks.flatMap((block) => (block.type === "text" && !block.phase && block.content.startsWith(ERROR_PREFIX) ? [block.content.slice(ERROR_PREFIX.length)] : []));
}

/**
 * One block of a turn's response, as it is shown and stored: text (the final answer's
 * phase marks the text delivered to the operator), a tool the agent used, or questions
 * it asked the user.
 */
export type ResponseBlock =
  | { readonly type: "text"; readonly content: string; readonly phase?: MessagePhase | null }
  | { readonly type: "tool"; readonly name: string }
  | {
    readonly type: "ask_user_question";
    readonly tool_use_id: string;
    readonly questions: readonly AskedQuestion[];
    readonly summary: string;
  };

/** Where a pending response's blocks are stored: alasio's store. */
export type PendingResponseStore = Pick<Store["Service"], "appendBlocksToPending" | "markPendingResponseComplete">;

/**
 * A turn's response in progress: its blocks so far, and those appended since it was
 * last stored. Projecting an event only appends; the turn stores what an event added
 * once it has handled it, in one statement.
 */
export interface ResponseProjection {
  readonly pendingResponseId: string;
  readonly blockSequence: ResponseBlock[];
  readonly unstored: ResponseBlock[];
}

/** A response with no blocks yet. */
export function responseProjection(pendingResponseId: string): ResponseProjection {
  return { pendingResponseId, blockSequence: [], unstored: [] };
}

// The tool's arguments are as the asking tool sent them; only the list itself is checked.
function isQuestionList(value: unknown): value is readonly AskedQuestion[] {
  return Array.isArray(value);
}

export function appendBlock(projection: ResponseProjection, block: ResponseBlock): void {
  projection.blockSequence.push(block);
  projection.unstored.push(block);
}

/** Stores the blocks appended since the response was last stored, if any were. */
export const storeBlocks = (projection: ResponseProjection, persistence: PendingResponseStore): Effect.Effect<void, StoreError> =>
  Effect.suspend(() => (projection.unstored.length === 0 ? Effect.void : persistence.appendBlocksToPending(projection.pendingResponseId, projection.unstored.splice(0))));

/** Stores the response's last blocks, then marks it complete, for delivery to read it whole. */
export const completeResponse = (projection: ResponseProjection, persistence: PendingResponseStore): Effect.Effect<void, StoreError> =>
  storeBlocks(projection, persistence).pipe(Effect.andThen(persistence.markPendingResponseComplete(projection.pendingResponseId)));

export function isVisibleCodexItem(item: CodexItem): boolean {
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

export function mapItemToBlocks(item: CodexItem, projection: ResponseProjection): void {
    switch (item.type) {
        case "agent_message":
            if (item.text?.trim()) {
                appendBlock(projection, {
                    type: "text",
                    content: item.text,
                    phase: item.phase ?? null,
                });
            }
            break;
        case "command_execution":
            appendBlock(projection, {
                type: "tool",
                name: "Bash",
            });
            break;
        case "file_change":
            if (Array.isArray(item.changes)) {
                for (const change of item.changes) {
                    appendBlock(projection, {
                        type: "tool",
                        name: isDeletion(change.kind) ? "Delete" : "Edit",
                    });
                }
            }
            break;
        case "mcp_tool_call": {
            const toolUseId = item.id;
            const toolName = `${item.server}.${item.tool}`;
            const args = item.arguments ?? {};
            const questions = typeof args === "object" && args !== null && "questions" in args ? args.questions : undefined;
            if (isQuestionList(questions) && questions.length > 0) {
                appendBlock(projection, {
                    type: "ask_user_question",
                    tool_use_id: toolUseId,
                    questions,
                    summary: `Questions: ${questions.map((q) => q.header ?? "Q").join(", ")}`,
                });
            }
            else {
                appendBlock(projection, {
                    type: "tool",
                    name: toolName,
                });
            }
            break;
        }
        case "web_search":
            appendBlock(projection, {
                type: "tool",
                name: "WebSearch",
            });
            break;
        case "reasoning":
        case "todo_list":
            break;
        case "error":
            if (item.message?.trim()) {
                appendBlock(projection, errorBlock(item.message));
            }
            break;
        default:
            break;
    }
}
