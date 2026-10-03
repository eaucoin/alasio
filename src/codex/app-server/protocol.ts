import type {
  ClientRequest,
  InitializeCapabilities,
  InitializeResponse,
  ServerNotification,
  ServerRequest,
  v2,
} from "../../../.types/codex/index.js";

/** The params Codex's protocol gives the request `method`. */
type ProtocolParams<M extends ClientRequest["method"]> = Extract<ClientRequest, { method: M }>["params"];

/**
 * initialize's params as the app-server takes them: its generated types require
 * `requestAttestation`, a capability the app-server accepts left out.
 */
type InitializeRequestParams = Omit<ProtocolParams<"initialize">, "capabilities"> & {
  readonly capabilities: Partial<InitializeCapabilities> | null;
};

/** The requests alasio makes of the app-server: each method's params and the result it answers with. */
export interface AppServerRequests {
  initialize: { params: InitializeRequestParams; result: InitializeResponse };
  "thread/loaded/list": { params: ProtocolParams<"thread/loaded/list">; result: v2.ThreadLoadedListResponse };
  "thread/resume": { params: ProtocolParams<"thread/resume">; result: v2.ThreadResumeResponse };
  "thread/start": { params: ProtocolParams<"thread/start">; result: v2.ThreadStartResponse };
  "thread/fork": { params: ProtocolParams<"thread/fork">; result: v2.ThreadForkResponse };
  "thread/list": { params: ProtocolParams<"thread/list">; result: v2.ThreadListResponse };
  "thread/turns/list": { params: ProtocolParams<"thread/turns/list">; result: v2.ThreadTurnsListResponse };
  "model/list": { params: ProtocolParams<"model/list">; result: v2.ModelListResponse };
  "turn/start": { params: ProtocolParams<"turn/start">; result: v2.TurnStartResponse };
  "turn/steer": { params: ProtocolParams<"turn/steer">; result: v2.TurnSteerResponse };
  "turn/interrupt": { params: ProtocolParams<"turn/interrupt">; result: v2.TurnInterruptResponse };
  "thread/goal/get": { params: ProtocolParams<"thread/goal/get">; result: v2.ThreadGoalGetResponse };
  "thread/goal/set": { params: ProtocolParams<"thread/goal/set">; result: v2.ThreadGoalSetResponse };
  "thread/goal/clear": { params: ProtocolParams<"thread/goal/clear">; result: v2.ThreadGoalClearResponse };
}

export type AppServerMethod = keyof AppServerRequests;
export type AppServerParams<M extends AppServerMethod> = AppServerRequests[M]["params"];
export type AppServerResult<M extends AppServerMethod> = AppServerRequests[M]["result"];

/**
 * item/updated, which the pinned protocol does not have (it reports an item as it
 * starts and as it completes); mapped as those are, should an app-server send it.
 */
interface ItemUpdatedNotification {
  readonly method: "item/updated";
  readonly params: { readonly item: v2.ThreadItem };
}

/** A notification from the app-server. */
export type AppServerNotification = ServerNotification | ItemUpdatedNotification;

/** What alasio answers each of the app-server's requests with: a refusal, or nothing. */
export type ServerRequestResult =
  | v2.CommandExecutionRequestApprovalResponse
  | v2.FileChangeRequestApprovalResponse
  | v2.ToolRequestUserInputResponse
  | v2.McpServerElicitationRequestResponse
  | Record<string, never>;

const IGNORABLE_NOTIFICATION_METHODS: ReadonlySet<AppServerNotification["method"]> = new Set<AppServerNotification["method"]>([
  "thread/status/changed",
  "thread/archived",
  "thread/deleted",
  "thread/unarchived",
  "thread/closed",
  "thread/name/updated",
  "thread/goal/updated",
  "thread/goal/cleared",
  "thread/settings/updated",
  "thread/compacted",
  "skills/changed",
  "hook/started",
  "hook/completed",
  "turn/diff/updated",
  "turn/plan/updated",
  "turn/moderationMetadata",
  "item/autoApprovalReview/started",
  "item/autoApprovalReview/completed",
  "item/agentMessage/delta",
  "item/plan/delta",
  "item/commandExecution/outputDelta",
  "item/commandExecution/terminalInteraction",
  "item/fileChange/outputDelta",
  "item/fileChange/patchUpdated",
  "item/mcpToolCall/progress",
  "rawResponseItem/completed",
  "command/exec/outputDelta",
  "process/outputDelta",
  "process/exited",
  "serverRequest/resolved",
  "mcpServer/oauthLogin/completed",
  "mcpServer/startupStatus/updated",
  "account/updated",
  "account/rateLimits/updated",
  "app/list/updated",
  "remoteControl/status/changed",
  "externalAgentConfig/import/completed",
  "fs/changed",
  "item/reasoning/summaryTextDelta",
  "item/reasoning/summaryPartAdded",
  "item/reasoning/textDelta",
  "thread/realtime/started",
  "thread/realtime/itemAdded",
  "thread/realtime/transcript/delta",
  "thread/realtime/transcript/done",
  "thread/realtime/outputAudio/delta",
  "thread/realtime/sdp",
  "thread/realtime/error",
  "thread/realtime/closed",
  "model/rerouted",
  "model/verification",
  "warning",
  "guardianWarning",
  "deprecationNotice",
  "configWarning",
  "fuzzyFileSearch/sessionUpdated",
  "fuzzyFileSearch/sessionCompleted",
  "windows/worldWritableWarning",
  "windowsSandbox/setupCompleted",
  "account/login/completed",
]);

/** The app-server item of type `T`. */
type ItemOf<T extends v2.ThreadItem["type"]> = Extract<v2.ThreadItem, { type: T }>;

/** An app-server item under the type the Codex SDK gives the same item. */
type Renamed<Item extends v2.ThreadItem, Type extends string> = Omit<Item, "type"> & { readonly type: Type };

/** The app-server's item types that are renamed into the Codex SDK's (normalizeAppServerItem). */
type RenamedItemType =
  | "agentMessage"
  | "commandExecution"
  | "fileChange"
  | "mcpToolCall"
  | "dynamicToolCall"
  | "webSearch"
  | "contextCompaction";

/** The app-server's items with no Codex SDK counterpart, reported as the app-server sends them. */
export type PassedThroughItem = Exclude<v2.ThreadItem, { type: RenamedItemType }>;

/**
 * An app-server item as alasio reports it: under the Codex SDK's type where the SDK has
 * the item, a dynamic tool call as an MCP tool call, and any other as it came.
 */
export type AppServerItem =
  | Renamed<ItemOf<"agentMessage">, "agent_message">
  | Renamed<ItemOf<"commandExecution">, "command_execution">
  | Renamed<ItemOf<"fileChange">, "file_change">
  | Renamed<ItemOf<"mcpToolCall">, "mcp_tool_call">
  | (Renamed<ItemOf<"dynamicToolCall">, "mcp_tool_call"> & {
    readonly server: string;
    readonly result: ItemOf<"dynamicToolCall">["contentItems"];
    readonly error: { readonly message: string } | null;
  })
  | Renamed<ItemOf<"webSearch">, "web_search">
  | Renamed<ItemOf<"contextCompaction">, "context_compaction">
  | PassedThroughItem;

/**
 * An app-server notification as alasio reports it, in the shape of the Codex SDK's
 * thread events, which the exec transport reports.
 */
export type AppServerEvent =
  | { readonly type: "thread.started"; readonly thread_id: string }
  | { readonly type: "turn.started" }
  | { readonly type: "item.started" | "item.updated" | "item.completed"; readonly item: AppServerItem }
  | { readonly type: "turn.completed"; readonly usage: null }
  | { readonly type: "turn.failed"; readonly error: { readonly message: string } }
  | { readonly type: "usage.updated"; readonly usage: v2.ThreadTokenUsage | null }
  | { readonly type: "error"; readonly message: string };

/** A web search's action, as the search's query is looked for in it. */
interface WebSearchActionQuery {
  readonly type: string;
  readonly query?: string | null;
  readonly queries?: readonly string[] | null;
}

/**
 * Where the turn and thread lookups look for ids in a notification's params or in an
 * object in them: the protocol's `turnId`, `threadId`, `turn`, and `thread`, and the
 * other names an id is looked for under.
 */
export interface IdFields {
  readonly turnId?: string | null;
  readonly turn_id?: string;
  readonly threadId?: string | null;
  readonly thread_id?: string;
  readonly turn?: { readonly id?: string };
  readonly thread?: { readonly id?: string };
  readonly [field: string]: unknown;
}

/** A notification's params as the turn and thread lookups read them. */
export interface NotificationIds extends IdFields {
  readonly turn?: IdFields & { readonly id?: string };
  readonly event?: IdFields;
  readonly item?: unknown;
}

/** An error notification's params as they are read for a message: by fields of their own. */
interface ErrorMessageFields {
  readonly message?: string;
  readonly summary?: string;
  readonly [field: string]: unknown;
}

function normalizeAppServerItem(item: v2.ThreadItem): AppServerItem {
  if (!item || typeof item !== "object") {
    return item;
  }
  switch (item.type) {
    case "agentMessage":
      return {
        ...item,
        type: "agent_message",
      };
    case "commandExecution":
      return {
        ...item,
        type: "command_execution",
      };
    case "fileChange":
      return {
        ...item,
        type: "file_change",
      };
    case "mcpToolCall":
      return {
        ...item,
        type: "mcp_tool_call",
      };
    case "dynamicToolCall":
      return {
        ...item,
        type: "mcp_tool_call",
        server: item.namespace ?? "dynamic",
        tool: item.tool,
        arguments: item.arguments,
        result: item.contentItems,
        error: item.success === false ? { message: "Dynamic tool call failed" } : null,
      };
    case "webSearch": {
      const action: WebSearchActionQuery | null = item.action;
      return {
        ...item,
        type: "web_search",
        query: item.query ?? action?.query ?? action?.queries?.join(", ") ?? "",
      };
    }
    case "contextCompaction":
      return {
        ...item,
        type: "context_compaction",
      };
    default:
      return item;
  }
}

/**
 * A notification's item, as its ids are read. Every item is an object but a realtime
 * item, which is JSON of any shape; one that is no object names no id.
 */
export function itemIds(item: unknown): IdFields | undefined {
  // The protocol's items name their ids as IdFields does; a realtime item's JSON is read
  // for them the same way, unchecked, as it always has been.
  return typeof item === "object" && item !== null ? item as IdFields : undefined;
}

export function getNotificationTurnId(message: AppServerNotification): string | null {
  const params: NotificationIds = message?.params ?? {};
  const item = itemIds(params.item);
  return params.turnId
    ?? params.turn_id
    ?? params.turn?.id
    ?? params.event?.turnId
    ?? params.event?.turn_id
    ?? params.event?.turn?.id
    ?? item?.turnId
    ?? item?.turn_id
    ?? item?.turn?.id
    ?? null;
}

export function notificationMatchesTurn(message: AppServerNotification, turnId: Set<string> | string | null | undefined): boolean {
  const notificationTurnId = getNotificationTurnId(message);
  const acceptedTurnIds = turnId instanceof Set ? turnId : new Set([turnId].filter(Boolean));
  return !notificationTurnId || acceptedTurnIds.size === 0 || acceptedTurnIds.has(notificationTurnId);
}

export function isIgnorableNotification(message: AppServerNotification): boolean {
  return IGNORABLE_NOTIFICATION_METHODS.has(message?.method);
}

export function mapNotificationToSdkEvent(message: AppServerNotification): AppServerEvent | null {
  const { method, params } = message;
  switch (method) {
    case "thread/started":
      return {
        type: "thread.started",
        thread_id: params?.thread?.id,
      };
    case "turn/started":
      return {
        type: "turn.started",
      };
    case "item/started":
      return {
        type: "item.started",
        item: normalizeAppServerItem(params?.item),
      };
    case "item/updated":
      return {
        type: "item.updated",
        item: normalizeAppServerItem(params?.item),
      };
    case "item/completed":
      return {
        type: "item.completed",
        item: normalizeAppServerItem(params?.item),
      };
    case "turn/completed": {
      const turn = params?.turn;
      // A turn's status is a string in the protocol, and was read as an object too.
      const status: unknown = turn?.status;
      const failedStatusObject = typeof status === "object" && status !== null && "type" in status && status.type === "failed";
      if (failedStatusObject || turn?.status === "failed" || turn?.error) {
        return {
          type: "turn.failed",
          error: {
            message: turn?.error?.message ?? "Codex turn failed",
          },
        };
      }
      return {
        type: "turn.completed",
        usage: null,
      };
    }
    case "thread/tokenUsage/updated":
      return {
        type: "usage.updated",
        usage: params?.tokenUsage ?? null,
      };
    case "error": {
      const fields: ErrorMessageFields = params;
      return {
        type: "error",
        message: fields?.message ?? fields?.summary ?? "Codex app-server error",
      };
    }
    default:
      return null;
  }
}

export function serverRequestResponse(method: ServerRequest["method"]): ServerRequestResult {
  switch (method) {
    case "item/commandExecution/requestApproval":
      return { decision: "decline" };
    case "item/fileChange/requestApproval":
      return { decision: "decline" };
    case "item/tool/requestUserInput":
      return { answers: {} };
    case "mcpServer/elicitation/request":
      return { action: "cancel", content: null, _meta: null };
    default:
      return {};
  }
}
