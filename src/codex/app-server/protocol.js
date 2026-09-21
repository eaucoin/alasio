const IGNORABLE_NOTIFICATION_METHODS = new Set([
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

function normalizeAppServerItem(item) {
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
    case "webSearch":
      return {
        ...item,
        type: "web_search",
        query: item.query ?? item.action?.query ?? item.action?.queries?.join(", ") ?? "",
      };
    case "contextCompaction":
      return {
        ...item,
        type: "context_compaction",
      };
    default:
      return item;
  }
}

export function getNotificationTurnId(message) {
  const params = message?.params ?? {};
  return params.turnId
    ?? params.turn_id
    ?? params.turn?.id
    ?? params.event?.turnId
    ?? params.event?.turn_id
    ?? params.event?.turn?.id
    ?? params.item?.turnId
    ?? params.item?.turn_id
    ?? params.item?.turn?.id
    ?? null;
}

export function notificationMatchesTurn(message, turnId) {
  const notificationTurnId = getNotificationTurnId(message);
  const acceptedTurnIds = turnId instanceof Set ? turnId : new Set([turnId].filter(Boolean));
  return !notificationTurnId || acceptedTurnIds.size === 0 || acceptedTurnIds.has(notificationTurnId);
}

export function isIgnorableNotification(message) {
  return IGNORABLE_NOTIFICATION_METHODS.has(message?.method);
}

export function mapNotificationToSdkEvent(message) {
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
      if (turn?.status?.type === "failed" || turn?.status === "failed" || turn?.error) {
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
    case "error":
      return {
        type: "error",
        message: params?.message ?? params?.summary ?? "Codex app-server error",
      };
    default:
      return null;
  }
}

export function serverRequestResponse(method) {
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
