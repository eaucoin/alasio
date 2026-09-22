## Concept Atlas
```mermaid
mindmap
  root((harness))
    Names
      names owns the codex and claude identifiers display names spelling normalization and the optional ALASIO_DEFAULT_HARNESS pre-mount which is null by default
    Registry
      index builds adapters lazily per harness and folder pair and resolves the adapter for a conversation from its persisted active harness and working directory
      resolveHarnessName and resolveWorkingDirectory return null while nothing is mounted and requireForConversation throws NO_SERVICE_MOUNTED or NO_WORKSPACE_MOUNTED so no caller can coerce a default harness or folder
      interruptActiveTurn aborts whichever harness owns the active query for a thread key
    Adapter contract
      startFreshSession warmSession executeTurn shutdown and a sessions api with listSessions getTotalSessionPages getSessionByNumber getSessionLastMessage listSessionMessages getTotalRewindPages and createForkedSession
      supportsGoals supportsWarmup and supportsSteer let operator controls hide or refuse features a harness lacks
      executeTurn returns the same blockSequence sessionId pendingResponseId interrupted and responseCompleted shape for every harness
    Codex adapter
      codex wraps the existing app-server runtime and rollout JSONL session discovery without changing them
    Claude Code adapter
      claude/runtime runs one Agent SDK query per Telegram prompt with bypassPermissions the claude_code system prompt preset bayma added to the MCP servers Claude Code loads from the operator's configuration
      claude/prompt-channel keeps streaming input open so Steer pushes guidance into the live session and the channel ends after the result message
      claude/event-projection maps assistant tool_use blocks onto Codex-shaped items and the SDK result onto the final_answer phase
      claude/sessions reads the SDK project transcript store scoped to the working directory and forks with upToMessageId before the chosen user message
      fresh Claude sessions are reserved ids passed as sessionId on the first query and resumed with resume afterwards
      Bash PreToolUse hooks reuse command-event-policy for restart provenance and deny forbidden database commands with guardrail guidance
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Turns as codex/turn-controller
  participant Registry as harness/index
  participant Store as persistence/store
  participant Adapter as codex or claude adapter
  Turns->>Registry: forConversation(store, conversationId)
  Registry->>Store: getActiveHarness(conversationId)
  Registry-->>Turns: adapter
  Turns->>Adapter: executeTurn with the active harness session pointer
  Adapter-->>Turns: blocks session id interruption and completion state
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> HarnessChoice
  HarnessChoice --> BranchingCallers: turn-controller or panels branch on codex versus claude mechanics
  BranchingCallers --> DriftingFeatures
  DriftingFeatures --> OperatorMistrust
  HarnessChoice --> ForeignMount: a session id from one harness is passed to the other harness
  ForeignMount --> OperatorMistrust
  HarnessChoice --> SilentDefault: a null harness is coerced to codex instead of surfacing the picker
  SilentDefault --> OperatorMistrust
  HarnessChoice --> AdapterBoundary
  AdapterBoundary --> ParkedPointers: each harness keeps its own session pointer and switching waits for idle
  ParkedPointers --> [*]
```
