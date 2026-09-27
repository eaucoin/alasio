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
      claude/runtime builds the Agent SDK options with bypassPermissions the claude_code system prompt preset and bayma added to the MCP servers Claude Code loads from the operator's configuration
      claude/live-sessions keeps one Claude Code process per conversation for as long as its session and model stay mounted and routes each result to the operator turn it names or to a turn Claude Code started itself
      claude/prompt-channel is the open streaming input that operator prompts and Steer are pushed into and it only ends when the live process is closed
      claude/event-projection maps assistant tool_use blocks onto Codex-shaped items and the SDK result onto the final_answer phase
      claude/sessions reads the SDK project transcript store scoped to the working directory and forks with upToMessageId before the chosen user message
      claude/session-store is NeonSessionStore the Agent SDK SessionStore on alasio's Neon keeping every transcript entry in order keyed by project session and subpath with entries re-delivered after a retried append kept once by uuid and session summaries folded in the same transaction
      entries are stored as json so each keeps its exact text and every row also gets doc a jsonb copy for SQL to query written with the entry from the parsed entry with only real NULs and unpaired surrogates as U+FFFD and null rather than a failed append where Postgres will not take it
      the durable copy of a Claude transcript is the store and the JSONL under the operator's Claude home is a cache Claude Code keeps working from
      each query gets mirrorOnly of the store with eager flushing so the SDK mirrors every entry into it as it is written rather than at the end of a turn while resuming stays on the local transcript because an SDK resume from a store runs without the operator's skills memory and plugins
      a failed mirror write surfaces as a mirror_error system message which live-sessions logs and adoption repairs at the next start
      claude/transcripts writes a transcript missing locally back from the store main transcript last and atomically with its subagents and their metadata before any session api call or resume and adoptTranscripts imports every session alasio points at into the store at startup then only adds the entries it lacks
      session panels read the store when one is configured and fall back to the local transcripts if it fails
      fresh Claude sessions are reserved ids passed as sessionId on the first query and resumed with resume afterwards
      Bash Monitor Grep and Glob are disallowed so bayma exec is Claude's only shell and a PreToolUse hook on it reuses command-event-policy on the shell commands policy/embedded-shell recovers from the code
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
  HarnessChoice --> StoreBackedResume: a query given the store to resume from
  StoreBackedResume --> SkillsAndMemoryLost
  SkillsAndMemoryLost --> OperatorMistrust
  HarnessChoice --> AdapterBoundary
  AdapterBoundary --> ParkedPointers: each harness keeps its own session pointer and switching waits for idle
  ParkedPointers --> [*]
```
