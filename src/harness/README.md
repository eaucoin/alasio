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
      createForkedSession takes the conversation as threadKey since the fork becomes that conversation's session
      supportsGoals supportsWarmup and supportsSteer let operator controls hide or refuse features a harness lacks and a harness that supports goals offers them as goals read set clear and waitForTurnId over its own app-server
      executeTurn returns the same blockSequence sessionId pendingResponseId interrupted and responseCompleted shape for every harness
      the registry hands each adapter where its harness's sessions are kept sessionStore for Claude Code and codexRollouts for Codex
    Codex adapter
      codex wraps the app-server runtime with codex/sessions as its sessions api
      with codexRollouts codex writes back a thread's missing rollout files from Neon before resuming warming or forking it and logs rather than fails when Neon cannot be read
      with codexRollouts each turn's thread is flushed to Neon before the turn's response is marked complete through the runtime's beforeResponseComplete
    Claude Code adapter
      claude/runtime builds the Agent SDK options with bypassPermissions the claude_code system prompt preset and bayma added to the MCP servers Claude Code loads from the operator's configuration
      claude/live-sessions gives a folder conversation's process its host bayma from mcp/bayma as an http MCP server once it answers and makes sure of a session filesystem's Sandbox before every turn since the process outlives turns and the Sandbox may have been suspended between them
      claude/live-sessions keeps one Claude Code process per conversation for as long as its session and model stay mounted and routes each result to the operator turn it names or to a turn Claude Code started itself
      claude/env hands Claude Code alasio's environment without alasio's telemetry settings and claude/telemetry gives the process its own each signal alasio exports with the conversation as a resource attribute
      claude/live-sessions starts each process outside the trace of the turn that starts it since the process outlives that turn so Claude Code's traces are its own and are found from a turn by session id
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
      claude/search indexes the store's entries into passages for full-text and trigram search off the SDK's path as its own README describes
      fresh Claude sessions are reserved ids passed as sessionId on the first query and resumed with resume afterwards
      Bash Monitor Grep and Glob are disallowed so bayma exec is Claude's only shell and a PreToolUse hook on it reuses command-event-policy on the shell commands policy/embedded-shell recovers from the code
    Reply instructions
      reply-instructions is what alasio tells every agent about its replies beyond the harness's own prompt how to show the operator an image or video with image syntax and a local path appended to Claude Code's system prompt and given to Codex as developer instructions after the operator's own
    Session filesystems
      the registry hands each adapter the sandbox the session-filesystem Codex and its rollout mirror and a folder workspace gets neither and runs in its folder with its conversation's host bayma
      a session-filesystem workspace's harness runs in alasio in the workspace's harness directory an empty one of its own and reaches the workspace only through its Sandbox's bayma with the session's token so every session api reads that directory's sessions and a thread's work brings the Sandbox up while lists goals and models do not
      the Claude Code adapter keeps alasio's login and Claude home so resume the Neon mirror adoption and search work as in a folder and claude/sessionfs confines it with a tools allowlist of subagents web search and the task list no setting sources strict MCP config and bayma as the one MCP server since web fetch would run in alasio's pod and reach its loopback
      the Codex adapter runs on the session-filesystem app-server codex/sessionfs with the workspace's bayma per thread and its rollouts mirrored from that app-server's own home
      workspace-instructions tells an agent in a session filesystem that its workspace is /workspace on an isolated machine reached through bayma and that its harness's directory holds nothing of its own
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
