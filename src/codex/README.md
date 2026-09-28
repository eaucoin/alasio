## Concept Atlas
```mermaid
mindmap
  root((codex))
    Runtime boundary
      runtime owns one Codex turn and its guardrail recovery loop
      turn-controller owns prompt orchestration queueing delegation and harness resolution and is shared by the Codex and Claude Code adapters
      turn-controller owns service switching and refuses it while a turn is active or prompt jobs are open
      accepted prompts are serialized per conversation through durable SQLite prompt jobs
      status-reporter owns operator progress and final response delivery
      runtime exposes fresh app-server thread creation for Telegram New Session and no-session goal bootstrap paths
      runtime forks a session before one of its turns for rewind through the app-server's thread/fork loaded with the same overrides as a resume under either transport
    Sessions boundary
      sessions is the Codex session panels' api read from the app-server's thread/list scoped to the working directory and thread/turns/list so alasio reads and writes none of Codex's files for them
      sessions are labelled by thread name else first prompt and each turn with an operator prompt is a rewind point whose uuid is the turn id
      rollouts/ keeps every rollout file in alasio's Neon byte for byte and writes back what a thread alasio points at needs as its own README describes
      runtime can attach to an app-server goal-created turn and persist it like an ordinary Telegram-started turn
      active goal controls fall back to ordinary mounted-session turn starts when app-server updates goal state without creating a turn
      active goal prompts use the same steer queue swerve discard decision surface as normal Telegram prompts when a turn is already running
      active Telegram guidance can steer an app-server turn when upstream Codex has accepted a current turn id
      intentional operator interruption is a turn control result rather than a user-visible response block
      local active-query ownership remains held until bounded transport cleanup finishes
      upstream terminal completion is persisted before final response delivery and active-turn cleanup is independent from outbox handoff success
      restart-recovery owns interrupted-turn continuation after service restarts
      restart recovery stages a distinct durable continuation before retiring the interrupted active turn
    Transport boundary
      transport chooses app-server or exec execution without leaking that choice upward
      both transports disable Codex plugin loading at process startup while retaining repository user and built-in skills
      thread-config owns the overrides every thread starts and resumes with which add bayma to the MCP servers in the operator's Codex config
      model selects gpt-5.6-sol with high reasoning for both app-server and exec transports so new resumed and steered continuation turns use one operator-selected authority
      app-server/ owns the long-lived stdio JSON-RPC process, thread RPCs, notification queue, and protocol mapping as separate concepts
      command-event-policy owns command-stream side effects such as restart provenance, workflow wait pings, and DB guardrail aborts
    Projection boundary
      event-projection converts Codex stream items into persisted response blocks
      upstream agent message phase distinguishes internal commentary from the only user-deliverable final answer
      response-markdown converts persisted response blocks into operator-readable text
      phase-less and missing final-answer output are not promoted or replaced with fabricated completion text
      periodic recovery scans terminal responses only and cannot render an active stream
      env builds the process environment passed to Codex and owns codexHome the one place CODEX_HOME is resolved
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Turns as turn-controller
  participant Runtime as runtime
  participant Transport as transport
  participant Policy as command-event-policy
  participant Store as persistence/store
  Turns->>Runtime: execute or resume a turn with explicit thread identity
  Runtime->>Transport: open event stream with configured environment and MCP
  Transport-->>Runtime: emit SDK-shaped events
  Runtime->>Policy: inspect command execution items for local operator policies
  Runtime-->>Store: persist visible blocks and restart provenance
  Runtime-->>Turns: return session id response blocks pending response identity and interruption state
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> CodexConcern
  CodexConcern --> TransportLeak: callers branch on app-server versus exec mechanics
  TransportLeak --> RestartAmbiguity
  RestartAmbiguity --> OperatorMistrust
  RestartAmbiguity --> FalseNoResponse: interrupted turns leak stale completion events into later turns
  FalseNoResponse --> OperatorMistrust
  CodexConcern --> EncapsulatedTurn
  EncapsulatedTurn --> RestartAwareResult
  RestartAwareResult --> [*]
```
