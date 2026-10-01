## Concept Atlas
```mermaid
mindmap
  root((codex))
    Runtime boundary
      runtime owns one Codex turn and its guardrail recovery loop
      turn-controller owns prompt orchestration queueing delegation and harness resolution and is shared by the Codex and Claude Code adapters
      turn-controller owns service switching and refuses it while a turn is active or prompt jobs are open
      accepted prompts are serialized per conversation through durable SQLite prompt jobs
      each turn is the span alasio.turn continuing the trace of the update that queued its prompt from the traceparent its prompt job keeps restarts included and its outcome labels it and alasio.turn.duration beside alasio.turn.active and alasio.prompt.wait
      messages queued while a turn ran run after it as a turn and a trace of their own so neither turn's duration holds the other
      turn-timing logs each turn's timeline in its harness records it as events of the turn's span and measures the first visible output as alasio.turn.first_output
      status-reporter owns operator progress and final response delivery its status line stating its start as a relative date-time the Telegram app keeps current
      reply-media resolves the files a final response shows with image syntax against the conversation's workspace reading a session filesystem's through its sandbox so a path reaches only what the agent can and copies them under the state directory until delivered
      env hands Codex alasio's environment without alasio's telemetry settings and the app-server gets settings of its own from app-server/telemetry
      config-toml reads the operator's own developer instructions from their Codex config so alasio's are added after them rather than replacing them
      runtime exposes fresh app-server thread creation for Telegram New Session and no-session goal bootstrap paths
      runtime awaits the caller's beforeResponseComplete once a turn completes and before its response is marked complete so nothing delivers a reply first
      runtime forks a session before one of its turns for rewind through the app-server's thread/fork loaded with the same overrides as a resume under either transport
    Sessions boundary
      sessions is the Codex session panels' api read from the app-server's thread/list scoped to the working directory and thread/turns/list so alasio reads and writes none of Codex's files for them through a listing scope the harness gives which is the shared app-server for a folder and the session-filesystem one for a session filesystem
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
      runtime runs every call against a scope cwd env config and client which it builds for a folder from the shared app-server and which a session filesystem's harness passes in
      sessionfs is Codex for session filesystems one app-server for all of them run here with a Codex home of alasio's own so none of the operator's configuration reaches an isolated workspace and with no environment at all so it registers no shell apply_patch or view_image by construction since thread resume and fork carry no environments field
      its per-thread config gives the workspace's bayma forward as the bayma MCP server with the forward's bearer and alasio's instructions and a thread runs in the workspace's harness directory which keeps each workspace's threads apart
      login-relay is how that home uses the operator's Codex login without a copy that would refresh on its own and invalidate theirs a loopback relay taking only the app-server's bearer and the model API's paths and sending each request on with the login read fresh from the operator's auth.json a ChatGPT login to the Codex backend with its account id and an API key to the OpenAI API
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
