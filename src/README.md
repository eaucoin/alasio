## Concept Atlas
```mermaid
mindmap
  root((alasio source))
    Root entrypoint
      config owns alasio environment loading and index starts telemetry before anything else loads and then main which starts Neon Codex's rollout mirror the Telegram Codex app and transcript search
      root stays intentionally thin so source domains do not collapse back into one flat namespace
    telegram/
      owns Bot API polling lifecycle authorization callbacks file downloads message projection Markdown-safe response rendering text splitting media-group buffering and durable outbox delivery
      filenames omit telegram prefixes because the directory supplies that context
    harness/
      owns harness names the adapter registry Codex and Claude Code adapters and the shared active-turn interrupt
      claude/ owns the Claude Agent SDK runtime prompt channel event projection session discovery MCP conversion model overrides the session store that keeps its transcripts in Neon and claude/search which indexes those transcripts for search by words and trigrams
      the registry resolves one adapter per conversation from the persisted active harness and folder and yields null while either is missing so ingress gates on the service then folder pickers instead of a default
      adapters are created lazily per harness and folder pair because Claude transcript stores and Codex thread cwds are folder scoped
      workspace owns the folder policy that keeps every Telegram-chosen folder under ALASIO_WORKSPACE_ROOT
    codex/
      owns Codex turn orchestration runtime transport event projection status reporting restart recovery app-server protocol the Codex session api and rollouts/ which keeps Codex's rollout files in Neon
      turn-controller and status-reporter are harness-neutral and receive the adapter plus display name per turn
      model owns the configured Codex model and reasoning effort shared by app-server and exec transports
      app-server/ splits protocol-heavy transport mechanics into explicit smaller modules
      exec transport remains a rollback path behind ALASIO_CODEX_TRANSPORT=exec
      filenames omit codex prefixes except where external protocol names make them useful
    operator/
      owns operator command parsing command execution Telegram-native session goal and service control panels session reply formatting restart prompts and shared operator-facing text helpers
      session panels render through the active harness adapter's session api so Codex rollouts and Claude transcripts never mix
    persistence/
      owns SQLite schema store facade and table-group repositories for callbacks conversations prompt jobs responses outbox restarts state Telegram content turns and usage
      conversations carry an active harness plus one parked session pointer per harness and turns and prompt jobs record their admitting harness
      store remains the only persistence facade imported by transport/runtime code
    policy/
      owns shell command parsing restart command recognition forbidden database command detection and workflow wait detection
    mcp/
      owns bayma the MCP server alasio adds to each harness's own servers its image launch command per-conversation state directory and readiness check
    neon/
      owns bringing up alasio's Neon stack from neon/compose.yml before the app starts and the pool alasio keeps to its compute
    workflow/
      owns the localhost hook server that receives CI wait notifications
    telemetry/
      owns OpenTelemetry for alasio configured from the standard OTEL_* variables alone the SDK started before the service loads and the spans calls and context hand-offs the rest of alasio records through
      keeps alasio's own telemetry settings out of the harnesses' environments which get settings of their own from harness/claude/telemetry and codex/app-server/telemetry
    shared/
      owns cross-domain helpers for async timing file prompt suffixes duration text ids scoped logging and runtime constants
    Persistence
      state belongs under the configured alasio data directory rather than ad hoc JSON files
      SQLite schema is canonical DDL for the local content database
      Claude Code transcripts and Codex rollouts are kept in Neon rather than SQLite as the durable copies of the harnesses' own sessions the transcripts through the Agent SDK's SessionStore and the rollouts as opaque files
      table-group repositories stay behind persistence/store.js
```

## Preference Atlas
```mermaid
sequenceDiagram
  autonumber
  participant Telegram
  participant App as telegram/app
  participant Store as persistence/store
  participant Turns as codex/turn-controller
  participant Codex as codex/runtime
  participant Client as telegram/client
  Telegram->>App: deliver authorized private-chat update
  App->>Store: persist offset, message, file, and conversation continuity
  App->>Turns: dispatch command, native session service or goal callback, queued message, restart continuation, or agent prompt through one turn boundary
  Turns->>Codex: resolve the active harness adapter then execute or resume a turn with explicit working directory and thread key
  Codex-->>Store: stream response blocks and restart provenance
  Turns->>Store: enqueue the final Telegram reply independently from Codex completion
  Store->>Client: drain durable outbound replies with rate-limit backoff
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> ExplicitRuntimeBoundary
  ExplicitRuntimeBoundary --> HiddenState: source modules write untracked ad hoc state beside code
  HiddenState --> RestartAmbiguity: service restarts cannot reconstruct transport or Codex continuity
  RestartAmbiguity --> OperatorConfusion
  OperatorConfusion --> HiddenState
  ExplicitRuntimeBoundary --> SQLiteBackedContinuity
  SQLiteBackedContinuity --> RestartSafeOperation
  RestartSafeOperation --> [*]
```
