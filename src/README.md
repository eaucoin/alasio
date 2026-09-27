## Concept Atlas
```mermaid
mindmap
  root((alasio source))
    Root entrypoint
      config owns alasio environment loading and index starts the Telegram Codex app
      root stays intentionally thin so source domains do not collapse back into one flat namespace
    telegram/
      owns Bot API polling lifecycle authorization callbacks file downloads message projection Markdown-safe response rendering text splitting media-group buffering and durable outbox delivery
      filenames omit telegram prefixes because the directory supplies that context
    harness/
      owns harness names the adapter registry Codex and Claude Code adapters and the shared active-turn interrupt
      claude/ owns the Claude Agent SDK runtime prompt channel event projection session discovery MCP conversion model overrides and the session store that keeps its transcripts in Neon
      the registry resolves one adapter per conversation from the persisted active harness and folder and yields null while either is missing so ingress gates on the service then folder pickers instead of a default
      adapters are created lazily per harness and folder pair because Claude transcript stores and Codex thread cwds are folder scoped
      workspace owns the folder policy that keeps every Telegram-chosen folder under ALASIO_WORKSPACE_ROOT
    codex/
      owns Codex turn orchestration runtime transport event projection status reporting restart recovery and app-server protocol
      turn-controller and status-reporter are harness-neutral and receive the adapter plus display name per turn
      model owns the configured Codex model and reasoning effort shared by app-server and exec transports
      app-server/ splits protocol-heavy transport mechanics into explicit smaller modules
      exec transport remains a rollback path behind ALASIO_CODEX_TRANSPORT=exec
      filenames omit codex prefixes except where external protocol names make them useful
    operator/
      owns operator command parsing command execution Telegram-native session goal and service control panels session reply formatting restart prompts and shared operator-facing text helpers
      session panels render through the active harness adapter's session api so Codex rollouts and Claude transcripts never mix
    sessions/
      owns Codex rollout JSONL discovery session listing and rewind forking
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
    shared/
      owns cross-domain helpers for async timing file prompt suffixes duration text ids scoped logging and runtime constants
    Persistence
      state belongs under the configured alasio data directory rather than ad hoc JSON files
      SQLite schema is canonical DDL for the local content database
      Claude Code transcripts are the one thing kept in Neon rather than SQLite because the Agent SDK's SessionStore is the interface they are mirrored through
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
