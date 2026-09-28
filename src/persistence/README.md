## Concept Atlas
```mermaid
mindmap
  root((persistence))
    Store facade
      store is the only persistence surface imported by transport/runtime code
      schema is canonical DDL for the local SQLite content database
      prompt jobs checkpoint upstream start and completion independently from Telegram delivery state
      restart provenance active-turn retirement and deterministic continuation enqueue commit in one SQLite transaction
      response blocks carry an explicit terminal marker written only from upstream terminal turn events
      terminal response recovery reconstructs all blocks only after that marker exists
      each pending response id maps to at most one durable Telegram outbox entry
      store translates runtime chat/message terms onto persisted response column names without leaking them upward
      conversations persist a nullable active_harness plus codex_session_id and claude_session_id so getSessionId returns nothing and setSessionId prompt job enqueue and active turn upsert refuse while no service is mounted
      schema v6 relaxes the v5 not-null active_harness constraint by swapping the column and keeps every existing mount while pre-harness rows stay mounted on codex
      schema v7 adds a nullable conversations.working_directory and a workspace_sessions table keyed by conversation harness and folder so setWorkingDirectory parks the current pointers and restores the chosen folder's own
      listHarnessSessionReferences returns every session one harness is pointed at from conversations parked folder sessions and active turns with its folder so startup can adopt each Claude transcript into the session store and write back each Codex thread's missing rollout files
      the store takes a state root or explicit dbPath plus an optional defaultWorkingDirectory that pre-mounts new conversations and backfills mounted rows that predate per-conversation folders
      turns and prompt jobs record their admitting harness and restart recovery restores the session pointer of that harness
      callback actions capture expectedHarness beside expectedSessionId so panels rendered under one harness go stale after a switch
      schema version 5 adds the harness columns with additive alter-table migrations
    Repositories
      table-group repositories own callbacks conversations prompt jobs responses Telegram outbox restarts state Telegram content turns and usage
      repository names keep Sqlite where the storage adapter is part of the contract
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant App as telegram/app
  participant Runtime as codex/runtime
  participant Store as store
  participant Repo as table repositories
  App->>Store: persist updates messages files offsets prompt jobs and outbound replies
  Runtime->>Store: persist turns blocks restarts and usage independently from Bot API delivery
  Store->>Repo: delegate table-specific behavior
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> RuntimeState
  RuntimeState --> AdHocJson: source modules write local JSON or bypass schema
  AdHocJson --> RestartLoss
  RestartLoss --> DuplicateRecoveryPath
  DuplicateRecoveryPath --> AdHocJson
  RuntimeState --> DirectTableReachIn: runtime code imports table-specific repositories
  DirectTableReachIn --> FacadeErosion
  FacadeErosion --> RestartLoss
  RuntimeState --> StoreFacade
  StoreFacade --> SchemaMigration
  SchemaMigration --> RepositoryDelegation
  StoreFacade --> SchemaBackedContinuity
  SchemaBackedContinuity --> [*]
```
