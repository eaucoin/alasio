## Concept Atlas
```mermaid
mindmap
  root((codex/rollouts))
    Why rollouts
      a rollout is Codex's record of one thread and the one source every index Codex keeps is rebuilt from so the files alone bring a thread back whole with its history and its prompt cache key
      Codex has no storage hook a program outside it can plug a database into so alasio keeps the files themselves byte for byte and never interprets them
      the only field alasio reads is session_meta.history_base in a file's first line which names the rollout a fork's or a revert's history starts in
    Files
      files finds rollouts by Codex's own naming rollout-timestamp-thread id with _rollout id after it for a reverted thread's newer file under sessions and archived_sessions of CODEX_HOME
      a file is named by its file name which archiving does not change and a compressed file keeps its plain name
    Store
      store is NeonRolloutStore on alasio's Neon schema codex_sessions with one rollouts row per file and its bytes as rollout_chunks in order
      each row records the file's place thread id rollout id history base size first-line digest and modification time which Codex dates the thread by
      lineage follows history_base transitively in one recursive query so a thread's files and every file its history starts in come together
    Mirror
      index is startCodexRollouts which src/index.js starts right after Neon so no turn runs unmirrored and which offers restore flush and close
      src/index.js starts one for the operator's Codex home into codex_sessions and when session filesystems are on one for the session-filesystem Codex home into codex_sessionfs_sessions so each thread is restored into the home it runs from
      the kernel reports each write to sessions and archived_sessions through recursive fs.watch and the file is mirrored within milliseconds including in day folders made later
      while a rollout directory does not exist yet as in a new Codex home the home itself is watched for it so alasio never makes directories in Codex's home
      a check every thirty seconds mirrors whatever no report covered such as changes made while alasio was down and restarts any watch that failed and a change it finds that was written over five seconds before is logged as one no watch reported
      every write to the store goes through one queue so two changes to one file are never mirrored at once
      a file that grew with the same first line gets only its new bytes as one more chunk since Codex only appends to a rollout
      any other change such as Codex's migration rewriting a file gets all its bytes again and a file moved by archiving only has its place updated
      a file without a whole first line yet waits for its next write and a file shorter than it was listed is read again
      the mirror is off Codex's path so a failure is logged once and the next check catches up on everything
    Turn completion
      flush mirrors one thread's files through the same queue and the Codex adapter awaits it before a turn's response is marked complete so any reply the operator can see is already in Neon
      a flush that fails or takes over five seconds is logged and the reply goes on without it rather than waiting on Neon
    Restore
      restore writes back every file a thread alasio points at needs that its Codex home lacks byte for byte where it was with its modification time and through a partial file renamed into place
      telegram/app restores every Codex thread alasio points at before any turn at startup and the Codex adapter restores a thread before it is resumed warmed or forked
      a file present here or present compressed is never touched and a thread nobody points at is never written back so deleting a thread locally sticks
      one file that cannot be written back is logged and the rest still are
    Limits
      Codex writes a file before alasio can copy it so a turn still running can lag the disk by the milliseconds a report and an insert take
      Codex's goals queued prompts and memories live in its other SQLite databases and are not kept
      files Codex's optional compression has compressed are not mirrored and keep the copy made before
      after a restore onto a fresh Codex index a thread's rewind points appear once it has been resumed and a fork's or revert's list only the turns of its own file
      the contract relied on rollouts as Codex's source of truth is Codex's design not a documented API so test/codex-sessions.test.js reruns it against each Codex upgrade
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Codex as Codex app-server
  participant Home as CODEX_HOME rollout files
  participant Mirror as rollouts/index and mirror
  participant Store as NeonRolloutStore
  participant Restore as rollouts/restore
  participant Adapter as harness/codex
  Codex->>Home: append to a thread's rollout
  Home-->>Mirror: the kernel reports the write
  Mirror->>Store: keep the new bytes or all of them
  Adapter->>Mirror: flush the turn's thread before its response is complete
  Restore->>Store: lineage of the threads alasio points at
  Restore->>Home: write back what is missing whole
  Codex->>Home: rebuild its indexes from the files
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> KeepingThreads
  KeepingThreads --> ParsedFormats: alasio parses or writes Codex's rollout formats itself
  ParsedFormats --> BreaksOnUpgrade
  KeepingThreads --> WrittenIndexes: alasio writes Codex's SQLite indexes
  WrittenIndexes --> BreaksOnUpgrade
  KeepingThreads --> ResurrectedThreads: every stored thread is written back
  ResurrectedThreads --> OperatorMistrust
  KeepingThreads --> PolledCopies: files copied only on a timer
  PolledCopies --> RepliesAheadOfNeon: the operator sees a reply Neon does not hold yet
  RepliesAheadOfNeon --> OperatorMistrust
  KeepingThreads --> OpaqueFiles: files kept and written back byte for byte
  OpaqueFiles --> CodexRebuilds: Codex rebuilds its indexes itself
  CodexRebuilds --> [*]
```
