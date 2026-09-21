## Concept Atlas
```mermaid
mindmap
  root((sessions))
    JSONL discovery
      discovery owns Codex rollout session-file traversal session ids and date labels
      jsonl owns JSONL parsing and bounded file-slice reads
      summary owns assistant-text extraction from large rollout files
      index owns numbered session and rewind listings
      forking owns rewind continuation creation from source JSONL mechanics
      session discovery intentionally reads Codex's canonical user session store rather than the alasio working directory
    Operator contract
      command-handler depends on this folder for resume and rewind truth
      numeric session references are display conveniences not persisted identifiers
      UUID session references must round-trip without prefix ambiguity
    Failure posture
      missing or malformed JSONL files are skipped rather than corrupting listings
      fork failures return explicit operator messages instead of changing active state
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Operator as command-handler
  participant Index as index
  participant Discovery as discovery
  participant Summary as summary
  participant Fork as forking
  Operator->>Index: list sessions or rewind points
  Index->>Discovery: find canonical Codex rollout files
  Index->>Summary: read bounded assistant summaries
  Operator->>Fork: create continuation before selected message
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> SessionLookup
  SessionLookup --> DuplicateJsonlWalker
  DuplicateJsonlWalker --> DivergentResumeBehavior
  DivergentResumeBehavior --> WrongForkPoint
  WrongForkPoint --> OperatorLosesContext
  OperatorLosesContext --> DuplicateJsonlWalker
  SessionLookup --> CanonicalFiles
  CanonicalFiles --> StableNumbering
  StableNumbering --> ExplicitFork
  CanonicalFiles --> PredictableResume
  PredictableResume --> [*]
```
