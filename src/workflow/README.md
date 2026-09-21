## Concept Atlas
```mermaid
mindmap
  root((workflow))
    Hook server
      hook-server owns the localhost HTTP endpoint used by CI wait notifications
      received events wake Codex status reporting without changing turn ownership
    Runtime integration
      codex runtime records workflow waits through policy detection
      status reporter reads shared wait maps and emits truthful progress text
      hook-server never starts Codex work and never mutates prompt queues
    Failure posture
      malformed hook payloads fail as HTTP errors with concrete messages
      missing session ids do not fabricate workflow state
      service shutdown closes the hook before persistence is closed
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant CI
  participant Hook as hook-server
  participant Store as persistence/store
  participant Waits as shared wait maps
  participant Status as codex/status-reporter
  CI->>Hook: POST workflow wait/wake event
  Hook->>Store: inspect active turns by session id
  Store-->>Hook: return matching thread key or empty evidence
  Hook->>Waits: record run id wait type command and started-at
  Hook-->>Status: resolve wake event through shared maps
  Status->>Waits: read latest workflow wait for the session
  Status-->>CI: no outbound CI mutation from this boundary
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> WorkflowWait
  WorkflowWait --> HiddenPolling
  HiddenPolling --> SlowOperatorFeedback
  SlowOperatorFeedback --> DuplicateStatusEdits
  DuplicateStatusEdits --> HiddenPolling
  WorkflowWait --> HookOwnsTooMuch: hook server tries to own turn execution
  HookOwnsTooMuch --> RestartRace
  WorkflowWait --> LocalHook
  LocalHook --> ActiveTurnLookup
  ActiveTurnLookup --> WaitMapWake
  LocalHook --> PromptStatusUpdate
  PromptStatusUpdate --> [*]
```
