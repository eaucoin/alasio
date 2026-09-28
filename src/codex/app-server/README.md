## Concept Atlas
```mermaid
mindmap
  root((app-server))
    Process
      process owns Codex app-server subprocess startup stdout reading and shutdown
      process disables plugins before app-server initialization so plugin skills and bundled capabilities are not loaded into Alasio
      log owns the app-server logging prefix
    Protocol
      protocol converts app-server notifications into SDK-shaped alasio events
      protocol answers server-side approval or elicitation requests conservatively
    Client
      client owns the public app-server facade used by transport
      rpc-client owns JSON-RPC request response pending timers and server request replies
      rpc-client heeds only the current process's lines exit and errors so a stopped process reporting its exit late cannot fail the requests of the one started after it
      thread-client owns Codex thread resume start fork turn start interrupt and goal calls and the thread turn and model lists every page of which it reads
      thread-client starts resumes and forks every thread with one set of overrides model approval sandbox and alasio's config
      thread-client lists the threads the Codex CLI editors and both alasio transports record and leaves sub-agent threads out
      notification-queue owns turn notification buffering waiter cleanup alias-aware active-turn identity and stale same-thread pruning
      protocol recognizes app-server camelCase and snake_case thread or turn ids across params event and item shapes
      protocol classifies high-volume app-server progress and delta notifications separately from stale turn events
      client bounds skipped-notification drains and yields between skips so Telegram polling and stop controls remain responsive without treating normal progress as fatal
      client treats unknown same-thread app-server notifications as logged progress rather than stale-turn evidence
      client adopts same-thread turn/started notification ids when the turn/start RPC response id is only a transport handle
      explicit mismatched turn ids remain the only notification stream condition that can trip the stale-event guard
      notification-queue retains response and notification ids as aliases of one logical active turn across RPC response handling and stale-discard passes
      notification-queue retires active ownership when completion names any retained alias and preserves ownership for unrelated stale completions
      completed turns keep one bounded alias record until the next turn begins so already-buffered terminal events remain matchable without appearing active
      notification waits have no wall-clock failure timer because silence is valid while explicit abort and transport failure remain cancellation authorities
      notification-queue treats thread goal updates with turn ids as active-turn handoffs so Alasio can stream goal-created work
      thread-client exposes turn steer and attached-turn claiming over the same app-server transport boundary
      client interrupts unfinished app-server turns when the stream exits abnormally so Alasio and upstream Codex do not diverge
      interrupt RPCs log their cleanup origin retain a short control-plane timeout and forget all local aliases before awaiting acknowledgement
      stale cleanup failures recycle app-server then resume the same mounted thread before retrying turn start
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Transport as transport
  participant Client as client
  participant Rpc as rpc-client
  participant Threads as thread-client
  participant Proc as process
  participant Queue as notification-queue
  participant Protocol as protocol
  Transport->>Client: ensure thread start turn attach goal turn steer active turn stream events or inspect thread goal state
  Client->>Threads: execute thread and turn RPC methods
  Threads->>Rpc: send JSON-RPC requests
  Rpc->>Proc: start long-lived stdio app-server
  Proc-->>Rpc: JSON-RPC lines
  Rpc->>Queue: buffer notifications by thread while preserving logical-turn aliases and pruning stale same-thread turn events
  Client->>Protocol: normalize SDK-shaped events and classify progress-only notifications as ignorable
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Monolith
  Monolith: process lifecycle RPC pending maps notification waits and event mapping collapse into one file
  Monolith --> HiddenRace
  HiddenRace --> SlowDebug
  SlowDebug --> OperatorLatency
  Monolith --> SplitBoundary
  SplitBoundary --> SmallProcess
  SplitBoundary --> SmallProtocol
  SplitBoundary --> SmallQueue
  SmallQueue --> LegibleTurnStream
  LegibleTurnStream --> StaleTurnFiltered: old interrupted turn events cannot satisfy or monopolize a later Telegram message
  LegibleTurnStream --> ProgressStorm: current-turn deltas are mistaken for stale backlog
  ProgressStorm --> PollerResponsive: progress notifications are ignored without crossing stale-failure thresholds
  LegibleTurnStream --> UnknownProgress: new app-server notification methods arrive before alasio maps them
  UnknownProgress --> PollerResponsive: same-thread unknown notifications are logged by method and ignored without aborting the stream
  LegibleTurnStream --> ResponseHandleMismatch: turn/start response id differs from rollout notification turn id
  ResponseHandleMismatch --> AliasedTurn: response and notification ids identify one logical active turn
  AliasedTurn --> LegibleTurnStream: completion under either identity retires ownership without interrupting later work
  LegibleTurnStream --> QuietTurn: no notification arrives during a long valid operation
  QuietTurn --> LegibleTurnStream: wait remains pending until notification explicit abort or transport failure
  PollerResponsive --> LegibleTurnStream
  LegibleTurnStream --> [*]
```
