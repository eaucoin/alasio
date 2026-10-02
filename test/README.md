## Concept Atlas
```mermaid
mindmap
  root((alasio tests))
    Regression contracts
      authorization tests pin one private operator across message and callback ingress and reject callbacks before consuming stored actions
      harness selection tests pin per-harness parked session pointers schema v5 migration switch refusal while working or queued restart recovery under the owning harness and the /service panel and callbacks
      Claude harness tests pin prompt channel ordering tool_use projection result-as-final-answer interruption classification steering guardrail denial and SDK transcript to session and rewind mapping without spawning Claude Code
      session store tests run the Agent SDK's SessionStore conformance cases against NeonSessionStore on a throwaway Postgres and pin retried-append dedupe exact text for entries holding a NUL or half a surrogate pair with a jsonb doc beside it that changes only real NULs and unpaired surrogates a null doc rather than an error where Postgres will not take one the doc backfill order past one insert's parameter limit summary folding the query's mirror-only view and the SDK's list read and fork helpers on the store
      codex rollout tests pin Codex's rollout names the mirror keeping a new file whole a grown one's new bytes only a rewritten or shorter one whole again an archived one by its new place and neither a file without a whole first line nor a compressed one and restore writing back a thread's missing files with every file its history starts in byte for byte with their modification times while leaving present and compressed files and threads not asked for alone against a throwaway Postgres and startCodexRollouts mirroring each write as it happens in a home and day folders made after it started long before its check and flush holding a thread's files when it returns
      codex runtime tests pin a turn's response marked complete only after beforeResponseComplete finishes
      codex session tests pin the session api's labels rewind points and unreadable threads and run the real app-server against a local stand-in for the Responses API with no login and no model through listing messages last answer and fork before a turn with its files mirrored from the start and exactly held after each turn's flush then lose the whole Codex home restore the fork and prove its next turn carries the history before the rewound turn and not after
      transcript tests pin writing a missing transcript back with its subagents and metadata leaving a local one alone and adoption importing a session whole then adding only what the mirror dropped
      search tests pin passage extraction by rule from each entry shape the indexer's batches settled mark and deletion cascade and search by words trigrams kind weighting and filters against a throwaway Postgres
      neon/test runs the same conformance cases on the chart's Neon in an installed release with its crash volume-loss garbage-collection backup point-in-time and lake cases through `npm run test:neon`
      bayma MCP tests pin a folder conversation's bayma as a host Sandbox with its own state directory and telemetry one per harness and conversation none without the host profile and the http bayma entry each harness adds beside the operator's own servers
      kube-sandboxes tests pin the deployment's templates checked at startup tokens naming their Sandbox and compared whole every Sandbox serving bayma with its token from a Secret it owns readiness only for the current generation ensure making resuming sharing and giving up with a reason a session's labels DNS egress gate and telemetry settings the egress gate against a reachable and an unreachable API server reading a session's files as its agent through exec and the receiver taking a session's OTLP with its token only rate-bounded and stamped
      otlp-resource tests stamp requests OpenTelemetry's own SDK and serializers made for every signal in both encodings and read them back with protobufjs as an independent decoder and telemetry-forward tests pin each signal's endpoint headers compression retries and timeout
      sandbox tests pin volume ids as DNS labels the two workspace kinds the session-filesystem Claude Code's tools and Codex home and each session's harness directory under the state directory
      lake tests run the real DuckDB the analytics lake runs against a throwaway Postgres written through alasio's own stores with the lake role's grants made as production makes them and pin typed entries kept whole messages counted once with their final usage tool calls paired with results deletions and late commits followed batches whole when a load is cut short JSON DuckDB cannot read kept and flagged rollout lines loaded as completed with rewrites moves and removals followed both Codex homes the model rebuilt on a version change maintenance recorded the loader's retries and health read-only queries and no reads with the lake off
      neon-setup tests pin the stack's setup configuration secrets made once and every service's rendered from them on each run a root predating a secret completed without losing the rest and an external object store's credentials used with no SeaweedFS identities made
      app-server protocol tests pin notification turn identity across direct nested and item-shaped payloads
      app-server request contracts pin gpt-5.6-sol with high reasoning for thread creation and turn execution
      queue tests assert stale interrupted-turn completions cannot clear or satisfy the active Telegram turn
      queue tests assert thread goal updates with turn ids are retained as attachable active turns
      queue tests assert turn/start response handles cannot overwrite the observed notification turn id
      queue tests assert response and notification aliases are retired by completion under either identity
      queue tests reproduce completion arriving before the turn/start response and prove the next turn performs no false leftover interrupt
      queue tests assert notification waits do not acquire a wall-clock timeout
      reliability tests assert stop waits for cleanup stale completion cannot clear a replacement and prompt plus outbox state survives restart
      reliability tests assert failed interrupt cleanup forgets every local alias while retaining its diagnostic origin
      reliability tests assert callback controls retain their expected mounted-session generation
      reliability tests assert app composition supplies final delivery and restart reconciliation preserves upstream-completed jobs
      reliability tests assert final delivery selects upstream final-answer phase while phase-less and active-stream blocks remain private
      reliability tests assert terminal response recovery produces one durable outbox handoff across repeated scans
      reliability tests reproduce self-restart recovery through a distinct durable continuation and final-answer-only handoff
      app-server stream tests assert turn/started notification ids are adopted before item turn filtering
      app-server stream tests assert unknown same-thread progress cannot exhaust the stale-notification guard
      app-server stream tests assert explicit mismatched turn ids still fail rather than contaminating a live turn
      goal-control tests pin the no-mounted no-active active-goal and replacement-confirmation panel shapes
      goal-control tests assert no-session goal objectives bootstrap fresh mounted sessions
      goal-control tests assert fresh objective writes clear stale completed upstream goal state before replacement
      goal-control tests assert active goals either attach to upstream goal turns or start fallback mounted-session turns
      goal-control tests assert active goal controls use the normal concurrent-turn decision surface while Codex is working
      session-control tests assert New Session callbacks create and mount fresh app-server sessions and rewind forks for the conversation and mounts the fork
      command parser tests pin Telegram-native slash forms for session and goal controls
      restart command tests pin a rollout restart of alasio's own Deployment however it is spelled the release's Deployment name other rollouts and commands left alone and a rollout of alasio among others as a near miss
    Chart tests
      `helm unittest charts/alasio` runs charts/alasio/tests whose alasio security and neon suites pin one alasio at a time its volume and templates the host profile sessions under gVisor or the default runtime Pod Security NetworkPolicies RBAC and Neon's rendering as charts/alasio/README.md describes
    End to end
      test/e2e/run.sh builds the images creates a k3d cluster with gVisor installs the chart with stand-ins for Telegram and OTLP and runs test/e2e/alasio.test.mjs then `npm run test:neon` as test/e2e/README.md describes
    Scope
      tests exercise alasio-local runtime behavior without booting Telegram polling or Codex subprocesses
      fixtures stay inline when the wire shape is the behavior under test
      `npm test` runs every test/*.test.js through node --test and needs no cluster while CI runs it the chart tests and the end-to-end run on demand
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Test as node:test
  participant Protocol as app-server/protocol
  participant Queue as app-server/notification-queue
  participant Thread as active turn identity
  participant Telegram as Telegram operator turn
  Test->>Protocol: assert turn id extraction and active-turn matching
  Protocol-->>Test: distinguish current turn notifications from stale completions
  Test->>Queue: assert stale completion leaves current-turn identity intact
  Test->>Queue: assert logical-turn aliases clear together and idle waits remain timer-free
  Queue->>Thread: retain current turn after unrelated completed notification
  Thread-->>Telegram: keep later Telegram messages from inheriting old completion state
  Test->>Telegram: assert goal control panels stay compact self-contained and callback-backed
  Queue-->>Test: expose deterministic in-memory state without external services or Codex app-server startup
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> StaleCompletion
  StaleCompletion: interrupted turn completion arrives after a later Telegram prompt has started
  StaleCompletion --> FalseNoResponse: later turn accepts old completion and emits no visible answer
  FalseNoResponse --> OperatorConfusion
  OperatorConfusion --> RegressionTest
  RegressionTest: pin app-server wire shapes that previously bypassed turn filtering
  RegressionTest --> TurnBoundedStream
  TurnBoundedStream --> HonestInterruption: interruption edits status without fabricating an error reply
  HonestInterruption --> RetrySafeConversation
  RetrySafeConversation: next Telegram message waits for its own Codex events
  TurnBoundedStream --> [*]
```
