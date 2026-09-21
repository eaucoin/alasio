## Concept Atlas
```mermaid
mindmap
  root((alasio))
    Telegram DM edge behavior
      ingress maps explicitly authorized private Bot API messages and callbacks to a single operator conversation
      egress emits progress edits and Telegram-rendered Markdown final responses in the same direct message stream
      Telegram media groups are buffered briefly so multi-file sends become one Codex turn
    Runtime control plane
      turn orchestrator coordinates execution and interruption boundaries
      Codex app-server boundary keeps linked Codex threads warm across Telegram turns
      linked-session warmup is opt-in so service startup and polling are not blocked by Codex resume latency
      Codex SDK exec transport remains an explicit rollback path for runtime isolation
      Codex sessions materialize required MCP config explicitly instead of trusting ambient CLI state alone
      skill selection and instruction loading constrain tool behavior
      SQLite content store manages update offsets, conversations, durable prompt jobs, files, streamed blocks, restart provenance, and resumable continuation
      SQLite Telegram outbox separates Codex completion from rate-limited Bot API delivery
      CI treats Alasio as a Node-native unit target whose semantic source surfaces select exact node:test files in the repository-wide pre-commit gate
    Canonical restart path
      README.md is the source of truth for Alasio operator instructions and AGENTS.md plus CLAUDE.md must remain symlinks to README.md
      Production runs from the isolated `/home/operator/monorepo-alasio-runtime/bots/alasio` deployment checkout while Codex works in `/home/operator/monorepo`
      Normal restarts run from the deployment checkout with `./restart-alasio-operator.sh`
      Equivalent absolute restart path is `/home/operator/monorepo-alasio-runtime/bots/alasio/restart-alasio-operator.sh`
      Wrapper calls from another checkout delegate to the systemd unit WorkingDirectory instead of restarting from source
      Raw `sudo systemctl restart alasio.service` is only a service-level last resort when no active turn provenance must be preserved
    Reliability guarantees
      restart-aware continuation protocol avoids silent context loss
      restart provenance is resolved independently from recovered-output flushing
      shutdown path inspects live descendant commands so self-restarts survive SDK event races
      streamed self-restart detection accepts absolute-path sudo/systemctl variants plus quoted and single-token shell wrappers so shell differences do not silently degrade provenance
      restart near-miss diagnostics trigger only for command-shaped restart attempts against the active systemd service
      operator-triggered restarts use the deployment checkout restart wrapper so outer-shell cutovers persist explicit provenance before systemd stops the unit
      external restarts degrade to explicit unknown provenance instead of falsely attributing them to the user
      tool-pattern guardrails re-enter Codex as internal synthetic user turns instead of fabricating user-visible transport replies
      MCP stdio preflight and isolated Bayma state dirs prevent silent no-tool sessions caused by shared state contention
      Unified Bayma REPL preflight additionally proves the monorepo Breadbutter Python and Rust quickstarts in disposable sessions keyed to the skill sources and locks so generic tool inventory cannot masquerade as repository capability
      service installation and runtime bind Bayma Python to the locked Breadbutter Python 3.12 interpreter instead of the host python3 fallback
      optional startup warmup resumes linked sessions through app-server without creating a new conversation or pruning rollout files
      Telegram New Session creates and mounts a fresh Codex app-server thread instead of only clearing the SQLite session pointer
      Telegram goal objectives with no mounted session create and mount a fresh app-server thread before setting the active goal
      fresh objective goals clear stale upstream thread goal state before setting the active replacement
      Telegram goal controls attach to app-server-created goal turns or start fallback mounted-session turns instead of only updating upstream goal state
      active-turn steering forwards operator guidance through Codex app-server turn/steer before falling back to queueing
      stop and swerve retain conversation ownership until bounded upstream interruption completes
      app-server event silence is not treated as turn failure and remains cancellable through explicit operator control or transport failure
      response handles and notification turn ids remain aliases of one logical turn so either completion identity retires ownership exactly once
      app-server cleanup interrupts record their origin so operator control stale recovery transport failure and consumer exit remain distinguishable
      stale interruption failures recycle only app-server and resume the mounted Codex thread before starting later work
      descendant OOM kills remain contained while main-process exits retain systemd restart recovery
      accepted prompts survive process restarts in a per-conversation SQLite queue
      restart continuations receive deterministic internal prompt identities and retire provenance only with durable queue staging
      Bot API retry-after responses defer durable outbox delivery instead of crashing the service
      upstream turn completion is checkpointed separately from Telegram delivery so restart recovery cannot duplicate completed work
      terminal response handoff retries continue during normal service uptime rather than waiting for another restart
      final replies contain only Codex final-answer phase text while commentary tool activity and phase-less text remain internal
      durable terminal markers prevent active streams from becoming recoverable output before upstream completion
      one outbox identity per terminal response makes live handoff and restart recovery converge on the same delivery
      observable action to reply consistency prevents fabricated completion signals
      Codex-native compaction remains the source of truth for long conversations
    Historical emphasis
      recent changes prioritized restart recovery, Codex-native compaction, and truthful status messaging
      service prompt/hook integration aims for deterministic operational handoffs
```

## Preference Atlas
```mermaid
classDiagram
  class CodexBridgeValues {
    +honestProgressSignalsWithEvidence
    +restartAwareRecoveryWithCheckpointTruth
    +skillDrivenExecutionWithExplicitBoundaries
    +boundedThreadContextWithSummarizationDiscipline
  }
  class TelegramIngress {
    +pollUpdates()
    +authorizePrivateMessagesAndCallbacks()
    +bufferMediaGroups()
  }
  class CodexRuntime {
    +executeTurn()
    +applySkills()
    +emitProgress()
    +emitFinalReplyWithCausalTrace()
  }
  class SQLiteContentStore {
    +checkpoint()
    +resume()
    +publishRecoveryMetadata()
    +storeTelegramOffsets()
    +storeDownloadedFiles()
    +queuePromptJobs()
    +queueTelegramOutbox()
  }
  class RestartWrapper {
    +runFromLiveAlasioDirectory()
    +recordActiveTurnProvenance()
    +restartAlasioService()
    +avoidRawSystemctlForNormalCutovers()
  }
  CodexBridgeValues --> TelegramIngress
  CodexBridgeValues --> CodexRuntime
  CodexBridgeValues --> SQLiteContentStore
  CodexBridgeValues --> RestartWrapper
  TelegramIngress --> CodexRuntime
  SQLiteContentStore --> CodexRuntime
  RestartWrapper --> SQLiteContentStore
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> EventProcessed
  EventProcessed --> ContextOverload: thread history grows without bounded summarization
  ContextOverload --> ToolingDrift: skill/tool behavior no longer traceable to intent
  ToolingDrift --> UserMistrust: replies appear inconsistent or opaque
  UserMistrust --> EventProcessed
  EventProcessed --> ScopedExecution: bounded context and explicit skill boundaries
  ScopedExecution --> TrustworthyReplies: progress and final replies match observable actions
  TrustworthyReplies --> [*]
```
