## Concept Atlas
```mermaid
mindmap
  root((policy))
    Shell parsing
      shell-command owns quoted and single-token shell command unwrapping tokenization and executable resolution
    Runtime policies
      restart-command recognises an agent restarting alasio as `kubectl rollout restart` of alasio's own Deployment ALASIO_DEPLOYMENT or alasio as deployment/name deploy/name or deployment name with kubectl by any path flags anywhere and one shell wrapper unwrapped and records it as self-induced provenance
      a rollout restart that names alasio's Deployment among other targets is a near miss logged as a warning rather than recorded
      db-guardrail detects forbidden local database operations and returns recovery prompts
      workflow-wait detects CI workflow wait commands and emits hook notifications
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Runtime as codex/runtime
  participant Policy as policy modules
  participant Shell as shell-command
  participant Store as persistence/store
  participant Operator as Telegram operator
  Runtime->>Policy: inspect visible tool commands
  Policy->>Shell: unwrap shell layers and normalize executable tokens
  Shell-->>Policy: return command identity without executing anything
  Policy-->>Runtime: return explicit decision and synthetic prompt when needed
  Runtime-->>Operator: surface recovery through Codex-visible continuation
  Policy-->>Store: record provenance only through approved runtime paths
  Store-->>Runtime: preserve restart and workflow evidence across service stops
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> ToolCommand
  ToolCommand --> SilentMutation: policy rewrites or ignores risky commands invisibly
  SilentMutation --> FalseOperatorState
  FalseOperatorState --> UnsafeRetry
  UnsafeRetry --> SilentMutation
  ToolCommand --> ParserDrift: each policy parses shell text differently
  ParserDrift --> InconsistentGuardrail
  ToolCommand --> ExplicitPolicyDecision
  ExplicitPolicyDecision --> SharedShellParser
  SharedShellParser --> SyntheticRecoveryPrompt
  ExplicitPolicyDecision --> RecoverableTurn
  RecoverableTurn --> [*]
```
