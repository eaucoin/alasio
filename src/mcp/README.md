## Concept Atlas
```mermaid
mindmap
  root((mcp))
    Config
      server-config owns MCP config loading and merging
      codex-config-toml owns the narrow MCP table extraction from Codex config.toml
      bayma-state owns per-thread and per-server Bayma state-dir materialization
      preflight-state gives each readiness probe ephemeral state separate from the live MCP runtime
      config output is explicit input to Codex runtime setup
    Preflight
      preflight owns stdio readiness checks and cache policy
      failures should explain which configured tool boundary is unavailable
      Rust capability provisioning derives the installed Bayma Cargo home from its immutable seed identity and fetches the exact Breadbutter package-set and bridge locks before the offline probe
      Breadbutter Python and Rust capabilities fingerprint their skill contracts and prove both quickstarts in disposable unified Bayma REPL sessions before caching readiness
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Runtime as codex/runtime
  participant Config as server-config
  participant Check as preflight
  participant FS as materialized state dir
  participant Cache as readiness cache
  Runtime->>Config: build Codex MCP config
  Config->>FS: create isolated Bayma state for this thread
  Config-->>Runtime: return explicit server definitions
  Runtime->>Check: verify configured stdio servers
  Check->>Check: provision the exact locked Rust graph into Bayma's derived private Cargo home
  Check->>Check: load monorepo Breadbutter in disposable Bayma Python and Rust states when those repository capabilities exist
  Check->>Cache: reuse known-good readiness only when inputs match
  Check-->>Config: report per-server command and startup evidence
  Check-->>Runtime: return usable or explicit failure evidence
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> MCPSetup
  MCPSetup --> AmbientState: shared temp or hidden CLI state controls tool availability
  AmbientState --> NoToolSession
  NoToolSession --> FalseCapability: Codex appears alive but cannot use expected tools
  FalseCapability --> OperatorDelay
  OperatorDelay --> AmbientState
  MCPSetup --> MaterializedConfig
  MaterializedConfig --> IsolatedStateDir
  IsolatedStateDir --> PreflightedServer
  PreflightedServer --> ExplicitFailure: server startup evidence is not good enough
  MaterializedConfig --> CheckedTools
  CheckedTools --> [*]
```
