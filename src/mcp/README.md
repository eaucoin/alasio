## Concept Atlas
```mermaid
mindmap
  root((mcp))
    bayma
      bayma is the MCP server alasio itself provides and serves Bun Python C# and Rust REPL sessions
      it runs from its image pinned by digest as a container beside alasio's own through the host's Docker with the same user paths network and host tools as alasio's container
      its container keeps its own processes so bayma snapshots idle REPL sessions as it stops and restores them whole in the next container
      a bayma starting on a state directory waits for the one still stopping on it to finish its snapshots
      baymaLaunch is the harness-neutral command and each harness adapter turns it into its own MCP config shape
      each harness and conversation gets its own state directory under the alasio state directory because bayma leases a directory to one server process
      sessions are checkpointed stated explicitly so they always outlive a alasio restart
      ensureBaymaReady proves once per process against a throwaway state directory that bayma starts and lists tools
    Operator servers
      each harness also loads the MCP servers the operator configured for it on this machine just as it would in a terminal
      Claude Code loads user and project .mcp.json servers and claude.ai connectors and Codex loads $CODEX_HOME/config.toml servers and the apps connector
      alasio does not copy servers between harnesses and does not check or isolate the operator's servers
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Runtime as harness runtime
  participant Bayma as mcp/bayma
  participant Harness as Codex or Claude Code
  Runtime->>Bayma: ensure bayma is ready
  Bayma->>Bayma: start bayma on a throwaway state directory and list its tools once per process
  Runtime->>Bayma: launch command for this harness and conversation
  Runtime->>Harness: add bayma to the harness's own MCP configuration
  Harness->>Bayma: start bayma on the conversation's own state directory
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> MCPSetup
  MCPSetup --> UnpinnedImage: bayma run from a tag without its digest or from anywhere but its image
  UnpinnedImage --> VersionDrift
  MCPSetup --> SharedPidNamespace: bayma's container sharing the host's processes
  SharedPidNamespace --> SnapshotsCannotRestore
  MCPSetup --> SharedState: conversations share one bayma state directory
  SharedState --> LeaseConflict
  MCPSetup --> EphemeralSessions: bayma launched with ephemeral durability
  EphemeralSessions --> SessionsLostOnRestart
  MCPSetup --> PinnedBayma
  PinnedBayma --> ReadyCheck
  ReadyCheck --> ExplicitFailure: bayma does not start or lists no tools
  ReadyCheck --> [*]
```
