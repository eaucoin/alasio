## Concept Atlas
```mermaid
mindmap
  root((mcp))
    bayma
      bayma is the one MCP server alasio gives its agents and serves Bun Python C# and Rust REPL sessions
      it is a pinned npm dependency run by alasio's own Node from the package's bin with runtimes from the payload its postinstall installs
      baymaLaunch is the harness-neutral command and each harness adapter turns it into its own MCP config shape
      each harness and conversation gets its own state directory under the alasio state directory because bayma leases a directory to one server process
      ensureBaymaReady proves once per process against a throwaway state directory that bayma starts and lists tools
    Harness isolation
      Claude Code receives bayma alone under strictMcpConfig so user project and claude.ai servers stay out
      Codex threads receive bayma plus overrides that switch off every server in $CODEX_HOME/config.toml and the apps connector because Codex merges overrides into its ambient config
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
  Runtime->>Harness: bayma as the only MCP server with ambient servers excluded
  Harness->>Bayma: start bayma on the conversation's own state directory
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> MCPSetup
  MCPSetup --> AmbientServers: harness adds servers from its own machine configuration
  AmbientServers --> UnownedTools: agent sees tools alasio never chose
  MCPSetup --> PathLookup: server resolved from PATH or a global install
  PathLookup --> VersionDrift
  MCPSetup --> PinnedBayma
  PinnedBayma --> ExcludedAmbient
  ExcludedAmbient --> ReadyCheck
  ReadyCheck --> ExplicitFailure: bayma does not start or lists no tools
  ReadyCheck --> [*]
```
