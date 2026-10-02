## Concept Atlas
```mermaid
mindmap
  root((mcp))
    bayma
      bayma is the MCP server alasio itself provides and serves REPL sessions in the language runtimes its image carries
      a session filesystem's bayma is its Sandbox's own which src/sandbox brings up and this module serves folder workspaces
      a folder workspace's bayma is a Sandbox per conversation and harness in the host namespace made through kube/sandboxes from the deployment's host template the operator's opt-in to agents that work on the machine's own files
      the host template runs bayma's own image pinned by digest as the operator's user in their home with what the operator mounts from the node and checkpointed durability with what it needs to snapshot its REPL sessions as it stops and restore them whole as it starts so they outlive its pod
      alasio adds only what is per conversation bayma's state directory under the template's stateRoot keyed by harness and conversation and the conversation's telemetry settings
      the state directory is keyed by harness as well as conversation because bayma leases it to one server and a Codex thread keeps its server alive after the conversation switches to Claude Code
      the Sandbox is named bayma- and a hash of harness and conversation with the conversation in an annotation and the harness and workload folder in labels
      folderBaymaServer resolves the server once it answers over its Service with the Sandbox's token as an http server with url and headers in Claude Code's MCP config shape and codex/thread-config turns it into Codex's
      a deployment without the host profile has no folder bayma and a folder turn fails saying the deployment offers no folder workspaces
      a harness waits up to a minute for bayma as it starts since a conversation's first turn makes its Sandbox and one that was suspended resumes restoring its REPL sessions
      bayma exports its own telemetry where alasio exports labelled with the conversation from conversationTelemetryEnv which bayma itself keeps from its REPL sessions
    Operator servers
      each harness also loads the MCP servers configured for it in alasio's home just as it would in a terminal which under the host profile is the operator's own home
      Claude Code loads user and project .mcp.json servers and claude.ai connectors and Codex loads $CODEX_HOME/config.toml servers and the apps connector
      alasio does not copy servers between harnesses and does not check or isolate the operator's servers
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Runtime as harness runtime
  participant Bayma as mcp/bayma
  participant Kube as kube/sandboxes
  participant Harness as Codex or Claude Code
  Runtime->>Bayma: folderBaymaServer(harness, threadKey)
  Bayma->>Kube: ensure the conversation's host Sandbox with its state directory and telemetry
  Kube-->>Bayma: url and bearer once bayma answers
  Bayma-->>Runtime: an http MCP server
  Runtime->>Harness: add bayma to the harness's own MCP configuration
  Harness->>Kube: MCP over HTTP to the Sandbox's Service with its token
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> MCPSetup
  MCPSetup --> UnpinnedImage: bayma run from a tag without its digest
  UnpinnedImage --> VersionDrift
  MCPSetup --> SharedState: conversations or harnesses share one bayma state directory
  SharedState --> LeaseConflict
  MCPSetup --> EphemeralSessions: folder bayma without checkpointed durability or the capabilities to snapshot
  EphemeralSessions --> SessionsLostOnRestart
  MCPSetup --> UnreadyServer: a harness given bayma before it answers
  UnreadyServer --> SilentNoToolSession
  MCPSetup --> SandboxPerConversation
  SandboxPerConversation --> AnswersWithToken
  AnswersWithToken --> [*]
```
