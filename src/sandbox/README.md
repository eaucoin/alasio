## Concept Atlas
```mermaid
mindmap
  root((src/sandbox))
    Purpose
      the runtime for session filesystems the empty isolated per-session workspace an operator may choose instead of a folder
      it drives the images and entrypoint under the repository sandbox directory and needs nothing installed on the host beyond the Docker alasio already uses
      every choice here is sized and de-risked by the session-fs-research study
    Composition
      index createSandbox assembles the subsystem from config or returns null when off so a deployment without Valkey a bucket built images and a login behaves exactly as before
      config reads the environment and fails loudly at startup when enabled but incomplete
    Volume
      names owns volume id validation to JuiceFS's three to sixty-three character rule and the derived session-host name and S3 prefix
      metadata-engine builds the JuiceFS metadata URL kept pluggable Valkey separates volumes by logical DB index one per volume and Postgres is the documented fallback
      volume reserves a namespace and records a volume creation only reserves and records the format happens on first session-host start and destroy runs juicefs destroy and purges the S3 prefix in a throwaway container since destroy alone orphans objects when metadata is gone
      one volume per session never shared because JuiceFS's control file reaches a whole volume
    Session host and sandbox
      session-host builds the docker run for the privileged per-session container and waits for the sandbox to be ready writes the agent env and gives the harness its exec command
      docker is the one place these modules touch Docker so the rest take a small faked-in-tests interface
      the boundary is gVisor inside the session host an escape lands in the privileged container exactly where a host-installed gVisor escape would
    Gateway
      gateway is the credential boundary and the one network peer a sandboxed agent always reaches it holds the real login issues a revocable per-session bearer allows only the providers API paths and swaps the bearer for the credential
      a bearer is not a credential valid only here only for its session and revoked when the session ends so a leaked or echoed bearer is worthless which is why the gateway not credential masking is the design
      one address serves both providers by path Claude Code messages Codex responses and a provider whose login is unset answers 503 while validation and the path allowlist stay live
    Harness adapters
      a folder workspace runs the CLI on the host unchanged a session-filesystem workspace runs it inside the sandbox in /workspace on the gateway with bayma reached over http not launched as a process
      Claude Code runs through the SDK's spawnClaudeCodeProcess hook in harness/claude/sandbox so the host binary path is dropped and claude runs inside
      Codex runs its own app-server per volume in codex/sandbox the app-server process spawn is injectable and the sandbox one runs codex app-server over the session's own spawn with the gateway as its only model provider reached with the bearer under the provider env_key
      the session host makes the agent home's config skeleton up front because Codex refuses to start when CODEX_HOME does not yet exist
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant App as telegram/app
  participant Sb as sandbox/index
  participant Vol as SessionVolumeManager
  participant Host as SessionHost
  participant Gw as SessionGateway
  App->>Sb: createSandbox(config, store) or null when off
  App->>Gw: startGateway() once
  App->>Vol: create(volumeId) on New empty workspace
  App->>Host: start(volumeId, mountEnv, netMode) when a turn needs it
  App->>Gw: issueBearer(session) then Host.writeAgentEnv
  App->>Host: execCommand(volumeId, [claude|codex ...]) the harness spawns inside
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Confined
  Confined --> SharedVolume: sessions share one JuiceFS volume
  SharedVolume --> CrossSessionReach
  CrossSessionReach --> OperatorMistrust
  Confined --> CredentialInside: the real login reaches the sandbox
  CredentialInside --> Exfiltration
  Confined --> HostCoupling: the design needs something installed on the host
  HostCoupling --> NotPortable
  Confined --> OneVolumeOneSandboxOneGateway: a volume and a gVisor sandbox per session the gateway holds the credential
  OneVolumeOneSandboxOneGateway --> [*]
```
