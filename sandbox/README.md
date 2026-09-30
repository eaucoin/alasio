## Concept Atlas
```mermaid
mindmap
  root((sandbox))
    Why it exists
      a session filesystem session runs the agent its harness and bayma inside one gVisor sandbox nested in a per-session privileged session-host container so the agent sees only its own volume and one network peer
      the boundary is gVisor and an escape from it lands in the privileged session host exactly where an escape from a host-installed gVisor would land so nesting costs no strength and needs nothing installed on the host beyond the Docker alasio already drives
      the choice is validated in session-fs-research the study whose experiments sized and de-risked every part of this    Images
      build.sh fetches gVisor and JuiceFS pinned by checksum and copies the Claude Code and Codex CLIs from alasio's node_modules then builds two images with no host install
      session-host is alpine plus gVisor runsc JuiceFS iproute2 iptables jq and the entrypoint and carries no agent software
      agent is bayma's image plus the Claude Code and Codex CLIs and codex-code-mode-host with an unprivileged user agent and it is mounted read-only as the sandbox root at agent-root so every session shares it
    Session host entrypoint
      entrypoint mounts the session's JuiceFS volume with no-agent hide-internal enable-xattr a bounded cache and a password from a file never the URL
      entrypoint builds the agent a network namespace joined by a veth pair and applies the firewall none reaches only the gateway full reaches the internet but never private link-local the host or the metadata service
      entrypoint runs the sandbox read-only root the volume's workspace and home bound a private tmp NoNewPrivs and nosuid binds and tini running bayma's http mcp server as the sandbox init so orphans are reaped and a checkpoint restore keeps every runtime
      the harness Claude Code or Codex is added later with agent-exec which runs runsc exec as the agent with the gateway bearer and CODEX_HOME from a written env file plus only the per-spawn variables alasio names in AGENT_EXEC_VARS never the session host's own env which holds the storage keys and metadata URL
      an idle stop can checkpoint compressed and a start can restore re-adding the veth address and route first
    What the agent sees
      an empty workspace its own home a private tmp and a read-only system image and its own gVisor kernel and nothing of the host no docker socket no host home no other session
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Alasio as alasio (src/sandbox)
  participant Host as session-host container
  participant JFS as JuiceFS volume
  participant Sbx as gVisor sandbox
  participant Agent as harness + bayma
  Alasio->>Host: docker run privileged, the agent image mounted read-only, volume + network + gateway env
  Host->>JFS: juicefs mount (credentials never leave the host)
  Host->>Sbx: runsc run, root = agent image, workspace + home bound, tini + bayma as init
  Alasio->>Host: docker exec agent-exec claude|codex  (runsc exec into the sandbox)
  Host->>Agent: the harness runs as uid 1000, reaching only the gateway (none) or the internet (full)
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Isolated
  Isolated --> HostInstall: the design needs gVisor or a mount setting on the host
  HostInstall --> NotPortable
  Isolated --> SharedVolume: sessions share one JuiceFS volume
  SharedVolume --> CrossSessionReach: the control file reaches another session
  CrossSessionReach --> OperatorMistrust
  Isolated --> CredentialInside: the real model login lives inside the sandbox
  CredentialInside --> Exfiltration
  Isolated --> HostEnvInside: agent-exec carries the session host's env into the sandbox
  HostEnvInside --> Exfiltration
  Isolated --> OneVolumeOneSandbox: a volume and a gVisor sandbox per session, the gateway holds the credential
  OneVolumeOneSandbox --> [*]
```
