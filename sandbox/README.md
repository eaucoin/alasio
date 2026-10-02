## Concept Atlas
```mermaid
mindmap
  root((sandbox))
    Why it exists
      a session filesystem is a workspace in its own gVisor sandbox nested in a per-session privileged session-host container holding bayma and every process the agent starts on its own volume
      the harness Claude Code or Codex runs in alasio on the operator's own login and reaches the workspace only through bayma so no login no harness and no harness state is ever inside a sandbox
      the boundary is gVisor and an escape from it lands in the privileged session host exactly where an escape from a host-installed gVisor would land so nesting costs no strength and needs nothing installed on the host beyond the Docker alasio already drives
      the choice is validated in session-fs-research the study whose experiments sized and de-risked every part of this
      the harness stays out because its tools are what reach a machine and bayma alone passed every workspace task for both harnesses while the alternatives failed claude mcp serve returns raw JSON so Read loses images and Edit skips read-before-edit and Codex exec-server is experimental with no reconnect and one app-server's environments are not isolated from each other
    Images
      build.sh fetches gVisor and JuiceFS pinned by checksum then builds two images with no host install
      session-host is alpine plus gVisor runsc JuiceFS iproute2 iptables jq the entrypoint agent-exec and agent-connect and carries no agent software
      agent is bayma's image with an unprivileged user agent and bayma's toolbelt installed and the telemetry drain and it is mounted read-only as the sandbox root at agent-root so every session shares it
    Session host entrypoint
      entrypoint mounts the session's JuiceFS volume with no-agent hide-internal enable-xattr a bounded cache and a password from a file never the URL
      entrypoint builds the agent a network namespace joined by a veth pair and applies the firewall none reaches no address at all full reaches the internet but never private link-local the host or the metadata service
      entrypoint runs the sandbox read-only root the volume's workspace and home bound a private tmp NoNewPrivs and nosuid binds and tini running bayma's http mcp server on the sandbox's own loopback as the sandbox init so orphans are reaped and a checkpoint restore keeps every runtime
      a stop checkpoints the sandbox onto the volume outside the workspace and home it binds and the next container restores it once then drops it so a crash never rolls processes back to a stale image and one that cannot be restored falls back to a fresh start
      its teardown is trapped from the first line so a stop during startup still unmounts
      given SANDBOX_TELEMETRY the OTEL variables alasio chose for bayma it adds them to bayma's environment and starts the telemetry drain before bayma in the sandbox's init so the drain is checkpointed and restored with bayma and a sandbox restored from before keeps the telemetry it started with
    Getting in
      agent-exec runs a command as the agent with only the agent's HOME USER and PATH never the session host's own env which holds the storage keys and metadata URL
      agent-connect pipes stdio to a port on the sandbox's own loopback from inside as the agent so alasio's bayma forward needs no listener or firewall opening anywhere
      the same agent-connect is how alasio reads the telemetry drain on port 7291 while bayma exports to the drain's OTLP port 4318 so telemetry needs no opening either
      telemetry-drain holds bayma's OTLP requests within a bound for one reader at a time greets each read with its protocol version and reports what it dropped and it is the agent's like everything in the sandbox so alasio trusts nothing it reads
    What the agent sees
      an empty workspace its own home a private tmp and a read-only system image and its own gVisor kernel and nothing of the host no docker socket no host home no other session and no model login
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Alasio as alasio (src/sandbox)
  participant Host as session-host container
  participant JFS as JuiceFS volume
  participant Sbx as gVisor sandbox
  participant Bayma as bayma
  Alasio->>Host: docker run privileged, the agent image mounted read-only, volume + network env
  Host->>JFS: juicefs mount (credentials never leave the host)
  Host->>Sbx: runsc run or restore, root = agent image, workspace + home bound, tini + bayma as init
  Alasio->>Host: docker exec agent-connect 7290, once per harness connection
  Host->>Bayma: piped from inside the sandbox, as the agent
  Bayma->>Sbx: OTLP to the telemetry drain on 127.0.0.1:4318, when alasio exports telemetry
  Alasio->>Host: docker exec agent-connect 7291, reading the drain while the session runs
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
  Isolated --> HarnessInside: the harness and its login run inside the sandbox
  HarnessInside --> Exfiltration
  Isolated --> HostEnvInside: agent-exec carries the session host's env into the sandbox
  HostEnvInside --> Exfiltration
  Isolated --> CheckpointInContainer: the checkpoint lives in the container a start removes
  CheckpointInContainer --> ProcessesLost
  Isolated --> OneVolumeOneSandbox: a volume and a gVisor sandbox per session, bayma the only door
  OneVolumeOneSandbox --> [*]
```
