## Concept Atlas
```mermaid
mindmap
  root((src/sandbox))
    Purpose
      the runtime for session filesystems the empty isolated per-session workspace an operator may choose instead of a folder
      it drives the images and entrypoint under the repository sandbox directory and needs nothing installed on the host beyond the Docker alasio already uses
      every choice here is sized and de-risked by the session-fs-research study
    Composition
      index createSandbox assembles the subsystem from config or returns null when off so a deployment without Valkey a bucket and built images behaves exactly as before
      config reads the environment and fails loudly at startup when enabled but incomplete and holds no model login since no harness runs in a sandbox
    Volume
      names owns volume id validation to JuiceFS's volume-name rule lowercase letters digits and hyphens three to sixty-three long starting and ending with a letter or digit and the derived session-host name and S3 prefix
      metadata-engine builds the JuiceFS metadata URL kept pluggable Valkey separates volumes by logical DB index one per volume and Postgres is the documented fallback
      volume reserves a namespace and records a volume creation only reserves and records the format happens on first session-host start and destroy runs juicefs destroy and purges the S3 prefix in a throwaway container since destroy alone orphans objects when metadata is gone
      one volume per session never shared because JuiceFS's control file reaches a whole volume
    Session host and sandbox
      session-host builds the docker run for the privileged per-session container waits until bayma answers inside it as the span alasio.session_host.start with an event for each step its entrypoint stamps reads a file as the agent and connects to a port on the sandbox's own loopback through agent-connect
      ensureSession starts a host once for callers that ask together and adopts one already running so a session host outlives a alasio restart with its processes and bayma's REPL sessions
      docker is the one place these modules touch Docker so the rest take a small faked-in-tests interface
      the boundary is gVisor inside the session host an escape lands in the privileged container exactly where a host-installed gVisor escape would
    The door
      bayma-forward is the harness's one way into a session a listener on alasio's loopback that pipes each connection through docker exec agent-connect to bayma inside like kubectl port-forward
      bayma has no authentication so a connection opens with the forward's bearer which is checked before anything reaches the sandbox and answered 401 otherwise
      each connection is its own docker exec so nothing listens inside and a session host that restarts is reached by the next connection
    Telemetry
      when alasio exports telemetry the session host is given bayma's OTEL settings and runs the telemetry drain beside bayma inside the sandbox and bayma exports to the drain on the sandbox's own loopback so no endpoint route or credential ever enters a sandbox
      telemetry relays each session's drain through agent-connect to its read port while the session runs reconnecting when a read ends and ending when the host stops or after five reads the drain never greets because a sandbox started before telemetry was configured has none until it starts fresh
      what the drain holds while alasio is away waits for the next read within its bound so a alasio restart loses nothing and a flood is dropped oldest first in the sandbox and counted
      the relay trusts nothing it reads otlp-resource strips service.name the deployment's attributes and every alasio attribute from each resource and stamps bayma the session's volume and the deployment's attributes rewriting binary protobuf at the wire level and re-encoding JSON with only OTLP's own fields
      a request that does not parse is dropped and reading is paced to a byte rate so whatever runs in the sandbox can only lie about its own session and only so fast and alasio.sandbox.telemetry.requests counts what was exported and dropped and why
    The harness's side
      harnessDirectory is the empty directory on this machine a session's harness runs in under the state directory keyed by volume so its sessions are the workspace's Claude Code's project and Codex's thread list
      Claude Code runs there on the operator's login confined to bayma by harness/claude/sessionfs and Codex runs on its own app-server with a home of its own and no environment by codex/sessionfs
      a file a reply shows is read inside the sandbox as the agent so its path and symlinks reach only the session's own files never the host's
      bayma's toolbelt is installed in the agent image by bayma's own installer and each home links to it because copying its 25k files onto a volume took minutes at every new session's start
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant App as telegram/app
  participant Sb as sandbox/index
  participant Vol as SessionVolumeManager
  participant Host as SessionHost
  participant Fwd as bayma-forward
  participant H as harness (in alasio)
  App->>Sb: createSandbox(config, store, stateDir) or null when off
  App->>Vol: create(volumeId) on New empty workspace
  H->>Sb: ensureSession(volumeId) when a turn needs the workspace
  Sb->>Host: start(volumeId, mountEnv, netMode) unless running
  Sb->>Fwd: startBaymaForward(connect) once per volume
  Sb-->>H: { bayma: { url, headers } }
  H->>Fwd: MCP over HTTP with the forward's bearer
  Fwd->>Host: connect(volumeId, 7290) per connection
  Sb->>Host: connect(volumeId, 7291) to read the telemetry drain while it runs, when alasio exports telemetry
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Confined
  Confined --> SharedVolume: sessions share one JuiceFS volume
  SharedVolume --> CrossSessionReach
  CrossSessionReach --> OperatorMistrust
  Confined --> CredentialInside: a model login or a harness reaches the sandbox
  CredentialInside --> Exfiltration
  Confined --> OpenPort: bayma listens where other local processes reach it unauthenticated
  OpenPort --> SessionTakeover
  Confined --> TelemetryRoute: bayma in the sandbox is given a route or a credential to export with
  TelemetryRoute --> Exfiltration
  Confined --> TrustedTelemetry: what the sandbox sends is believed about where it came from
  TrustedTelemetry --> ForgedOrigin
  Confined --> HostCoupling: the design needs something installed on the host
  HostCoupling --> NotPortable
  Confined --> OneVolumeOneSandboxOneDoor: a volume and a gVisor sandbox per session, reached only through its authenticated bayma forward
  OneVolumeOneSandboxOneDoor --> [*]
```
