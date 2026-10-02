## Concept Atlas
```mermaid
mindmap
  root((src/sandbox))
    Purpose
      the runtime for session filesystems the empty isolated per-session workspace an operator may choose instead of a folder
      each session is an agent-sandbox Sandbox made through kube/sandboxes from the deployment's sessions template whose volume claim is the workspace and whose pod runs bayma from the agent image under the RuntimeClass the template names gVisor by default
      every choice here is sized and de-risked by the session-fs-research study
    Composition
      index createSandbox returns null when the deployment renders no sessions template and callers then offer only folders
      names owns volume id validation a DNS label of three to sixty-three lowercase letters digits and hyphens starting and ending with a letter or digit since the Sandbox its Service and its pod are named for it and newVolumeId makes fs- ids valid by construction
      volumes.create makes the session's Sandbox at once so its volume is ready by its first turn and records its internet mode none or full in its labels for its life
      ensureSession brings the Sandbox up before a turn needs it resuming one that was suspended and making one that is gone again empty and without internet
      a session's Sandbox outlives a alasio restart and the next alasio reaches it at the same address with the token in its Secret
    Confinement
      the sessions namespace enforces Pod Security restricted and the chart's NetworkPolicies admit alasio alone to bayma and let a session out only to alasio's telemetry receiver or with internet to public addresses too
      the net-mode and workload labels set here are what those policies select on
      DNS matches the mode public resolvers with internet and only 127.0.0.1 without so no name carries anything out through the cluster's resolver
      no session pod mounts a ServiceAccount token or gets service links so nothing in it speaks to the cluster as anyone or learns its services
      policies reach a new pod asynchronously so an egress gate init container on the agent image's Node holds the pod until three connections in a row to the API server fail and fails it if they still succeed after two minutes so a cluster that does not enforce NetworkPolicy never starts a session
      bayma is the one door reached by alasio over MCP HTTP with the Sandbox's bearer token and no harness and no model login is ever inside a session
    Telemetry
      when alasio exports telemetry bayma in a session is given each signal alasio exports over HTTP in alasio's protocol the receiver's address by IP since a session without internet has no DNS and its token as the OTLP bearer
      telemetry-receiver is alasio's OTLP over HTTP server on the chart's telemetry Service the one egress of a session without internet and authenticates each request by the token naming its session
      what a session sends is untrusted so otlp-resource strips the stamped keys and every alasio attribute from each resource and stamps service.name bayma the session's volume id and the deployment's attributes rewriting binary protobuf at the wire level and re-encoding JSON with only OTLP's own fields
      a request that does not parse is dropped and each session is held to a byte rate with a burst past which it is answered 429 with retry-after so its exporter backs off and alasio.sandbox.telemetry.received counts what was exported refused and dropped and why
      the receiver exports through telemetry/forward where alasio exports its own and starts with alasio so sessions that outlived the last alasio export as soon as it is up while their exporters retry what failed meanwhile
    The harness's side
      harnessDirectory is the empty directory in alasio a session's harness runs in under the state directory keyed by volume so its sessions are the workspace's Claude Code's project and Codex's thread list
      Claude Code runs there on alasio's login confined to bayma by harness/claude/sessionfs and Codex runs on its own app-server with a home of its own and no environment by codex/sessionfs
      readFile reads a file a reply shows inside the session's bayma container through exec as the agent relative to the workspace so its path and symlinks reach only the session's own files and a suspended session is not read
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant App as telegram/app
  participant Sb as sandbox/index
  participant Kube as kube/sandboxes
  participant Pod as session pod
  participant H as harness in alasio
  participant Rx as telemetry-receiver
  App->>Sb: createSandbox(templates, stateDir) or null without a sessions template
  App->>Sb: volumes.create(volumeId, netMode) on New empty workspace
  Sb->>Kube: ensure the Sandbox with its labels, DNS, egress gate and telemetry settings
  Kube->>Pod: egress gate passes once NetworkPolicy confines it, then bayma serves
  H->>Sb: ensureSession(volumeId) before a turn
  Sb-->>H: bayma url and bearer
  H->>Pod: MCP over HTTP with the session's token
  Pod->>Rx: OTLP with the session's token, when alasio exports telemetry
  Rx->>Rx: authenticate, rate-bound, stamp, export where alasio exports
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Confined
  Confined --> SharedVolume: sessions share one volume
  SharedVolume --> CrossSessionReach
  CrossSessionReach --> OperatorMistrust
  Confined --> CredentialInside: a model login a harness or a ServiceAccount token reaches the session
  CredentialInside --> Exfiltration
  Confined --> OpenBayma: bayma answers anything but alasio with the session's token
  OpenBayma --> SessionTakeover
  Confined --> StartsOpen: a session runs before NetworkPolicy confines it
  StartsOpen --> Exfiltration
  Confined --> DnsChannel: a session without internet resolves names through the cluster
  DnsChannel --> Exfiltration
  Confined --> TrustedTelemetry: what the session sends is believed about where it came from
  TrustedTelemetry --> ForgedOrigin
  Confined --> OneVolumeOneSandboxOneDoor: a volume and a gVisor Sandbox per session, reached only through its authenticated bayma
  OneVolumeOneSandboxOneDoor --> [*]
```
