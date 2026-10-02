## Concept Atlas
```mermaid
mindmap
  root((src/kube))
    Purpose
      kube is alasio's one door to the Kubernetes API and the place every workspace becomes an agent-sandbox Sandbox
      session filesystems in src/sandbox and folder workspaces' bayma in src/mcp are both Sandboxes and differ only in the template they are made from and what each adds
    client
      createKubeClient is a narrow client over @kubernetes/client-node so the modules that drive workloads take its small interface and their tests a fake of it
      in a pod it authenticates as the pod's ServiceAccount and elsewhere as the current context of KUBECONFIG or ~/.kube/config
      objects are addressed by apiVersion kind namespace and name so one set of calls serves core objects and Sandboxes alike
      read returns null for an absent object create throws with code 409 for an existing one patch is a JSON merge patch and remove deletes dependents in the background and treats an object already gone as removed
      exec runs a command in a container and resolves its exit code from the status channel its stdout bounded by maxBytes past which the command is ended and an error thrown
    config
      loadKubeTemplates reads ALASIO_KUBE_TEMPLATES the path of the JSON the chart renders into its sandbox-templates ConfigMap and checks it once at startup so a wrongly rendered deployment fails as it starts rather than at a workspace's first turn
      the file holds a sessions profile and a host profile either of which may be absent so no sessions profile means no session filesystems and no host profile means no folder workspaces
      each profile names its namespace bayma's port and a podTemplate with a container named bayma serving MCP over HTTP and sessions adds workspaceDir egressGate fullModeNameservers and volumeClaimTemplates while host adds stateRoot
      alasio refuses to start without ALASIO_KUBE_TEMPLATES since it runs only where its Helm chart deploys it
    sandboxes
      a Sandbox is agents.x-k8s.io/v1beta1 one pod running bayma's MCP over HTTP with its own Service volumes and bearer token which alasio creates resumes suspends and deletes
      sandboxManifest builds a Sandbox from a profile's template with the managed-by and alasio.dev/sandbox labels operatingMode Running a Service and bayma given --token-file from a Secret volume readable by the pod's group and the caller's configure adds what is its own
      a Sandbox's token is `<name>.<random>` so whatever presents one can be traced to its Sandbox without a table of tokens and it is compared in constant time
      the token's Secret `<name>-bayma-token` is owned by its Sandbox so it is deleted with it and a Sandbox left without one gets one at the next ensure since its pod waits for the Secret
      bayma refuses any request without the token so a Sandbox is closed to everything but alasio even before the NetworkPolicy that admits only alasio applies to its new pod
      ensure makes the Sandbox when absent resumes it when suspended waits until its Ready condition has observed its current generation then until bayma answers over the Service with the token and resolves the MCP url and headers
      callers asking for one Sandbox at once share one ensure and a first start is given five minutes since it pulls the image and provisions the volume
      bringing a Sandbox up is the span alasio.sandbox.ensure
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Caller as sandbox or mcp/bayma
  participant Sb as kube/sandboxes
  participant Api as Kubernetes API
  participant Ctl as agent-sandbox controller
  participant Bayma as bayma in the Sandbox
  Caller->>Sb: ensure(name, manifest)
  Sb->>Api: read the Sandbox, create it from the template when absent
  Sb->>Api: create its token Secret owned by the Sandbox unless it has one
  Sb->>Api: patch operatingMode Running when it was suspended
  Ctl->>Api: run the pod and its Service, mark the Sandbox Ready
  Sb->>Api: poll until Ready for the current generation
  Sb->>Bayma: GET the MCP endpoint with the bearer until it answers
  Sb-->>Caller: url and Authorization header
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Sandbox
  Sandbox --> OpenBayma: bayma serves before or without a token
  OpenBayma --> SessionTakeover
  Sandbox --> OrphanedToken: a token Secret that outlives its Sandbox
  OrphanedToken --> StaleCredentials
  Sandbox --> StaleReady: Ready read from a status the controller has not updated for the current spec
  StaleReady --> HarnessWithoutTools
  Sandbox --> ScatteredApiCalls: modules reaching the Kubernetes API each their own way
  ScatteredApiCalls --> UntestableDrivers
  Sandbox --> OneClientTokenPerSandbox
  OneClientTokenPerSandbox --> [*]
```
