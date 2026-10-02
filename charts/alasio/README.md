## Concept Atlas
```mermaid
mindmap
  root((charts/alasio))
    What it deploys
      alasio as one replica of a Deployment under the Recreate strategy with its ServiceAccount and Role its state and home on a PersistentVolumeClaim that uninstalling keeps and the telemetry Service sessions export to
      the sandbox-templates ConfigMap alasio makes its workspaces from whose checksum is on alasio's pod so a change to the templates restarts alasio
      the sessions namespace alasio-sessions enforcing Pod Security restricted and kept on uninstall since sessions' volumes are in it and with host.enabled the privileged host namespace alasio-host
      NetworkPolicies confining sessions folder workspaces' bayma alasio and the data stack unless networkPolicies.enabled is false
      Neon its object store backups lake and collector as neon/README.md and neon/lake/README.md describe
      agent-sandbox's Sandbox API and controller as the subchart charts/agent-sandbox pinned to v1.0.5 by digest and installed once per cluster
      every value is checked against values.schema.json at install and upgrade secrets are referenced by name and never inlined and every image is pinned by digest
      every resource is named after the release's full name which is the release name when it contains alasio and `<release>-alasio` otherwise and `<release>` below stands for it
    Prerequisites
      Kubernetes 1.30 or later and Helm with a default StorageClass or a storageClassName given for alasio sessions and each of the stack's volumes
      a cluster that enforces NetworkPolicy since each session's egress gate holds its pod until its egress is confined so a session never starts without it and sessions.egressGate false turns the gate off only where that is understood
      a RuntimeClass named gvisor or sessions.runtimeClassName naming another and empty runs sessions under the cluster's default runtime which is a weaker boundary and deploy/k3d makes a cluster with gVisor for a single machine
      agent-sandbox is installed by the subchart unless agent-sandbox.enabled is false which is for a cluster that already has it
    Installing
      the Telegram Secret holds the bot's token under key token and the Telegram user ids allowed to use it comma-separated under key allowedUserIds as `kubectl create namespace alasio` then `kubectl -n alasio create secret generic alasio-telegram --from-literal=token=<token> --from-literal=allowedUserIds=<ids>`
      `helm install alasio oci://ghcr.io/eaucoin/charts/alasio --version <v> -n alasio --create-namespace --set alasio.telegram.existingSecret=alasio-telegram` installs a release whose images are the ones that chart version pinned
      the chart refuses to render without alasio.telegram.existingSecret
      Claude Code's login is a long-lived token from `claude setup-token` in the Secret alasio.claude.existingSecret under alasio.claude.key given as CLAUDE_CODE_OAUTH_TOKEN or else the login in alasio's home
      Codex uses the login in alasio's home which under the host profile is the operator's own and is otherwise made in alasio's pod with `kubectl -n alasio exec -it deployment/alasio -- codex login --device-auth` and kept on its volume
      alasio.defaultHarness pre-mounts claude or codex on new conversations and alasio.env and alasio.envFrom add any other variable alasio reads
      alasio.persistence sizes alasio's volume or names an existingClaim and imagePullSecrets serve private registries in the release's sessions and host namespaces
      the install notes print how to follow alasio's logs restart it and query the lake
    Host profile
      host.enabled off by default is the operator's opt-in to folder workspaces in which the agents work on the machine's own files and it is privileged by nature and meant for single-node clusters the operator owns
      host.mounts lists name hostPath mountPath readOnly and type of what alasio and every folder bayma mount from the node and holds the operator's home at least
      host.uid host.gid and host.supplementalGroups are the operator's user which alasio and each folder bayma run as and host.home their home
      host.alasioHome makes alasio's home host.home through the mounts rather than its volume so the harnesses use the operator's logins sessions and configuration
      host.workspaceRoot a path of the mounts is where folder workspaces are listed and made and host.stateRoot under a mount is where folder bayma keeps its state so it outlives each pod
      host.baymaImage is bayma's own image pinned by digest and host.env gives each folder bayma its environment such as PATH
      folder bayma runs as the host-agent ServiceAccount which may get and patch alasio's Deployment so an agent can restart alasio and do nothing else with the cluster
      ci/host-values.yaml is a single machine's host profile with telemetry
    Telemetry
      telemetry.otlpEndpoint otlpProtocol resourceAttributes and headersSecret with headersKey set OpenTelemetry's standard variables for alasio and nothing is exported while the endpoint is empty
      Claude Code Codex and folder bayma export where alasio does and sessions' bayma exports to alasio's receiver on telemetry.receiverPort 4318 by default through the Service `<release>-telemetry` which exports it where alasio does
      the stack's collector sends Neon's SeaweedFS's and the lake's metrics to neon.collector.otlpEndpoint or telemetry.otlpEndpoint with the headers Secret's key headersKey.yaml as a YAML map and compute_ctl traces where alasio does
    External database and object store
      neon.enabled false runs none of the stack and alasio uses the database in the Secret neon.external.existingSecret with keys url for alasio's role and lake-password for the lake's role which alasio makes and the lake does not run
      objectStore.bundled.enabled false replaces SeaweedFS with the operator's S3-compatible store at objectStore.external.endpoint and region with a Secret objectStore.external.existingSecret holding accessKey and secretKey and the buckets objectStore.buckets names for Neon the lake and backups
    Operating
      `kubectl -n alasio logs deployment/alasio --follow` follows alasio
      `kubectl -n alasio rollout restart deployment/alasio` restarts alasio and is the restart its agents are told to use
      `kubectl -n alasio exec deployment/alasio-lake -- node src/query.js "<SQL>"` queries the analytics lake read-only and `npm run lake -- "<SQL>"` does the same from a checkout
      `helm upgrade` runs the stack's setup hook again which completes its secrets without rotating them and an upgrade that changes nothing of the stack restarts none of it
      uninstalling keeps alasio's volume the sessions and host namespaces and the volumes in them
    Tests
      `helm unittest charts/alasio` runs tests/alasio_test.yaml security_test.yaml and neon_test.yaml which pin alasio's rendering Pod Security NetworkPolicies RBAC and the stack's rendering
      ci/default-values.yaml and ci/host-values.yaml are what CI lints the chart with and test/e2e/run.sh installs the chart from this directory
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Op as operator
  participant Helm
  participant Setup as neon setup hook
  participant Ctl as agent-sandbox controller
  participant Stack as Neon and the object store
  participant Alasio as alasio
  Op->>Op: create the namespace and the Telegram Secret
  Op->>Helm: helm install with alasio.telegram.existingSecret
  Helm->>Setup: make the stack's secrets once and render each service's
  Helm->>Ctl: install the Sandbox API and controller
  Helm->>Stack: start the storage services, neon-control, the compute and the lake
  Helm->>Alasio: start one alasio with its templates, its volume and the database Secret
  Alasio->>Stack: connect, waiting until the compute answers
  Alasio->>Ctl: Sandboxes for workspaces as conversations need them
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Install
  Install --> InlinedSecrets: a token or password written into values
  InlinedSecrets --> CredentialsInReleaseHistory
  Install --> FloatingImages: an image by tag without its digest
  FloatingImages --> UnreviewedChange
  Install --> TwoAlasio: a rolling update or a second replica
  TwoAlasio --> DuplicateTelegramPolling
  Install --> HostByDefault: the privileged host profile on without the operator asking
  HostByDefault --> MachineExposed
  Install --> UnenforcedPolicies: sessions on a cluster that ignores NetworkPolicy
  UnenforcedPolicies --> SessionsNeverStart
  Install --> ReferencedSecretsPinnedImagesOneReplica
  ReferencedSecretsPinnedImagesOneReplica --> [*]
```
