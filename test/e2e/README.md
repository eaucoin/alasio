## Concept Atlas
```mermaid
mindmap
  root((test/e2e))
    What it proves
      alasio installed by its chart works end to end on Kubernetes driven through a Telegram stand-in as its operator drives it with its sessions' confinement checked from inside them its own code run in its pod with its ServiceAccount and its telemetry looked for where the deployment's goes
    run.sh
      `test/e2e/run.sh` builds the alasio alasio-agent and alasio-lake images and pushes them to a k3d registry beside the cluster as a release publishes them
      it creates the cluster with deploy/k3d/cluster.sh and AGENTS agent nodes zero unless set so CI runs it on one node and on a server with two agents
      it runs the stand-ins in the namespace alasio-test makes the Telegram Secret and installs the chart from this checkout with TELEGRAM_API_ROOT and telemetry.otlpEndpoint pointing at the stand-ins and smaller volumes
      it then runs alasio.test.mjs with ALASIO_E2E_TELEMETRY=1 and `npm run test:neon` against the same release and deletes the cluster and registry afterwards unless KEEP=1
      it needs docker k3d kubectl helm and node and its kubeconfig is its own so no other cluster is touched
    Stand-ins
      telegram-stub.mjs is a dependency-free Bot API stand-in that long-polls getUpdates with what the test queued records every other call and lets the test send messages and press buttons through /control
      otlp-sink.mjs is a dependency-free OTLP over HTTP endpoint that accepts every export and reports per request its signal encoding and whether its bytes contain a given value
      telegram-stub.yaml and otlp-sink.yaml run them from ConfigMaps as Services telegram and otlp
    alasio.test.mjs
      a new empty workspace without internet is a session of its own and confined
      a new workspace with internet reaches the internet and nothing private
      a session's bayma answers alasio alone and only with the session's token
      alasio reads a session's files as its agent and a suspended session resumes with them through session-roundtrip.mjs run in alasio's pod with alasio's own driver and ServiceAccount
      a session's telemetry reaches the deployment's backend stamped with the session
      alasio comes back from a rollout restart with its conversation's workspace
      it runs against an installed release named by KUBECONFIG ALASIO_E2E_NAMESPACE and ALASIO_E2E_RELEASE alasio and alasio unless set and is skipped without KUBECONFIG
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Run as run.sh
  participant Reg as k3d registry
  participant Cluster as k3d cluster with gVisor
  participant Stubs as telegram and otlp stand-ins
  participant Suite as alasio.test.mjs and test:neon
  Run->>Reg: build and push alasio, alasio-agent and alasio-lake
  Run->>Cluster: cluster.sh, then the stand-ins and the Telegram Secret
  Run->>Cluster: helm install the chart against the stand-ins
  Suite->>Stubs: send messages and press buttons as the operator
  Suite->>Cluster: check sessions from inside, restart alasio, kill the stack's parts
  Suite->>Stubs: find the stamped telemetry and alasio's replies
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> EndToEnd
  EndToEnd --> RealTelegram: tests sending to a real bot or chat
  RealTelegram --> OperatorFlooded
  EndToEnd --> ImagesFromElsewhere: a run against images other than the checkout's
  ImagesFromElsewhere --> UntestedCode
  EndToEnd --> SharedCluster: a run against the operator's own cluster or context
  SharedCluster --> OperatorDataTouched
  EndToEnd --> ThrowawayClusterStandIns
  ThrowawayClusterStandIns --> [*]
```
