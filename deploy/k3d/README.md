## Concept Atlas
```mermaid
mindmap
  root((deploy/k3d))
    What it is
      a k3d cluster that runs alasio's chart on a single machine and in the end-to-end run whose k3s nodes run gVisor so sessions get the gvisor RuntimeClass the chart asks for by default
      k3s enforces NetworkPolicy so sessions' egress gate passes and their confinement holds as on any cluster that enforces it
    node
      node/Dockerfile builds alasio-k3s-gvisor from rancher/k3s v1.37.1-k3s1 pinned by digest with runsc its containerd shim and the Sentry from a gVisor release pinned by name and verified by its sha512
      node/config-v3.toml.tmpl registers runsc as a containerd runtime on k3s's own base template
    runtimeclass
      runtimeclass.yaml is the RuntimeClass gvisor whose handler is runsc
    cluster.sh
      `deploy/k3d/cluster.sh NAME` followed by any k3d cluster create flags builds the node image unless present creates the cluster without traefik or a load balancer waits for it writes its kubeconfig mode 0600 and applies the RuntimeClass
      AGENTS sets the agent nodes beside the server zero unless set REGISTRY_CONFIG a k3s registries.yaml for a private registry's credentials and KUBECONFIG_OUT where the kubeconfig goes ./kubeconfig-NAME unless set
      it never changes the default kubeconfig or its current context so the cluster is reached only through the kubeconfig it writes
      kubelet evicts only when a disk is nearly full and collects unused images only then because the cluster shares the machine's disk with everything else on it and evicting images in use would stall the stack
      further flags go to k3d as they are such as the volumes the host profile mounts from the machine into the node
    Use
      after cluster.sh the chart installs as charts/alasio/README.md describes with KUBECONFIG naming the written kubeconfig
      test/e2e/run.sh uses cluster.sh with a registry beside the cluster that the images it builds are pushed to
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Op as operator or test/e2e/run.sh
  participant Sh as cluster.sh
  participant Docker
  participant K3d as k3d
  participant Api as the cluster
  Op->>Sh: cluster.sh NAME with any k3d flags
  Sh->>Docker: build alasio-k3s-gvisor unless present
  Sh->>K3d: create the cluster from it, waiting, kubeconfig left alone
  Sh->>Sh: write the cluster's kubeconfig mode 0600
  Sh->>Api: apply the gvisor RuntimeClass
  Op->>Api: helm install the chart
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Cluster
  Cluster --> NoSandboxRuntime: sessions under runc because the nodes lack gVisor
  NoSandboxRuntime --> WeakerBoundary
  Cluster --> UnverifiedRuntime: gVisor fetched without its checksum
  UnverifiedRuntime --> TamperedSandbox
  Cluster --> HijackedContext: the operator's default kubeconfig switched to the new cluster
  HijackedContext --> CommandsAgainstTheWrongCluster
  Cluster --> EagerEviction: kubelet's default disk thresholds on a shared disk
  EagerEviction --> ImagesInUseEvicted
  Cluster --> PinnedGvisorNode
  PinnedGvisorNode --> [*]
```
