## Concept Atlas
```mermaid
mindmap
  root((neon))
    What it is
      alasio's own Neon is Neon's open-source storage engine run in the release's namespace by the chart's templates under charts/alasio/templates/neon and it keeps Claude Code's transcripts and Codex's rollout files
      it is frozen at neon release-9129 and compute release-compute-9073 on Postgres 17 with every image pinned by digest in the chart's values so nothing about it changes unless the chart changes it
      this directory holds what alasio adds to Neon control/ the stand-in for Neon's control plane and the setup that makes the stack's secrets lake/ the analytics lake and test/ the stack's own test
      alasio only connects to it through src/neon/connect.js and with neon.enabled false the chart runs none of the stack and alasio connects to the operator's database named by neon.external.existingSecret
      a NetworkPolicy lets the stack's pods labelled alasio.dev/stack neon reach each other and admits alasio only to the compute on port 55433
    Services
      SeaweedFS is the bundled S3 object store one StatefulSet with its S3 gateway whose buckets container makes the neon lake and backups buckets idempotently on every start the neon bucket versioned and writes fsynced and the pod is ready only once they exist
      with objectStore.bundled.enabled false the stack stores in an S3-compatible store of the operator's at objectStore.external with its buckets named in objectStore.buckets
      SeaweedFS's lifecycle container sets rules expiring the pageserver's replaced object versions after seven days as Neon's own point-in-time window does and the safekeepers' after one day since each re-uploads the WAL segment it is still writing whole as it grows and the seaweedfs-lifecycle CronJob applies them every six hours because SeaweedFS applies lifecycle rules only when asked
      storage-broker carries timeline state between safekeepers and the pageserver
      storage-controller runs one at a time in strict mode on its own Postgres the controller-db StatefulSet places each timeline on as many safekeepers as run and validates every deletion the pageserver makes
      the safekeepers are a StatefulSet of neon.safekeepers.replicas three by default each its own availability zone on its own volume spread across nodes and zones where the cluster has them so a commit returns once a quorum has it and each offloads WAL to the object store
      the pageserver serves pages to the compute from WAL and layers it uploads to the object store its volume a cache and registers itself with the controller from metadata.json
      neon-control is the control plane a Node service in control/service.js that registers the safekeepers creates the tenant and timeline once records them in bootstrap.json on a volume of its own answers the storage controller's hooks and every thirty seconds rebuilds any safekeeper that lost its timeline from a healthy peer
      neon-control serves the compute its spec over HTTP as Neon's compute_ctl fetches one from a control plane only to the compute's own token and only once bootstrapped
      the compute is Postgres with the neon extension one primary under the Recreate strategy so never two on one timeline started by compute_ctl from that spec holding the `alasio` database and role its disk rebuilt from the safekeepers and pageserver on every start
      the backup CronJob writes a pg_dump of the `alasio` database every day on neon.backup.schedule into the backups bucket and keeps the last neon.backup.keep fourteen by default
      the lake Deployment runs when lake.enabled is on as lake/README.md describes
      the collector runs when neon.collector.enabled is on and an endpoint is set and scrapes every thirty seconds the Prometheus metrics the pageserver safekeepers storage controller storage broker compute_ctl SeaweedFS and the lake serve sending them as OTLP over HTTP with service.namespace alasio-neon to neon.collector.otlpEndpoint or telemetry.otlpEndpoint
      compute_ctl sends its traces where the deployment's telemetry goes and none at all when it goes nowhere
    Security
      the setup hook Job runs control/kube-setup.js with control/secrets.js before every install and upgrade making the stack's secrets once into the Secret `<release>-neon-root` only ever adding to them never rotating and rendering every service's Secret from them
      the services authenticate to each other with EdDSA JWTs signed by one key whose private half is only in the root Secret which only neon-control mounts
      alasio reaches the compute as role `alasio` with a SCRAM password from the url key of the Secret `<release>-database`
      on the bundled store Neon writes with an identity limited to the neon bucket the lake with one limited to the lake bucket and only the bucket lifecycle and backups use the admin identity while with an external store every service uses the operator's one credential
      the analytics lake's password and storage identity are made here too and alasio makes role `lake` itself as a login that is a member of no other role because Neon makes every role in the compute's spec a member of neon_superuser which reads and writes every table so the lake reads alasio's transcripts and rollouts only while it runs and uses only its own `lake` database
    State
      every stateful service keeps its data on a PersistentVolumeClaim of the size the chart's values give the safekeepers the pageserver the controller-db neon-control and SeaweedFS
      SeaweedFS stops accepting writes with less than 5 GiB free on its volume so layer uploads and WAL offload stall until space is freed
      durability is the safekeepers' quorum across their volumes the versioned neon bucket and the daily dump so process crashes and a lost volume lose nothing committed while on one node every volume shares that node's disks unless the object store is external
    Operations
      `kubectl -n <namespace> get pods -l alasio.dev/stack=neon` shows the stack and each service is `<release>-neon-<component>` for kubectl logs and rollout restart
      transcripts are searched with claude_sessions.search which src/harness/claude/search describes
      transcripts and rollouts are analysed with `npm run lake -- "<SQL>"` when the analytics lake runs as lake/README.md describes
      transcripts are queried in SQL through claude_sessions.entries.doc the jsonb copy of each entry since Postgres's JSON operators fail on the entry column wherever an entry holds a NUL or half a surrogate pair
      Codex's rollout files are kept byte for byte in codex_sessions.rollouts and codex_sessions.rollout_chunks which src/codex/rollouts describes
      a restore takes the newest dump from the backups bucket into any Postgres 17 with `pg_restore --clean --if-exists` and alasio needs only its `claude_sessions` and `codex_sessions` schemas
      a safekeeper whose volume is lost is repaired by neon-control within thirty seconds of starting again by pulling the timeline from a peer
      a past moment is read by asking the pageserver's `get_lsn_by_timestamp` for its LSN and starting a second compute from the same spec with mode Static at that LSN and no safekeepers as neon/test/stack.test.js does
      an upgrade moves every Neon image to one newer release together in the chart's values then runs `npm run test:neon` against a release of it before alasio runs it because Neon publishes no compatibility promise for self-hosting
      the cost of owning it is that Neon's hosted control plane is replaced by neon-control so a Neon release that changes the storage controller's safekeeper or compute APIs needs neon-control changed with it
    Tests
      `npm run test:neon` runs neon/test/stack.test.js against a release already installed such as the one test/e2e/run.sh installs with KUBECONFIG naming the cluster and ALASIO_E2E_NAMESPACE and ALASIO_E2E_RELEASE the release
      it proves the tenant and a timeline are bootstrapped on three safekeepers an upgrade that changes nothing restarts nothing of the stack and every row survives the whole stack stopping at once and any one component killed mid-write
      it proves a safekeeper that lost its volume is rebuilt from its peers garbage the pageserver collects is deleted only once the storage controller validates it the daily dump restores into any Postgres and the database reads as it was at a past moment
      it runs the session store conformance suite on the compute and proves the lake loads what alasio's stores hold keeps each entry once through its loader killed mid-load answers queries read-only and its role reads nothing it was not granted and is no superuser's member
      test/neon-setup.test.js pins the setup's configuration secrets made once and rendered on each run a root predating a secret completed without losing the rest and an external object store's credentials used with no SeaweedFS identities made
      charts/alasio/tests/neon_test.yaml pins the stack's rendering the setup hook safekeeper count and registration one compute fetching its spec image digests the external store and database and the collector
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Helm as helm install or upgrade
  participant Setup as control/kube-setup hook Job
  participant Storage as SeaweedFS broker controller safekeepers pageserver
  participant Control as neon-control
  participant Compute as compute
  participant Alasio as src/neon/connect
  Helm->>Setup: before anything of the release starts
  Setup->>Setup: make the stack's secrets once and render every service's Secret
  Helm->>Storage: start the storage services, the object store's buckets first
  Control->>Storage: register safekeepers and create the tenant and timeline once
  Control->>Control: record them in bootstrap.json and make the compute's spec
  Compute->>Control: fetch the spec with the compute's token
  Alasio->>Compute: connect as alasio and ensure the claude_sessions and codex_sessions schemas
  Alasio->>Compute: make role lake and its database, and grant it its reads only while the analytics lake runs
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Stack
  Stack --> FloatingImages: a Neon image by tag without its digest or one image moved alone
  FloatingImages --> IncompatibleServices
  Stack --> EmergencyDeletions: a pageserver with no control plane deleting unvalidated
  EmergencyDeletions --> SplitBrainLoss
  Stack --> TwoPrimaries: two computes or two storage controllers at once
  TwoPrimaries --> SplitBrainLoss
  Stack --> OneSafekeeper: timelines on fewer than three safekeepers
  OneSafekeeper --> CommitsLostWithOneVolume
  Stack --> RotatedSecrets: secrets made again on an upgrade
  RotatedSecrets --> ServicesLockedOut
  Stack --> UnexpiredVersions: bucket versioning with no lifecycle pass
  UnexpiredVersions --> DiskFillsUp
  Stack --> PinnedStrictQuorum
  PinnedStrictQuorum --> [*]
```
