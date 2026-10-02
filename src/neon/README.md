## Concept Atlas
```mermaid
mindmap
  root((src/neon))
    connect
      connectNeon connects to the Neon the deployment runs from ALASIO_DATABASE_URL and ALASIO_LAKE_PASSWORD or their _FILE forms which the chart mounts from the Secret `<release>-database` or the operator's neon.external.existingSecret
      the stack starts beside alasio so connectNeon retries every five seconds for up to ten minutes as the span alasio.neon.connect logging only now and then while it waits
      it opens one pg pool ensures the schemas of the Claude session store and the Codex rollout store makes the lake's role and catalog database and grants or revokes the lake's reads and returns the pool the store the rollouts lake and close
      an idle connection the compute drops when it restarts is logged and replaced on the next checkout instead of crashing alasio
      alasio starts or stops nothing of the stack since the chart runs it
    lake
      lake makes role lake as a login that is a member of no other role and its own lake database and grants it select on the transcript and rollout tables only while ALASIO_LAKE_ENABLED is 1 which the chart sets from lake.enabled
    lake-query
      lake-query is `npm run lake -- "<SQL>"` with an optional --format of table csv or json and runs the query in the lake's own pod through kubectl exec into deployment alasio-lake in namespace alasio or ALASIO_LAKE_DEPLOYMENT and ALASIO_NAMESPACE as the current kubectl context reaches the cluster so nothing of the lake is published
    Boundary
      the stack itself and how to operate it are described in neon/README.md and the lake in neon/lake/README.md
      main connects to Neon before the Telegram app so no turn runs without the session and rollout stores and closes the pool at shutdown
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Main as src/main
  participant Connect as neon/connect
  participant Compute as the chart's compute
  participant Lake as neon/lake
  Main->>Connect: connectNeon from the mounted database Secret
  loop until it answers, up to ten minutes
    Connect->>Compute: open the pool and ensure the stores' schemas
  end
  Connect->>Lake: make role lake and its database, grant or revoke its reads
  Connect-->>Main: pool store rollouts lake close
  Main->>Main: start the Telegram app with the stores
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Startup
  Startup --> ServingWithoutStore: the app starts before Neon answers
  ServingWithoutStore --> TranscriptsMissedByTheMirror
  Startup --> AlasioRunsTheStack: alasio starting or reconfiguring the stack it depends on
  AlasioRunsTheStack --> TwoOwnersOfOneDatabase
  Startup --> PublishedLake: the lake reached from outside the cluster to query it
  PublishedLake --> CatalogAndFilesExposed
  Startup --> ConnectAndWait
  ConnectAndWait --> [*]
```
