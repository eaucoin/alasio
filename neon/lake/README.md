## Concept Atlas
```mermaid
mindmap
  root((neon/lake))
    What it is
      the analytics lake holds every Claude Code transcript entry and every Codex rollout line alasio keeps in its Neon as typed rows to analyse however one likes
      it is DuckLake v1.0 whose catalog is the compute's `lake` database and whose Parquet files are in SeaweedFS's `lake` bucket so its compute is stateless and everything durable sits on the stack's own storage
      it is derived and Neon stays the source of truth so a lake lost or of another model version is dropped and loaded again from Neon
      it runs only when ALASIO_LAKE_ENABLED is 1 and its role database password and storage identity exist on the stack either way so turning it on or off restarts nothing but the lake
    The service
      lake is the stack's compose service of profile lake built from this directory by alasio into alasio-neon-lake tagged with the revision of what it is built from so a change to it is a new image that is never pulled
      the image is glibc Node with DuckDB's Node API pinned exactly and the ducklake postgres_scanner and httpfs extensions installed at build so nothing is fetched at run time
      lake-init makes the unversioned lake bucket since DuckLake never changes a file it wrote
      it connects as role lake which alasio makes itself in src/neon/lake.js as a login that is a member of no other role since Neon makes a role in the compute's spec a member of neon_superuser which reads and writes every table
      role lake is granted select on claude_sessions.entries and the rollout tables only while the lake runs and may connect to create in and stage temporary tables in only its own lake database where its catalog lives
      one loader runs at a time held by a Postgres advisory lock on the catalog that a second waits on
      the loop opens the lock and the lake itself and a load that fails drops both and is tried on fresh connections so the compute restarting a lost lock or reads not yet granted are ridden out without the process restarting
      it serves /healthz from the moment it starts unhealthy only once loads have failed for three intervals and Prometheus /metrics which the stack's collector scrapes when telemetry is on
    Loading
      a load runs as the service starts and every five minutes and a failed one is tried again within thirty seconds which is what happens until alasio grants the lake its reads
      Claude entries are compared by seq the source assigns and never reuses so appends committed out of order and deleted sessions are both followed with nothing read but seqs until an entry is known missing
      entries load in batches of a thousand each one transaction of the entries and their content blocks so a load cut short leaves whole batches
      Codex rollout files are compared by size and the digest of their first line a new or rewritten or shrunk file is loaded whole again a grown one from where its last complete line ended a moved one has its path followed and a removed one goes
      only complete lines are loaded and a file's chunks are read one at a time so a large rollout never sits whole in memory and each file's load is one transaction of its lines and its record
      text DuckDB cannot read as JSON such as an escaped lone surrogate Postgres accepts is kept as a JSON string flagged malformed rather than stop the load
    Model
      claude.entries is a row per entry with its keys stored_at type occurred_at parent and sidechain fields cwd git_branch version message_id role model stop_reason and usage typed and the whole entry as JSON
      claude.content_blocks is a row per block of a message with its type role text length tool use id tool name tool input as JSON and whether a tool result was an error
      claude.messages is a row per assistant message with the values its last entry carries because Claude Code writes an entry per content block each with the usage so far and summing entries overcounts
      claude.tool_calls pairs each tool call with its result its timing error flag and result length
      codex.lines is a row per rollout line with home folder or sessionfs file thread rollout line number byte offset occurred_at ordinal type payload_type turn_id and the whole line as JSON and codex.files records how far each file is loaded
      codex.turns codex.token_usage and codex.tool_calls shape turns with their model and timings each response's usage once and each function or custom tool call with its output
      whole entries are JSON rather than VARIANT because DuckLake's writer shreds a VARIANT column into a Parquet column per path which for entries this varied ran out of memory and past what a Postgres catalog can describe
      MODEL_VERSION in model.js is bumped with any change of a table's shape and a lake of another version is rebuilt from Neon as the service starts
    Keeping
      a maintenance pass at most daily recorded in loader.meta so a restart neither repeats nor skips one is DuckLake's CHECKPOINT which merges small files expires snapshots older than seven days and deletes files nothing has needed for a day
    Querying
      npm run lake -- with optional --format table csv or json and the SQL runs src/query.js in the lake's container with the lake attached read-only and as the default database so tables are named claude.entries codex.turns and so on
      nothing of the lake is published on the host as its catalog and files are reached on the stack's private network only
    Tests
      test/lake.test.js runs the real DuckDB against a throwaway Postgres written through alasio's own stores with grants made as production makes them and pins every loading rule above with the model's views and the loader's health
      npm run test:neon brings the stack up with the lake on and proves it loads what the stores hold keeps each entry once through its loader killed mid-load answers queries read-only and turned off stops and reads nothing
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Alasio as src/neon/stack
  participant Compose as docker compose
  participant Lake as lake service
  participant Source as compute alasio database
  participant Catalog as compute lake database
  participant Store as seaweedfs lake bucket
  Alasio->>Compose: build alasio-neon-lake at this revision unless built, then up with profile lake
  Compose->>Lake: start once lake-init made the bucket and the compute is healthy
  Lake->>Catalog: hold the loader lock, attach DuckLake, build or keep the model
  Alasio->>Source: make role lake and its database, and grant it its reads once the stores' schemas exist
  loop every five minutes
    Lake->>Source: seqs and rollout records, then only what is missing
    Lake->>Store: Parquet files, one transaction per batch or file
    Lake->>Catalog: the snapshot that commits them
  end
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Lake
  Lake --> VariantShredding: whole entries as VARIANT
  VariantShredding --> OutOfMemoryAndCatalogLimits
  Lake --> NaiveUsageSums: tokens summed over entries
  NaiveUsageSums --> UsageOvercountedTwofold
  Lake --> HighWaterMark: loading past the highest seq loaded
  HighWaterMark --> EntriesCommittedLateLost
  Lake --> FetchedExtensions: extensions installed at run time
  FetchedExtensions --> StartsThatDependOnTheInternet
  Lake --> TwoLoaders: two loaders at once
  TwoLoaders --> DuplicateRows
  Lake --> SpecRole: the lake's role in the compute's spec
  SpecRole --> ReadsAndWritesEveryTable
  Lake --> DerivedAndRebuildable
  DerivedAndRebuildable --> [*]
```
