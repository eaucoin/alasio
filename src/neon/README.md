## Concept Atlas
```mermaid
mindmap
  root((src/neon))
    stack
      startNeon renders the stack's configuration through neon/control/setup.js brings the `alasio-neon` compose project up and waits until every service is healthy leaving services already running alone
      it then opens one pg pool to the compute ensures the Claude session store's schema and returns the pool the store and close
      an idle connection the compute drops when it restarts is logged and replaced on the next checkout instead of crashing alasio
      pullNeon pulls every image of the stack and fetches pgrag's models so install-alasio-standalone-service.sh can spare the first start the downloads
      project and computePort are parameters only so tests can run a stack of their own beside alasio's
    models
      ensurePgragModels keeps pgrag's two ONNX models under the stack's models directory for the pgrag-models service to serve to the compute fetching once the pgrag v0.1.2 release Neon's compute image is built from and checking it against Neon's pinned checksum and each model against its own
      startNeon fetches them if missing but starts the stack without them if they cannot be had since only embedding and reranking need them
      tarMember reads a file out of a tar archive so no tar binary is needed
    Boundary
      the stack itself and how to operate it are described in neon/README.md
      index.js starts Neon before the Telegram app so no turn runs without the session store and closes the pool at shutdown
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Index as src/index
  participant Stack as neon/stack
  participant Compose as docker compose
  participant Store as harness/claude/session-store
  Index->>Stack: startNeon with the alasio state directory
  Stack->>Compose: up --detach --wait
  Stack->>Store: ensureSchema on the compute
  Stack-->>Index: pool store close
  Index->>Index: start the Telegram app with the store
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Startup
  Startup --> ServingWithoutStore: the app starts before Neon is healthy
  ServingWithoutStore --> TranscriptsMissedByTheMirror
  Startup --> SharedTestStack: tests using alasio's own project or port
  SharedTestStack --> OperatorDataTouched
  Startup --> HealthyStackFirst
  HealthyStackFirst --> [*]
```
