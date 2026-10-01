## Concept Atlas
```mermaid
mindmap
  root((src/neon))
    stack
      startNeon renders the stack's configuration through neon/control/setup.js brings the `alasio-neon` compose project up and waits until every service is healthy leaving services already running alone and removing any no longer in compose.yml
      it then opens one pg pool to the compute ensures the schemas of the Claude session store and the Codex rollout store and returns the pool the store the rollouts and close
      an idle connection the compute drops when it restarts is logged and replaced on the next checkout instead of crashing alasio
      bringing the stack up is the span alasio.neon.start
      neonTelemetry reads ALASIO_NEON_OTLP_ENDPOINT where the stack's own collector sends its metrics as the stack's network reaches it with the headers alasio's metrics go with and with it unset a collector an earlier start left running is stopped
      pullNeon pulls every image of the stack so install-alasio-standalone-service.sh can spare the first start the downloads
      project and computePort are parameters only so tests can run a stack of their own beside alasio's
    Boundary
      the stack itself and how to operate it are described in neon/README.md
      index.js starts Neon before the Telegram app so no turn runs without the session and rollout stores and closes the pool at shutdown
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
