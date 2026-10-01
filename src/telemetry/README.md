## Concept Atlas
```mermaid
mindmap
  root((telemetry))
    Configuration
      alasio exports OpenTelemetry traces metrics and logs over OTLP to whatever backend the standard OTEL_* variables name and assumes none
      config resolves each signal from the standard variables alone and a signal is exported when it has an endpoint of its own or the shared one and its OTEL_SIGNAL_EXPORTER is unset or otlp
      OTEL_SDK_DISABLED=true turns every signal off and with no endpoint at all alasio loads no SDK and exports nothing
      the shared endpoint gains each signal's path for OTLP over HTTP as the standard says and per-signal endpoints are used as given
      OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES override the resource alasio gives itself service.name alasio and the commit it runs from as vcs.ref.head.revision
    Start
      src/index.js starts telemetry before the service's modules load and then imports src/main.js so the import hook patches pg and http as the service loads them
      start sets each signal's exporter to otlp or none because the SDK would default a signal left unset to OTLP on localhost
      the SDK's own problems such as failed exports go to the console only so they never feed back into the log export that failed
      main stops telemetry before alasio exits so what is buffered is flushed
    Recording
      index is the face the rest of alasio records through and holds only OpenTelemetry API calls which cost nothing until start registers the SDK
      inSpan runs a function in a span that records what it throws and continues the active span or a stored traceparent or starts a trace of its own
      rpcCall is the one shape of a call alasio makes to the Telegram Bot API or the Codex app-server a client span named service slash method and rpc.client.call.duration
      currentTraceparent and traceCarrier hand a span's W3C context to work that runs later or elsewhere and outsideTraces keeps work that outlives a span out of it
      meter is where alasio's own instruments are created next to what they measure
      shared/log writes every line to the console and as a log record of its scope in the active trace
    Children
      withoutTelemetry keeps alasio's OTEL_* settings and any TRACEPARENT from the environments of Claude Code Codex and bayma because they would relabel or redirect the harnesses' own telemetry and reach every command an agent runs
      each harness gets explicit settings of its own instead in its own dialect harness/claude/telemetry for Claude Code and codex/app-server/telemetry for Codex
      conversationTelemetryEnv is the standard variables that have a process serving one conversation export each signal alasio exports labelled with the conversation which Claude Code's settings build on and mcp/bayma hands bayma through its MCP config
      sharedResourceAttributes passes the operator's deployment attributes on to the harnesses without service.name which is alasio's
      the Neon stack's collector is configured from ALASIO_NEON_OTLP_ENDPOINT because its endpoint is reached from the stack's own network see neon/README.md
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Index as src/index
  participant Start as telemetry/start
  participant Main as src/main
  participant Code as alasio modules
  participant Api as telemetry/index
  Index->>Start: startTelemetry from the standard variables
  Start->>Start: register the import hook and the SDK only when a signal has an endpoint
  Index->>Main: import the service once instrumentation is in place
  Main->>Code: run the service
  Code->>Api: inSpan rpcCall meter and loggers through the OpenTelemetry API
  Main->>Start: stopTelemetry before exit so buffered telemetry is flushed
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Telemetry
  Telemetry --> VendorLockIn: a backend named or assumed in code configuration or defaults
  VendorLockIn --> OneBackendOnly
  Telemetry --> ExportWithoutEndpoint: the SDK started with nothing configured
  ExportWithoutEndpoint --> ExportsToLocalhostFailing
  Telemetry --> LeakedSettings: alasio's OTEL_* settings inherited by the harnesses and their commands
  LeakedSettings --> MislabelledOrMisdirectedTelemetry
  Telemetry --> TokenInSpans: Bot API URLs recorded by HTTP instrumentation
  TokenInSpans --> CredentialLeak
  Telemetry --> StandardVariablesOnly
  StandardVariablesOnly --> ExplicitChildSettings
  ExplicitChildSettings --> [*]
```
