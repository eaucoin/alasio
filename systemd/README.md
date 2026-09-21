## Concept Atlas
```mermaid
mindmap
  root((alasio systemd))
    Unit
      alasio.service runs the Telegram bridge from the isolated deployment checkout
      EnvironmentFile loads operator credentials from the protected source workspace
      ExecStart binds the installed Node 24 runtime instead of assuming a system-wide /usr/bin/node exists
      PATH exposes the operator-owned Bun installation so repository hooks and generators can resolve both bun and bunx without session-local repair
      BAYMA_PYTHON_BIN binds Bayma sessions to the locked Breadbutter Python 3.12 environment instead of the older host python3 fallback
      WORKING_DIRECTORY keeps upstream Codex operating in the mutable agent workspace
      OOMPolicy continue contains descendant memory failures instead of stopping the healthy Telegram bridge
      Restart always still recovers main-process exits so containment does not weaken service supervision
    Installation
      install-alasio-service.sh installs the Alasio runtime from its committed npm lock before running capability checks
      install-alasio-service.sh synchronizes the frozen Breadbutter Bun and Python 3.12 environments and runs the real Bayma Python and Rust capability doctor before changing systemd state
      install-alasio-service.sh exports the same provisioned Python interpreter used by the installed service before running the doctor
      install-alasio-service.sh seeds Bayma's private Cargo home and fetches the exact Breadbutter lock before the offline Rust quickstart probe
      install-alasio-service.sh installs enables and reloads the canonical unit
      restart-alasio-operator.sh remains the normal provenance-aware restart path
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Operator
  participant Installer
  participant Systemd
  participant Alasio
  Operator->>Installer: install canonical unit
  Installer->>Alasio: install the locked Alasio runtime dependencies
  Installer->>Alasio: provision frozen Bun plus Python Breadbutter dependencies and prove the disposable Bayma Python and Rust quickstarts
  Installer->>Systemd: reload and enable alasio.service
  Systemd->>Alasio: expose the operator Bun and bunx toolchain on the canonical service PATH
  Systemd->>Alasio: launch through the installed Node 24 runtime with the same Node directory available to child processes
  Systemd->>Alasio: expose the locked Breadbutter Python 3.12 interpreter to Bayma
  Systemd->>Alasio: contain descendant OOM kills while retaining main-process restart recovery
  Systemd->>Alasio: start isolated runtime against operator workspace
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> MutableCheckoutUnit
  MutableCheckoutUnit --> BranchCoupling
  BranchCoupling: service loads source from the branch being edited by the connected agent
  BranchCoupling --> RestartDrift
  RestartDrift: an unrelated restart silently changes the bot binary and dependencies
  RestartDrift --> ConversationRisk
  ConversationRisk: transport behavior changes inside an otherwise continuous Telegram session
  MutableCheckoutUnit --> IsolatedDeployment
  IsolatedDeployment: service source and dependencies are pinned outside the operator workspace
  IsolatedDeployment --> StableRuntime
  StableRuntime --> DescendantPressure: a child tool or language runtime exhausts its cgroup memory allowance
  DescendantPressure --> StableRuntime: systemd records the OOM without stopping a surviving alasio main process
  StableRuntime --> ProvenanceAwareRestart
  ProvenanceAwareRestart --> StableRuntime
  StableRuntime --> [*]
```
