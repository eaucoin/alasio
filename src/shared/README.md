## Concept Atlas
```mermaid
mindmap
  root((shared))
    Small utilities
      async owns abort-aware sleep
      file-prompt owns attachment prompt suffix rendering
      human-time owns duration text
      ids owns runtime UUID generation
      log owns scoped loggers that write each line to the console and as a log record of its scope in the active trace
      runtime-constants owns constants shared across domains
      session-labels owns the session panels' one-line labels and dates for both harnesses' session apis
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Telegram as telegram/*
  participant Codex as codex/*
  participant Operator as operator/*
  participant Shared as shared/*
  Telegram->>Shared: split delays ids and constants through tiny helpers
  Codex->>Shared: reuse abort-aware sleep duration text and file prompt suffixes
  Operator->>Shared: keep text helpers out of shared unless multiple domains need them
  Shared-->>Telegram: return values without touching Bot API state
  Shared-->>Codex: return values without touching Codex transport state
  Shared-->>Operator: return values without reading session files or persistence
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Helper
  Helper --> JunkDrawer: domain behavior migrates into shared
  JunkDrawer --> HiddenCoupling
  HiddenCoupling --> CrossDomainRegression
  CrossDomainRegression --> JunkDrawer
  Helper --> MisplacedPolicy: command or persistence decisions hide behind utility names
  MisplacedPolicy --> HiddenCoupling
  Helper --> SmallPureUtility
  SmallPureUtility --> NoDomainImports
  NoDomainImports --> EasyInlineAudit
  SmallPureUtility --> [*]
```
