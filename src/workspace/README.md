## Concept Atlas
```mermaid
mindmap
  root((workspace))
    Policy
      policy resolves operator supplied folders to canonical realpaths and refuses anything that is not an existing directory under ALASIO_WORKSPACE_ROOT
      symlinks are followed before the containment check so a link that escapes the root is rejected
      listWorkspaceCandidates returns top level folders under the root with git repositories first and hidden entries skipped
      createWorkspace makes one git initialized folder directly under the root from a constrained name and refuses names that already exist
    Boundaries
      only turn-controller switchWorkspace and createWorkspace call the policy so every mounted folder passed to a harness came through it
      WORKING_DIRECTORY is operator configuration and is trusted as a pre-mount without going through the root check
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Operator as Telegram /workspace
  participant Turns as codex/turn-controller
  participant Policy as workspace/policy
  participant Store as persistence/store
  Operator->>Turns: switchWorkspace target or createWorkspace name
  Turns->>Policy: resolveWorkspacePath or createWorkspace under workspaceRoot
  Policy-->>Turns: canonical folder or WorkspaceError
  Turns->>Store: setWorkingDirectory parks and restores per folder session pointers
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> FolderRequest
  FolderRequest --> RootEscape: relative dots absolute paths or symlinks resolve outside the root
  RootEscape --> OperatorMistrust
  FolderRequest --> ImplicitFolder: a harness runs in a folder the operator never chose
  ImplicitFolder --> OperatorMistrust
  FolderRequest --> PolicedMount: canonical folder under the root mounted per conversation
  PolicedMount --> [*]
```
