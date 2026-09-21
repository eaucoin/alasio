## Concept Atlas
```mermaid
mindmap
  root((operator))
    Commands
      command-parser maps Telegram text to the command algebra including /service and /service codex or claude
      service-control owns the Telegram-native service panel that shows the active harness or none both parked session pointers and Use Codex or Use Claude Code switches plus sendChooseServicePanel which is the only reply an unmounted conversation receives
      service switches route through the turn controller which refuses while a turn is active or prompts are queued
      command-handler resolves the active harness adapter for stop sessions new-session rewind and resume so each control acts on that harness's own session store and answers with the service picker when nothing is mounted
      /goal replies that goals are Codex-only while Claude Code is active
      command-handler executes stop sessions new-session goal rewind and resume behavior
      new-session commands and callbacks create then mount real Codex app-server threads
      session-control owns Telegram-native intercession and intersection panels over Codex session primitives
      no-mounted goal panels route to Sessions and New Session controls instead of dead-ending on Close
      goal-control owns Telegram-native goal panels over Codex thread goal RPC primitives and ensures active goals attach start or enter the normal concurrent-turn decision flow
      fresh objective goal writes clear stale upstream goal state before setting replacements
      pause and clear goal controls interrupt the tracked active Telegram turn after updating upstream goal state
      stop controls report completion only after bounded transport cleanup has finished
      every session-control panel exposes Close so the operator can dismiss stale inline keyboards
      session and goal callbacks record the harness generation they were rendered under and are rejected after a service switch
    Replies
      session-replies formats session and rewind listings for Telegram
      text owns truncation and compact command-list formatting
      restart-prompts owns synthetic continuation prompts after service restarts and names the harness that owned the interrupted turn
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Telegram
  participant Handler as command-handler
  participant Sessions as sessions
  participant Client as telegram/client
  Telegram->>Handler: command text
  Handler->>Sessions: inspect mount fork or preview Codex rollout state
  Handler-->>Client: send bounded operator-readable reply
  Telegram->>Handler: inline keyboard callback
  Handler-->>Client: edit the same control-panel message with the next session view
  Telegram->>Handler: /goal or goal inline callback
  Handler-->>Client: render empty-state goal panel current mounted-thread goal controls active goal turn status or concurrent-turn choices
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> OperatorIntent
  OperatorIntent --> HiddenPrompt: commands fall through as normal Codex prompts
  HiddenPrompt --> ConfusingSessionState
  ConfusingSessionState --> RepeatedManualRecovery
  RepeatedManualRecovery --> HiddenPrompt
  OperatorIntent --> AmbiguousResume: session ids and numbers are parsed inconsistently
  AmbiguousResume --> WrongConversationContext
  OperatorIntent --> ExplicitCommand
  ExplicitCommand --> ParsedAlgebra
  ParsedAlgebra --> BoundedExecution
  ExplicitCommand --> BoundedReply
  BoundedReply --> StableOperatorState
  BoundedReply --> [*]
```
