## Concept Atlas
```mermaid
mindmap
  root((telegram))
    App
      app owns lifecycle wiring and delegates update-specific policy
      update-poller owns Bot API long polling offset advancement and retry backoff
      authorizer owns private-chat identity explicit allowlist bootstrap-user and callback access
      callback-handler owns queue discard swerve session-control and goal-control callback actions with shared active-turn status
      outbox owns durable final-response delivery retries and Telegram rate-limit backoff
      app validates outbox composition at construction and retries completed response handoff during normal uptime
      recovery timers ignore active response streams and hand off only terminal final-answer output
      callback ingress reauthorizes the private sender before consuming an action and acknowledges long controls before app-server work so Telegram buttons remain responsive
      callback-handler passes New Session callbacks through to turn-controller so inline buttons mount real Codex threads
      message-handler owns authorized message persistence downloads media-group buffering and prompt dispatch
      media-group-buffer owns multi-file grouping timers and due flush recovery
    Bot API
      client owns raw Telegram HTTP calls file downloads and message chunking
      client pins Node Bot API networking to IPv4-first without family autoselection because this host has no usable IPv6 route
      client also owns bot command and menu-button registration for the native session and goal control entrypoints
      client serializes outbound calls and honors Bot API retry-after responses
      markdown owns Telegram HTML rendering for Codex Markdown plus plaintext fallback safety
      message owns Telegram update to alasio message/file projection
      text owns Telegram-safe message splitting
    Rich replies
      final responses go as Bot API 10.1 rich messages which render tables headings lists and code natively in parts of about 30k characters while status lines and panels stay Telegram HTML
      rich-markdown escapes what Telegram's rich Markdown would read that the agent did not mean dollar as a formula pipes as a spoiler equals as a highlight and angle brackets as a tag it then drops leaving code as written
      rich-media places each file an agent shows with image syntax and a local path as a media block below its block or a collage when one block shows several and identifies files by their bytes
      client uploads a part's media with its rich message in one request and when Telegram rejects the part sends its text the classic way and its media as photos videos or albums silently
      outbox deletes a reply's media copies once it is sent
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Telegram
  participant App as app
  participant Store as persistence/store
  participant Turns as codex/turn-controller
  Telegram->>App: private update or callback
  App->>Store: persist raw content and continuity
  App->>Turns: dispatch a single prompt native session panel goal action or operator decision with truthful active-turn context
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> TelegramUpdate
  TelegramUpdate --> MixedPolicy: polling app accumulates auth callbacks media and turn logic
  MixedPolicy --> FragileRestart
  FragileRestart --> DuplicatePrompt
  DuplicatePrompt --> OperatorConfusion
  OperatorConfusion --> MixedPolicy
  TelegramUpdate --> UnauthorizedIngress: non-private or wrong-user messages or callbacks enter runtime
  UnauthorizedIngress --> StateLeak
  TelegramUpdate --> FocusedDelegate
  FocusedDelegate --> AuthorizedCallback: callback sender and private chat match the configured operator before action consumption
  FocusedDelegate --> AuthorizedMessage
  AuthorizedMessage --> PersistedContent
  PersistedContent --> SingleTurnDispatch
  FocusedDelegate --> StableDMBridge
  StableDMBridge --> [*]
```
