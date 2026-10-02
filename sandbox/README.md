## Concept Atlas
```mermaid
mindmap
  root((sandbox))
    Why it exists
      a session filesystem is a workspace in a Sandbox of its own whose pod runs this directory's agent image under gVisor by default holding bayma and every process the agent starts on the session's own volume
      the harness Claude Code or Codex runs in alasio and reaches the workspace only through bayma so no login no harness and no harness state is ever inside a session
      the choice is validated in session-fs-research the study whose experiments sized and de-risked every part of this
      the harness stays out because its tools are what reach a machine and bayma alone passed every workspace task for both harnesses while the alternatives failed claude mcp serve returns raw JSON so Read loses images and Edit skips read-before-edit and Codex exec-server is experimental with no reconnect and one app-server's environments are not isolated from each other
    The agent image
      agent/Dockerfile builds alasio-agent from bayma's image pinned by digest with its user renamed to the unprivileged agent and /workspace and /home/agent made for it
      bayma's toolbelt about 25k files is installed once in the image under /opt/bayma-data by bayma's own installer because copying it onto every new session's volume took minutes and each session's home links to it so bayma finds its version current and copies nothing
      release.yml publishes it as ghcr.io/eaucoin/alasio-agent and the chart pins it by digest and test/e2e/run.sh builds it for the end-to-end run
    In a session
      the chart's sessions template runs the image as uid 1000 with no privilege escalation every capability dropped and the runtime's default seccomp profile as Pod Security restricted requires
      a prepare init container makes the workspace and home on the session's volume and links the toolbelt into the home and the egress gate init container runs on the image's Node
      bayma runs under tini as its MCP over HTTP server on port 7290 with its state in the home and alasio adds the token it requires
      /workspace and /home/agent are subpaths of the session's volume and /tmp is a bounded emptyDir
    What the agent sees
      an empty workspace its own home a private tmp the image's system and the RuntimeClass's kernel gVisor's by default and nothing of the cluster no ServiceAccount token no service links no other session and no model login
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant Alasio as alasio (src/sandbox)
  participant Ctl as agent-sandbox controller
  participant Init as prepare and egress-gate
  participant Bayma as bayma
  Alasio->>Ctl: a Sandbox from the sessions template, with its token Secret
  Ctl->>Init: run the pod under gVisor with the session's volume
  Init->>Init: make workspace and home, link the toolbelt, wait until egress is confined
  Ctl->>Bayma: start bayma's MCP HTTP server as the agent
  Alasio->>Bayma: MCP over HTTP with the session's token
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Isolated
  Isolated --> HarnessInside: the harness and its login run inside the session
  HarnessInside --> Exfiltration
  Isolated --> PrivilegedPod: the session's pod runs as root or with added capabilities
  PrivilegedPod --> WeakerBoundary
  Isolated --> ToolbeltCopiedPerSession: bayma's toolbelt copied onto each new volume
  ToolbeltCopiedPerSession --> MinutesToStart
  Isolated --> UnpinnedBase: bayma's image by tag without its digest
  UnpinnedBase --> VersionDrift
  Isolated --> OneVolumeOneSandbox: a volume and a gVisor Sandbox per session, bayma the only door
  OneVolumeOneSandbox --> [*]
```
