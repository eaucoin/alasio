# alasio

Coding agents you talk to from Telegram. alasio runs Claude Code and Codex as
long-lived conversations, each with a workspace of its own: a folder on your
machine, or an empty filesystem in a gVisor sandbox, with or without internet.
A conversation outlives restarts of alasio, its transcripts kept in a Neon
database that runs beside it, and in every workspace the agent works through
bayma's REPL sessions.

## Get Started

alasio runs on Kubernetes, from its Helm chart. The cluster needs a
`gvisor` RuntimeClass and to enforce NetworkPolicy; on a single machine with
Docker and k3d, `deploy/k3d/cluster.sh` makes one:

```sh
deploy/k3d/cluster.sh alasio
export KUBECONFIG=$PWD/kubeconfig-alasio
```

The chart and its images are private, in GitHub's container registry: log
Helm in with `helm registry login ghcr.io`, and give the cluster credentials to
pull with, through the chart's `imagePullSecrets` or, for that k3d cluster,
`REGISTRY_CONFIG`.

Make a bot with BotFather, then put its token and the Telegram user IDs
allowed to use it in a Secret, and install:

```sh
kubectl create namespace alasio
kubectl -n alasio create secret generic alasio-telegram \
  --from-literal=token=<bot token> --from-literal=allowedUserIds=<user ids>
helm install alasio oci://ghcr.io/eaucoin/charts/alasio --version 3.0.1 \
  -n alasio --set alasio.telegram.existingSecret=alasio-telegram
```

Claude Code logs in with a token from `claude setup-token`, in a Secret the
chart's `alasio.claude.existingSecret` names. Codex logs in once, in alasio's
pod, and keeps the login on alasio's volume:

```sh
kubectl -n alasio exec -it deployment/alasio -c alasio -- \
  /opt/alasio/node_modules/.bin/codex login --device-auth
```

Then message the bot. `/service` chooses the agent, Claude Code or Codex, and
`/workspace` where it works. A new empty workspace is a filesystem of its own,
in a sandbox that reaches the internet only if you chose so and never the
cluster. Folder workspaces are your machine's own folders, worked on as you:
they need the chart's host profile, `host.enabled`, which is privileged by
nature and meant for a single-node cluster you own; its values say which of
the machine's paths to mount and which user to run as.

`charts/alasio/values.yaml` documents every value. Among them, `neon.external`
uses a Postgres of your own instead of the Neon the chart runs, and
`objectStore.external` an S3-compatible store instead of the bundled
SeaweedFS, where Neon keeps its storage and a daily backup goes.

Restart alasio with

```sh
kubectl -n alasio rollout restart deployment/alasio
```

which an agent in a folder workspace may run too: its turn continues once
alasio is back. Workspaces are pods of their own, so their REPL sessions keep
running through it.

Every Claude Code transcript entry and Codex rollout line is also a row in an
analytics lake, which you query read-only with

```sh
kubectl -n alasio exec deployment/alasio-lake -- node src/query.ts "SELECT count(*) FROM claude.entries"
```

alasio exports OpenTelemetry traces, metrics, and logs, over OTLP, to wherever
the chart's `telemetry.otlpEndpoint` says, and Claude Code, Codex, and bayma
export theirs to the same place; with no endpoint set, nothing is exported.

## Development

alasio is TypeScript that Node 24 runs as it is, checked by `npm run typecheck`
against `tsconfig.json`. Its control plane is written in Effect: its services are
layers, composed in `src/alasio.ts`. See `package.json` for the development
scripts, `.env.example` for running alasio outside the cluster against one,
`Dockerfile`, `sandbox/`, and `neon/lake/` for the images, `charts/alasio/` for
the chart and its tests, `test/e2e/run.sh` for the end-to-end run, and
`.github/workflows/` for the GitHub Actions workflows.
