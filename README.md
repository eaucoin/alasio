# alasio

Coding agents you talk to from Telegram. alasio runs Claude Code and Codex as
long-lived conversations, each with a workspace of its own: a folder on your
machine, or an empty filesystem in a gVisor sandbox, with or without internet.
A conversation outlives restarts of alasio, its transcripts kept in a Neon
database that runs beside it, and in every workspace the agent works through
bayma's REPL sessions.

## Get Started

alasio installs and runs itself with its command line, the npm package
`alasio`. It needs a Linux x86-64 machine with Docker and Node 24, whose
inotify limits hold the cluster's containers: `fs.inotify.max_user_instances`
at least 1024 and `fs.inotify.max_user_watches` at least 524288, which `init`
says how to raise. On such a machine, make a bot with BotFather, then:

```sh
npx alasio init
npx alasio up
```

`init` asks for the bot's token, which it checks with Telegram, the Telegram
user IDs allowed to use it, Claude Code's token from `claude setup-token`,
whether agents may work in this machine's folders, and where telemetry goes. It
writes what you answer to `~/.config/alasio/config.json`, which holds no
secret: the tokens are kept in the cluster alone, as Secrets. That cluster is
one alasio makes on this machine, k3s with gVisor in Docker, its volumes under
`~/.local/share/alasio/storage`. `up` starts it, and alasio in it, and waits
until alasio runs. Run `init` again to change an answer; every question has a
flag that answers it instead, for scripts (`npx alasio init --help`). Each
command below runs as `npx alasio <command>`, or as `alasio <command>` once
`npm install --global alasio` has installed it.

Then message the bot. `/service` chooses the agent, Claude Code or Codex, and
`/workspace` where it works. A new empty workspace is a filesystem of its own,
in a sandbox that reaches the internet only if you chose so and never the
cluster. Folder workspaces are your machine's own folders, worked on as you:
`init` asks which, and as whom; they are privileged by nature, and meant for a
machine you own.

A new sandbox's filesystem is a directory of one JuiceFS file system, as large
as `sessions.storage.size` says, its files kept in the object store beside
Neon's and its metadata in a Valkey alasio runs; JuiceFS's CSI driver, which
alasio installs, makes each one's volume and mounts it. A deleted workspace's
files stay in the file system's trash for a day, and JuiceFS copies the file
system's metadata to the object store every hour, into the workspaces bucket's
`workspaces/meta/`; the daily backup copies the newest of these beside Neon's
dump, and a daily check repairs what a crashed client left uncounted of each
workspace's size.

Should Valkey lose the metadata, or hold metadata gone wrong, restore a copy
from a pod in alasio's namespace labelled `alasio.dev/workload: juicefs-admin`,
the one kind of pod besides JuiceFS's own that reaches Valkey and the bucket,
on the JuiceFS mount image the config pins. The Secret
`alasio-workspaces-juicefs` holds what it needs: the file system's `metaurl`,
Valkey's database 1, and the bucket and its keys. Fetch the copy with `juicefs
sync`, `juicefs load` it into an empty Valkey database, give that the bucket's
keys, which copies leave out, with `juicefs config --access-key --secret-key`,
and check it with `juicefs fsck`. Load into database 1 once it is empty, or
point the Secret's `metaurl` at the database you loaded, until `alasio up`
renders it again. Then delete the sessions' pods, in `alasio-sessions`: each
comes back mounted on what you restored. Never mount a restored copy read-write
while the file system it was copied from runs on the same bucket: both would
write and delete the same objects. A copy restores whole only within
`workspaceStorage.trashDays` of when it was made, as the data of files deleted
since leaves the bucket with the trash.

Codex logs in once, in alasio, and keeps the login on alasio's volume:

```sh
npx alasio login codex
```

`alasio status` says what runs and whether it is healthy, `alasio logs` what
alasio logs (`--follow` follows it, and `alasio logs lake` a component's), and
`alasio restart` restarts it: a turn continues once alasio is back, and
workspaces are pods of their own, so their REPL sessions keep running through
it. `npx alasio@latest upgrade` upgrades alasio to the newest version. An
upgrade that changes a workspace's pod, as a newer bayma does, replaces the pod
at the workspace's next turn, its files kept. `alasio down` stops the cluster,
keeping everything, and `alasio up` starts it again.

Every Claude Code transcript entry and Codex rollout line is also a row in an
analytics lake, which you query read-only with

```sh
npx alasio lake "SELECT count(*) FROM claude.entries"
```

alasio exports OpenTelemetry traces, metrics, and logs, over OTLP, to the
endpoint `init` asks for, and Claude Code, Codex, and bayma export theirs to
the same place; with none, nothing is exported.

The config's `install` holds the rest of how alasio is installed, every key of
it optional, and `cli/src/manifests/config.ts` documents each. Among them,
`neon.external` uses a Postgres of your own instead of the Neon alasio runs,
and `objectStore.external` an S3-compatible store instead of the bundled
SeaweedFS, where Neon keeps its storage, a daily backup goes, and workspaces
keep their files, each in a bucket the store must already have.
`workspaceStorage.enabled: false` makes new workspaces volumes of the
cluster's default StorageClass instead of JuiceFS's; a workspace keeps the
volume it was made with either way. `alasio up` applies what you change.

The config's `target.local` is the cluster alasio makes, and `registries` in it
says where its nodes pull images from, as k3s's `registries.yaml` does, its
keys in camel case: `mirrors`, the endpoints that stand for a registry, and
`configs`, a registry's TLS, its files the nodes' own, as `mounts` mounts them.
A registry that asks for a login is not one of them, as the config holds no
secret. `alasio up` makes the nodes anew with what you change, keeping their
data.

`alasio uninstall` removes alasio and keeps its data, which `alasio up`
installs it again with; `alasio uninstall --purge` removes the data too, and
the cluster alasio made.

alasio runs in a cluster of yours as well: `npx alasio init --kubeconfig
<path>`, with `--context <name>` for another than the kubeconfig's current
one, installs it in the cluster that kubeconfig reaches. The cluster must
enforce NetworkPolicy, and have a `gvisor` RuntimeClass, unless the config's
`sessions.runtimeClassName` names another. JuiceFS's CSI driver goes in
`kube-system`, as its own manifests put it; where the cluster has the driver
already, `workspaceStorage.csi.enabled: false` uses that one.

## Development

alasio is TypeScript that Node 24 runs as it is, checked by `npm run typecheck`
against `tsconfig.json` and tested by `npm test`. `src/` is alasio's server,
the Telegram bridge that runs the agents, in alasio's image (`Dockerfile`); its
control plane is written in Effect, its services layers composed in
`src/alasio.ts`, and `.env.example` is for running it outside the cluster
against one. `cli/` is the command line, the npm package: it builds the
Kubernetes objects it installs in `cli/src/manifests/`, JuiceFS's CSI driver
among them as JuiceFS's own manifests have it, applies them through
the Kubernetes API, and makes the cluster on this machine through Docker's
Engine API, in `cli/src/cluster/`, from the node image in `cluster/node/`.
`sandbox/agent/` is the image sessions run, and `neon/` what alasio adds to
Neon: neon-control, and the analytics lake, with its image.

`npm run test:e2e` is the end-to-end run: it packs the command line's package
and installs it, builds the images, makes a cluster with `alasio init` whose
nodes pull them from a registry of the run's own, starts alasio with `alasio up`, drives it through a Telegram stand-in,
puts its Neon through crashes, and removes it all with `alasio uninstall
--purge`. It needs Docker, and much of its disk; `ALASIO_E2E_AGENTS` gives the
cluster agent nodes beside its server.

The GitHub Actions workflows in `.github/workflows/` run on demand: `ci.yml`
the tests, a check of the package npm packs, and the end-to-end run, on one
node and on three; `release.yml`, from a version tag, the release, which
publishes the images to GitHub's container registry and the command line,
pinned to them by digest, to npm.
