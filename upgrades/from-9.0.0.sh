#!/usr/bin/env bash
# An installation of alasio 9.0.0 upgraded to this release, with what the release needs done
# by hand around its upgrade: what the box's cut-over runs, and what the end-to-end run's
# upgrade shard proves, on an installation of 9.0.0 it put data in (test/e2e/alasio.test.ts).
# Run as root on the host, kubectl reaching the cluster (KUBECONFIG=/etc/rancher/k3s/k3s.yaml
# for k3s on this machine). Everything is recorded in OUT, which is to be on a disk that
# outlives a restart of the machine, as /tmp may not.
#
#   UPGRADE=<command> UPGRADE_CHECK=<command> SETUP_POD=<command> SETUP_JOB=<command> CONFIG=<file> OUT=<dir> [FAIL_AT=<point>] upgrades/from-9.0.0.sh
#
# CONFIG is the configuration's file, the owner's alasio/config.json, which UPGRADE upgrades
# by and the steps below read, and change where they say.
#
# UPGRADE runs the release's upgrade as the configuration's owner, which replaces alasio,
# ending the turn it runs, with this release's: started at its count where that is fixed, or,
# where KEDA scales it, once an update, event or bound harness wakes it. UPGRADE_CHECK proves,
# before the upgrade, that it can, and that the release accepts the installation as it is. On
# a machine whose node is on the owner's PATH alone, as sudo's secure_path drops it:
#
#   as='sudo -u <owner> env HOME=/home/<owner> PATH=<node bin>:/usr/local/bin:/usr/bin:/bin'
#   UPGRADE="$as npx --yes alasio@<v> upgrade --timeout 30m"
#   UPGRADE_CHECK="$as npx --yes alasio@<v> upgrade --check && sudo -u <owner> sudo -n true"
#   SETUP_POD="$as npx --yes alasio@<v> upgrade --setup-pod"
#   SETUP_JOB="$as npx --yes alasio@<v> upgrade --setup-job"
#
# SETUP_POD prints, changing nothing, the release's pod that makes alasio's state alone (its
# `alasio setup`), which the cut-over runs before the upgrade, as its sessions' conversations
# say. SETUP_JOB prints, changing nothing, the pod of the release's stack setup, which makes what
# the stack's secrets lack, which the cut-over runs before the point of no return, as the
# release's setup pod reads a secret 9.0.0's never made. The last of UPGRADE_CHECK proving that the owner's sudo asks for no password, which the upgrade does what it
# does as root through. FAIL_AT=before-the-point-of-no-return fails the run there, once every
# step before the upgrade has run; FAIL_AT=past-the-point-of-no-return past it, before the
# upgrade; and FAIL_AT=after-the-upgrade once the upgrade is through, before what follows it:
# as the upgrade shard rehearses a failure at each.
#
# It upgrades main alone, as alasio upgrade does, which refuses while a branch environment
# 9.0.0 made exists: it refuses then too, before it changes anything. Delete each first
# (alasio branch delete <name>), and make it again once upgraded.
#
# The changes this release is made of add their steps to it, with what those steps need:
# before the point of no return, past it, before the upgrade, and once the upgrade is
# through, after it. Each change's steps are a section of their own below, between its BEGIN
# and END lines, which the flow at the end of the script calls. None changes, before the
# point of no return, what a run that fails there would have to make again: a step that does
# brings what makes it again, for the flow to run as such a run ends.
#
# Run again, it goes on from where it stopped: a run that failed before the point of no
# return starts over. Once past it (OUT/point-of-no-return), a run again goes on from there,
# and once the upgrade went through (OUT/upgraded), with what follows it alone.
set -euo pipefail
umask 077
: "${UPGRADE:?the upgrade of the release, run as the owner of the configuration}" "${UPGRADE_CHECK:?a command proving UPGRADE can run}" "${SETUP_POD:?a command printing the release's pod that makes alasio's state}" "${SETUP_JOB:?a command printing the pod of the release that makes the secrets of the stack}" "${CONFIG:?the file of the configuration}" "${OUT:?where to record}"
mkdir -p "$OUT"
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$OUT/events.log" >&2; }
fail() {
  log "FAILED: $*"
  exit 1
}
# Whether UPGRADE_CHECK passes now, what it said in OUT/upgrade-check.log.
accepted() { bash -c "$UPGRADE_CHECK" >"$OUT/upgrade-check.log" 2>&1; }
# Where the node's filesystem is: the machine's own, but in the cut-over's unit test.
NODE_ROOT=${NODE_ROOT:-}

# BEGIN the sandbox image: its content whole.
#
# 9.0.0 held the pods' sandbox image unpinned, under the name k3s pins it by, which is not the
# one containerd's CRI pulls it by: a prune of the node's images deleted it while pods ran on
# its snapshot, and the next pod pulled it again onto that snapshot, which containerd fetches
# no layer of, so that it is recorded without its layer's content. This release pins it
# (cli/src/cluster/k3s.ts SANDBOX_IMAGE), which nothing deletes then, but a node whose record
# of it lacks its content keeps it so, and makes no pod once that snapshot is gone. So, before
# anything changes, what containerd lacks of the sandbox image is fetched, by the digest its
# name names, as it was pulled. Of it alone: an image stargz-snapshotter's daemon pulls lazily
# is recorded without its layers' content by design, and runs on the daemon's snapshots.

# k3s's containerd's command line, on the CRI's images.
images_ctr() { "$NODE_ROOT/usr/local/bin/k3s" ctr --namespace k8s.io "$@"; }
CONTAINERD_CONFIG=$NODE_ROOT/var/lib/rancher/k3s/agent/etc/containerd
# The sandbox image's digest, as containerd records it, and whether it holds all its content
# for the node's platform, "complete" if it does: `$1`'s line of containerd's check of it.
sandbox_image_checked() { images_ctr images check "name==$1" | awk -v name="$1" '$1 == name { print $3, $4 }'; }
# Before the point of no return: what containerd lacks of the sandbox image fetched.
sandbox_image_before() {
  local pinned name repository checked digest
  pinned=$(sed -n 's/^ *sandbox = "\(.*\)"$/\1/p' "$CONTAINERD_CONFIG/config.toml") && [ -n "$pinned" ] ||
    fail "containerd's configuration names no sandbox image ($CONTAINERD_CONFIG/config.toml)"
  # Its name as containerd records it, Docker's references normalized: docker.io where it names
  # no registry, and library/ for one of docker.io's own.
  case ${pinned%%/*} in
    "$pinned") name=docker.io/library/$pinned ;;
    *.* | *:* | localhost) name=$pinned ;;
    *) name=docker.io/$pinned ;;
  esac
  checked=$(sandbox_image_checked "$name") || fail "containerd's record of the sandbox image $name could not be checked"
  # None recorded, the CRI pulls it whole.
  [ -n "$checked" ] && [ "${checked#* }" != complete ] || return 0
  digest=${checked%% *}
  # Its tag, where it has one: what follows a colon in its last part.
  repository=${name%@*}
  [[ ${repository##*/} != *:* ]] || repository=${repository%:*}
  log "fetching what containerd lacks of the sandbox image $name, recorded without it, as 9.0.0 let a prune delete it while pods ran on it"
  images_ctr content fetch --hosts-dir "$CONTAINERD_CONFIG/certs.d" "$repository@$digest" >/dev/null ||
    fail "what containerd lacks of the sandbox image $name could not be fetched from its registry"
  checked=$(sandbox_image_checked "$name") || fail "containerd's record of the sandbox image $name could not be checked"
  [ "${checked#* }" = complete ] || fail "containerd lacks content of the sandbox image $name still, fetched"
}
# END the sandbox image.

# BEGIN alasio stopped.
#
# This release's sandbox gateway refuses the caller tokens 9.0.0's alasio mints, good for six
# hours, and answers a bind with 200 and the binding, where 9.0.0's alasio takes 204 alone;
# and the upgrade rolls the two apart: a turn 9.0.0's alasio ran as the gateway rolled would
# lose its workspace's bayma for as long as that alasio ran on. So, past the point of no
# return, before the upgrade, alasio is waited for until it runs no turn, as its busy label
# says, then stopped, so none starts until the upgrade starts this release's. A turn that
# starts between the last look and the stop ends with alasio, as any restart of alasio ends
# one. Once, as OUT/alasio-stopped says it was, the pod gone: a run again after the upgrade
# failed finds alasio the release's, or stopped still. Each wait is bounded, by
# TURN_WAIT_SECONDS (30 min) and STOP_WAIT_SECONDS (10 min), looking every POLL_SECONDS (5).

NS=alasio
TURN_WAIT_SECONDS=${TURN_WAIT_SECONDS:-1800}
STOP_WAIT_SECONDS=${STOP_WAIT_SECONDS:-600}
POLL_SECONDS=${POLL_SECONDS:-5}
k() { kubectl -n "$NS" "$@"; }
# Whether alasio's namespace has no pod of the selector `$1` within `$2` seconds, looking every
# POLL_SECONDS; the run failing where its pods cannot be listed, as nothing can be told then.
gone() {
  local waited=0 listed
  while :; do
    listed=$(k get pod -l "$1" -o name) || fail "the pods of $1 in $NS could not be listed"
    [ -n "$listed" ] || return 0
    [ "$waited" -lt "$2" ] || return 1
    sleep "$POLL_SECONDS"
    waited=$((waited + POLL_SECONDS))
  done
}
alasio_stopped_past() {
  [ ! -e "$OUT/alasio-stopped" ] || return 0
  log "waiting for alasio to run no turn, then stopping it, so none runs on as the sandbox gateway rolls"
  gone app.kubernetes.io/component=alasio,alasio.dev/busy=true "$TURN_WAIT_SECONDS" ||
    fail "alasio ran a turn on for ${TURN_WAIT_SECONDS} s: let it end, and run the cut-over again"
  k scale deployment alasio --replicas=0 >/dev/null || fail "alasio could not be stopped"
  gone app.kubernetes.io/component=alasio "$STOP_WAIT_SECONDS" || fail "alasio did not stop within ${STOP_WAIT_SECONDS} s"
  date -u +%FT%TZ >"$OUT/alasio-stopped"
  log "alasio stopped, to be started again by the upgrade"
}
# END alasio stopped.

# BEGIN the sessions' conversations.
#
# This release holds each harness pool's pod to its own conversation's rows, a transcript's or a
# rollout's among them by the sessions its conversation has (state.conversation_sessions), which
# its schema makes, and fills as rows naming sessions are written from then on. 9.0.0's rows name
# sessions it never recorded so, and a pod resuming one of them would find nothing of it, a Codex
# thread's lineage among it, as the prompts queued through the upgrade run the moment the
# release's alasio serves. So, past the point of no return, once alasio is stopped, before the
# upgrade, the release's own setup (`alasio setup`, the pod SETUP_POD prints, which checked before
# the point of no return that it could) makes the release's state as its alasio would as it
# starts, which that alasio then finds made; and a pod of the same spec gives each conversation
# the sessions its rows name. Once, as OUT/sessions-claimed says it was; each pod's last lines
# kept in OUT, waited for SETUP_WAIT_SECONDS (10 min) at most, and deleted after.

SETUP_WAIT_SECONDS=${SETUP_WAIT_SECONDS:-600}
SETUP_POD_NAME=alasio-setup
# Every session a conversation's row names, the conversation's: the rows this release's schema
# records them from as they are written (src/persistence/schema.ts SESSION_ROWS); and of each, the
# Codex threads its history starts in, transitively, as its rollouts kept in Neon say them, which
# 9.0.0's alasio wrote, so a thread resumed has its lineage, as this release gives it them as a row
# names a thread (state.claim_lineage). How many sessions the conversations have then. And
# 9.0.0's tally of the albums whose files it failed to write, which this release keeps none of,
# as a turn's host writes a prompt's files, and its restart events, which nothing of this release
# reads, as it attaches again to a turn 9.0.0's stop left, by its turn's row, which names its
# session: their tables dropped. And first, the column of the binding a turn runs on, which a
# binding's loss ends that turn alone by (src/persistence/schema.ts turns.binding), which 9.0.0's
# turns have not. And 9.0.0's presses kept as their controls ran on past their updates, which
# this release keeps with its commands as controls (state.control_jobs): moved there, as presses,
# their table dropped, so one 9.0.0's stop let go of runs again.

SESSIONS_CLAIMED="
alter table state.turns add column if not exists binding text;
insert into state.conversation_sessions (session_id, conversation_id)
select named.session_id, named.conversation_id from (
  select codex_session_id as session_id, id as conversation_id from state.conversations
  union select claude_session_id, id from state.conversations
  union select session_id, conversation_id from state.workspace_sessions
  union select session_id, conversation_id from state.messages
  union select session_id, conversation_id from state.turns
  union select session_id, thread_key from state.responses
  union select upstream_session_id, conversation_id from state.prompt_jobs
  union select expected_session_id, conversation_id from state.callback_actions
) named join state.conversations c on c.id = named.conversation_id
where named.session_id is not null
on conflict do nothing;
select count(state.claim_lineage(session_id, conversation_id)) from (select session_id, conversation_id from state.conversation_sessions) claimed;
drop table if exists state.media_group_failures;
drop table if exists state.restart_events;
do \$\$\$\$ begin
  if to_regclass('state.press_jobs') is not null then
    insert into state.control_jobs (id, conversation_id, control, holder, held_until, answer, created_at)
    select id, conversation_id, jsonb_build_object('kind', 'press', 'callbackQuery', callback_query, 'action', action), holder, held_until, answer, created_at
    from state.press_jobs on conflict do nothing;
    drop table state.press_jobs;
  end if;
end \$\$\$\$;
select count(*) from state.conversation_sessions"
# The release's setup pod, as SETUP_POD printed it, before the point of no return: it changes nothing.
sessions_claimed_before() {
  bash -c "$SETUP_POD" >"$OUT/setup-pod.json" 2>"$OUT/setup-pod.log" || fail "SETUP_POD failed, so the release's setup could not run (setup-pod.log): $(tail -n 3 "$OUT/setup-pod.log")"
  jq -e '.kind == "Pod" and (.spec.containers | length) == 1' "$OUT/setup-pod.json" >/dev/null || fail "SETUP_POD printed no pod of one container (setup-pod.json)"
}
# Runs the setup pod, as named `$1`, with its container's command `$2` (a JSON array) and, where
# given, its SQL `$3`, to its end (run_pod).
one_off() {
  local name=$1
  jq --arg name "$name" --argjson command "$2" --arg sql "${3:-}" '
    .metadata.name = $name
    | .spec.containers[0].command = $command
    | if $sql == "" then . else .spec.containers[0].env += [{ name: "ALASIO_CUTOVER_SQL", value: $sql }] end
  ' "$OUT/setup-pod.json" >"$OUT/$name.json" || fail "the pod $name could not be made of setup-pod.json"
  run_pod "$name"
}
# Runs the pod OUT/`$1`.json names `$1` to its end, waited for SETUP_WAIT_SECONDS at most: its
# last lines in OUT/<name>.log, the pod deleted after; failing where it did not succeed.
run_pod() {
  local name=$1 phase waited=0
  k delete pod "$name" --ignore-not-found --wait >/dev/null || fail "a pod $name left before could not be deleted"
  k apply -f "$OUT/$name.json" >/dev/null || fail "the pod $name could not be made"
  while :; do
    phase=$(k get pod "$name" -o jsonpath='{.status.phase}') || fail "the pod $name could not be read"
    [ "$phase" != Succeeded ] && [ "$phase" != Failed ] || break
    [ "$waited" -lt "$SETUP_WAIT_SECONDS" ] || fail "the pod $name had not ended within ${SETUP_WAIT_SECONDS} s"
    sleep "$POLL_SECONDS"
    waited=$((waited + POLL_SECONDS))
  done
  k logs "$name" >"$OUT/$name.log" 2>&1 || true
  k delete pod "$name" --wait >/dev/null || fail "the pod $name could not be deleted"
  [ "$phase" = Succeeded ] || fail "the pod $name failed ($name.log): $(tail -n 3 "$OUT/$name.log")"
}
# What runs SQL on alasio's database, as alasio reaches it, from its pod: ALASIO_CUTOVER_SQL's
# statements, the rows of the last printed a line each, tab-separated. Its pod's address is let
# in by the network policies of what it reaches only as they next sync, just after it starts, so
# its first connections may be refused, as the stack's pods' are (cli/src/manifests/common.ts
# oncePolicyApplies): it connects as alasio's setup does (src/neon/connect.ts), trying again
# every 5 s, for 5 minutes, saying why, where its connection failed (one of Node's errors of a
# connection, as ECONNREFUSED, ECONNRESET or EPIPE, or pg's of one ended) or the database said it
# takes none yet (SQLSTATE classes 08, a connection's, and 57P, as 57P03 of a compute starting);
# any other the database answered, as of a password it does not take, fails at once, as no wait
# mends it. The SQL runs once, connected.
ON_DATABASE='import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
const pg = createRequire("/opt/alasio/package.json")("pg");
const CONNECTION_ERRORS = new Set(["ECONNREFUSED", "ECONNRESET", "EPIPE", "ETIMEDOUT", "ECONNABORTED", "EHOSTUNREACH", "ENETUNREACH", "ENOTFOUND", "EAI_AGAIN"]);
const waitedOut = (error) =>
  error instanceof pg.DatabaseError
    ? /^(08|57P)/.test(error.code ?? "")
    : CONNECTION_ERRORS.has(error.code) || (error.code === undefined && /^Connection terminated/.test(error.message));
const connected = async (url) => {
  for (const until = Date.now() + 300000; ;) {
    const client = new pg.Client({ connectionString: url, connectionTimeoutMillis: 120000 });
    try {
      await client.connect();
      return client;
    } catch (error) {
      await client.end().catch(() => {});
      if (!waitedOut(error) || Date.now() >= until) throw error;
      console.error(`waiting for the database: ${error.code ?? "no code"}: ${error.message}`);
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
};
try {
  const url = process.env.ALASIO_DATABASE_URL ?? readFileSync(process.env.ALASIO_DATABASE_URL_FILE, "utf8");
  const client = await connected(url.trim());
  try {
    for (const { rows } of [await client.query({ text: process.env.ALASIO_CUTOVER_SQL, rowMode: "array" })].flat()) {
      for (const row of rows ?? []) console.log(row.map((value) => String(value ?? "")).join("\t"));
    }
  } finally {
    await client.end();
  }
} catch (error) {
  // Said on one line, the last of its log the cut-over says: what failed, reaching the database or running the SQL, with its code.
  console.error(`it failed: ${error.code ?? "no code"}: ${error.message}${error.where ? ` (${error.where})` : ""}`);
  process.exitCode = 1;
}'
# Past the point of no return, alasio stopped, before the upgrade: the release's state made, and the sessions given, once.
sessions_claimed_past() {
  [ ! -e "$OUT/sessions-claimed" ] || return 0
  log "making this release's state with its own setup, as its alasio would as it starts (alasio-setup.log)"
  one_off "$SETUP_POD_NAME" '["node", "/opt/alasio/src/setup/main.ts"]'
  log "giving each conversation the sessions its rows name (alasio-sessions-claimed.log)"
  one_off alasio-sessions-claimed "$(jq -cn --arg script "$ON_DATABASE" '["node", "--input-type=module", "-e", $script]')" "$SESSIONS_CLAIMED"
  tail -n 1 "$OUT/alasio-sessions-claimed.log" >"$OUT/sessions-claimed"
  log "the conversations have $(cat "$OUT/sessions-claimed") sessions, of their rows and the threads their histories start in"
}
# END the sessions' conversations.

# BEGIN the stack's secrets.
#
# This release's alasio reads a secret of the stack's that 9.0.0's setup never made, the Codex
# login relay's role's verifier, in the database Secret alasio mounts, beside the relay's own
# Secrets: the release's stack setup (neon/control/kube-setup.ts) makes them, which the upgrade
# runs as applying alasio does, before anything of the release starts. But the release's setup
# pod, which mounts that Secret, runs before the upgrade, and would wait for the key to its
# bound. So, before the point of no return, once the upgrade is checked, the release's stack
# setup, the pod SETUP_JOB prints (its neon-setup Job's), runs once to its end, as the upgrade
# runs it again after: it makes what the stack's secrets lack, and renders each service's from
# them again, changing none of 9.0.0's, as it keeps every secret made and rotates none. A run
# that fails before the point of no return leaves nothing to make again, and runs it again. Its
# last lines in OUT/alasio-stack-setup.log, waited for SETUP_WAIT_SECONDS at most.

STACK_SETUP_POD_NAME=alasio-stack-setup
# Before the point of no return: the release's stack setup run, the secrets it lacks made.
stack_setup_before() {
  bash -c "$SETUP_JOB" >"$OUT/setup-job.json" 2>"$OUT/setup-job.log" || fail "SETUP_JOB failed, so the release's stack setup could not run (setup-job.log): $(tail -n 3 "$OUT/setup-job.log")"
  jq -e '.kind == "Pod" and (.spec.containers | length) == 1' "$OUT/setup-job.json" >/dev/null || fail "SETUP_JOB printed no pod of one container (setup-job.json)"
  jq --arg name "$STACK_SETUP_POD_NAME" '.metadata.name = $name' "$OUT/setup-job.json" >"$OUT/$STACK_SETUP_POD_NAME.json" || fail "the pod $STACK_SETUP_POD_NAME could not be made of setup-job.json"
  log "making what the stack's secrets lack with this release's stack setup, changing none of those there ($STACK_SETUP_POD_NAME.log)"
  run_pod "$STACK_SETUP_POD_NAME"
}
# END the stack's secrets.


# BEGIN telegram partitions.
#
# This release reads an environment's Telegram topic in a partition for each alasio it may run
# (TELEGRAM_PARTITIONS, 6), where 9.0.0's has one. The log's access applier gives a topic read
# in order more only while its group has no member and none of it left to read
# (src/log/access.ts), and the upgrade starts this release's alasio as it rolls the log, so the
# applier finds alasio reading it. So, once the upgrade is through, alasio is stopped again,
# once it runs no turn, as above, the applier, which looks again every few seconds, given
# PARTITIONS_WAIT_SECONDS (2 min) to grow the topic, and alasio started again. Where KEDA
# scales alasio, as it does a range (install.alasio.scaling, from none to three by default),
# a scale of alasio's Deployment would be undone at KEDA's next look, as any update, event or
# bound harness wakes it: so its ScaledObject is paused at none instead, and the pause lifted
# after, KEDA starting alasio again as those say. A fixed count is scaled to none, and back to
# that count after. What was stopped is recorded in OUT/partitions-stopped until alasio is
# started again, so a run again after a failure in between starts it. Where an update came
# while alasio was stopped, and so is unread, the topic is left as it is, said: each alasio
# reads what partitions it is given, the one there is serving all, and the next stop of alasio
# with nothing unread grows it.

TELEGRAM_TOPIC=telegram.main
TELEGRAM_PARTITIONS=6
PARTITIONS_WAIT_SECONDS=${PARTITIONS_WAIT_SECONDS:-120}
PAUSED=autoscaling.keda.sh/paused-replicas
# How many partitions the topic `$1` has, as the log's access applier reads them.
partitions() {
  k exec alasio-log-0 -c access -- node /opt/alasio/src/log/access.ts partitions "$1"
}
# alasio started again, as OUT/partitions-stopped says it was stopped: its ScaledObject's
# pause lifted, or its Deployment scaled back to its count.
alasio_started_again() {
  local stopped
  stopped=$(cat "$OUT/partitions-stopped")
  if [ "$stopped" = keda ]; then
    k annotate scaledobject alasio "$PAUSED-" >/dev/null || fail "the pause of alasio's ScaledObject could not be lifted: remove its annotation $PAUSED"
    log "alasio's ScaledObject is paused no more: KEDA starts alasio as its updates, events and bound harnesses ask"
  else
    k scale deployment alasio --replicas="$stopped" >/dev/null || fail "alasio could not be started again"
    k rollout status deployment/alasio --timeout="${STOP_WAIT_SECONDS}s" >/dev/null || fail "alasio did not start again within ${STOP_WAIT_SECONDS} s"
  fi
  rm -f "$OUT/partitions-stopped"
}
telegram_partitions_after() {
  local has waited=0 scaler replicas
  has=$(partitions "$TELEGRAM_TOPIC") || fail "the partitions of $TELEGRAM_TOPIC could not be read"
  if [ "$has" -ge "$TELEGRAM_PARTITIONS" ]; then
    # A run before stopped alasio, and failed before it started it again.
    [ ! -e "$OUT/partitions-stopped" ] || alasio_started_again
    return 0
  fi
  log "waiting for alasio to run no turn, then stopping it again, for the log to give $TELEGRAM_TOPIC its $TELEGRAM_PARTITIONS partitions"
  gone app.kubernetes.io/component=alasio,alasio.dev/busy=true "$TURN_WAIT_SECONDS" ||
    fail "alasio ran a turn on for ${TURN_WAIT_SECONDS} s: let it end, and run the cut-over again"
  if [ ! -e "$OUT/partitions-stopped" ]; then
    scaler=$(k get scaledobject alasio --ignore-not-found -o name) || fail "whether KEDA scales alasio could not be read"
    if [ -n "$scaler" ]; then
      echo keda >"$OUT/partitions-stopped"
    else
      replicas=$(k get deployment alasio -o jsonpath='{.spec.replicas}') || fail "alasio's count could not be read"
      echo "$replicas" >"$OUT/partitions-stopped"
    fi
  fi
  if [ "$(cat "$OUT/partitions-stopped")" = keda ]; then
    k annotate --overwrite scaledobject alasio "$PAUSED=0" >/dev/null || fail "alasio's ScaledObject could not be paused"
  else
    k scale deployment alasio --replicas=0 >/dev/null || fail "alasio could not be stopped"
  fi
  gone app.kubernetes.io/component=alasio "$STOP_WAIT_SECONDS" || fail "alasio did not stop within ${STOP_WAIT_SECONDS} s"
  while has=$(partitions "$TELEGRAM_TOPIC") && [ "$has" -lt "$TELEGRAM_PARTITIONS" ] && [ "$waited" -lt "$PARTITIONS_WAIT_SECONDS" ]; do
    sleep "$POLL_SECONDS"
    waited=$((waited + POLL_SECONDS))
  done
  [ -n "$has" ] || fail "the partitions of $TELEGRAM_TOPIC could not be read"
  if [ "$has" -lt "$TELEGRAM_PARTITIONS" ]; then
    log "$TELEGRAM_TOPIC is left as it is, of $has of its $TELEGRAM_PARTITIONS partitions, as an update came for it while alasio was stopped: the next stop of alasio with nothing unread grows it"
  else
    log "$TELEGRAM_TOPIC has its $has partitions"
  fi
  alasio_started_again
}
# END telegram partitions.

for tool in jq kubectl; do command -v "$tool" >/dev/null || fail "$tool is not on root's PATH"; done
[ -r "$CONFIG" ] || fail "CONFIG $CONFIG cannot be read"
kubectl get --raw=/readyz >/dev/null 2>&1 || fail "kubectl does not reach the cluster: give it k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"

if [ ! -e "$OUT/point-of-no-return" ]; then
  log "cutting over from 9.0.0, recording in $OUT"
  # What UPGRADE_CHECK refuses that needs no release to read, read before anything changes.
  log "checking that no branch environment exists, as the upgrade refuses one 9.0.0 made"
  # Each by its own namespace, alasio-branch-<name>, which its sessions' is labelled as too.
  namespaces=$(kubectl get namespace -l alasio.dev/branch -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.alasio\.dev/branch}{"\n"}{end}') ||
    fail "the cluster's namespaces could not be listed, so whether a branch environment exists cannot be told"
  branches=$(printf '%s\n' "$namespaces" | awk -F '\t' 'NF == 2 && $1 == "alasio-branch-" $2 { print $2 }')
  [ -z "$branches" ] ||
    fail "alasio upgrades main alone, and the branch environments 9.0.0 made would not run on what upgrading changes: delete each first ($(printf '%s\n' "$branches" | awk 'NR > 1 { printf ", " } { printf "alasio branch delete %s", $0 }')), and make them again once alasio is upgraded"
  sandbox_image_before
  sessions_claimed_before
  log "checking that the upgrade can run (upgrade-check.log)"
  accepted || fail "UPGRADE_CHECK failed, so UPGRADE could not run (upgrade-check.log): $(tail -n 3 "$OUT/upgrade-check.log")"
  stack_setup_before
  [ "${FAIL_AT:-}" != before-the-point-of-no-return ] || fail "a failure injected before the point of no return"
  date -u +%FT%TZ >"$OUT/point-of-no-return"
  log "past the point of no return: a run again goes on from here"
fi

if [ ! -e "$OUT/upgraded" ]; then
  [ "${FAIL_AT:-}" != past-the-point-of-no-return ] || fail "a failure injected past the point of no return, before the upgrade"
  alasio_stopped_past
  sessions_claimed_past
  log "upgrading, from the cut-over begun at $(cat "$OUT/point-of-no-return"): alasio restarts"
  printf '%s\n' "$UPGRADE" >"$OUT/upgrade"
  bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" ||
    fail "the upgrade failed (upgrade.log): alasio stays stopped until an upgrade gets as far as starting it; the cut-over run again goes on with it"
  telegram_partitions_after
  date -u +%FT%TZ >"$OUT/upgraded"
fi
[ "${FAIL_AT:-}" != after-the-upgrade ] || fail "a failure injected once the upgrade was through"

log "upgraded at $(cat "$OUT/upgraded")"
log "through"
