#!/usr/bin/env bash
# 03's gate: AutoMQ at its smallest documented tier on the bundled SeaweedFS, as alasio
# deploys it, capped at 03's budget, under the stack's telemetry rate, a burst and a bulk
# topic; its memory, CPU and the object churn it makes, sampled throughout; its
# produce-to-consume latency; whether a restart loses anything; and SeaweedFS's answer to
# the S3 calls AutoMQ's compaction makes (UploadPartCopy, seaweedfs#11622). Everything it
# measures goes to $OUT, and the summary to the job's.
#
# The stack's telemetry, measured on the box over 6.5 h (program/03-telemetry-log.md):
# ~10 KB/s of OTLP in ~0.24 requests/s; here one record a second on each signal's topic,
# 6, 3 and 1 KB, a consumer group reading all three.
#
#   STEADY_MINUTES=60 BURST_MINUTES=10 gate/03/run.sh
set -euo pipefail

here=$(dirname "$0")
OUT=${OUT:-$RUNNER_TEMP/gate-03}
STEADY_MINUTES=${STEADY_MINUTES:-60}
BURST_MINUTES=${BURST_MINUTES:-10}
BULK_RECORDS=${BULK_RECORDS:-200000}
mkdir -p "$OUT"

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$OUT/events.log"; }
k() { kubectl -n alasio "$@"; }
kafka() { k exec load -- "$@"; }
BOOTSTRAP=automq-0.automq.alasio.svc.cluster.local:9092

# The gate's own S3 identity, from the runner, on the store's Service.
AWS_ACCESS_KEY_ID=$(k get secret gate-s3 -o jsonpath='{.data.accessKey}' | base64 -d)
AWS_SECRET_ACCESS_KEY=$(k get secret gate-s3 -o jsonpath='{.data.secretKey}' | base64 -d)
BUCKET=$(k get secret gate-s3 -o jsonpath='{.data.bucket}' | base64 -d)
S3=http://$(k get svc alasio-seaweedfs -o jsonpath='{.spec.clusterIP}'):8333
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION=us-east-1
s3api() { aws --endpoint-url "$S3" s3api "$@"; }
seaweed_metrics() { curl -fsS "http://$(k get pod alasio-seaweedfs-0 -o jsonpath='{.status.podIP}'):9327/metrics"; }

log "AutoMQ starting"
started=$(date +%s)
k apply -f "$here/automq.yaml" -f "$here/load.yaml"
k rollout status statefulset/automq --timeout=900s
k wait --for=condition=Ready pod/load --timeout=300s
until kafka /opt/automq/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do sleep 2; done
log "AutoMQ serving after $(($(date +%s) - started)) s"

for topic in otel.spans otel.logs otel.metrics bulk latency; do
  kafka /opt/automq/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic "$topic" --partitions 1
done
seaweed_metrics >"$OUT/seaweedfs-metrics-start.txt"

# Every 30 s: the broker's cgroup (memory, anonymous memory, CPU time), the JVM's resident
# set, SeaweedFS's working set, and the bucket's objects and bytes.
sample() {
  echo "time,broker_memory_current_mib,broker_anon_mib,broker_java_rss_mib,broker_cpu_usec,seaweedfs_working_set_mib,objects,bytes" >"$OUT/samples.csv"
  while :; do
    local stat rss objects seaweed
    stat=$(k exec automq-0 -- sh -c 'cat /sys/fs/cgroup/memory.current; grep "^anon " /sys/fs/cgroup/memory.stat; grep "^usage_usec" /sys/fs/cgroup/cpu.stat' 2>/dev/null | tr '\n' ' ') || stat=""
    rss=$(k exec automq-0 -- sh -c 'for p in /proc/[0-9]*; do grep -q java "$p/cmdline" 2>/dev/null && grep VmRSS "$p/status"; done' 2>/dev/null | awk '{s+=$2} END {print int(s/1024)}') || rss=""
    objects=$(s3api list-objects-v2 --bucket "$BUCKET" --query '[length(Contents || `[]`), sum(Contents[].Size || `[0]`)]' --output text 2>/dev/null | tr '\t' ',') || objects=","
    seaweed=$(kubectl top pod -n alasio alasio-seaweedfs-0 --containers --no-headers 2>/dev/null | awk '$2 == "seaweedfs" {print $4}' | tr -d Mi) || seaweed=""
    read -r current _ anon _ cpu <<<"$stat" || true
    echo "$(date -u +%FT%TZ),$((${current:-0} / 1048576)),$((${anon:-0} / 1048576)),$rss,${cpu:-},$seaweed,$objects" >>"$OUT/samples.csv"
    sleep 30
  done
}
sample &
sampler=$!
# Whatever happens, the store's counters, both servers' logs and the summary at the end.
finish() {
  kill "$sampler" 2>/dev/null || true
  seaweed_metrics >"$OUT/seaweedfs-metrics-end.txt" || true
  k logs automq-0 >"$OUT/automq-after-restart.log" 2>/dev/null || true
  k logs alasio-seaweedfs-0 -c seaweedfs >"$OUT/seaweedfs.log" || true
  node "$here/summary.ts" "$OUT" | tee -a "$GITHUB_STEP_SUMMARY"
}
trap finish EXIT

# A consumer group reading the telemetry topics throughout, as the lake's intake would.
k exec load -- sh -c "/opt/automq/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --include 'otel\\..*' --group lake-intake >/dev/null 2>/tmp/consumer.log" &
consumer=$!

# The bulk topic first, so its stream objects are large enough by the end for compaction
# to copy parts of them server-side (UploadPartCopy) rather than rewrite them.
log "bulk: $BULK_RECORDS records of 1 KB, unthrottled"
kafka /opt/automq/kafka/bin/kafka-producer-perf-test.sh --topic bulk --num-records "$BULK_RECORDS" --record-size 1000 \
  --throughput -1 --producer-props bootstrap.servers="$BOOTSTRAP" acks=all | tail -1 | tee "$OUT/bulk-produce.txt"

produce() { # <topic> <record size> <records a second> <seconds>
  kafka /opt/automq/kafka/bin/kafka-producer-perf-test.sh --topic "$1" --num-records "$(($3 * $4))" --record-size "$2" \
    --throughput "$3" --producer-props bootstrap.servers="$BOOTSTRAP" acks=all linger.ms=100 | tail -1
}
phase() { # <name> <records a second> <minutes>
  log "$1: $2 record(s)/s on each telemetry topic for $3 min"
  local producers=()
  produce otel.metrics 6000 "$2" "$(($3 * 60))" >"$OUT/$1-metrics.txt" &
  producers+=($!)
  produce otel.spans 3000 "$2" "$(($3 * 60))" >"$OUT/$1-spans.txt" &
  producers+=($!)
  produce otel.logs 1000 "$2" "$(($3 * 60))" >"$OUT/$1-logs.txt" &
  producers+=($!)
  wait "${producers[@]}"
  cat "$OUT/$1-"*.txt
}
phase steady 1 "$STEADY_MINUTES"
phase burst 10 "$BURST_MINUTES"

log "produce-to-consume latency, 300 records of 1 KB, acks=all"
kafka /opt/automq/kafka/bin/kafka-e2e-latency.sh "$BOOTSTRAP" latency 300 all 1000 | tail -3 | tee "$OUT/latency.txt"

log "lake-intake's lag"
kafka /opt/automq/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --describe --group lake-intake | tee "$OUT/lag-before-restart.txt"
kill "$consumer" 2>/dev/null || true

count() { # <topic>: the records read back from its start
  kafka /opt/automq/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic "$1" --from-beginning \
    --timeout-ms 30000 2>&1 >/dev/null | sed -n 's/^Processed a total of \([0-9]*\) messages$/\1/p'
}
before=$(count bulk)
log "bulk read back: $before of $BULK_RECORDS"

log "broker restarted"
k logs automq-0 >"$OUT/automq-before-restart.log"
k delete pod automq-0 --wait=true
restarted=$(date +%s)
k rollout status statefulset/automq --timeout=900s
until kafka /opt/automq/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do sleep 2; done
log "AutoMQ serving again after $(($(date +%s) - restarted)) s"
after=$(count bulk)
log "bulk read back after the restart: $after of $BULK_RECORDS"
for topic in otel.spans otel.logs otel.metrics; do
  log "$topic read back after the restart: $(count "$topic")"
done

"$here/etag.sh"
