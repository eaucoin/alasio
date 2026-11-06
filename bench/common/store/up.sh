#!/usr/bin/env bash
# The runner systems' store (DESIGN.md section 4): SeaweedFS 4.48, the release alasio 10.0.0
# bundles, as one `weed server` with its S3 gateway on :8333, its data in $1, one bucket
# `snapshots`, its masters' settings alasio's (a volume of 1 GiB, compacted once a tenth of it is
# deleted; a harness that deletes a run's objects may ask a vacuum between runs, untimed:
# GET :9333/vol/vacuum?garbageThreshold=0.01); and toxiproxy v2.13.0, its API on :8474, which
# shape.ts puts a profile on.
# toxiproxy listens on :28333, as SeaweedFS takes its S3 port plus 10000 for the gateway's gRPC.
# Writes $1/store.env: the variables s3.ts and probe.ts read, the endpoint the profile $2's
# (the store itself for local, toxiproxy's :28333 otherwise).
#
#   bench/common/store/up.sh <data dir> <profile>
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
data=$1
profile=$2
SEAWEEDFS=chrislusf/seaweedfs:4.48@sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d
TOXIPROXY=ghcr.io/shopify/toxiproxy:2.13.0

mkdir -p "$data/seaweedfs"
key=$(openssl rand -hex 16)
secret=$(openssl rand -hex 32)
cat >"$data/s3.json" <<EOF
{"identities": [{"name": "bench", "credentials": [{"accessKey": "$key", "secretKey": "$secret"}], "actions": ["Admin", "Read", "Write", "List", "Tagging"]}]}
EOF
docker run --detach --name bench-seaweedfs --network host \
  --volume "$data/seaweedfs:/data" --volume "$data/s3.json:/etc/seaweedfs/s3.json:ro" \
  "$SEAWEEDFS" server -dir=/data -ip=127.0.0.1 -ip.bind=0.0.0.0 -master.volumeSizeLimitMB=1024 -master.garbageThreshold=0.1 -volume.max=0 \
  -s3 -s3.port=8333 -s3.config=/etc/seaweedfs/s3.json >/dev/null
docker run --detach --name bench-toxiproxy --network host "$TOXIPROXY" -host 0.0.0.0 -port 8474 >/dev/null

endpoint=http://127.0.0.1:8333
[ "$profile" = local ] || endpoint=http://127.0.0.1:28333
cat >"$data/store.env" <<EOF
S3_ENDPOINT=$endpoint
S3_BUCKET=snapshots
S3_REGION=us-east-1
AWS_ACCESS_KEY_ID=$key
AWS_SECRET_ACCESS_KEY=$secret
EOF

for _ in $(seq 120); do
  if curl --silent --output /dev/null http://127.0.0.1:8333 && curl --silent --output /dev/null http://127.0.0.1:8474/version; then break; fi
  sleep 1
done
node "$here/shape.ts" --profile "$profile" --upstream 127.0.0.1:8333 --listen 0.0.0.0:28333
set -a
. "$data/store.env"
set +a
# Its S3 API takes a bucket a few seconds after it answers.
for attempt in $(seq 30); do
  node "$here/s3.ts" mkbucket && break
  [ "$attempt" -lt 30 ] || exit 1
  sleep 2
done
