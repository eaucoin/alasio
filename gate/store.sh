#!/usr/bin/env bash
# The bundled object store, as alasio deploys it (gate/store.ts), on the cluster kubectl
# reaches, and the gate's bucket made in it, as the stack's setup makes alasio's.
#
#   gate/store.sh <bucket>
set -euo pipefail

bucket=$1
node "$(dirname "$0")/store.ts" "$bucket" | kubectl apply -f -
kubectl -n alasio rollout status statefulset/alasio-seaweedfs --timeout=600s
printf 's3.bucket.create -name %s\n' "$bucket" |
  kubectl -n alasio exec -i alasio-seaweedfs-0 -c seaweedfs -- weed shell -master=127.0.0.1:9333 -filer=127.0.0.1:8888
printf 's3.bucket.list\n' |
  kubectl -n alasio exec -i alasio-seaweedfs-0 -c seaweedfs -- weed shell -master=127.0.0.1:9333 -filer=127.0.0.1:8888 | grep -qw "$bucket"
