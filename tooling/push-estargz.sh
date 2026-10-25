#!/usr/bin/env bash
# Builds the sessions' agent image, alasio-agent, the one of alasio's own pushed so (the others
# are pushed whole, tooling/push-whole.sh), with buildx and pushes it as eStargz, every layer,
# in an OCI manifest: the nodes alasio makes pull it lazily, a layer mounted as it is read
# (cli/src/cluster/stargz.ts), as it is 2.1 GiB, 98 s to pull whole on a runner, and a
# session's first pod waits on it; any other runtime pulls it whole, as gzip. Needs a buildx
# builder of the docker-container driver in use, which can push such an image; with METADATA
# set, buildx's metadata goes there, the digest pushed in it.
#
#   tooling/push-estargz.sh <reference> <context> [docker buildx build arguments...]
set -euo pipefail

reference=$1
context=$2
shift 2
docker buildx build \
  --output "type=image,name=$reference,push=true,oci-mediatypes=true,compression=estargz,force-compression=true" \
  --provenance=false \
  ${METADATA:+--metadata-file "$METADATA"} \
  "$@" "$context"
