#!/usr/bin/env bash
# Builds one of alasio's own images with buildx and pushes it whole, every layer gzip, in an
# OCI manifest: no eStargz, so the nodes alasio makes pull it whole, each layer unpacked to
# overlayfs, and nothing of it is read through stargz-snapshotter's FUSE as it runs. alasio's
# own image is pushed so (tooling/push-estargz.sh says what the others are pushed as): its
# start, which a press waits on, reads thousands of its modules, each a round trip of FUSE's
# when the image is pulled lazily. Needs a buildx builder of the docker-container driver in use;
# with METADATA set, buildx's metadata goes there, the digest pushed in it.
#
#   tooling/push-whole.sh <reference> <context> [docker buildx build arguments...]
set -euo pipefail

reference=$1
context=$2
shift 2
docker buildx build \
  --output "type=image,name=$reference,push=true,oci-mediatypes=true,compression=gzip" \
  --provenance=false \
  ${METADATA:+--metadata-file "$METADATA"} \
  "$@" "$context"
