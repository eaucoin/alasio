#!/usr/bin/env bash
# Builds one of alasio's own images with buildx and pushes it whole, every layer gzip, in an
# OCI manifest: no eStargz, so the nodes alasio makes pull it whole, each layer unpacked to
# overlayfs, and nothing of it is read through stargz-snapshotter's FUSE as it runs, nor
# depends on its FUSE manager's living: each of alasio's own images is pushed so, but the
# sessions' agent image (tooling/push-estargz.sh). alasio's start, which a press waits on,
# reads thousands of its modules, each a round trip of FUSE's when its image is pulled lazily;
# and a container left on its lazily pulled layers once its FUSE manager is killed runs on a
# rootfs gone, which nothing starts again where it reads none of it then. Whole, each pulls in
# seconds a first time on a node (on a runner: the lake's 222 MiB in 7.3 s, the log's 240 MiB
# in 5.7 s, the collector's 37 MiB in 2.4 s, grafana's 500 MiB in 15.7 s, the webhook's
# 56 MiB in 4.0 s), and those that wake from none are held on their nodes
# (cli/src/manifests/image-holder.ts). Needs a buildx builder of the docker-container driver
# in use; with METADATA set, buildx's metadata goes there, the digest pushed in it.
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
