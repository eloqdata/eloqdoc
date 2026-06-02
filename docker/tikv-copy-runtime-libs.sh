#!/usr/bin/env bash
set -euo pipefail

# Copy non-system runtime libraries from the Docker build image into the EloqDoc
# install prefix so the Docker-built binaries can be launched on the host with
# LD_LIBRARY_PATH pointing at install/tikv-docker/lib. Run this script inside
# the build container from the repository root.

dest="${1:-/work/eloqdoc/install/tikv-docker/lib}"
mkdir -p "$dest"
shopt -s nullglob

patterns=(
  /usr/local/lib/libmimalloc.so*
  /usr/local/lib/libbrpc.so*
  /usr/local/lib/libbraft.so*
  /usr/local/lib/libprometheus-cpp-core.so*
  /usr/local/lib/libprometheus-cpp-pull.so*
  /usr/lib/x86_64-linux-gnu/libboost_context.so*
  /usr/lib/x86_64-linux-gnu/libprotobuf.so*
  /usr/lib/x86_64-linux-gnu/libgrpc++.so*
  /usr/lib/x86_64-linux-gnu/libgrpc.so*
  /usr/lib/x86_64-linux-gnu/libgpr.so*
  /usr/lib/x86_64-linux-gnu/libPoco*.so*
  /usr/lib/x86_64-linux-gnu/librocksdb.so*
  /usr/lib/x86_64-linux-gnu/libleveldb.so*
  /usr/lib/x86_64-linux-gnu/libabsl*.so*
  /usr/lib/x86_64-linux-gnu/libupb*.so*
  /usr/lib/x86_64-linux-gnu/libre2.so*
  /usr/lib/x86_64-linux-gnu/libaddress_sorting.so*
  /usr/lib/x86_64-linux-gnu/libcares.so*
  /usr/lib/x86_64-linux-gnu/libpcre.so.3*
  /usr/lib/x86_64-linux-gnu/libsnappy.so*
  /usr/lib/x86_64-linux-gnu/liblz4.so*
  /usr/lib/x86_64-linux-gnu/libzstd.so*
  /usr/lib/x86_64-linux-gnu/liburing.so*
)

for pattern in "${patterns[@]}"; do
  for file in $pattern; do
    cp -a "$file" "$dest"/
  done
done
