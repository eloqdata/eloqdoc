#!/usr/bin/env bash
set -euo pipefail

# Apply local build fixes to submodules that are required for the EloqDoc + TiKV
# CentOS 7 release builder. The fixes live in the parent repository because this
# branch is published to the pingkai EloqDoc repository, while the upstream
# submodule repositories may not be writable from this workspace.

repo="${1:-src/mongo/db/modules/eloq/data_substrate}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
patch_file="$script_dir/patches/tikv-centos7-data-substrate.patch"

if [[ ! -d "$repo/.git" && ! -f "$repo/.git" ]]; then
  echo "ERROR: submodule path not found or not initialized: $repo" >&2
  echo "Run: git submodule update --init --recursive" >&2
  exit 1
fi

if git -C "$repo" apply --unidiff-zero --reverse --check "$patch_file" >/dev/null 2>&1; then
  echo "TiKV data_substrate build patch already applied."
  exit 0
fi

git -C "$repo" apply --unidiff-zero --check "$patch_file"
git -C "$repo" apply --unidiff-zero "$patch_file"
echo "Applied TiKV data_substrate build patch."
