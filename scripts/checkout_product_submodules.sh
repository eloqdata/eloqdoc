#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

cd "${REPO_ROOT}"

git submodule sync --recursive

DATA_SUBSTRATE_DIR=src/mongo/db/modules/eloq/data_substrate
if git diff --quiet -- "${DATA_SUBSTRATE_DIR}"; then
  git submodule update --init "${DATA_SUBSTRATE_DIR}"
else
  echo "Keeping locally checked-out ${DATA_SUBSTRATE_DIR}; parent gitlink has uncommitted changes."
fi

# tx-log-protos, eloq_log_service, and raft_host_manager are now bundled
# in-tree by data_substrate. Only eloqstore and its external deps remain
# product submodules; third_party/src is built through the cached workspace.
git -C "${DATA_SUBSTRATE_DIR}" submodule sync --recursive
git -C "${DATA_SUBSTRATE_DIR}" submodule update --init \
  store_handler/eloq_data_store_service/eloqstore

ELOQSTORE_DIR=${DATA_SUBSTRATE_DIR}/store_handler/eloq_data_store_service/eloqstore
if [ -f "${ELOQSTORE_DIR}/.gitmodules" ]; then
  git -C "${ELOQSTORE_DIR}" submodule sync --recursive
  git -C "${ELOQSTORE_DIR}" submodule update --init \
    external/concurrentqueue \
    external/inih \
    external/abseil
fi
