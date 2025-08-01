#!/bin/bash
set -exo pipefail

source "$(dirname "$0")/common.sh"

CWDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ls
export WORKSPACE=$PWD

cd $WORKSPACE
whoami
pwd
ls
sudo chown -R mono $PWD

# make coredump dir writable.
if [ ! -d "/var/crash" ]; then sudo mkdir -p /var/crash; fi
sudo chmod 777 /var/crash

cd $WORKSPACE/eloqdoc_pr
pr_branch_name=$(cat .git/resource/metadata.json | jq -r '.[] | select(.name=="head_name") | .value')

sudo chown -R mono /home/mono/workspace
cd /home/mono/workspace
ln -s $WORKSPACE/py_tpcc_src py-tpcc
ln -s $WORKSPACE/eloqdoc_pr mongo
cd mongo
git config remote.origin.fetch "+refs/heads/${pr_branch_name}:refs/remotes/origin/${pr_branch_name}"
git submodule sync
git submodule update --init --recursive

cd src/mongo/db/modules/eloq
ln -s $WORKSPACE/eloq_logservice_src eloq_log_service

pushd eloq_log_service
if [ -n "$pr_branch_name" ] && git ls-remote --exit-code --heads origin "$pr_branch_name" > /dev/null; then
  git fetch origin '+refs/heads/*:refs/remotes/origin/*'
  git checkout -b ${pr_branch_name} origin/${pr_branch_name}
  git submodule update --init --recursive
fi
popd

pushd tx_service
ln -s $WORKSPACE/raft_host_manager_src raft_host_manager
popd

cd /home/mono/workspace/mongo

# Generate unique bucket names for pr test
BUCKET_NAME="pr-test"
BUCKET_PREFIX="rocksdb-cloud-"

compile_and_install_ent
cleanup_all_buckets "$BUCKET_NAME" "$BUCKET_PREFIX"
launch_mongod_fast "$BUCKET_NAME" "$BUCKET_PREFIX"
try_connect
run_jstests
run_tpcc
cleanup_all_buckets "$BUCKET_NAME" "$BUCKET_PREFIX"
