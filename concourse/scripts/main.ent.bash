#!/bin/bash
set -exo pipefail

source "$(dirname "$0")/common.sh"

CWDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ls
export WORKSPACE=$PWD
export CASS_HOST=$CASS_HOST

cd $WORKSPACE
whoami
pwd
ls
current_user=$(whoami)
sudo chown -R $current_user $PWD

# make coredump dir writable.
if [ ! -d "/var/crash" ]; then sudo mkdir -p /var/crash; fi
sudo chmod 777 /var/crash

sudo chown -R $current_user /home/$current_user/workspace
cd /home/$current_user/workspace
ln -s $WORKSPACE/py_tpcc_src py-tpcc
ln -s $WORKSPACE/eloqdoc_src mongo
cd mongo
git submodule sync
git submodule update --init --recursive

cd src/mongo/db/modules/eloq
ln -s $WORKSPACE/eloq_logservice_src eloq_log_service

pushd tx_service
ln -s $WORKSPACE/raft_host_manager_src raft_host_manager
popd

cd /home/$current_user/workspace/mongo

# Generate unique bucket names for main test
BUCKET_NAME="main-test"
BUCKET_PREFIX="rocksdb-cloud-"
DATA_DIR="/home/eloq/workspace/mongo/install/data"

compile_and_install_ent
cleanup_all "$DATA_DIR" "$BUCKET_NAME" "$BUCKET_PREFIX"
launch_mongod "$BUCKET_NAME" "$BUCKET_PREFIX"
try_connect
run_jstests
shutdown_mongod
cleanup_all "$DATA_DIR" "$BUCKET_NAME" "$BUCKET_PREFIX"
launch_mongod "$BUCKET_NAME" "$BUCKET_PREFIX"
try_connect
run_tpcc
cleanup_all "$DATA_DIR" "$BUCKET_NAME" "$BUCKET_PREFIX"
