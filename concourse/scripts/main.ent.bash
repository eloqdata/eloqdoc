#!/bin/bash
set -exo pipefail

export WORKSPACE=$PWD
export CASS_HOST=$CASS_HOST

export PREFIX="/home/eloq/workspace/mongo/install"

CWDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ls

cd $WORKSPACE
whoami
pwd
ls
current_user=$(whoami)
sudo chown -R $current_user $PWD

# make coredump dir writable.
if [ ! -d "/var/crash" ]; then sudo mkdir -p /var/crash; fi
sudo chmod 777 /var/crash

if [ ! -d "/home/$current_user/workspace" ]; then
  mkdir /home/$current_user/workspace
fi

sudo chown -R $current_user /home/$current_user/workspace

source "$(dirname "$0")/common.sh"

cd /home/$current_user/workspace
ln -s $WORKSPACE/py_tpcc_src py-tpcc
ln -s $WORKSPACE/eloqdoc_src mongo
cd mongo
git submodule sync
git submodule update --init --recursive

cd src/mongo/db/modules/eloq/data_substrate
ln -s $WORKSPACE/eloq_logservice_src eloq_log_service

pushd tx_service
ln -s $WORKSPACE/raft_host_manager_src raft_host_manager
popd

cd /home/$current_user/workspace/mongo

# Generate unique bucket names for main test
timestamp=$(($(date +%s%N)/1000000))
export BUCKET_NAME="main-test-${timestamp}"
export BUCKET_PREFIX="elqdoc-"
echo "bucket_name is ${BUCKET_PREFIX}${BUCKET_NAME}"
DATA_DIR="/home/eloq/workspace/mongo/install/data"

compile_and_install_ent
cleanup_all "$DATA_DIR" "$BUCKET_NAME" "$BUCKET_PREFIX"
launch_eloqdoc "$BUCKET_NAME" "$BUCKET_PREFIX"
try_connect
run_jstests
shutdown_eloqdoc
cleanup_all "$DATA_DIR" "$BUCKET_NAME" "$BUCKET_PREFIX"
launch_eloqdoc "$BUCKET_NAME" "$BUCKET_PREFIX"
try_connect
run_tpcc
cleanup_all "$DATA_DIR" "$BUCKET_NAME" "$BUCKET_PREFIX"
