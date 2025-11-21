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

if [ ! -d "/home/$current_user/workspace" ]; then
  mkdir /home/$current_user/workspace
fi

sudo chown -R $current_user /home/$current_user/workspace
cd /home/$current_user/workspace
ln -s $WORKSPACE/py_tpcc_src py-tpcc
ln -s $WORKSPACE/eloqdoc_src mongo
cd mongo
git submodule sync
git submodule update --init --recursive

cd src/mongo/db/modules/eloq

cd /home/$current_user/workspace/mongo

# Generate unique bucket names for main oss test
BUCKET_NAME="main-oss-test"
BUCKET_PREFIX="rocksdb-cloud-"
DATA_DIR="/home/eloq/workspace/mongo/install/data"

compile_and_install
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
