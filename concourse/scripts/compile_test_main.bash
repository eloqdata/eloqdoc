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
sudo chown -R mono $PWD

# make coredump dir writable.
if [ ! -d "/var/crash" ]; then sudo mkdir -p /var/crash; fi
sudo chmod 777 /var/crash

sudo chown -R mono /home/mono/workspace
cd /home/mono/workspace
ln -s $WORKSPACE/eloqdoc_src mongo
cd mongo
git submodule sync
git submodule update --init --recursive

cd src/mongo/db/modules/eloq
ln -s $WORKSPACE/logservice_src log_service

cd /home/mono/workspace/mongo

# Generate unique bucket names for main oss test
BUCKET_NAME="main-oss-test"
BUCKET_PREFIX="rocksdb-cloud-"

compile_and_install
cleanup_all_buckets "$BUCKET_NAME" "$BUCKET_PREFIX"
launch_mongod "$BUCKET_NAME" "$BUCKET_PREFIX"
try_connect
run_jstests
cleanup_all_buckets "$BUCKET_NAME" "$BUCKET_PREFIX"
