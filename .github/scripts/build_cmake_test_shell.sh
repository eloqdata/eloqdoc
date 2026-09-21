#!/usr/bin/env bash
# Only the legacy JS test client is built by SCons; never build/install a SCons server here.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
shell_build_dir=${1:?usage: $0 shell-build-dir shell-install-prefix}
shell_install_prefix=${2:?usage: $0 shell-build-dir shell-install-prefix}
runtime_env
setup_python2
cd "$ELOQDOC_BASE_PATH"
run_with_heartbeat "SCons legacy JavaScript test shell" \
  python2 scripts/buildscripts/scons.py --silent \
  MONGO_VERSION=4.0.3 VARIANT_DIR="${SCONS_VARIANT_DIR:-RelWithDebInfo}" \
  CC="${CC:-gcc}" CXX="${CXX:-g++}" \
  CFLAGS="-isystem ${ELOQ_THIRD_PARTY_PREFIX}/include -Wno-nonnull" \
  CXXFLAGS="-isystem ${ELOQ_THIRD_PARTY_PREFIX}/include -Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
  CPPDEFINES="ELOQ_MODULE_ENABLED EXT_TX_PROC_ENABLED" \
  LIBPATH="${shell_install_prefix}/lib ${ELOQ_THIRD_PARTY_PREFIX}/lib ${ELOQ_THIRD_PARTY_PREFIX}/lib64" \
  --build-dir="$shell_build_dir" --prefix="$shell_install_prefix" \
  --allocator=system --link-model=dynamic --install-mode=hygienic \
  --disable-warnings-as-errors --jlink=1 -j"$BUILD_JOBS" install-shell
"${shell_install_prefix}/bin/eloqdoc-cli" --version
