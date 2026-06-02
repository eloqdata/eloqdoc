#!/usr/bin/env bash
set -euo pipefail

# Copy non-glibc runtime libraries required by the Docker-built EloqDoc
# artifact into the install prefix. Run this script inside the CentOS7
# release builder container after CMake/SCons install completes.
#
# Usage:
#   docker/tikv-copy-runtime-libs.sh <install-prefix>/lib [extra-scan-root ...]
#
# The optional extra roots are useful for libraries built by submodules but not
# installed by SCons, for example tikv-client-c's libkv_client.so and kvproto.

dest="${1:-/work/eloqdoc/install/tikv-centos7/lib}"
shift || true
mkdir -p "$dest"
dest="$(cd "$dest" && pwd)"
install_prefix="$(cd "$dest/.." && pwd)"

extra_scan_roots=()
scan_roots=("$install_prefix/bin" "$install_prefix/lib")
for root in "$@"; do
  if [[ -e "$root" ]]; then
    scan_roots+=("$root")
    extra_scan_roots+=("$root")
  fi
done

# Prefer the packaged lib dir and builder toolchain while resolving ldd output.
export LD_LIBRARY_PATH="$dest:/usr/local/lib64:/usr/local/lib:/opt/rh/devtoolset-10/root/usr/lib64:${LD_LIBRARY_PATH:-}"

is_glibc_runtime() {
  local base="$1"
  case "$base" in
    ld-linux*|linux-vdso*|libc.so*|libpthread.so*|librt.so*|libdl.so*|libm.so*|\
    libresolv.so*|libnsl.so*|libutil.so*|libanl.so*|libBrokenLocale.so*|\
    libnss_*.so*)
      return 0
      ;;
  esac
  return 1
}

copy_one() {
  local src="$1"
  [[ -f "$src" ]] || return 1
  local base
  base="$(basename "$src")"
  is_glibc_runtime "$base" && return 1
  [[ -e "$dest/$base" ]] && return 1
  cp -aL "$src" "$dest/$base"
  echo "copied $base <- $src"
  return 0
}

copy_root_shared_objects() {
  local root
  for root in "${scan_roots[@]}"; do
    [[ -e "$root" ]] || continue
    while IFS= read -r -d '' file; do
      copy_one "$file" || true
    done < <(find -L "$root" -maxdepth 3 -type f -name '*.so*' -print0 2>/dev/null)
  done
}

copy_ldd_dependencies() {
  local -a queue=()
  local -A seen=()
  local root file

  # Start from runnable entry points and submodule-produced shared libraries.
  # ldd on the executable recursively reports the Mongo shared-library graph, so
  # avoid repeatedly ldd'ing every SCons-installed .so; that makes the packaging
  # step unnecessarily slow on the large dynamic Mongo build.
  for root in "$install_prefix/bin" "${extra_scan_roots[@]}"; do
    [[ -e "$root" ]] || continue
    while IFS= read -r -d '' file; do
      [[ -n "${seen[$file]:-}" ]] && continue
      seen["$file"]=queued
      queue+=("$file")
    done < <(find -L "$root" -maxdepth 3 -type f \( -perm /111 -o -name '*.so*' \) -print0 2>/dev/null)
  done

  local idx=0
  while (( idx < ${#queue[@]} )); do
    file="${queue[$idx]}"
    idx=$((idx + 1))

    local ldd_out dep base dest_file
    ldd_out="$(ldd "$file" 2>/dev/null || true)"
    [[ -n "$ldd_out" ]] || continue
    while IFS= read -r dep; do
      [[ -n "$dep" ]] || continue
      base="$(basename "$dep")"
      dest_file="$dest/$base"
      if copy_one "$dep"; then
        if [[ -z "${seen[$dest_file]:-}" ]]; then
          seen["$dest_file"]=queued
          queue+=("$dest_file")
        fi
      fi
    done < <(printf '%s\n' "$ldd_out" | awk '/=> \/.*\// {print $3} /^\// {print $1}')
  done
}

rewrite_rpaths() {
  if ! command -v patchelf >/dev/null 2>&1; then
    echo 'ERROR: patchelf is required to rewrite release rpaths' >&2
    exit 1
  fi

  local file
  while IFS= read -r -d '' file; do
    if file -b "$file" | grep -q 'ELF'; then
      patchelf --set-rpath '$ORIGIN/../lib' "$file"
    fi
  done < <(find -L "$install_prefix/bin" -maxdepth 1 -type f -perm /111 -print0 2>/dev/null)

  while IFS= read -r -d '' file; do
    if file -b "$file" | grep -q 'ELF'; then
      patchelf --set-rpath '$ORIGIN' "$file"
    fi
  done < <(find -L "$dest" -maxdepth 1 -type f \( -perm /111 -o -name '*.so*' \) -print0 2>/dev/null)
}

copy_root_shared_objects
copy_ldd_dependencies
rewrite_rpaths

if find -L "$install_prefix/bin" -maxdepth 1 -type f -perm /111 -print0 2>/dev/null \
  | xargs -0 -r -n 32 ldd 2>/dev/null | grep -q 'not found'; then
  echo 'ERROR: some runtime libraries are still not found:' >&2
  find -L "$install_prefix/bin" -maxdepth 1 -type f -perm /111 -print0 2>/dev/null \
    | xargs -0 -r -n 32 ldd 2>/dev/null | grep 'not found' >&2 || true
  exit 1
fi
