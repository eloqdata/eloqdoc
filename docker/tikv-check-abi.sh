#!/usr/bin/env bash
set -euo pipefail

# Verify that a Docker-built EloqDoc + TiKV release package keeps the intended
# CentOS 7 ABI baseline and has no missing runtime libraries for its entry
# points. Run this after docker/tikv-copy-runtime-libs.sh.
#
# Usage:
#   docker/tikv-check-abi.sh [install-prefix] [max-glibc-version]

prefix="${1:-/work/eloqdoc/install/tikv-centos7}"
max_glibc_allowed="${2:-2.17}"
prefix="$(cd "$prefix" && pwd)"

export LD_LIBRARY_PATH="$prefix/lib:${LD_LIBRARY_PATH:-}"

glibc_runtime_re='^(ld-linux|linux-vdso|libc\.so|libpthread\.so|librt\.so|libdl\.so|libm\.so|libresolv\.so|libnsl\.so|libutil\.so|libanl\.so|libBrokenLocale\.so|libnss_.*\.so)'

mapfile -t bins < <(find "$prefix/bin" -maxdepth 1 -type f -perm /111 | sort)
if ((${#bins[@]} == 0)); then
  echo "ERROR: no executable files found under $prefix/bin" >&2
  exit 1
fi

echo "Runtime ldd check:"
for bin in "${bins[@]}"; do
  echo "  - $(basename "$bin")"
  if ldd "$bin" 2>&1 | tee /tmp/tikv-check-abi-ldd.$$ | grep -q 'not found'; then
    cat /tmp/tikv-check-abi-ldd.$$ >&2
    rm -f /tmp/tikv-check-abi-ldd.$$
    exit 1
  fi
  rm -f /tmp/tikv-check-abi-ldd.$$

  outside_non_glibc="$(
    ldd "$bin" 2>/dev/null \
      | awk '/=> \/.*\// {print $1 "\t" $3} /^\// {print $1 "\t" $1}' \
      | while IFS=$'\t' read -r lib path; do
          [[ -n "$path" ]] || continue
          base="$(basename "$path")"
          if [[ "$base" =~ $glibc_runtime_re ]]; then
            continue
          fi
          case "$path" in
            "$prefix/lib"/*) ;;
            *) printf '%s => %s\n' "$lib" "$path" ;;
          esac
        done
  )"
  if [[ -n "$outside_non_glibc" ]]; then
    echo "ERROR: non-glibc dependencies resolved outside $prefix/lib for $bin:" >&2
    printf '%s\n' "$outside_non_glibc" >&2
    exit 1
  fi
done

mapfile -t elf_files < <(
  find "$prefix/bin" "$prefix/lib" -maxdepth 1 -type f \( -perm /111 -o -name '*.so*' \) -print \
    | while IFS= read -r file; do
        if file -b "$file" | grep -q 'ELF'; then
          printf '%s\n' "$file"
        fi
      done \
    | sort
)

max_glibc="$(
  readelf -Ws "${elf_files[@]}" 2>/dev/null \
    | grep -o 'GLIBC_[0-9][0-9.]*' \
    | sort -Vu \
    | tail -1 || true
)"
max_glibc="${max_glibc#GLIBC_}"
if [[ -n "$max_glibc" ]]; then
  highest="$(printf '%s\n%s\n' "$max_glibc" "$max_glibc_allowed" | sort -V | tail -1)"
  if [[ "$highest" != "$max_glibc_allowed" ]]; then
    echo "ERROR: max GLIBC requirement is $max_glibc, allowed <= $max_glibc_allowed" >&2
    exit 1
  fi
fi

max_glibcxx="$(
  readelf -Ws "${elf_files[@]}" 2>/dev/null \
    | grep -o 'GLIBCXX_[0-9][0-9.]*' \
    | sort -Vu \
    | tail -1 || true
)"
if [[ -n "$max_glibcxx" ]]; then
  if ! compgen -G "$prefix/lib/libstdc++.so*" >/dev/null; then
    echo "ERROR: found $max_glibcxx references but no packaged libstdc++.so* under $prefix/lib" >&2
    exit 1
  fi
fi

echo "ABI summary:"
echo "  max GLIBC: ${max_glibc:-none} (allowed <= $max_glibc_allowed)"
echo "  max GLIBCXX: ${max_glibcxx:-none}"
if compgen -G "$prefix/lib/libstdc++.so*" >/dev/null; then
  echo "  packaged libstdc++: yes"
else
  echo "  packaged libstdc++: no"
fi
echo "  non-glibc runtime deps: resolved from $prefix/lib"
