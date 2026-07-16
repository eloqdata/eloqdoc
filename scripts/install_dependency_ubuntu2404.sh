#!/bin/bash
set -euo pipefail

# Installs EloqDoc build dependencies for a from-scratch Ubuntu 24.04 build.
#
# Usage:
#   scripts/install_dependency_ubuntu2404.sh [TEMP_DIR] [--skip_eloq_common]
#
# TEMP_DIR controls temporary downloads and build artifacts. It defaults to
# /tmp/eloqdoc-deps. The legacy --skip_eloq_common option installs only the
# Python 2.7 build environment.
#
# Set ELOQ_SKIP_THIRD_PARTY=1 to skip the Data Substrate third-party workspace
# build. CI uses that mode when the third-party prefix is restored from cache.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
DATA_SUBSTRATE_ROOT="${REPO_ROOT}/src/mongo/db/modules/eloq/data_substrate"
THIRD_PARTY_INSTALLER="${DATA_SUBSTRATE_ROOT}/scripts/third_party/install-ubuntu2404.sh"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" >&2
}

run_privileged() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
    elif command -v sudo >/dev/null 2>&1; then
        sudo "$@"
    else
        log_error "This script must run as root or with sudo available: $*"
        exit 1
    fi
}

apt_get_with_retry() {
    local attempt=1
    local max_attempts="${APT_RETRY_ATTEMPTS:-5}"
    local delay="${APT_RETRY_DELAY_SECONDS:-15}"

    until run_privileged env DEBIAN_FRONTEND=noninteractive \
        apt-get \
            -o APT::Update::Error-Mode=any \
            -o Acquire::Retries=5 \
            -o Acquire::http::Timeout=30 \
            -o Acquire::https::Timeout=30 \
            "$@"; do
        if [ "${attempt}" -ge "${max_attempts}" ]; then
            return 1
        fi
        log_warning "apt-get $* failed; retrying in ${delay}s (${attempt}/${max_attempts})"
        sleep "${delay}"
        attempt=$((attempt + 1))
        delay=$((delay * 2))
    done
}

apt_install() {
    apt_get_with_retry update
    apt_get_with_retry install -y --no-install-recommends "$@"
}

run_with_failure_log() {
    local description="$1"
    shift
    local safe_name="${description//[^A-Za-z0-9_.-]/_}"
    local log_file="${TEMP_DIR}/${safe_name}.log"
    local interval="${CI_HEARTBEAT_INTERVAL_SECONDS:-60}"
    local pid
    local status=0

    log_info "${description}"
    if [ "${ELOQ_VERBOSE_DEPS:-0}" = "1" ]; then
        "$@"
        return
    fi

    "$@" >"${log_file}" 2>&1 &
    pid=$!
    while kill -0 "${pid}" 2>/dev/null; do
        sleep "${interval}" || true
        if kill -0 "${pid}" 2>/dev/null; then
            log_info "${description} still running ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
        fi
    done
    wait "${pid}" || status=$?
    if [ "${status}" -ne 0 ]; then
        log_error "${description} failed; full log follows (${log_file})"
        cat "${log_file}" >&2 || true
        return "${status}"
    fi
}

configure_timezone() {
    local needs_tz_config=false
    if [ ! -f /etc/timezone ] || ! grep -qE '^(Etc/UTC|UTC)$' /etc/timezone; then
        needs_tz_config=true
    fi
    if [ ! -L /etc/localtime ] || [ "$(readlink -f /etc/localtime)" != "/usr/share/zoneinfo/Etc/UTC" ]; then
        needs_tz_config=true
    fi

    if ${needs_tz_config}; then
        echo 'tzdata tzdata/Areas select Etc' | run_privileged debconf-set-selections || true
        echo 'tzdata tzdata/Zones/Etc select UTC' | run_privileged debconf-set-selections || true
        echo 'Etc/UTC' | run_privileged tee /etc/timezone >/dev/null
        run_privileged ln -sf /usr/share/zoneinfo/Etc/UTC /etc/localtime
    fi
}

install_python2() {
    export PYENV_ROOT="${PYENV_ROOT:-${HOME}/.pyenv}"
    if [ -x "${PYENV_ROOT}/bin/pyenv" ]; then
        export PATH="${PYENV_ROOT}/bin:${PATH}"
        eval "$(pyenv init -)" || true
        eval "$(pyenv virtualenv-init -)" || true
        pyenv shell 2.7.18 >/dev/null 2>&1 || pyenv global 2.7.18 >/dev/null 2>&1 || true
        hash -r
    fi

    if ! command -v python2 >/dev/null 2>&1; then
        log_info "Installing Python 2.7.18 with pyenv"
        apt_install \
            build-essential zlib1g-dev libbz2-dev liblzma-dev libreadline-dev \
            libsqlite3-dev libffi-dev libssl-dev libncurses5-dev \
            libncursesw5-dev xz-utils tk-dev wget unzip git curl ca-certificates

        if [ ! -x "${PYENV_ROOT}/bin/pyenv" ]; then
            curl -fsSL https://pyenv.run | bash
        fi

        export PATH="${PYENV_ROOT}/bin:${PATH}"
        eval "$(pyenv init -)" || true
        eval "$(pyenv virtualenv-init -)" || true

        if ! pyenv versions --bare | grep -qx "2.7.18"; then
            run_with_failure_log "Building Python 2.7.18 with pyenv" pyenv install 2.7.18
        fi
        pyenv global 2.7.18
        hash -r
    fi

    if ! command -v python2 >/dev/null 2>&1; then
        log_error "python2 was not found after installing Python 2.7.18"
        exit 1
    fi

    if ! python2 -m pip --version >/dev/null 2>&1; then
        curl -fsSL https://bootstrap.pypa.io/pip/2.7/get-pip.py -o "${TEMP_DIR}/get-pip.py"
        run_with_failure_log "Installing pip for Python 2.7" \
            python2 "${TEMP_DIR}/get-pip.py" 'pip<21' 'setuptools<45' 'wheel<0.38'
    fi

    python2 --version
    run_with_failure_log "Installing Python dependencies from buildscripts/requirements.txt" \
        python2 -m pip install --no-cache-dir -r "${SCRIPT_DIR}/buildscripts/requirements.txt"
}

SKIP_ELOQ_COMMON=false
TEMP_DIR=""

while [ "$#" -gt 0 ]; do
    case "$1" in
        --skip_eloq_common)
            SKIP_ELOQ_COMMON=true
            shift
            ;;
        *)
            if [ -z "${TEMP_DIR}" ]; then
                TEMP_DIR="$1"
            else
                log_warning "Unknown option: $1"
            fi
            shift
            ;;
    esac
done

TEMP_DIR="${TEMP_DIR:-/tmp/eloqdoc-deps}"
TEMP_DIR=$(realpath -m "${TEMP_DIR}")
mkdir -p "${TEMP_DIR}"
log_info "Using dependency workspace: ${TEMP_DIR}"

export DEBIAN_FRONTEND=noninteractive
export TZ="${TZ:-UTC}"

configure_timezone

if [ "${SKIP_ELOQ_COMMON}" = false ]; then
    log_info "Installing EloqDoc system build packages"
    system_packages=(
        sudo wget curl apt-utils python3 python3-dev python3-pip python3-venv
        libcurl4-openssl-dev build-essential libncurses5-dev libncursesw5-dev
        gnutls-dev bison zlib1g-dev ccache rsync cmake ninja-build libuv1-dev
        git g++ gcc make openssh-client libssl-dev libgflags-dev
        libleveldb-dev libsnappy-dev openssl libbz2-dev liblz4-dev libzstd-dev
        libboost-context-dev ca-certificates libc-ares-dev libc-ares2 m4
        pkg-config tar xz-utils libreadline-dev libsqlite3-dev ncurses-dev
        tk-dev libffi-dev liblzma-dev patchelf libprotobuf-dev
        protobuf-compiler libjsoncpp-dev unzip
    )
    apt_install "${system_packages[@]}"

    if [ "${ELOQ_SKIP_THIRD_PARTY:-0}" = "1" ]; then
        log_info "Skipping Data Substrate third-party build because ELOQ_SKIP_THIRD_PARTY=1"
    else
        if [ ! -x "${THIRD_PARTY_INSTALLER}" ]; then
            log_error "Missing ${THIRD_PARTY_INSTALLER}. Initialize submodules first:"
            echo "  git submodule update --init src/mongo/db/modules/eloq/data_substrate" >&2
            exit 1
        fi

        log_info "Installing Data Substrate third-party dependencies"
        "${THIRD_PARTY_INSTALLER}"
    fi
else
    log_info "Skipping EloqDoc system and Data Substrate third-party dependencies"
fi

install_python2

log_info "EloqDoc dependency installation completed successfully"
