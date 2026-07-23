#!/usr/bin/env bash
set -Eeuo pipefail

umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=paths.env
source "${SCRIPT_DIR}/paths.env"

BUNDLE_ROOT="$(resolve_bundle_root "${SCRIPT_DIR}/bootstrap.sh")" ||
    die_paths "unable to locate bundle root (formal-defaults.env)"

# shellcheck source=../formal-defaults.env
source "${BUNDLE_ROOT}/formal-defaults.env"

RUNTIME_DIR="${BUNDLE_ROOT}/pixels-retina-benchmark"
PIXELS_CONFIG="${RUNTIME_DIR}/etc/pixels.properties"
BOOTSTRAP_LOG="${BOOTSTRAP_LOG:-/data/retina-benchmark/bootstrap.log}"

log()
{
    printf '[bootstrap] %s\n' "$*" | tee -a "${BOOTSTRAP_LOG}"
}

die()
{
    printf '[bootstrap] ERROR: %s\n' "$*" >&2
    exit 1
}

die_paths()
{
    printf '[bootstrap] ERROR: %s\n' "$*" >&2
    exit 1
}

require_command()
{
    local name="$1"
    command -v "${name}" >/dev/null 2>&1 || die "required command not found: ${name}"
}

ensure_directories()
{
    local dir
    for dir in \
        /data/retina-benchmark \
        "${SNAPSHOT_DIR%/*}" \
        "${SNAPSHOT_DIR}" \
        "${RESULTS_ROOT}" \
        "${WORK_ROOT}" \
        "${STATE_ROOT}" \
        /var/tmp/pixels-retina-benchmark
    do
        sudo mkdir -p "${dir}"
        sudo chown "$(id -un)":"$(id -gn)" "${dir}" 2>/dev/null || true
    done
    mkdir -p "$(dirname "${BOOTSTRAP_LOG}")"
    touch "${BOOTSTRAP_LOG}"
}

validate_pixels_config()
{
    [[ -f "${PIXELS_CONFIG}" ]] || die "pixels.properties not found: ${PIXELS_CONFIG}"
    if grep -Eq '^[[:space:]]*[A-Za-z0-9_.-]+=.*REPLACE_WITH_' "${PIXELS_CONFIG}"
    then
        die "pixels.properties still contains REPLACE_WITH placeholders"
    fi
    chmod 600 "${PIXELS_CONFIG}"
}

sync_snapshot()
{
    require_command aws
    if [[ -f "${SNAPSHOT_DIR}/snapshot.json" ]]
    then
        log "snapshot already present at ${SNAPSHOT_DIR}; skipping download"
        return
    fi
    log "downloading snapshot from ${SNAPSHOT_S3_URI} to ${SNAPSHOT_DIR}"
    mkdir -p "${SNAPSHOT_DIR}"
    aws s3 sync "${SNAPSHOT_S3_URI%/}/" "${SNAPSHOT_DIR}/"
    [[ -f "${SNAPSHOT_DIR}/snapshot.json" ]] ||
        die "snapshot download did not produce ${SNAPSHOT_DIR}/snapshot.json"
    [[ -d "${SNAPSHOT_DIR}/state/index/rocksdb" ]] ||
        die "snapshot download is missing state/index/rocksdb"
    [[ -d "${SNAPSHOT_DIR}/state/index/sqlite" ]] ||
        die "snapshot download is missing state/index/sqlite"
    log "snapshot download completed"
}

preflight_disk()
{
    local snapshot_index_kib available_kib minimum_kib
    snapshot_index_kib="$(du -sk "${SNAPSHOT_DIR}/state/index" | awk '{ print $1 }')"
    available_kib="$(df -Pk "${WORK_ROOT}" | awk 'NR == 2 { print $4 }')"
    minimum_kib=$((snapshot_index_kib * 3))
    if (( available_kib < minimum_kib ))
    then
        die "insufficient work disk on ${WORK_ROOT}: available=${available_kib} KiB, need>=${minimum_kib} KiB"
    fi
    log "disk check passed: work_root=${WORK_ROOT} available=${available_kib} KiB"
}

preflight_memory()
{
    local mem_kib
    mem_kib="$(awk '/MemTotal:/ { print $2 }' /proc/meminfo)"
    if (( mem_kib < 100000000 ))
    then
        log "warning: MemTotal=${mem_kib} KiB is below 100 GiB; formal suites use 40 GiB JVM heaps"
    fi
}

write_bootstrap_info()
{
    local output="/data/retina-benchmark/bootstrap-info.txt"
    {
        date -u --iso-8601=seconds
        printf 'bundle_root=%s\n' "${BUNDLE_ROOT}"
        printf 'snapshot_dir=%s\n' "${SNAPSHOT_DIR}"
        printf 'snapshot_s3_uri=%s\n' "${SNAPSHOT_S3_URI}"
        printf 'results_root=%s\n' "${RESULTS_ROOT}"
        printf 'results_s3_root=%s\n' "${RESULTS_S3_ROOT}"
        uname -a
        java -version 2>&1 || true
        free -h 2>/dev/null || true
        df -h "${WORK_ROOT}" "${RESULTS_ROOT}" "${SNAPSHOT_DIR}" 2>/dev/null || true
    } > "${output}"
    log "wrote ${output}"
}

require_command java
require_command aws
ensure_directories
log "starting bootstrap for ${BUNDLE_ROOT}"
validate_pixels_config
BUNDLE_ROOT="${BUNDLE_ROOT}" "${SCRIPT_DIR}/install-mysql.sh"
sync_snapshot
preflight_disk
preflight_memory
write_bootstrap_info
log "bootstrap completed successfully"
