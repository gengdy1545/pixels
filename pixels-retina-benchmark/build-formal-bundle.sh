#!/usr/bin/env bash
set -Eeuo pipefail

MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${MODULE_DIR}/.." && pwd)"

PIXELS_CONFIG_SOURCE="${PIXELS_CONFIG_SOURCE:-/mnt/disk2/pixels-retina-benchmark/etc/pixels.properties}"
ETCD_ARCHIVE="${ETCD_ARCHIVE:-${REPO_ROOT}/scripts/tars/etcd-v3.3.4-linux-amd64.tar.xz}"
OUTPUT_DIR="${OUTPUT_DIR:-${MODULE_DIR}/target/formal-bundle}"
ARCHIVE_PATH="${ARCHIVE_PATH:-${MODULE_DIR}/target/retina-formal-bundle-linux-amd64.tar.gz}"
SKIP_MAVEN=false

usage()
{
    cat <<'EOF'
Usage: build-formal-bundle.sh [options]

Builds retina-formal-bundle-linux-amd64.tar.gz containing:
  - pixels-retina-benchmark runtime (final pixels.properties)
  - etcd 3.3.4 binaries
  - install/bootstrap.sh and install/install-mysql.sh
  - formal-defaults.env and run-formal-benchmark.sh

Options:
  --config FILE          Final pixels.properties to package
  --output-dir DIR       Staging directory (default: target/formal-bundle)
  --archive FILE         Output tar.gz path
  --skip-maven           Reuse the existing fat JAR/runtime build
  -h, --help             Show this help
EOF
}

log()
{
    printf '[build-formal-bundle] %s\n' "$*"
}

die()
{
    printf '[build-formal-bundle] ERROR: %s\n' "$*" >&2
    exit 1
}

while [[ $# -gt 0 ]]
do
    case "$1" in
        --config) PIXELS_CONFIG_SOURCE="$2"; shift 2 ;;
        --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        --archive) ARCHIVE_PATH="$2"; shift 2 ;;
        --skip-maven) SKIP_MAVEN=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

[[ -f "${PIXELS_CONFIG_SOURCE}" ]] ||
    die "final pixels.properties not found: ${PIXELS_CONFIG_SOURCE}"
if grep -Eq '^[[:space:]]*[A-Za-z0-9_.-]+=.*REPLACE_WITH_' "${PIXELS_CONFIG_SOURCE}"
then
    die "refusing to package template pixels.properties: ${PIXELS_CONFIG_SOURCE}"
fi
[[ -f "${ETCD_ARCHIVE}" ]] || die "etcd archive not found: ${ETCD_ARCHIVE}"

if [[ "${SKIP_MAVEN}" == "false" ]]
then
    log "building pixels-retina-benchmark runtime"
    # -Dmaven.test.skip=true skips both test compile and test run.
    # -DskipTests alone still compiles tests and can fail the packager.
    mvn -f "${REPO_ROOT}/pom.xml" -pl pixels-retina-benchmark -am package \
        -Dmaven.test.skip=true
fi

JAR_PATH="${MODULE_DIR}/target/pixels-retina-benchmark-*-full.jar"
JAR_FILE="$(ls -1 ${JAR_PATH} 2>/dev/null | tail -1)"
[[ -n "${JAR_FILE}" && -f "${JAR_FILE}" ]] ||
    die "benchmark fat JAR not found under ${MODULE_DIR}/target"

log "using jar ${JAR_FILE}"
log "using config ${PIXELS_CONFIG_SOURCE}"

rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}/pixels-retina-benchmark"/{bin,etc,mysql-init,lib}
mkdir -p "${OUTPUT_DIR}/install"
mkdir -p "${OUTPUT_DIR}/third-party/etcd"

cp -a "${MODULE_DIR}/deploy/bin/." "${OUTPUT_DIR}/pixels-retina-benchmark/bin/"
cp -a "${MODULE_DIR}/deploy/etc/retina-benchmark-log4j2.xml" "${OUTPUT_DIR}/pixels-retina-benchmark/etc/"
cp "${PIXELS_CONFIG_SOURCE}" "${OUTPUT_DIR}/pixels-retina-benchmark/etc/pixels.properties"
chmod 600 "${OUTPUT_DIR}/pixels-retina-benchmark/etc/pixels.properties"
cp "${REPO_ROOT}/scripts/sql/metadata_schema.sql" \
    "${OUTPUT_DIR}/pixels-retina-benchmark/mysql-init/metadata_schema.sql"
cp "${JAR_FILE}" "${OUTPUT_DIR}/pixels-retina-benchmark/pixels-retina-benchmark-full.jar"
cp "${MODULE_DIR}/README.md" "${OUTPUT_DIR}/pixels-retina-benchmark/README.md"
cp "${MODULE_DIR}/SNAPSHOT.md" "${OUTPUT_DIR}/pixels-retina-benchmark/SNAPSHOT.md"
cp "${MODULE_DIR}/deploy/formal-defaults.env" "${OUTPUT_DIR}/formal-defaults.env"
cp "${MODULE_DIR}/deploy/run-formal-benchmark.sh" "${OUTPUT_DIR}/run-formal-benchmark.sh"
cp -a "${MODULE_DIR}/deploy/install/." "${OUTPUT_DIR}/install/"
cp "${MODULE_DIR}/deploy/README-FORMAL.md" "${OUTPUT_DIR}/README-FORMAL.md"

tar -xJf "${ETCD_ARCHIVE}" -C "${OUTPUT_DIR}/third-party/etcd" --strip-components=1

chmod +x "${OUTPUT_DIR}/run-formal-benchmark.sh"
chmod +x "${OUTPUT_DIR}/install/"*.sh
chmod +x "${OUTPUT_DIR}/pixels-retina-benchmark/bin/"*

mkdir -p "$(dirname "${ARCHIVE_PATH}")"
tar -C "${OUTPUT_DIR%/*}" -czf "${ARCHIVE_PATH}" "$(basename "${OUTPUT_DIR}")"

log "bundle staged at ${OUTPUT_DIR}"
log "archive written to ${ARCHIVE_PATH}"
du -h "${ARCHIVE_PATH}"
