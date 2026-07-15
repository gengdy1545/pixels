#!/usr/bin/env bash
set -Eeuo pipefail

BUNDLE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="${BUNDLE_ROOT}/pixels-retina-benchmark"

export RETINA_BENCHMARK_HOME="${RUNTIME_DIR}"
export PIXELS_HOME="${RUNTIME_DIR}"
export PIXELS_CONFIG="${RUNTIME_DIR}/etc/pixels.properties"

exec "${RUNTIME_DIR}/bin/run-retina-formal-suite" "$@"
