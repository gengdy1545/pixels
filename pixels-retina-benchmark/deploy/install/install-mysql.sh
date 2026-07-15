#!/usr/bin/env bash
set -Eeuo pipefail

umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../paths.env
source "${SCRIPT_DIR}/paths.env"

log()
{
    printf '[install-mysql] %s\n' "$*"
}

die()
{
    printf '[install-mysql] ERROR: %s\n' "$*" >&2
    exit 1
}

BUNDLE_ROOT="${BUNDLE_ROOT:-}"
if [[ -z "${BUNDLE_ROOT}" ]]
then
    BUNDLE_ROOT="$(resolve_bundle_root "${SCRIPT_DIR}/install-mysql.sh")" ||
        die "unable to locate bundle root (formal-defaults.env)"
fi

# shellcheck source=../formal-defaults.env
source "${BUNDLE_ROOT}/formal-defaults.env"

RUNTIME_DIR="${BUNDLE_ROOT}/pixels-retina-benchmark"
SCHEMA_SQL="${RUNTIME_DIR}/mysql-init/metadata_schema.sql"

mysql_ping()
{
    mysqladmin --host="${MYSQL_HOST}" --port="${MYSQL_PORT}" \
        --user="${MYSQL_USER}" --password="${MYSQL_PASSWORD}" \
        ping --silent >/dev/null 2>&1
}

schema_ready()
{
    mysql --host="${MYSQL_HOST}" --port="${MYSQL_PORT}" \
        --user="${MYSQL_USER}" --password="${MYSQL_PASSWORD}" \
        --database="${MYSQL_DATABASE}" \
        --execute="SHOW TABLES LIKE 'DBS';" 2>/dev/null |
        grep -q '^DBS$'
}

ensure_mysql_server()
{
    if command -v mysqladmin >/dev/null 2>&1 &&
       mysqladmin --host="${MYSQL_HOST}" --port="${MYSQL_PORT}" ping --silent >/dev/null 2>&1
    then
        log "MySQL server is already running on ${MYSQL_HOST}:${MYSQL_PORT}"
        return
    fi

    log "installing MySQL server via apt (see docs/INSTALL.md)"
    if ! command -v apt-get >/dev/null 2>&1
    then
        die "apt-get is required to install mysql-server on Ubuntu"
    fi
    export DEBIAN_FRONTEND=noninteractive
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server

    for _ in $(seq 1 60)
    do
        if mysqladmin --host="${MYSQL_HOST}" --port="${MYSQL_PORT}" ping --silent >/dev/null 2>&1
        then
            log "MySQL server is ready"
            return
        fi
        sleep 1
    done
    die "MySQL server did not become ready after apt install"
}

ensure_pixels_user()
{
    log "ensuring Pixels metadata user and database"
    sudo mysql --host="${MYSQL_HOST}" --port="${MYSQL_PORT}" <<SQL
CREATE DATABASE IF NOT EXISTS \`${MYSQL_DATABASE}\`;
CREATE USER IF NOT EXISTS '${MYSQL_USER}'@'localhost' IDENTIFIED BY '${MYSQL_PASSWORD}';
ALTER USER '${MYSQL_USER}'@'localhost' IDENTIFIED BY '${MYSQL_PASSWORD}';
GRANT ALL PRIVILEGES ON \`${MYSQL_DATABASE}\`.* TO '${MYSQL_USER}'@'localhost';
FLUSH PRIVILEGES;
SQL
}

import_schema()
{
    [[ -f "${SCHEMA_SQL}" ]] || die "metadata schema not found: ${SCHEMA_SQL}"
    if schema_ready
    then
        log "metadata schema already present in ${MYSQL_DATABASE}"
        return
    fi
    log "importing metadata schema from ${SCHEMA_SQL}"
    mysql --host="${MYSQL_HOST}" --port="${MYSQL_PORT}" \
        --user="${MYSQL_USER}" --password="${MYSQL_PASSWORD}" \
        "${MYSQL_DATABASE}" < "${SCHEMA_SQL}"
    schema_ready || die "metadata schema import did not create expected tables"
}

ensure_mysql_server
ensure_pixels_user
import_schema
mysql_ping || die "MySQL is not reachable as ${MYSQL_USER}@${MYSQL_HOST}:${MYSQL_PORT}"
log "MySQL metadata database is ready"
