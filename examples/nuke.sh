#!/usr/bin/env bash
set -euo pipefail

# Reset the Postgres database inside your docker container.
# Defaults match your setup script.
# Usage:
#   ./scripts/reset_postgres.sh               # drop & recreate DB
#   ./scripts/reset_postgres.sh schema-only   # drop & recreate 'public' schema only

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres-test-db}"
POSTGRES_DB="${POSTGRES_DB:-migration_target}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_SCHEMA="${POSTGRES_SCHEMA:-public}"

psql_exec() {
  docker exec -i "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -v ON_ERROR_STOP=1 -tA -c "$1"
}

# Drop & recreate the whole database
reset_database() {
  echo "[*] Terminating connections to '${POSTGRES_DB}'..."
  psql_exec "SELECT pg_terminate_backend(pid)
             FROM pg_stat_activity
             WHERE datname='${POSTGRES_DB}' AND pid <> pg_backend_pid();"

  echo "[*] Dropping database '${POSTGRES_DB}' (if exists)..."
  psql_exec "DROP DATABASE IF EXISTS ${POSTGRES_DB};"

  echo "[*] Recreating database '${POSTGRES_DB}'..."
  psql_exec "CREATE DATABASE ${POSTGRES_DB};"

  echo "[✅] Database recreated."
}

# Drop & recreate just the schema inside the DB
reset_schema() {
  echo "[*] Dropping & recreating schema '${POSTGRES_SCHEMA}' in DB '${POSTGRES_DB}'..."
  docker exec -i "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c "
    DROP SCHEMA IF EXISTS ${POSTGRES_SCHEMA} CASCADE;
    CREATE SCHEMA ${POSTGRES_SCHEMA};
    GRANT ALL ON SCHEMA ${POSTGRES_SCHEMA} TO ${POSTGRES_USER};
  "
  echo "[✅] Schema reset."
}

# Sanity checks
if ! docker ps --format '{{.Names}}' | grep -q "^${POSTGRES_CONTAINER}\$"; then
  echo "[!] Container '${POSTGRES_CONTAINER}' not running. Start it first."
  exit 1
fi

case "${1:-}" in
  schema-only)
    reset_schema
    ;;
  *)
    reset_database
    ;;
esac