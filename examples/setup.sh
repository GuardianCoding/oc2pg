#!/usr/bin/env bash
set -euo pipefail

# CREATED with OpenAI GPT5

# =========================
# CONFIG
# =========================
ORACLE_IMAGE="gvenzl/oracle-xe:21-slim"
ORACLE_CONTAINER="oracle-test-db"
ORACLE_PASSWORD="oracle"
ORACLE_PORT=1521
# Default PDB for XE 21c is XEPDB1
ORACLE_SERVICE="XEPDB1"

POSTGRES_IMAGE="postgres:15"
POSTGRES_CONTAINER="postgres-test-db"
POSTGRES_PASSWORD="postgres"
POSTGRES_PORT=5432
POSTGRES_DB="migration_target"

# =========================
# FUNCTIONS
# =========================
wait_for_oracle_healthy () {
  echo "[*] Waiting for Oracle healthcheck to report healthy..."
  for i in {1..120}; do
    status=$(docker inspect --format='{{.State.Health.Status}}' "$ORACLE_CONTAINER" 2>/dev/null || echo "starting")
    if [[ "$status" == "healthy" ]]; then
      echo "[*] Oracle is healthy."
      return 0
    fi
    if (( i % 10 == 0 )); then
      echo "    ..still waiting (status: $status, ${i}s)"
    fi
    sleep 1
  done
  echo "[!] Oracle did not become healthy in time. Showing last 100 lines of logs:"
  docker logs --tail 100 "$ORACLE_CONTAINER" || true
}

oracle_exec_sql () {
  local sql="$1"
  docker exec -i "$ORACLE_CONTAINER" bash -lc "sqlplus -s system/${ORACLE_PASSWORD}@localhost:${ORACLE_PORT}/${ORACLE_SERVICE} <<'SQL'
whenever sqlerror exit failure
set feedback off verify off heading on pages 50
${sql}
exit
SQL"
}

# =========================
# RESET OPTION
# =========================
if [[ "${1:-}" == "reset" ]]; then
  echo "[*] Resetting containers..."
  docker rm -f "$ORACLE_CONTAINER" >/dev/null 2>&1 || true
  docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
fi

# =========================
# ORACLE
# =========================
echo "[*] Starting Oracle XE container..."
docker rm -f "$ORACLE_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --name "$ORACLE_CONTAINER" \
  -e ORACLE_PASSWORD="$ORACLE_PASSWORD" \
  -p ${ORACLE_PORT}:1521 \
  "$ORACLE_IMAGE" >/dev/null

wait_for_oracle_healthy

echo "[*] Creating sample schema and data in Oracle..."
oracle_exec_sql "
-- Create app user/schema
declare
  v_count number;
begin
  select count(*) into v_count from dba_users where username = 'TEST_USER';
  if v_count = 0 then
    execute immediate 'CREATE USER test_user IDENTIFIED BY test_pass QUOTA UNLIMITED ON USERS';
    execute immediate 'GRANT CREATE SESSION, CREATE TABLE TO test_user';
  end if;
end;
/
"

oracle_exec_sql "
-- Create sample table and rows (idempotent)
begin
  execute immediate '
    CREATE TABLE test_user.employees (
      id   NUMBER PRIMARY KEY,
      name VARCHAR2(50),
      role VARCHAR2(50)
    )';
exception when others then
  if SQLCODE != -955 then raise; end if; -- -955: name is already used by existing object
end;
/
merge into test_user.employees t
using (select 1 id,'Alice' name,'Engineer' role from dual union all
       select 2,'Bob','Manager' from dual union all
       select 3,'Charlie','Analyst' from dual) s
on (t.id=s.id)
when not matched then insert (id,name,role) values (s.id,s.name,s.role);
commit;
"

# =========================
# POSTGRES
# =========================
echo "[*] Starting PostgreSQL container..."
docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --name "$POSTGRES_CONTAINER" \
  -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  -p "${POSTGRES_PORT}:5432" \
  "$POSTGRES_IMAGE" >/dev/null

echo "[*] Waiting for PostgreSQL to accept connections..."
for i in {1..60}; do
  if docker exec "$POSTGRES_CONTAINER" pg_isready -U postgres >/dev/null 2>&1; then
    echo "    Postgres is ready."
    break
  fi
  sleep 1
  if [[ $i -eq 60 ]]; then
    echo "[!] Postgres did not become ready. Logs:"
    docker logs "$POSTGRES_CONTAINER" | tail -n 100
    exit 1
  fi
done

echo "[*] Creating empty target database in PostgreSQL (idempotent)..."
if docker exec -i "$POSTGRES_CONTAINER" \
  psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'" | grep -q 1; then
  echo "    Database '${POSTGRES_DB}' already exists. Skipping."
else
  echo "    Creating database '${POSTGRES_DB}'..."
  docker exec -i "$POSTGRES_CONTAINER" \
    psql -U postgres -c "CREATE DATABASE ${POSTGRES_DB};"
fi


# Fallback if dblink isn't there; try plain CREATE (ignore error if exists)
docker exec -i "$POSTGRES_CONTAINER" psql -U postgres -c "CREATE DATABASE ${POSTGRES_DB};" >/dev/null 2>&1 || true

# =========================
# CONNECTION INFO
# =========================
cat <<INFO

[✅] Both databases are ready!

Oracle:
  host: localhost
  port: ${ORACLE_PORT}
  service: ${ORACLE_SERVICE}
  user: system
  pass: ${ORACLE_PASSWORD}
  sample schema: test_user / test_pass
  sample table: test_user.employees

PostgreSQL:
  host: localhost
  port: ${POSTGRES_PORT}
  db: ${POSTGRES_DB}
  user: postgres
  pass: ${POSTGRES_PASSWORD}

Tip: rerun with './$(basename "$0") reset' to rebuild both fresh.
INFO
