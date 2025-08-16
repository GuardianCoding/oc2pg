import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1] / "src"))

def _split_sql(sql: str):
    buf, in_s, esc = [], False, False
    for ch in sql:
        if in_s:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == "'":
                in_s = False
        else:
            if ch == "'":
                in_s = True
            elif ch == ";":
                stmt = "".join(buf).strip()
                if stmt:
                    yield stmt
                buf = []
                continue
        buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        yield tail

import psycopg
import apply_ddl
import oracle_introspect as intro

def group_by_table(rows, key="table_name"):
    d = {}
    for r in rows:
        d.setdefault(r[key], []).append(r)
    return d

def main():
    # --- Step 1: Oracle introspection ---
    ora = intro.OracleIntrospector("system", "oracle", "localhost:1521/XEPDB1")
    print("Connected to Oracle.")
    # Target only the application schema we created in Docker, not SYSTEM
    target_owner = "TEST_USER"
    try:
        with ora.conn.cursor() as c:
            c.execute("ALTER SESSION SET CURRENT_SCHEMA = TEST_USER")
    except Exception:
        # harmless if it fails; we'll pass owner explicitly below
        pass
    owner = target_owner
    print(f"Owner={owner}")

    cols = ora.get_columns()
    pks  = ora.get_pk()
    fks  = ora.get_fk()
    idxs = ora.get_indexes()
    seqs = ora.get_sequences()
    tables_list = ora.get_tables()

    print(f"Tables: {len(tables_list)}, PKs: {len(pks)}, FKs: {len(fks)}, Indexes: {len(idxs)}, Seqs: {len(seqs)}")

    # --- Build (table_def, columns, pkeys) triples ---
    cols_by_tbl = group_by_table(cols, "table_name")
    pk_by_tbl = {p["table_name"]: p.get("columns", []) for p in pks}
    table_names = [t["table_name"] for t in tables_list] or sorted(cols_by_tbl.keys())
    tables = [({"table_name": t}, cols_by_tbl.get(t, []), pk_by_tbl.get(t)) for t in table_names]

    # --- Step 2: Generate Postgres DDL ---
    sql = apply_ddl.compose_plan(
        schema="public",
        seq_defs=seqs,
        tables=tables,
        fks=fks,
        indexes=idxs,
        namemap=apply_ddl.NameMapper()
    )
    with open("output.sql", "w") as f:
        f.write(sql)

    # --- Step 3: Apply to PostgreSQL (autocommit so one error doesn't abort all) ---
    conninfo = "postgresql://postgres:postgres@localhost:5432/migration_target"
    first_error = None
    try:
        with psycopg.connect(conninfo, autocommit=True) as pgconn:
            with pgconn.cursor() as cur:
                statements = list(_split_sql(sql))
                print(f"Applying {len(statements)} statements to migration_target ...")
                for i, stmt in enumerate(statements, 1):
                    try:
                        cur.execute(stmt)
                        head = stmt.splitlines()[0]
                        print(f"[OK] {i}: {head[:100]}")
                    except Exception as e:
                        if first_error is None:
                            first_error = (i, stmt, e)
                        print(f"\n[ERR] {i}: {stmt.splitlines()[0][:120]}\n--> {e}\n")
    except Exception as e:
        print(f"Failed to connect/apply to Postgres: {e}")
        return

    if first_error:
        i, stmt, e = first_error
        print("----- FIRST REAL ERROR SUMMARY -----")
        print(f"Stmt #{i}:\n{stmt}\n\nError:\n{e}\n")
    else:
        print("✅ DDL applied to PostgreSQL (migration_target) with no errors.")


if __name__ == "__main__":
    main()