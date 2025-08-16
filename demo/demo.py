# demo.py
from __future__ import annotations

import sys, pathlib, time, os
from typing import List, Dict
import psycopg
from psycopg import sql
import pandas as pd

# Make "src" importable when running from repo root
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

import streamlit as st

# Backend (your modules)
from src import oracle_introspect as intro
from src import ddl_emit as emit
from src import data_loader
from src import valid
from src import config as cf

# ---------- Small helpers ----------

def pg_list_tables(dsn: str, schema: str):
    """Return a list of table names in a Postgres schema."""
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tablename
                FROM pg_catalog.pg_tables
                WHERE schemaname = %s
                ORDER BY tablename
                """,
                (schema,)
            )
            return [r[0] for r in cur.fetchall()]

def expected_pg_tables_from_oracle_names(oracle_table_names: list[str]) -> list[str]:
    """Normalize Oracle table names the same way DDL emitter does, to compare against PG."""
    nm = emit.NameMapper()
    return [nm.pg_ident(t) for t in oracle_table_names]

def log(msg: str):
    # Only append to log buffer; do not render inline in the main area.
    st.session_state.logs.append(msg)

def ensure_state():
    if "logs" not in st.session_state:
        st.session_state.logs = []
    if "discovery" not in st.session_state:
        st.session_state.discovery = None
    if "ddl_sql" not in st.session_state:
        st.session_state.ddl_sql = ""
    if "tables" not in st.session_state:
        st.session_state.tables = []

def _mk_progress(total: int):
    """
    Creates a top-of-page progress bar and status line.
    Returns (update, done) callables.
    - update(i, msg=None): set progress for step i of total, optionally show a status message
    - done(): clear the progress UI
    """
    # Use persistent placeholders created near the "Progress" header
    ph = st.session_state.get("progress_ph")
    sh = st.session_state.get("status_ph")
    if ph is None or sh is None:
        ph = st.empty()
        sh = st.empty()
        st.session_state.progress_ph = ph
        st.session_state.status_ph = sh

    bar = ph.progress(0)

    def update(i: int, msg: str | None = None):
        pct = int(max(0, min(100, round((i / max(1, total)) * 100))))
        bar.progress(pct)
        if msg is not None:
            sh.write(msg)

    def done():
        ph.empty()
        sh.empty()

    return update, done

def apply_statements_verbose(pg: cf.PostgresCfg, statements: List[str], on_progress=None):
    """
    Apply SQL statements to Postgres and stream verbose progress into the Streamlit UI.
    Returns a list of (sql, exception) for failed statements (empty if none).
    """
    errs: List[tuple[str, Exception]] = []
    try:
        with psycopg.connect(pg.dsn, autocommit=True) as conn:
            dbname = getattr(conn.info, "dbname", "postgres")
            log(f"Applying {len(statements)} statements to {dbname} ...")
            with conn.cursor() as cur:
                for i, stmt in enumerate(statements, 1):
                    head = (stmt or "").splitlines()[0]
                    if on_progress:
                        on_progress(i, f"Applying {i}/{len(statements)}: {head[:60]}")
                    try:
                        cur.execute(stmt)
                        log(f"[OK] {i}: {head[:100]}")
                    except Exception as e:
                        errs.append((stmt, e))
                        log(f"\n[ERR] {i}: {head[:120]}\n--> {e}\n")
            if errs:
                first_stmt, first_err = errs[0]
                first_head = (first_stmt or "").splitlines()[0]
                log("----- FIRST REAL ERROR SUMMARY -----")
                log(f"Statement #{statements.index(first_stmt)+1}:\n{first_stmt}\n\nError:\n{first_err}\n")
            else:
                log(f" DDL applied to PostgreSQL ({dbname}) with no errors.")
    except Exception as e:
        log(f"Failed to connect/apply to Postgres: {e}")
    return errs

def pg_table_rowcount(dsn: str, schema: str, table: str) -> int:
    """Return row count for a table: schema.table."""
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{}")
                   .format(sql.Identifier(schema), sql.Identifier(table))
            )
            return int(cur.fetchone()[0])

def pg_sample_rows(dsn: str, schema: str, table: str, limit: int = 50):
    """Return (columns, rows) sample from schema.table."""
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT * FROM {}.{} LIMIT {}")
            cur.execute(query.format(sql.Identifier(schema), sql.Identifier(table), sql.Literal(limit)))
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return cols, rows

# ---------- UI ----------
st.set_page_config(page_title="OC2PG Demo", layout="wide")
st.title("Oracle → PostgreSQL Demo")

ensure_state()

col1, col2 = st.columns([2, 1])
with col1:
    st.subheader("Progress")
    # Persistent placeholders for the top progress UI
    if "progress_ph" not in st.session_state:
        st.session_state.progress_ph = st.empty()
    if "status_ph" not in st.session_state:
        st.session_state.status_ph = st.empty()
with col2:
    st.subheader("Quick Stats")

with st.sidebar:
    st.header("Login")
    # Oracle
    ora_host = st.text_input("Oracle host", "localhost")
    ora_port = st.number_input("Oracle port", 1, 65535, 1521)
    ora_service = st.text_input("Oracle service", "XEPDB1")
    ora_user = st.text_input("Oracle user", "test_user")
    ora_pass = st.text_input("Oracle password", type="password", value="test_pass")
    ora_arraysize = st.number_input("Oracle arraysize", 1000, 500000, 10000, step=1000)

    # Postgres
    pg_user = st.text_input("Postgres user", "postgres")
    pg_pass = st.text_input("Postgres password", type="password", value="postgres")
    pg_host = st.text_input("Postgres host", "localhost")
    pg_port = st.number_input("Postgres port", 1, 65535, 5432)
    pg_db   = st.text_input("Postgres database", "migration_target")
    pg_schema = st.text_input("Target schema", "public")
    pg_parallel = st.number_input("COPY parallelism", 1, 64, 4)
    pg_batch_rows = st.number_input("COPY batch rows", 1000, 1_000_000, 50_000, step=1000)

    st.header("Filters")
    owner = st.text_input("Oracle owner/schema", "TEST_USER")
    include_csv = st.text_input("Include tables (comma-separated)", "")
    exclude_csv = st.text_input("Exclude tables (comma-separated)", "")

    st.header("Actions")
    btn_discover = st.button("1) Discover")
    btn_emit     = st.button("2) Generate DDL")
    btn_apply    = st.button("3) Apply DDL")
    btn_copy     = st.button("4) Copy Data (COPY)")
    btn_validate = st.button("5) Validate Row Counts")

# Build cfgs from sidebar
oracle_dsn = f"{ora_host}:{ora_port}/{ora_service}"
oracle_cfg = cf.OracleCfg(owner=owner, dsn=oracle_dsn, user=ora_user, password=ora_pass, arraysize=int(ora_arraysize))
pg_dsn = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
postgres_cfg = cf.PostgresCfg(dsn=pg_dsn, schema=pg_schema, copy_parallelism=int(pg_parallel), copy_batch_rows=int(pg_batch_rows))

include_tables = [t.strip() for t in include_csv.split(",") if t.strip()]
exclude_tables = [t.strip() for t in exclude_csv.split(",") if t.strip()]

# ---------- Main area ----------

# 1) DISCOVER
if btn_discover:
    st.session_state.logs = []
    st.session_state.discovery = None
    st.session_state.ddl_sql = ""
    st.session_state.tables = []

    update, done = _mk_progress(4)
    update(1, "Connecting to Oracle…")

    log("Connecting to Oracle and discovering schema...")
    try:
        intro = intro.OracleIntrospector(cfg=oracle_cfg)
        if owner:
            intro.set_current_schema(owner)
            ora_owner = owner
        else:
            ora_owner = intro.cfg.owner

        log(f"Owner = {ora_owner or '(unknown)'}")
        update(2, "Fetching metadata from Oracle…")

        tables_raw = intro.get_tables(owner=ora_owner)
        # apply include/exclude filters client-side as well
        if include_tables:
            want = {t.lower() for t in include_tables}
            tables_raw = [r for r in tables_raw if r["table_name"].lower() in want]
        if exclude_tables:
            drop = {t.lower() for t in exclude_tables}
            tables_raw = [r for r in tables_raw if r["table_name"].lower() not in drop]

        tables = [r["table_name"] for r in tables_raw]
        cols   = intro.get_columns(owner=ora_owner)
        pks    = intro.get_pk(owner=ora_owner)
        fks    = intro.get_fk(owner=ora_owner)
        idxs   = intro.get_indexes(owner=ora_owner)
        seqs   = intro.get_sequences(owner=ora_owner)

        st.session_state.discovery = dict(tables=tables, cols=cols, pks=pks, fks=fks, idxs=idxs, seqs=seqs, owner=ora_owner)
        st.session_state.tables = tables

        update(3, "Rendering previews…")

        if tables:
            st.write("### Discovered Tables in Orcale")
            st.table(tables)
        else:
            st.info("No tables discovered.")

        # New code: Query and display PostgreSQL tables
        try:
            pg_tables = pg_list_tables(postgres_cfg.dsn, postgres_cfg.schema)
            st.write("### PostgreSQL Tables")
            st.table(pg_tables)
        except Exception as e:
            st.error(f"Failed to retrieve PostgreSQL tables: {e}")

        log(f"Discovered: {len(tables)} tables, {len(pks)} PKs, {len(fks)} FKs, {len(idxs)} indexes, {len(seqs)} sequences")
        with col2:
            st.metric("Tables", len(tables))
            st.metric("PKs", len(pks))
            st.metric("FKs", len(fks))
            st.metric("Indexes", len(idxs))
            st.metric("Sequences", len(seqs))
        update(4, "Discovery complete.")
        done()
    except Exception as e:
        done()
        st.error(f"Discovery failed: {e}")

# 2) EMIT DDL
if btn_emit:
    if not st.session_state.discovery:
        st.warning("Run discovery first.")
    else:
        d = st.session_state.discovery
        log("Generating Postgres DDL plan…")

        update, done = _mk_progress(2)
        update(1, "Composing DDL plan…")

        # Build (tdef, cols, pks) triples expected by compose_plan
        cols_by_tbl: Dict[str, List[dict]] = {}
        for c in d["cols"]:
            cols_by_tbl.setdefault(c["table_name"], []).append({
                "column_name": c["column_name"],
                "data_type": c["data_type"],
                "data_precision": c.get("data_precision"),
                "data_scale": c.get("data_scale"),
                "nullable": c.get("nullable", True),
                "data_default": c.get("data_default"),
            })

        pk_by_tbl = {p["table_name"]: p.get("columns", []) for p in d["pks"]}
        triples = []
        for t in d["tables"]:
            triples.append(({"table_name": t}, cols_by_tbl.get(t, []), pk_by_tbl.get(t)))

        nm = emit.NameMapper()
        ddl_sql = emit.compose_plan(
            schema=postgres_cfg.schema,
            seq_defs=d["seqs"],
            tables=triples,
            fks=d["fks"],
            indexes=d["idxs"],
            namemap=nm
        )
        st.session_state.ddl_sql = ddl_sql
        st.code(ddl_sql, language="sql")
        update(2, "DDL generation complete.")
        done()
        log("DDL generated.")

# 3) APPLY DDL
if btn_apply:
    if not st.session_state.ddl_sql:
        st.warning("Generate DDL first.")
    else:
        log("Applying DDL to Postgres…")
        stmts = [s.strip() + ";" for s in st.session_state.ddl_sql.split(";") if s.strip()]
        update, done = _mk_progress(len(stmts))
        errs = apply_statements_verbose(postgres_cfg, stmts, on_progress=update)
        if errs:
            st.error(f"Some statements failed: {len(errs)}")
            for i, (sql, err) in enumerate(errs[:10], 1):
                st.write(f"[ERR {i}] {err}")
                with st.expander(f"SQL {i}"):
                    st.code(sql, language="sql")
        else:
            st.success("DDL applied cleanly.")
        done()
        log("DDL phase done.")

        # --- Proof the DDL applied: compare expected vs actual in Postgres ---
        if st.session_state.discovery:
            d = st.session_state.discovery
            exp = set(expected_pg_tables_from_oracle_names(d["tables"]))
            try:
                actual = set(pg_list_tables(postgres_cfg.dsn, postgres_cfg.schema))
            except Exception as e:
                st.error(f"Could not verify in Postgres: {e}")
                actual = set()

            st.subheader("DDL Proof & Data Preview")
            common = sorted(list(exp & actual))
            missing = sorted(list(exp - actual))

            m1, m2, m3, m4 = st.columns(4)
            with m1:
                st.metric("Expected tables", len(exp))
            with m2:
                st.metric("Actual tables in PG", len(actual))
            with m3:
                st.metric("Matched", len(common))
            with m4:
                st.metric("Missing", len(missing))

            if missing:
                with st.expander("Missing tables in Postgres", expanded=False):
                    st.write(missing[:100])

            if common:
                st.write("\n**Pick a table to preview data:**")
                sel = st.selectbox("Table", options=common, index=0, key="preview_table")
                try:
                    cnt = pg_table_rowcount(postgres_cfg.dsn, postgres_cfg.schema, sel)
                    st.info(f"Row count for `{sel}`: {cnt}")
                    cols, rows = pg_sample_rows(postgres_cfg.dsn, postgres_cfg.schema, sel, limit=100)
                    if rows:
                        df = pd.DataFrame(rows, columns=cols)
                        st.dataframe(df, use_container_width=True, height=300)
                    else:
                        st.warning("No rows to show (table is empty).")
                except Exception as e:
                    st.error(f"Preview failed for {sel}: {e}")

                with st.expander("Quick summary counts"):
                    summary = []
                    for t in common:
                        try:
                            c = pg_table_rowcount(postgres_cfg.dsn, postgres_cfg.schema, t)
                        except Exception as e:
                            c = None
                        summary.append({"table": t, "rows": c})
                    sdf = pd.DataFrame(summary)
                    st.dataframe(sdf, use_container_width=True, height=260)
            else:
                st.warning("No common tables between expected (from Oracle) and actual Postgres schema.")

# 4) COPY DATA
if btn_copy:
    if not st.session_state.discovery:
        st.warning("Run discovery first.")
    else:
        d = st.session_state.discovery
        log("Copying data with COPY (parallel)…")

        # Build TableSpec list for the DataLoader from discovery metadata
        cols_by_tbl: Dict[str, List[str]] = {}
        for c in d["cols"]:
            cols_by_tbl.setdefault(c["table_name"], []).append(c["column_name"])

        wanted_tables: List[str] = d["tables"]  # already filtered by include/exclude during discovery
        specs: List[cf.TableSpec] = [
            cf.TableSpec(
                owner=d["owner"],
                name=tname,
                columns=cols_by_tbl.get(tname, []),
                pg_schema=postgres_cfg.schema,
                where_clause=None,
            )
            for tname in wanted_tables
        ]

        if not specs:
            st.info("No tables to copy (specs list is empty).")
        else:
            try:
                # Top progress UI
                update, done = _mk_progress(len(specs))
                loader = data_loader.DataLoader(oracle_cfg, postgres_cfg, cf.OutputCfg())

                # Use the DataLoader's bulk API to migrate all tables
                stats = loader.load_schema(specs)
                done()

                ok = sum(1 for s in stats.values() if s.get("status") == "ok")
                st.success(f"Loaded {ok}/{len(stats)} tables")
                st.json(stats)
                log(f"Loaded {ok}/{len(stats)} tables")

                # Optional: quick preview of the first successfully loaded table
                good_tables = [t for t, s in stats.items() if s.get("status") == "ok"]
                if good_tables:
                    sel = st.selectbox("Preview a loaded table", options=sorted(good_tables))
                    # Map Oracle table name -> Postgres identifier (same normalization as DDL emitter)
                    nm_preview = emit.NameMapper()
                    pg_sel = nm_preview.pg_ident(sel)
                    try:
                        cnt = pg_table_rowcount(postgres_cfg.dsn, postgres_cfg.schema, pg_sel)
                        st.info(f"Row count for `{sel}` (PG: `{pg_sel}`): {cnt}")
                        cols, rows = pg_sample_rows(postgres_cfg.dsn, postgres_cfg.schema, pg_sel, limit=100)
                        if rows:
                            df = pd.DataFrame(rows, columns=cols)
                            st.dataframe(df, use_container_width=True, height=300)
                        else:
                            st.warning("No rows to show (table is empty).")
                    except Exception as e:
                        st.error(f"Preview failed for {sel} (PG: {pg_sel}): {e}")
                else:
                    st.info("No tables were loaded successfully to preview.")
            except Exception as e:
                st.error(f"Copy failed: {e}")

# 5) VALIDATE
if btn_validate:
    if not st.session_state.discovery:
        st.warning("Run discovery first.")
    else:
        d = st.session_state.discovery
        log("Validating row counts (Oracle vs Postgres)…")
        try:
            counts = valid.validate_counts(oracle_cfg, postgres_cfg, d["tables"], d["owner"], report=None)
            mismatches = [t for t, r in counts.items() if not r["match"]]
            st.json(counts)
            if mismatches:
                st.warning(f"Rowcount mismatches: {mismatches}")
            else:
                st.success("All table counts match!")
            log("Validation finished.")
        except Exception as e:
            st.error(f"Validation failed: {e}")

st.divider()
st.subheader("Logs")
for line in st.session_state.logs:
    st.text(line)