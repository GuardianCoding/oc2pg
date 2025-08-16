from __future__ import annotations
import sys
from collections import defaultdict
from typing import List, Dict
import typer

import oracledb
import psycopg

from oracle_introspect import OracleIntrospector
import valid
from ddl_emit import emit_create_table, emit_constraints, emit_indexes, emit_sequences, compose_plan, NameMapper
import apply_ddl 
import data_loader
import config as cf
import report as rp

ERROR_MSG = "Usage: python3 cli.py migrate "

app = typer.Typer(add_completion=False, help="Prototype: Oracle → Postgres one-shot migration")

def build_structures(intro: OracleIntrospector, owner: str):
    """
    Returns:
      tables:     [table_name, ...]
      table_defs: {table_name: [column_dict, ...]}
      pk_defs:    {table_name: [pk_col, ...]}
      fk_defs:    [fk_dict, ...]
      idx_defs:   [index_dict, ...]
      seq_defs:   [seq_dict, ...]
    Shapes match ddl_emit.emit_* expectations.
    """

    # Tables
    tables = [r["table_name"] for r in intro.get_tables()]   # uses ALL_TABLES filtered by owner

    # Columns grouped per table
    cols = intro.get_columns()
    table_defs: Dict[str, List[dict]] = {}
    for c in cols:
        table_defs.setdefault(c["table_name"], []).append({
            "column_name":  c["column_name"],
            "data_type":    c["data_type"],
            "data_precision": c.get("data_precision"),
            "data_scale":     c.get("data_scale"),
            "nullable":       c.get("nullable", True),
            "data_default":   c.get("data_default"),
        })

    # Primary keys (convert list of dicts → dict[table] = [cols])
    pk_defs: Dict[str, List[str]] = {}
    for pk in intro.get_pk():
        pk_defs[pk["table_name"]] = pk["columns"]
    
    # Foreign keys
    fk_defs = intro.get_fk()

    # Indexes (already shaped: index_name/table_name/columns/uniqueness)
    idx_defs = intro.get_indexes()

    # Sequences (already shaped)
    seq_defs = intro.get_sequences()

    return tables, table_defs, pk_defs, fk_defs, idx_defs, seq_defs

def make_tablespecs(owner: str, pg_schema: str, table_defs: Dict[str, List[dict]]) -> List[cf.TableSpec]:
    specs: List[cf.TableSpec] = []
    for tname, cols in table_defs.items():
        # keep the discovered column order
        colnames = [c["column_name"].lower() for c in cols]
        specs.append(cf.TableSpec(owner=owner, name=tname.lower(), columns=colnames, pg_schema=pg_schema))
    return specs

#TODO add alternative way to launch program with yaml file

@app.command()
def migrate(
    oracle_dsn: str = typer.Option(..., help="host:port/service or EZCONNECT"),
    oracle_user: str = typer.Option(...),
    oracle_password: str = typer.Option(..., prompt=True, hide_input=True),
    pg_dsn: str = typer.Option(..., help="postgresql://user:pass@host:5432/db"),
    pg_schema: str = typer.Option("public", help="Target schema in Postgres"),
    parallel: int = typer.Option(4, help="Parallel tables during copy"),
    arraysize: int = typer.Option(10000, help="Oracle fetch arraysize"),
    batch_rows: int = typer.Option(50000, help="Rows per COPY chunk")
):
    """
    One-shot: discover → DDL → apply → copy → rowcount-validate.
    """
    oracle = cf.OracleCfg(oracle_dsn, oracle_user, oracle_password, arraysize)
    postgres = cf.PostgresCfg(pg_dsn, pg_schema, parallel, batch_rows)
    output = cf.OutputCfg()

    report = rp.Report(output)

    # Discovering Oracle Schema
    typer.secho("1) Discovering Oracle schema...", fg="cyan")
    report.log_report("1) Discovering Oracle schema...")

    intro = OracleIntrospector(oracle)
    tables, table_defs, pk_defs, fk_defs, idx_defs, seq_defs = build_structures(intro, intro.owner)

    if not tables:
        report.log_report("No tables found. Exit code 1.")
        typer.secho("No tables found.", fg="red"); raise typer.Exit(code=1)

    typer.secho(f"Found {len(tables)} tables", fg="green")
    report.log_report(f"Found {len(tables)} tables")

    # TODO: fix how emitter works here
    typer.secho("2) Emitting Postgres DDL...", fg="cyan")
    report.log_report("2) Emitting Postgres DDL...")

    # Prepare the triplets the emitter expects
    tables_triplets = []
    for tname in tables:
        cols_for_t = table_defs.get(tname, [])
        pks_for_t  = pk_defs.get(tname)
        tables_triplets.append(({"table_name": tname}, cols_for_t, pks_for_t))

    # One deterministic plan: sequences → tables (with inline PK) → FKs → indexes
    namemap = NameMapper()
    ddl_sql = compose_plan(
        schema=postgres.schema,
        seq_defs=seq_defs,
        tables=tables_triplets,
        fks=fk_defs,
        indexes=idx_defs,
        namemap=namemap,
    )

    # Split into statements and apply (skip empties)
    stmts = [s.strip() + ";" for s in ddl_sql.split(";") if s.strip()]

    typer.secho("3) Applying DDL on Postgres...", fg="cyan")
    report.log_report("3) Applying DDL on Postgres...")
    apply_ddl.apply_statements(postgres, stmts)
    typer.secho("DDL applied", fg="green")
    report.log_report("DDL applied")

    typer.secho("4) Copying data with COPY ...", fg="cyan")
    report.log_report("4) Copying data with COPY ...")
    specs = make_tablespecs(intro.owner, postgres.schema, table_defs)
    loader = data_loader.DataLoader(oracle, postgres, output)

    stats = loader.load_schema(specs)
    ok_tables = sum(1 for s in stats.values() if s.get("status") == "ok")
    typer.secho(f"Loaded {ok_tables}/{len(specs)} tables", fg="green")
    report.log_report(f"Loaded {ok_tables}/{len(specs)} tables")

    typer.secho("5) Validating (row counts)...", fg="cyan")
    report.log_report("5) Validating (row counts)...")

    counts = valid.validate_counts(oracle, postgres, tables, intro.owner, report)
    mismatches = [t for t, r in counts.items() if not r["match"]]
    if mismatches:
        typer.secho(f"Rowcount mismatches: {mismatches}. Exit code 2", fg="yellow")
        typer.echo(counts)
        report.log_report(f"Rowcount mismatches: {mismatches}")
        report.log_report(counts)
        raise typer.Exit(code=2)

    typer.secho("Migration complete!", fg="green")
    report.log_report("Migration complete!")
    return


if __name__ == "__main__":
    app()