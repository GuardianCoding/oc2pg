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
    return

def make_tablespecs(owner: str, pg_schema: str, table_defs: Dict[str, List[dict]]) -> List[data_loader.TableSpec]:
    return

@app.command()
def migrate(
    owner: str = typer.Option(..., help="Oracle schema/owner (e.g., HR)"),
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
    oracle = cf.OracleCfg(owner, oracle_dsn, oracle_user, oracle_password, arraysize)
    postgres = cf.PostgresCfg(pg_dsn, pg_schema, parallel, batch_rows)
    output = cf.OutputCfg()

    report = rp.Report(output)

    # Discovering Oracle Schema
    # TODO
    typer.secho("1) Discovering Oracle schema...", fg="cyan")
    report.log_report("1) Discovering Oracle schema...")
    intro = OracleIntrospector(oracle)
    tables, table_defs, pk_defs = build_structures(intro, oracle.owner)

    if not tables:
        report.log_report("No tables found. Exit code 1.")
        typer.secho("No tables found.", fg="red"); raise typer.Exit(code=1)

    typer.secho(f"Found {len(tables)} tables", fg="green")
    report.log_report(f"Found {len(tables)} tables")

    # TODO: fix how emitter works here
    typer.secho("2) Emitting Postgres DDL...", fg="cyan")
    report.log_report("2) Emitting Postgres DDL...")

    # emitter = DDLEmitter(postgres.schema, fks_deferrable=True)
    stmts = []
    # stmts += emitter.emit_create_schema()
    # stmts += emitter.emit_tables(table_defs)
    # stmts += emitter.emit_pks(pk_defs)

    typer.secho("3) Applying DDL on Postgres...", fg="cyan")
    report.log_report("3) Applying DDL on Postgres...")
    apply_ddl.apply_statements(postgres.dsn, stmts)
    typer.secho("DDL applied", fg="green")
    report.log_report("DDL applied")

    typer.secho("4) Copying data with COPY ...", fg="cyan")
    report.log_report("4) Copying data with COPY ...")
    specs = make_tablespecs(oracle.owner, postgres.schema, table_defs)
    loader = data_loader.DataLoader(oracle, postgres, output)

    stats = loader.load_schema(specs)
    ok_tables = sum(1 for s in stats.values() if s.get("status") == "ok")
    typer.secho(f"Loaded {ok_tables}/{len(specs)} tables", fg="green")
    report.log_report(f"Loaded {ok_tables}/{len(specs)} tables")

    typer.secho("5) Validating (row counts)...", fg="cyan")
    report.log_report("5) Validating (row counts)...")

    counts = valid.validate_counts(oracle, postgres, tables)
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