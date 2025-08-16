from __future__ import annotations
import sys
from collections import defaultdict
from typing import List, Dict
import typer

import oracledb
import psycopg

import oracle_introspect
from oracle_introspect import OracleIntrospector
from ddl_emit import DDLEmitter
import apply_ddl 
import data_loader

ERROR_MSG = "Usage: python3 cli.py migrate "

app = typer.Typer(add_completion=False, help="Prototype: Oracle → Postgres one-shot migration")

def build_structures(intro: oracle_introspect.OracleIntrospector, owner: str):
    return

def make_tablespecs(owner: str, pg_schema: str, table_defs: Dict[str, List[dict]]) -> List[data_loader.TableSpec]:
    return

def validate_counts(oracle_dsn: str, oracle_user: str, oracle_password: str,
                    pg_dsn: str, owner: str, pg_schema: str, tables: List[str]) -> Dict[str, dict]:
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
    typer.secho("1) Discovering Oracle schema...", fg="cyan")
    intro = OracleIntrospector(oracle_dsn, oracle_user, oracle_password, arraysize=arraysize)
    tables, table_defs, pk_defs = build_structures(intro, owner)

    if not tables:
        typer.secho("No tables found.", fg="red"); raise typer.Exit(code=1)

    typer.secho(f"Found {len(tables)} tables", fg="green")

    typer.secho("2) Emitting Postgres DDL...", fg="cyan")
    emitter = DDLEmitter(pg_schema, fks_deferrable=True)
    stmts = []
    stmts += emitter.emit_create_schema()
    stmts += emitter.emit_tables(table_defs)
    stmts += emitter.emit_pks(pk_defs)

    typer.secho("3) Applying DDL on Postgres...", fg="cyan")
    apply_ddl.apply_statements(pg_dsn, stmts)
    typer.secho("DDL applied", fg="green")

    typer.secho("4) Copying data with COPY ...", fg="cyan")
    specs = make_tablespecs(owner, pg_schema, table_defs)
    loader = data_loader.DataLoader(
        ora_dsn=oracle_dsn, ora_user=oracle_user, ora_password=oracle_password,
        pg_dsn=pg_dsn, arraysize=arraysize, copy_batch_rows=batch_rows,
        parallelism=parallel, out_dir="./out"
    )
    stats = loader.load_schema(specs)
    ok_tables = sum(1 for s in stats.values() if s.get("status") == "ok")
    typer.secho(f"Loaded {ok_tables}/{len(specs)} tables", fg="green")

    typer.secho("5) Validating (row counts)...", fg="cyan")
    counts = validate_counts(oracle_dsn, oracle_user, oracle_password, pg_dsn, owner, pg_schema, tables)
    mismatches = [t for t, r in counts.items() if not r["match"]]
    if mismatches:
        typer.secho(f"Rowcount mismatches: {mismatches}", fg="yellow")
        typer.echo(counts)
        raise typer.Exit(code=2)

    typer.secho("Migration complete!", fg="green")
    return

def parse_inputs():

    for input in sys.argv:
        print(input)

    return

if __name__ == "__main__":
    app()