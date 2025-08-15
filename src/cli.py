from __future__ import annotations
import sys
from collections import defaultdict
from typing import List, Dict
import typer

import oracledb
import psycopg

import oracle_introspect
import ddl_emit
import apply_ddl 
import data_loader

ERROR_MSG = "Usage: python3 cli.py migrate "

app = typer.Typer(add_completion=False, help="Prototype: Oracle → Postgres one-shot migration")

def build_structures(intro: oracle_introspect.OracleIntrospector, owner: str):
    return

#def make_tablespecs(owner: str, pg_schema: str, table_defs: Dict[str, List[dict]]) -> List[TableSpec]:
#    return

def validate_counts(oracle_dsn: str, oracle_user: str, oracle_password: str,
                    pg_dsn: str, owner: str, pg_schema: str, tables: List[str]) -> Dict[str, dict]:
    return

@app.command()
def migrate(
    owner: str = typer.Option(..., help="Oracle schema/owner (e.g., HR)"),
    oracle_dsn: str = typer.Option(..., help="host:port/service or EZCONNECT"),
    oracle_user: str = typer.Option(...),
    oracle_password: str = typer.Option(..., prompt=True, hide_input=True),
    #pg_dsn: str = typer.Option(..., help="postgresql://user:pass@host:5432/db"),
    pg_schema: str = typer.Option("public", help="Target schema in Postgres"),
    parallel: int = typer.Option(4, help="Parallel tables during copy"),
    arraysize: int = typer.Option(10000, help="Oracle fetch arraysize"),
    batch_rows: int = typer.Option(50000, help="Rows per COPY chunk")
):
    # TODO: Implement migration logic here
    #print("Migration command invoked with:")
    #print(f"owner={owner}, oracle_dsn={oracle_dsn}, oracle_user={oracle_user}, pg_dsn={pg_dsn}, pg_schema={pg_schema}")

    oracle = oracle_introspect.connect_oracle(oracle_user, oracle_password, oracle_dsn)

    schema = oracle_introspect.get_schema(oracle)

    print(schema)    

    oracle_introspect.get_tables(oracle, owner=owner)

    oracle_introspect.get_columns(oracle, owner)
    return

def parse_inputs():

    for input in sys.argv:
        print(input)

    return

if __name__ == "__main__":
    app()