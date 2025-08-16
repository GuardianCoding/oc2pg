from typing import Iterable
import psycopg
from config import PostgresCfg
from typer import secho

def apply_statements(pg: PostgresCfg, statements: Iterable[str]) -> None:
    """ Applies list of SQL statements to PostgresSQL database. """

    first_error = None
    try:
        with psycopg.connect(pg.dsn, autocommit=True) as pgconn:
            with pgconn.cursor() as cur:
                secho(f"Applying {len(statements)} statements to migration_target ...", fg="cyan")
                for i, stmt in enumerate(statements, 1):
                    try:
                        cur.execute(stmt)
                        head = stmt.splitlines()[0]
                        secho(f"[OK] {i}: {head[:100]}", fg="cyan")
                    except Exception as e:
                        if first_error is None:
                            first_error = (i, stmt, e)
                        secho(f"\n[ERR] {i}: {stmt.splitlines()[0][:120]}\n--> {e}\n", fg="red")
    except Exception as e:
        secho(f"Failed to connect/apply to Postgres: {e}", fg="red")
        return

    if first_error:
        i, stmt, e = first_error
        secho("----- FIRST REAL ERROR SUMMARY -----", fg="yellow")
        secho(f"Statement #{i}:\n{stmt}\n\nError:\n{e}\n", fg="red")
    else:
        secho(" DDL applied to PostgreSQL (migration_target) with no errors.", fg="cyan")

def apply_sql_file(pg: PostgresCfg, path: str) -> None:
    """ Executes SQL commands on PostgresSQL from file. """
    
    sql = open(path, "r", encoding="utf-8").read()
    with psycopg.connect(pg.dsn, autocommit=True) as conn:
        conn.execute(sql)
