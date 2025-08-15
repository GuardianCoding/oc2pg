from typing import Iterable
import psycopg

def apply_statements(pg_dsn: str, statements: Iterable[str]) -> None:
    """ Applies list of SQL statements to PostgresSQL database. """

    with psycopg.connect(pg_dsn) as conn:
        with conn.cursor() as cur:
            for s in statements:
                cur.execute(s)
        conn.commit()

def apply_sql_file(pg_dsn: str, path: str) -> None:
    """ Executes SQL commands on PostgresSQL from file. """
    
    sql = open(path, "r", encoding="utf-8").read()
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(sql)
