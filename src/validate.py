import oracledb
import psycopg
from sys import stderr
from typing import List, Dict

def validate_counts(oracle_dsn: str, oracle_user: str, oracle_password: str,
                    pg_dsn: str, owner: str, pg_schema: str, tables: List[str]) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    ora = oracledb.connect(user=oracle_user, password=oracle_password, dsn=oracle_dsn)
    pg = psycopg.connect(pg_dsn)
    try:
        oc = ora.cursor()
        pc = pg.cursor()
        for t in tables:
            oc.execute(f'SELECT COUNT(*) FROM "{owner.upper()}"."{t.upper()}"')
            ocount = oc.fetchone()[0]
            pc.execute(f'SELECT COUNT(*) FROM {pg_schema}.{t.lower()}')
            pcount = pc.fetchone()[0]
            out[t] = {"oracle": ocount, "postgres": pcount, "match": (ocount == pcount)}
    except:
        print("Error in validating row counts.", file=stderr)
    finally:
        ora.close(); pg.close()
    return out
