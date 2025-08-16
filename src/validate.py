import oracledb
import psycopg
from sys import stderr
from typing import List, Dict
from config import OracleCfg, PostgresCfg
from report import Report

def validate_counts(oracle: OracleCfg, pg: PostgresCfg, tables: List[str]) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    ora = oracledb.connect(user=oracle.user, password=oracle.password, dsn=oracle.dsn)
    pg = psycopg.connect(pg.dsn)
    try:
        oc = ora.cursor()
        pc = pg.cursor()
        for t in tables:
            oc.execute(f'SELECT COUNT(*) FROM "{oracle.owner.upper()}"."{t.upper()}"')
            ocount = oc.fetchone()[0]
            pc.execute(f'SELECT COUNT(*) FROM {pg.schema}.{t.lower()}')
            pcount = pc.fetchone()[0]
            out[t] = {"oracle": ocount, "postgres": pcount, "match": (ocount == pcount)}
    except:
        print("Error in validating row counts.", file=stderr)
    finally:
        ora.close(); pg.close()
    return out
