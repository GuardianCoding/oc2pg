import oracledb
import psycopg
from sys import stderr
from typing import List, Dict
from config import OracleCfg, PostgresCfg
from report import Report
import typer

def validate_counts(oracle: OracleCfg, postgres: PostgresCfg, tables: List[str], owner:str, report: Report) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    ora = oracledb.connect(user=oracle.user, password=oracle.password, dsn=oracle.dsn)
    pg = psycopg.connect(postgres.dsn)
    try:
        oc = ora.cursor()
        pc = pg.cursor()
        for t in tables:
            oc.execute(f'SELECT COUNT(*) FROM "{owner.upper()}"."{t.upper()}"')
            ocount = oc.fetchone()[0]
            pc.execute(f'SELECT COUNT(*) FROM {postgres.schema}.{t.lower()}')
            pcount = pc.fetchone()[0]
            out[t] = {"oracle": ocount, "postgres": pcount, "match": (ocount == pcount)}
    except:
        typer.secho("Error in validating row counts.", fg="yellow")
        report.log_report("Error in validating row counts.")
    finally:
        ora.close(); pg.close()
    return out
