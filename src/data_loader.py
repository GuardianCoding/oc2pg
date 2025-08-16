from typing import Iterable, Iterator, List, Optional, Sequence, Tuple, Any, Dict
import io, csv, decimal, datetime
from pathlib import Path
import oracledb
import psycopg
import cli
from config import OracleCfg, PostgresCfg, OutputCfg

class DataLoader:
    """
    Streams rows from Oracle to Postgres using COPY CSV.
    Keep transforms minimal; depend on DDL/type mapping to have compatible target types.
    """
    def __init__(
        self,
        ora: OracleCfg,
        pg: PostgresCfg,
        out: OutputCfg
    ):
        self.ora = ora
        self.pg = pg
        self.out = out
        self.out_dir = Path(out.dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
    
    def load_schema(self):
        """ Loads all tables in the schema from Oracle to PostgresSQL in parallel. Returns stats per table. """
        return
    
    def load_table(self):
        """ Loads single table from Oracle to PostgresSQL. To be used as a thread of load_schema. """
        return