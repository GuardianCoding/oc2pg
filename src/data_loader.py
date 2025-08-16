from typing import Iterable, Iterator, List, Optional, Sequence, Tuple, Any, Dict
import io, csv, decimal, datetime
from pathlib import Path
import oracledb
import psycopg
import cli

class DataLoader:
    """
    Streams rows from Oracle to Postgres using COPY CSV.
    Keep transforms minimal; depend on DDL/type mapping to have compatible target types.
    """
    def __init__(
        self,
        ora_dsn: str,
        ora_user: str,
        ora_password: str,
        pg_dsn: str,
        arraysize: int = 10_000,
        copy_batch_rows: int = 50_000,
        parallelism: int = 4,
        out_dir: str = "./out",
    ):
        self.ora_dsn = ora_dsn
        self.ora_user = ora_user
        self.ora_password = ora_password
        self.pg_dsn = pg_dsn
        self.arraysize = arraysize
        self.copy_batch_rows = copy_batch_rows
        self.parallelism = parallelism
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    
