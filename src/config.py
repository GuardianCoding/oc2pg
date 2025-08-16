from dataclasses import dataclass
from typing import List, Optional
import yaml
from pathlib import Path

@dataclass
class OracleCfg:
    owner: str
    dsn: str
    user: str
    password: str
    arraysize: int = 10000

@dataclass
class PostgresCfg:
    dsn: str
    schema: str = "public"
    copy_parallelism: int = 4
    copy_batch_rows: int = 50000

@dataclass
class MigrateCfg:
    include_tables: List[str]
    exclude_tables: List[str]
    create_indexes_after_load: bool = True
    fks_deferrable: bool = True
    dry_run: bool = False

@dataclass
class OutputCfg:
    dir: str = "./out"
    plan_sql: str = "plan.sql"
    report_md: str = "report.md"

@dataclass
class Config:
    oracle: OracleCfg
    postgres: PostgresCfg
    migrate: MigrateCfg
    output: OutputCfg

@dataclass
class TableSpec:
    """
    Description of one table to load.

    Attributes:
        owner: Oracle schema/owner (e.g., 'HR').
        name: Oracle table name (e.g., 'EMPLOYEES').
        columns: Ordered list of column names to select/copy.
        pg_schema: Target PostgreSQL schema (e.g., 'hr').
        estimated_rows: Optional row count for progress (from Oracle stats).
        where_clause: Optional Oracle SQL predicate (without 'WHERE') to restrict rows.
        pg_table: Optional Postgres table name override (normalized via NameMapper).
        pg_columns: Optional Postgres column name list override (normalized via NameMapper).
    """
    owner: str
    name: str
    columns: List[str]
    pg_schema: str
    estimated_rows: Optional[int] = None
    where_clause: Optional[str] = None
    pg_table: Optional[str] = None
    pg_columns: Optional[List[str]] = None

def load_config(path: str) -> Config:
    data = yaml.safe_load(Path(path).read_text())
    return Config(
        oracle=OracleCfg(**data["oracle"]),
        postgres=PostgresCfg(**data["postgres"]),
        migrate=MigrateCfg(**data["migrate"]),
        output=OutputCfg(**data["output"]),
    )