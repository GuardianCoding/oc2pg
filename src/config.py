from dataclasses import dataclass
from typing import List
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

def load_config(path: str) -> Config:
    data = yaml.safe_load(Path(path).read_text())
    return Config(
        oracle=OracleCfg(**data["oracle"]),
        postgres=PostgresCfg(**data["postgres"]),
        migrate=MigrateCfg(**data["migrate"]),
        output=OutputCfg(**data["output"]),
    )