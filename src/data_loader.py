from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple
import io, csv, decimal, datetime, sys, hashlib
from contextlib import contextmanager
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import oracledb
import psycopg
from psycopg import sql

from config import OracleCfg, PostgresCfg, OutputCfg, TableSpec

NULL_SENTINEL = r"\N"

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
    ) -> None:
        self.ora = ora
        self.pg = pg
        self.out = out
        self.out_dir = Path(out.dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
    
    # Public APIs

    def load_schema(self, tables: List[TableSpec]) -> Dict[str, Dict[str, Any]]:
        """ Loads all tables in the schema from Oracle to PostgresSQL, optionally in parallel. Returns stats per table. 

        Args:
            tables: List of TableSpec objects to migrate.

        Returns:
            Mapping of table name -> stats dict, e.g.:
            {
              "EMPLOYEES": {"status": "ok", "rows": 107, "failed_batches": 0},
              "DEPARTMENTS": {"status": "error", "error": "..."}
            }
        """
        stats: Dict[str, Dict[str, Any]] = {}
        
        # Sequential path
        if self.pg.copy_parallelism <= 1 or len(tables) <= 1:
            for spec in tables:
                try:
                    stats[spec.name] = self.load_table(spec)
                except Exception as e:
                    stats[spec.name] = {"status": "error", "error": repr(e)}
            return stats

        # Parallel across tables
        with ThreadPoolExecutor(max_workers=self.pg.copy_parallelism) as ex:
            futs = {ex.submit(self.load_table, spec): spec for spec in tables}
            for fut in as_completed(futs):
                spec = futs[fut]
                try:
                    stats[spec.name] = fut.result()
                except Exception as e:
                    stats[spec.name] = {"status": "error", "error": repr(e)}
        return stats
    
    def load_table(self, spec: TableSpec) -> Dict[str, Any]:
        """ Loads single table from Oracle to PostgresSQL. To be used as a thread of load_schema. 
        
                Behavior:
            - Builds a SELECT for the specified columns (and optional WHERE).
            - Iterates Oracle rows in batches.
            - Serializes batches to CSV bytes with NULL_SENTINEL.
            - Writes to Postgres using COPY ... FROM STDIN.
            - Defers constraints for the session if target FKs are deferrable.

        Args:
            spec: TableSpec describing owner, table, columns, and target schema.

        Returns:
            Stats dict with at least: {'status': 'ok'|'error', 'rows': int, 'failed_batches': int}.

        Raises:
            Exception: Propagates unexpected connection/IO errors (caller may catch).
        """
        total_rows = 0
        failed_batches = 0

        ora_sql = self._oracle_select(spec)

        with self._oracle_conn() as oc, self._pg_conn() as pc:
            with oc.cursor() as cur:
                cur.arraysize = self.ora.arraysize
                self._set_output_handlers(cur)
                # Execute SELECT
                cur.execute(ora_sql)

                # Prepare COPY statement
                cols_sql = sql.SQL(',').join(sql.Identifier(c) for c in spec.columns)
                copy_stmt = sql.SQL(
                    "COPY {}.{} ({}) FROM STDIN WITH (FORMAT csv, NULL '\\N', QUOTE '\"', ESCAPE '\"')"
                ).format(
                    sql.Identifier(spec.pg_schema),
                    sql.Identifier(spec.name),
                    cols_sql,
                )

                with pc.cursor() as pgc:
                    # Best-effort: defer constraints if target FKs are deferrable
                    try:
                        pgc.execute("SET CONSTRAINTS ALL DEFERRED;")
                    except Exception:
                        pass

                    with pgc.copy(copy_stmt) as cp:
                        for batch in self._iter_oracle_batches(cur, len(spec.columns)):
                            try:
                                payload = self._rows_to_csv_bytes(batch)
                                cp.write(payload)
                                total_rows += len(batch)
                            except Exception as e:
                                failed_batches += 1
                                self._log_bad_batch(spec, batch, e)

        return {"status": "ok", "rows": total_rows, "failed_batches": failed_batches}
    
    # Oracle helpers

    def _oracle_select(self, spec: TableSpec) -> str:
        """
        Build a minimal Oracle SELECT for the given table and columns.

        Args:
            spec: TableSpec with owner, table name, columns, and optional where clause.

        Returns:
            SQL text like: SELECT c1, c2 FROM "OWNER"."TABLE" [WHERE <where_clause>]
        """
        cols = ", ".join(self._quote_ora_ident(c) for c in spec.columns)
        base = f"SELECT {cols} FROM {self._quote_ora_ident(spec.owner)}.{self._quote_ora_ident(spec.name)}"
        if spec.where_clause:
            return base + " WHERE " + spec.where_clause
        return base

    @contextmanager
    def _oracle_conn(self) -> Iterator[oracledb.Connection]:
        """
        Context manager yielding an Oracle connection.

        Yields:
            Open `oracledb.Connection` configured for bulk fetches.

        Notes:
            Caller is responsible for creating cursors and closing is handled by the context.
        """
        conn = oracledb.connect(user=self.ora.user, password=self.ora.password, dsn=self.ora.dsn)
        try:
            # Speeding up connections
            conn.stmtcachesize = 50
            yield conn
        finally:
            try:
                # Gracefully closing connections at the end of copying
                conn.close()
            except Exception:
                pass


    def _set_output_handlers(self, cur: oracledb.Cursor) -> None:
        """
        Install an outputtypehandler on the given cursor so that LOBs are fetched
        as Python-native types (str for CLOB/NCLOB, bytes for BLOB/RAW).

        Args:
            cur: Oracle cursor to configure.
        """
        def handler(cursor, name, default_type, size, precision, scale):
            if default_type in (oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_NCLOB):
                return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
            if default_type in (oracledb.DB_TYPE_BLOB, oracledb.DB_TYPE_LONG_RAW, oracledb.DB_TYPE_RAW):
                return cursor.var(oracledb.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)
        cur.outputtypehandler = handler
    
    def _iter_oracle_batches(
        self,
        cur: oracledb.Cursor,
        ncols: int,
    ) -> Iterator[List[Tuple[Any, ...]]]:
        """
        Yield lists of rows from the Oracle cursor, respecting `copy_batch_rows`. Defensively
        protects against truncated rows or rows that are too long, which could break the copying and cause
        data anamolaies during the migration.

        Args:
            cur: Executed Oracle cursor (already .execute()'d).
            ncols: Expected number of columns in each row (defensive padding/truncation).

        Yields:
            Lists (batches) of tuples representing rows.
        """
        batch: List[Tuple[Any, ...]] = []
        for row in cur:
            if not isinstance(row, tuple):
                row = tuple(row)
            if len(row) != ncols:
                # Defensive padding/truncation
                row = tuple(row[:ncols]) + (None,) * max(0, ncols - len(row))
            batch.append(row)
            if len(batch) >= self.pg.copy_batch_rows:
                yield batch
                batch = []
        if batch:
            yield batch

    # CSV Serialisation

    def _rows_to_csv_bytes(self, rows: Sequence[Sequence[Any]]) -> bytes:
        """
        Convert a batch of rows to a UTF-8 CSV payload suitable for Postgres COPY.

        Conventions:
            - NULLs become NULL_SENTINEL.
            - Bytea values encoded as Postgres hex format: '\\xDEADBEEF'.
            - Timestamps/Date/Time use ISO-8601; parsed by Postgres text input.
            - Newlines are preserved; CSV quoting handles embedded delimiters.

        Args:
            rows: Sequence of row sequences.

        Returns:
            Bytes ready to feed into psycopg COPY.write().
        """
        buf = io.StringIO()
        writer = csv.writer(
            buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL, escapechar='"', doublequote=True
        )
        for row in rows:
            writer.writerow([self._to_csv_field(v) for v in row])
        data = buf.getvalue().encode("utf-8", "strict")
        buf.close()
        return data

    def _to_csv_field(self, v: Any) -> str:
        """
        Serialize one Python value into a CSV field string matching Postgres text input.

        Args:
            v: Python value from Oracle driver (int, Decimal, datetime, str, bytes, None, etc.)

        Returns:
            String representation for CSV (with NULL_SENTINEL for None).
        """
        if v is None:
            return NULL_SENTINEL
        if isinstance(v, (int, float, decimal.Decimal)):
            return str(v)
        if isinstance(v, datetime.datetime):
            return v.isoformat(sep=" ")
        if isinstance(v, (datetime.date, datetime.time)):
            return v.isoformat()
        if isinstance(v, (bytes, bytearray, memoryview)):
            return "\\x" + bytes(v).hex()
        
        # Fallback to string if all else fails
        return str(v)
    
    # Postgres Helpers

    @contextmanager
    def _pg_conn(self) -> Iterator[psycopg.Connection]:
        """
        Context manager yielding a PostgreSQL connection.

        Yields:
            Open `psycopg.Connection` with autocommit disabled (caller commits per table).

        Notes:
            Ensures connection is closed; caller handles cursor/COPY lifecycle.
        """
        conn = psycopg.connect(self.pg.dsn)
        try:
            yield conn
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass


    def _quote_ident(self, name: str) -> str:
        """
        Quote a PostgreSQL identifier for use in column lists (COPY target list).

        Args:
            name: Identifier to quote.

        Returns:
            Either a bare lower-case identifier or a double-quoted string with escapes.
        """
        if name.isidentifier() and name == name.lower():
            return name
        return '"' + name.replace('"', '""') + '"'


    def _quote_ora_ident(self, name: str) -> str:
        """
        Quote an Oracle identifier for SELECT statements.

        Args:
            name: Oracle owner/table/column identifier.

        Returns:
            Double-quoted identifier, preserving case/special chars as needed.
        """
        return '"' + name.replace('"', '""').upper() + '"'
    
    # Diagnostics

    def _log_bad_batch(
        self,
        spec: TableSpec,
        rows: Sequence[Sequence[Any]],
        err: Exception,
        out_dir: Optional[Path] = None,
    ) -> None:
        """
        Persist a failed batch to disk for debugging, then continue with the next batch.

        Args:
            spec: TableSpec of the failing table (used in filename).
            rows: The batch of rows that failed to COPY.
            err: The exception raised while writing the batch.
            out_dir: Directory to write the CSV (defaults to self.out_dir).

        Notes:
            The CSV is encoded as produced by `_rows_to_csv_bytes`.
            Implementations should write to `<out_dir>/badbatch_<table>_<hash>.csv`.
        """
        try:
            out = Path(out_dir) if out_dir else self.out_dir
            h = hashlib.sha1((repr(err) + spec.name + str(len(rows))).encode("utf-8")).hexdigest()[:10]
            path = out / f"badbatch_{spec.name}_{h}.csv"
            with open(path, "wb") as f:
                f.write(self._rows_to_csv_bytes(rows))
            sys.stderr.write(f"[WARN] Failed batch for {spec.name}: {err!r}. Saved to {path}\n")
        except Exception:
            # best-effort logging only
            pass