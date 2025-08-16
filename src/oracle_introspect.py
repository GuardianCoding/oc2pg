# oracle_introspect.py
from __future__ import annotations
from typing import Optional, Iterable, List, Dict, Tuple
import oracledb

from config import OracleCfg


class OracleIntrospector:
    def __init__(self, cfg: OracleCfg):
        """
        Build from a Config. No direct user/password/dsn args.
        """
        if cfg is None:
            raise ValueError("OracleIntrospector requires cfg: OracleCfg")
        self.cfg = cfg
        self.conn = oracledb.connect(user=cfg.user, password=cfg.password, dsn=cfg.dsn)
        self.conn.stmtcachesize = 50
        self.arraysize = cfg.arraysize

    def _cursor(self):
        cur = self.conn.cursor()
        cur.arraysize = self.arraysize
        return cur

    def _rows(self, cursor) -> List[Dict]:
        cols = [d[0].lower() for d in cursor.description]
        return [dict(zip(cols, r)) for r in cursor.fetchall()]

    def set_current_schema(self, owner: str) -> None:
        """Best-effort: point USER* views at a specific schema."""
        with self._cursor() as cur:
            try:
                cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {owner}")
                self.cfg.owner = owner
            except Exception:
                # harmless if lacking privileges; callers can pass owner explicitly
                pass

    def get_schema(self) -> List[Dict]:
        with self._cursor() as cur:
            cur.execute("""
                SELECT object_type, object_name
                FROM user_objects
                ORDER BY object_type, object_name
            """)
            return self._rows(cur)

    def get_tables(
        self,
        owner: Optional[str] = None,
        *,
        include_tables: Optional[Iterable[str]] = None,
        exclude_tables: Optional[Iterable[str]] = None,
    ) -> List[Dict]:
        owner = owner or self.cfg.owner
        sql = """
            SELECT table_name, temporary
            FROM   all_tables
            WHERE  owner = :owner
            ORDER  BY table_name
        """
        with self._cursor() as cur:
            cur.execute(sql, owner=owner)
            rows = self._rows(cur)

        # prototype-friendly name filters (case-insensitive exact matches)
        if include_tables:
            want = {t.lower() for t in include_tables}
            rows = [r for r in rows if r["table_name"].lower() in want]
        if exclude_tables:
            drop = {t.lower() for t in exclude_tables}
            rows = [r for r in rows if r["table_name"].lower() not in drop]
        return rows

    def get_columns(self, owner: Optional[str] = None) -> List[Dict]:
        owner = owner or self.cfg.owner
        sql = """
            SELECT table_name,
                   column_name,
                   data_type,
                   data_precision,
                   data_scale,
                   nullable,
                   data_default
            FROM   all_tab_columns
            WHERE  owner = :owner
            ORDER  BY table_name, column_id
        """
        with self._cursor() as cur:
            cur.execute(sql, owner=owner)
            rows = self._rows(cur)

        for r in rows:
            r["nullable"] = (r.get("nullable") == "Y")
            if isinstance(r.get("data_default"), str):
                r["data_default"] = r["data_default"].strip()
        return rows

    def get_pk(self, owner: Optional[str] = None) -> List[Dict]:
        owner = owner or self.cfg.owner
        sql = """
            SELECT c.table_name,
                   c.constraint_name,
                   cc.column_name,
                   cc.position
            FROM   all_constraints c
            JOIN   all_cons_columns cc
              ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
            WHERE  c.owner = :owner
              AND  c.constraint_type = 'P'
            ORDER  BY c.table_name, c.constraint_name, cc.position
        """
        with self._cursor() as cur:
            cur.execute(sql, owner=owner)
            rows = self._rows(cur)

        grouped: Dict[Tuple[str, str], List[Tuple[int, str]]] = {}
        for r in rows:
            key = (r["table_name"], r["constraint_name"])
            grouped.setdefault(key, []).append((r["position"], r["column_name"]))

        result: List[Dict] = []
        for (tbl, cn), cols in grouped.items():
            cols.sort(key=lambda x: x[0])
            result.append({"table_name": tbl, "constraint_name": cn, "columns": [c for _, c in cols]})
        return result

    def get_fk(self, owner: Optional[str] = None) -> List[Dict]:
        owner = owner or self.cfg.owner
        sql = """
            SELECT fk.constraint_name AS fk_name,
                   fk.table_name      AS fk_table,
                   fk.delete_rule,
                   fkc.column_name    AS fk_col,
                   fkc.position       AS pos,
                   pk.table_name      AS pk_table,
                   pkc.column_name    AS pk_col
            FROM   all_constraints fk
            JOIN   all_constraints pk
              ON pk.owner = fk.r_owner
             AND pk.constraint_name = fk.r_constraint_name
            JOIN   all_cons_columns fkc
              ON fkc.owner = fk.owner
             AND fkc.constraint_name = fk.constraint_name
            JOIN   all_cons_columns pkc
              ON pkc.owner = pk.owner
             AND pkc.constraint_name = pk.constraint_name
             AND pkc.position = fkc.position
            WHERE  fk.owner = :owner
              AND  fk.constraint_type = 'R'
            ORDER  BY fk.table_name, fk.constraint_name, fkc.position
        """
        with self._cursor() as cur:
            cur.execute(sql, owner=owner)
            rows = self._rows(cur)

        grouped, meta = {}, {}
        for r in rows:
            k = (r["fk_name"], r["fk_table"])
            grouped.setdefault(k, []).append((r["pos"], r["fk_col"], r["pk_col"]))
            meta[k] = {"r_table_name": r["pk_table"], "delete_rule": r["delete_rule"]}

        out: List[Dict] = []
        for (name, table), triples in grouped.items():
            triples.sort(key=lambda x: x[0])
            out.append({
                "constraint_name": name,
                "table_name": table,
                "columns": [c for _, c, _ in triples],
                "r_table_name": meta[(name, table)]["r_table_name"],
                "r_columns": [c for _, _, c in triples],
                "delete_rule": meta[(name, table)]["delete_rule"],
            })
        return out

    def get_indexes(self, owner: Optional[str] = None) -> List[Dict]:
        owner = owner or self.cfg.owner
        idx_sql = """
            SELECT index_name, table_name, uniqueness
            FROM   all_indexes
            WHERE  owner = :owner
            ORDER  BY table_name, index_name
        """
        ic_sql = """
            SELECT index_name, table_name, column_name, column_position
            FROM   all_ind_columns
            WHERE  index_owner = :owner
            ORDER  BY table_name, index_name, column_position
        """
        with self._cursor() as cur:
            cur.execute(idx_sql, owner=owner)
            idxs = self._rows(cur)
            cur.execute(ic_sql, owner=owner)
            cols = self._rows(cur)

        by_idx: Dict[Tuple[str, str], List[Tuple[int, str]]] = {}
        for r in cols:
            key = (r["table_name"], r["index_name"])
            by_idx.setdefault(key, []).append((r["column_position"], r["column_name"]))

        out: List[Dict] = []
        for r in idxs:
            key = (r["table_name"], r["index_name"])
            collist = [c for _, c in sorted(by_idx.get(key, []))]
            out.append({
                "index_name": r["index_name"],
                "table_name": r["table_name"],
                "uniqueness": r["uniqueness"],  # 'UNIQUE' / 'NONUNIQUE'
                "columns": collist
            })
        return out

    def get_sequences(self, owner: Optional[str] = None) -> List[Dict]:
        owner = owner or self.cfg.owner
        sql = """
            SELECT sequence_name,
                   increment_by,
                   min_value,
                   max_value,
                   cache_size,
                   cycle_flag,
                   order_flag,
                   last_number
            FROM   all_sequences
            WHERE  sequence_owner = :owner
            ORDER  BY sequence_name
        """
        with self._cursor() as cur:
            cur.execute(sql, owner=owner)
            return self._rows(cur)