# oracle_introspect.py
import oracledb

class OracleIntrospector:
    def __init__(self, user: str, password: str, dsn: str):
        self.conn = oracledb.connect(user=user, password=password, dsn=dsn)
        self.conn.stmtcachesize = 50
        self.owner = self.get_owner()

    def _rows(self, cursor):
        cols = [d[0].lower() for d in cursor.description]
        return [dict(zip(cols, r)) for r in cursor.fetchall()]

    def get_schema(self):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT object_type, object_name
            FROM user_objects
            ORDER BY object_type, object_name
        """)
        return self._rows(cur)

    def get_owner(self):
        cur = self.conn.cursor()
        cur.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual")
        row = cur.fetchone()
        owner = (row[0] if row and row[0] else None)

        if not owner:
            cur.execute("SELECT USER FROM dual")
            row = cur.fetchone()
            owner = row[0] if row else None

        return owner

    def get_tables(self):
        sql = """
            SELECT table_name, temporary
            FROM   all_tables
            WHERE  owner = :owner
            ORDER  BY table_name
        """
        cur = self.conn.cursor()
        cur.execute(sql, owner=self.owner)
        return self._rows(cur)

    def get_columns(self):
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
        cur = self.conn.cursor()
        cur.execute(sql, owner=self.owner)
        rows = self._rows(cur)
        for r in rows:
            r["nullable"] = (r["nullable"] == "Y")
            if isinstance(r.get("data_default"), str):
                r["data_default"] = r["data_default"].strip()
        return rows

    def get_pk(self):
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
        cur = self.conn.cursor()
        cur.execute(sql, owner=self.owner)
        grouped = {}
        for r in self._rows(cur):
            key = (r["table_name"], r["constraint_name"])
            grouped.setdefault(key, []).append((r["position"], r["column_name"]))
        result = []
        for (tbl, cn), cols in grouped.items():
            cols.sort(key=lambda x: x[0])
            result.append({
                "table_name": tbl,
                "constraint_name": cn,
                "columns": [c for _, c in cols]
            })
        return result

    def get_fk(self):
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
        cur = self.conn.cursor()
        cur.execute(sql, owner=self.owner)
        grouped, meta = {}, {}
        for r in self._rows(cur):
            k = (r["fk_name"], r["fk_table"])
            grouped.setdefault(k, []).append((r["pos"], r["fk_col"], r["pk_col"]))
            meta[k] = {"r_table_name": r["pk_table"], "delete_rule": r["delete_rule"]}
        out = []
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

    def get_indexes(self):
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
        cur = self.conn.cursor()
        cur.execute(idx_sql, owner=self.owner)
        idxs = self._rows(cur)

        cur.execute(ic_sql, owner=self.owner)
        cols = self._rows(cur)

        by_idx = {}
        for r in cols:
            key = (r["table_name"], r["index_name"])
            by_idx.setdefault(key, []).append((r["column_position"], r["column_name"]))

        out = []
        for r in idxs:
            key = (r["table_name"], r["index_name"])
            collist = [c for _, c in sorted(by_idx.get(key, []))]
            out.append({
                "index_name": r["index_name"],
                "table_name": r["table_name"],
                "uniqueness": r["uniqueness"],
                "columns": collist
            })
        return out

    def get_sequences(self):
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
        cur = self.conn.cursor()
        cur.execute(sql, owner=self.owner)
        return self._rows(cur)