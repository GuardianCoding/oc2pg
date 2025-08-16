import re
import hashlib
from typing import Dict, List, Tuple, Optional

from type_map import map_type as map_type

PG_MAX_IDENT = 63
_NEEDS_QUOTE = re.compile(r'[^a-z0-9_]|^[^a-z_]|^[0-9]')
_RESERVED = {
    "offset", "limit", "user", "schema", "table", "column", "order", "group",
    "primary", "foreign", "unique", "constraint", "references", "timestamp",
    "type", "name", "value", "values"
}
BIGINT_MAX = 9223372036854775807

class NameMapper:
    """
    Make Oracle names safe in Postgres:
      - lowercases
      - replaces non [a-z0-9_] with _
      - ensures starts-with letter/_ (prefix '_' if needed)
      - truncates >63 chars with stable 8-hex hash
      - keeps a mapping original -> normalized
    """
    def __init__(self):
        self.map: Dict[str, str] = {}
        self.used: Dict[str, str] = {}

    def _normalize(self, name: str) -> str:
        n = (name or "").strip().lower()
        n = re.sub(r'[^a-z0-9_]', '_', n)
        if not n or not re.match(r'[a-z_]', n[0]):
            n = f"_{n}"
        return n

    def _shorten(self, n: str) -> str:
        if len(n) <= PG_MAX_IDENT:
            return n
        h = hashlib.blake2b(n.encode('utf-8'), digest_size=4).hexdigest()  # 8 chars
        keep = PG_MAX_IDENT - 1 - len(h)
        return f"{n[:keep]}_{h}"

    def pg_ident(self, original: str) -> str:
        if original in self.map:
            return self.map[original]
        n = self._shorten(self._normalize(original))
        base = n
        i = 1
        while n in self.used and self.used[n] != original:
            suffix = f"_{i}"
            n = self._shorten(base[: max(0, PG_MAX_IDENT - len(suffix))] + suffix)
            i += 1
        self.map[original] = n
        self.used[n] = original
        return n

    def quote(self, ident: str) -> str:
        if not ident:
            return '""'
        needs = _NEEDS_QUOTE.search(ident) or (ident.lower() in _RESERVED)
        if needs:
            return f'"{ident.replace(chr(34), chr(34)*2)}"'
        return ident

def _table_ident(nm: NameMapper, table_name: str, schema: Optional[str]) -> str:
    t = nm.quote(nm.pg_ident(table_name))
    if schema:
        s = nm.quote(nm.pg_ident(schema))
        return f"{s}.{t}"
    return t

def _column_def(nm: NameMapper, col: Dict) -> str:
    """
    Expected col fields (prototype-friendly):
      - column_name (Oracle)
      - data_type (Oracle)
      - data_precision (optional)
      - data_scale (optional)
      - nullable (bool, default True)
      - data_default (optional raw default string, already PG-friendly if possible)
      - pg_type (optional explicit override)
    """
    name = nm.quote(nm.pg_ident(col["column_name"]))
    pg_type = col.get("pg_type")
    if not pg_type:
        pg_type = map_type(
            col.get("data_type", "TEXT"),
            col.get("data_precision"),
            col.get("data_scale")
        ) or "text"

    # --- sanitize problematic mapper outputs (prototype-friendly) ---
    if isinstance(pg_type, str):
        # convert 'ctid' (not a type) to 'text'
        if pg_type.strip().lower() == "ctid":
            pg_type = "text"
        # drop '(None, ...)' or '(None)' patterns e.g. numeric(None), numeric(None,0)
        pg_type = re.sub(r'\(\s*None\s*(?:,\s*None\s*)?\)', '', pg_type, flags=re.I)
        # remove stray empty parentheses 'numeric()' -> 'numeric'
        pg_type = re.sub(r'\(\s*\)$', '', pg_type)

    parts = [name, pg_type]
    dflt = col.get("data_default")
    if dflt not in (None, ""):
        parts += ["DEFAULT", dflt]
    if not col.get("nullable", True):
        parts.append("NOT NULL")
    return " ".join(parts)


def emit_create_table(
    table_def: Dict,
    columns: List[Dict],
    pkeys: Optional[List[str]] = None,
    schema: Optional[str] = None,
    namemap: Optional[NameMapper] = None
) -> str:
    """
    Emit a CREATE TABLE with optional inline PRIMARY KEY.
      table_def: { 'table_name': 'EMP' }
      columns:   list of column dicts (see _column_def)
      pkeys:     list of Oracle column names for PK (optional)
    """
    nm = namemap or NameMapper()
    tbl = _table_ident(nm, table_def["table_name"], schema)

    lines = [_column_def(nm, c) for c in columns]
    if pkeys:
        pk_cols = ", ".join(nm.quote(nm.pg_ident(c)) for c in pkeys)
        lines.append(f"PRIMARY KEY ({pk_cols})")

    body = ",\n  ".join(lines) if lines else ""
    return f"CREATE TABLE IF NOT EXISTS {tbl} (\n  {body}\n);"

def emit_constraints(
    fks: List[Dict],
    schema: Optional[str] = None,
    namemap: Optional[NameMapper] = None,
    *,
    deferrable: bool = True
) -> List[str]:
    """
    Emit FOREIGN KEY constraints as ALTER TABLE statements.
      fk dict expects:
        'constraint_name','table_name','columns',
        'r_table_name','r_columns','delete_rule' (NO ACTION/CASCADE/SET NULL)
    """
    nm = namemap or NameMapper()
    stmts: List[str] = []
    for fk in fks:
        tbl = _table_ident(nm, fk["table_name"], schema)
        rtbl = _table_ident(nm, fk["r_table_name"], schema)
        cname = nm.quote(nm.pg_ident(fk["constraint_name"]))
        cols = ", ".join(nm.quote(nm.pg_ident(c)) for c in fk["columns"])
        rcols = ", ".join(nm.quote(nm.pg_ident(c)) for c in fk["r_columns"])
        suffix = ""
        dr = (fk.get("delete_rule") or "NO ACTION").upper()
        if dr != "NO ACTION":
            suffix += f" ON DELETE {dr}"
        if deferrable:
            suffix += " DEFERRABLE INITIALLY DEFERRED"
        stmts.append(
            f"ALTER TABLE {tbl} ADD CONSTRAINT {cname} "
            f"FOREIGN KEY ({cols}) REFERENCES {rtbl} ({rcols}){suffix};"
        )
    return stmts

def emit_indexes(
    index_defs: List[Dict],
    schema: Optional[str] = None,
    namemap: Optional[NameMapper] = None
) -> List[str]:
    """
    Emit (UNIQUE) INDEX DDL.
      index_defs: [{'index_name','table_name','columns',[...],'uniqueness':'UNIQUE'|'NONUNIQUE'}]
    """
    nm = namemap or NameMapper()
    out: List[str] = []
    for ix in index_defs:
        ixname = nm.quote(nm.pg_ident(ix["index_name"]))
        tbl = _table_ident(nm, ix["table_name"], schema)
        cols = ", ".join(nm.quote(nm.pg_ident(c)) for c in ix.get("columns", []))
        uniq = "UNIQUE " if ix.get("uniqueness") == "UNIQUE" else ""
        out.append(f"CREATE {uniq}INDEX IF NOT EXISTS {ixname} ON {tbl} ({cols});")
    return out

def emit_sequences(
    seq_defs: List[Dict],
    schema: Optional[str] = None,
    namemap: Optional[NameMapper] = None
) -> List[str]:
    """
    Emit CREATE SEQUENCE for each definition (Postgres-friendly):
      - Drop ORDER / NO ORDER (unsupported in PG)
      - Force CACHE >= 1
      - Omit MAXVALUE if it exceeds BIGINT
    """
    nm = namemap or NameMapper()
    out: List[str] = []

    for s in seq_defs:
        sname = nm.quote(nm.pg_ident(s["sequence_name"]))
        fq = sname if not schema else f"{nm.quote(nm.pg_ident(schema))}.{sname}"
        parts = [f"CREATE SEQUENCE IF NOT EXISTS {fq}"]

        inc = s.get("increment_by")
        if inc is not None:
            parts.append(f"INCREMENT BY {int(inc)}")

        minv = s.get("min_value")
        if minv is not None:
            parts.append(f"MINVALUE {int(minv)}")

        maxv = s.get("max_value")
        try:
            if maxv is not None and int(maxv) <= BIGINT_MAX:
                parts.append(f"MAXVALUE {int(maxv)}")
            # else: omit invalid/huge MAXVALUE
        except Exception:
            pass

        cache = s.get("cache_size")
        try:
            cache_i = int(cache) if cache is not None else 1
            if cache_i < 1:
                cache_i = 1
            parts.append(f"CACHE {cache_i}")
        except Exception:
            parts.append("CACHE 1")

        parts.append("CYCLE" if (s.get("cycle_flag") == "Y") else "NO CYCLE")
        # DO NOT emit ORDER/NO ORDER in PG
        out.append(" ".join(parts) + ";")
    return out

def compose_plan(
    schema: Optional[str],
    seq_defs: List[Dict],
    tables: List[Tuple[Dict, List[Dict], Optional[List[str]]]],
    fks: List[Dict],
    indexes: List[Dict],
    namemap: Optional[NameMapper] = None
) -> str:
    """
    Produce a single SQL string in deterministic order:
      sequences → tables (with inline PK) → FKs → indexes
    tables: list of (table_def, columns, pkeys)
    """
    nm = namemap or NameMapper()
    parts: List[str] = []
    parts += emit_sequences(seq_defs, schema, nm)
    for tdef, cols, pks in tables:
        parts.append(emit_create_table(tdef, cols, pks, schema, nm))
    parts += emit_constraints(fks, schema, nm)
    parts += emit_indexes(indexes, schema, nm)
    return "\n".join(parts)