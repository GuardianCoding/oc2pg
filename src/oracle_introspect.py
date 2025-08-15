import oracledb

def connect(usr: str, pswd: str, dsn:str)->oracledb.Connection:
    conn = oracledb.connect(usr=usr, password= pswd, dsn = dsn)
    conn.stmtcachesize = 50
    return conn

def get_tables(conn: oracledb.Connection, schema: str) :
    sql =  """
        SELECT table_name, temporary
        FROM   all_tables
        WHERE  owner = :owner
        ORDER  BY table_name
            """
    curs = conn.cursor()
    curs.execute(sql, owner=schema.upper())
    for column in curs.description:
        print (column)

def get_columns(conn: oracledb.Connection, schema: str):
    sql = """
        SELECT table_name,
               column_name,
               data_type,
               data_length,
               char_used,
               char_length,
               data_precision,
               data_scale,
               nullable,
               data_default
        FROM   all_tab_columns
        WHERE  owner = :owner
        ORDER  BY table_name, column_id
    """
    curs = conn.cursor()
    curs.execute(sql, owner=schema.upper())
    for collumn in curs.description:
        print(collumn)

def get_pk(schema):


def get_fk(schema):
    return

def get_indexes(schema):
    return

def get_sequences(schema):
    return
