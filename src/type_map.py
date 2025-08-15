
dict = {
    "NUMBER" : "numeric",
    "VARCHAR2" : "varchar",
    "NVARCHAR2" : "char",
    "CHAR" : "char",
    "NCHAR" : "char",
    "DATE" : "timestamp(0)",
    "INTEGER" : "integer",
    "INT" : "integer",
    "SMALLINT" : "smallint",
    "DOUBLE" : "double precision",
    "LONG" : "text",
    "FLOAT":  "double precision",
    "BINARY_FLOAT" : "real",
    "BINARY_DOUBLE" : "real",
    "RAW" : "bytea",
    "TIMESTAMP" : "timestamp",
    "BLOB" : "bytea",
    "CLOB" : "text",
    "NCLOB" : "text",
    "BFILE" : "bytea",
    "ROWID" : "ctid",
    "UROWID" : "UUID",
    "CHARACTER" : "char",
    #"spatial_types" : "",
    #"media_types" : "",
    #"SYS.AnyData" : "",
    #"SYS.AnyType" : "",
    #"SYS.AnyDataSet" : "",
    "XMLType" : "xml",
    #"URIType" : "",  no equivlant found
    "SDO_Geometry" : "geometry",
    "SDO_Topo_Geometry" : "geometry",
    "SDO_GeoRaster" : "geometry",
    "BOOLEAN" : "boolean",
    "JSON" : "jsonb",
}



def map(ora_type, precision=None, scale=None):
    """
    """
    mapping = dict.get(ora_type)
    if mapping is None:
        return
    else :
        if precision is not None and scale is not None:
            return f"{mapping}({precision},{scale})"
        if scale is not None:
            return f"{mapping}({precision})"
    return mapping
