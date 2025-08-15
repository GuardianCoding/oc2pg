from typing import Iterable, Dict, List, Tuple
import io, csv, datetime
import oracledb
import psycopg
import cli

def copy_data(oracle_database: oracledb.Connection, postgres_database: psycopg.Connection):
    """ Copy all the tables from OracleDB to PostgresDB connections provided. Assumes database schema has been transferred 
        to postgresSQL database. """
    return

def move_table(oracle_database: oracledb.Connection, postgres_database: psycopg.Connection, table: str):
    """ Moves table from OracleDB to PostgresSQL. Extracts by ROWID in Oracle Database to allow for order retention and completeness
        in progressive scans of the same database. Requires consistent snapshot or static database to ensure no data loss
        and no duplicates. """
    return
    
