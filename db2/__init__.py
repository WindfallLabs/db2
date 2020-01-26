from db import DB
from db import SQLiteDB, MSSQLDB, PostgresDB

__author__ = """Garin Wally"""
__email__ = 'garwall101@gmail.com'
__version__ = '0.0.1'


SQLITE_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
"""Format to strftime() datetime objects to on INSERT (SQLite)."""
