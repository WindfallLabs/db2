from __future__ import unicode_literals

from .db import DB
from .db import SQLiteDB, MSSQLDB, PostgresDB

__author__ = """Garin Wally"""
__email__ = 'garwall101@gmail.com'
__version__ = '1.0b1.dev1'


options = {
    "sqlite_datetime_format": u"%Y-%m-%d %H:%M:%S"
}

