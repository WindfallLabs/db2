# !/usr/bin/env python2
"""
Misc functions and helpers.
"""

from __future__ import unicode_literals

import inspect
import re
from dateutil.parser import parse as parse_date
from decimal import Decimal


import pandas as pd
import sqlparse

import db2


# =============================================================================
# SQLite Adapter Functions:
# These functions must be registered with the sqlite3 library
# =============================================================================

def sqlite_adapt_datetime(date_time):
    """Adapts ``datetime.datetime`` types to SQLite TEXT with."""
    return date_time.strftime(db2.options["sqlite_datetime_format"])


def sqlite_adapt_decimal(decimal):
    """Adapts ``decimal.Decimal`` types to FLOAT."""
    return float(decimal)


# =============================================================================
# Pandas Vector Functions:
# =============================================================================

def dates_to_strs(col):
    """
    Converts all DataFrame Series of datetime types to strings.
    """
    if str(col.dtype).startswith("date"):
        return col.astype(str)
    return col


def decimals_to_floats(col):
    """
    Converts all DataFrame Series of decimal.Decimal types to floats.

    NOTE: SQLite automatically handles this on INSERT with ``adapt_decimals``.
    """
    if any(set(col.apply(lambda x: isinstance(x, Decimal)))):
        return pd.to_numeric(col)
    return col


# =============================================================================
# Parsed SQL Functions:
# =============================================================================

def is_query(parsed_sql):
    """
    Determine if the input SQL statement is a query.

    Parameters
    ----------
    parsed_sql: sqlparse.sql.Statement
        SQL statement that has been parsed with ``sqlparse.parse()``.

    Returns
    -------
    bool
        True if statement type is SELECT and the second non-whitespace token is
        not a function.
    """
    tokens = [t for t in parsed_sql.tokens if not t.is_whitespace]
    return parsed_sql.get_type() == "SELECT" and not type(
        tokens[1]).__name__ == "Function"


# =============================================================================
# SQL Functions:
# =============================================================================

def make_sqlite_function(conn, func):
    """
    Load a Python function into an SQLite database for use in SQL statements.

    Parameters
    ----------
    conn: sqlite3.connection
        The DB API connection to the SQLite database.
    func: function
        The Python function to use in the SQLite database.
    """
    name = func.__name__
    try:
        num_args = len(inspect.getfullargspec(func).args)
    except AttributeError:
        num_args = len(inspect.getargspec(func).args)
    conn.create_function(name, num_args, func)
    return


def pystrftime(directive, timestring):
    """
    Provides a ``strftime``-comparable SQL function that leverages Python's
    datetime module. For example, this function adds support for the following
    directives that SQLite's ``strftime()`` function does not:

    %A, %B, %I, %J, %U, %X, %Z, %a, %b, %c, %p, %s, %x, %y, %z

    Parameters
    ----------
    directive: str
        Format code used to reformat timestring
    timestring: str
        The date, time, or datetime to reformat

    Example
    -------
    .. code-block:: python

        # Use as a Python function
        >>> pystrftime("%b", "2020-01-01")
        'Jan'

        # Use as an SQL function
        >>> from db2 import SQLiteDB, utils
        >>> d = SQLiteDB(":memory:")

        # Pass the name "pystrftime" and the db2.utils.pystrftime function that
        # takes 2 arguments to 'create_function'.
        >>> d.dbapi_con.create_function("pystrftime", 2, utils.pystrftime)
        >>> month = d.sql("SELECT pystrftime('%b', '2020-01-01') AS abbr;")
        >>> month["abbr"].iat[0]
        'Jan'


    Also see SQLite_ and datetime_ references.

    .. _SQLite: https://sqlite.org/lang_datefunc.html
    .. _datetime: https://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
    """
    return parse_date(timestring).strftime(directive)



from prettytable import PrettyTable
from sqlalchemy import MetaData

'''
sql = d.sql("SELECT * FROM sqlite_master WHERE type='table' and name='Album'")[
    "sql"].iloc[0]

rgx = "FOREIGN KEY \(\[(.*)\]\) REFERENCES \[(.*)\] \(\[(.*)\]\)"

column_name, foreign_table, foreign_key = re.findall(rgx, sql)[0]

self.assertEqual(column_name, "ArtistId")
self.assertEqual(foreign_key, "ArtistId")
self.assertEqual(foreign_table, "Artist")
'''


def df_to_prettytable(df, name=None):
    pt = prettytable.PrettyTable(df.columns.tolist())
    for row in df.itertuples(index=False):
        pt.add_row(list(row))

    # Add name
    if name:
        r = str(pt).split('\n')[0]
        brk = "+" + "-" * (len(r) - 2) + "+"
        title = "|" + name.center(len(r) - 2) + "|"
        pt = (brk + "\n" + title + "\n" + str(pt))
    return str(pt)


"""
+------------------------------------------------------------------------+
|                                 Track                                  |
+--------------+----------------+-----------------------+----------------+
|   Columns    |      Type      |      Foreign Keys     | Reference Keys |
+--------------+----------------+-----------------------+----------------+
|   TrackId    |    INTEGER     |                       |                |
|     Name     | NVARCHAR(200)  |                       |                |
|   AlbumId    |    INTEGER     |     Album.AlbumId     |                |
| MediaTypeId  |    INTEGER     | MediaType.MediaTypeId |                |
|   GenreId    |    INTEGER     |     Genre.GenreId     |                |
|   Composer   | NVARCHAR(220)  |                       |                |
| Milliseconds |    INTEGER     |                       |                |
|    Bytes     |    INTEGER     |                       |                |
|  UnitPrice   | NUMERIC(10, 2) |                       |                |
+--------------+----------------+-----------------------+----------------+
"""

'''
def get_reference(target_table):
    refs = []
    for name in d.table_names:
        schema = make_table_schema(name)
        schema.apply(
            lambda x: refs.append("{}.{}".format(name, x["Column"]))
            if target_table in x["Foreign Key"] else '', axis=1)
    return ", ".join(refs)


def make_table_schema(table_name):
    cols = ["Column", "Type", "Foreign Key", "Reference Keys"]
    schema = pd.DataFrame([], columns=cols)

    target = d.meta.tables[table_name]

    for column in target.columns:
        fkeys = list(column.foreign_keys)
        for_key = ""
        if fkeys:
            for_key = fkeys[0].target_fullname
        ref_key = ""  # TODO:

        # print("{} {} {}".format(column.name, column.type, fkey))
        row = pd.DataFrame([
            [column.name,
             column.type,
             for_key,
             ref_key]],
            columns=cols)
        schema = schema.append(row)
    return schema.reset_index(drop=True)



schema = make_table_schema("Album")
print(df_to_prettytable(schema, name="Album"))
"""
+-------------------------------------------------------------+
|                            Track                            |
+----------+---------------+-----------------+----------------+
|  Column  |      Type     |   Foreign Key   | Reference Keys |
+----------+---------------+-----------------+----------------+
| AlbumId  |    INTEGER    |                 |                |
|  Title   | NVARCHAR(160) |                 |                |
| ArtistId |    INTEGER    | Artist.ArtistId |                |
+----------+---------------+-----------------+----------------+
"""


.cached_schema

.refresh_schema()


def make_reference_table():
    ref_cols = ["Table", "Primary Key", "Foreign Table", "Foreign Key"]
    reference_keys = pd.DataFrame([], columns=ref_cols)
    for tbl in d.table_names:  # self
        obj = d.meta.tables[tbl]  # self.meta
        row = [(obj.fullname,) + tuple(i.target_fullname.split(".")) for i in
               obj.foreign_keys]
        reference_keys = reference_keys.append(
            pd.DataFrame(row, columns=ref_cols))
    return reference_keys.reset_index(drop=True)


a = d.meta.tables["Track"]
[[re.findall("'(.*?)'", i.columns.__str__())[0], i.elements[0].target_fullname] for i in a.foreign_key_constraints]

ref = make_reference_table()


get_reference("Album")

# Album.AlbumId is ref'd by Track.AlbumId, and others
track = make_table_schema("Track")
album = make_table_schema("Album")

ref = make_reference_table()
make_table_schema("Album")
ref[ref["Table"] == "Album"]
ref[ref["Foreign Table"] == "Album"]
'''
