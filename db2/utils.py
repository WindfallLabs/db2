# !/usr/bin/env python2
"""
Misc functions and helpers.
"""

from __future__ import unicode_literals

import base64
import inspect
import json
import re
import os
from dateutil.parser import parse as parse_date
from decimal import Decimal

import pandas as pd
import sqlparse
from prettytable import PrettyTable

import db2


# =============================================================================
# SQLite Adapter Functions:
# These functions must be registered with the sqlite3 library
# =============================================================================

def sqlite_adapt_datetime(date_time):
    """
    Adapts ``datetime.datetime`` types to SQLite TEXT with.
    """
    return date_time.strftime(db2.options["sqlite_datetime_format"])


def sqlite_adapt_decimal(decimal):
    """
    Adapts ``decimal.Decimal`` types to FLOAT.
    """
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

    Example
    -------
    >>> from decimal import Decimal
    >>> import pandas as pd
    >>> from db2.utils import decimals_to_floats
    >>> df = pd.DataFrame([[Decimal(2.0)], [Decimal(1.0)]], columns=["D"])
    >>> assert df["D"].tolist() == [Decimal(2.0), Decimal(1.0)]
    >>> converted = decimals_to_floats(df)
    >>> converted["D"].tolist() == [2.0, 1.0]
    True
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

    Example
    -------
    >>> import sqlparse
    >>> from db2.utils import is_query
    >>> p = sqlparse.parse(
    ...     "SELECT * FROM sqlite_master WHERE type='table'")
    >>> is_query(p[0])
    True
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
    # Use as a Python function
    >>> pystrftime("%b", "2020-01-01")
    'Jan'

    # Use as an SQL function
    >>> from db2 import SQLiteDB, utils
    >>> d = SQLiteDB(":memory:", functions=[utils.pystrftime])
    >>> month = d.sql("SELECT pystrftime('%b', '2020-01-01') AS abbr;")
    >>> month["abbr"].iat[0]
    u'Jan'


    Also see SQLite_ and datetime_ references.

    .. _SQLite: https://sqlite.org/lang_datefunc.html
    .. _datetime: https://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
    """
    return parse_date(timestring).strftime(directive)


# =============================================================================
# Misc Functions:
# =============================================================================


def df_to_prettytable(df, name=None):
    """
    Convert a DataFrame as to a PrettyTable string.

    Parameters
    ----------
    df: DataFrame
        DataFrame to convert to PrettyTable-style string.
    name: str (optional)
        Optionally add a string centered above the pretty table.

    Example
    -------
    >>> import pandas as pd
    >>> df = pd.DataFrame([[0, "Cat"], [1, "Dog"]], columns=["ID", "Name"])
    >>> print(df_to_prettytable(df, "Animals"))  # doctest: +NORMALIZE_WHITESPACE
    <BLANKLINE>
    +-----------+
    |  Animals  |
    +----+------+
    | ID | Name |
    +----+------+
    | 0  | Cat  |
    | 1  | Dog  |
    +----+------+
    <BLANKLINE>
    """
    pt = PrettyTable(df.columns.tolist())
    for row in df.itertuples(index=False):
        pt.add_row(list(row))

    # Add name
    if name:
        r = str(pt).split('\n')[0]
        brk = "+" + "-" * (len(r) - 2) + "+"
        title = "|" + name.center(len(r) - 2) + "|"
        pt = (brk + "\n" + title + "\n" + str(pt))
    return "\n{}\n".format(str(pt))


class ProfileHandler:
    # ProfileHandler.load("/path/to/my/profile.json")
    # ms = db2.MSSQLDB(profile="/path/to/my/profile")
    @staticmethod
    def load(profile_path):
        if not os.path.exists(profile_path):
            raise AttributeError("File does not exist")
        with open(profile_path, "r") as f:
            return json.loads(ProfileHandler.decode(f.read()))

    @staticmethod
    def save(profile_path, data):
        with open(profile_path, "w") as f:
            f.write(ProfileHandler.encode(data))
        return

    @staticmethod
    def encode(data):
        return base64.b64encode(json.dumps(data).encode("utf-8"))

    @staticmethod
    def decode(data):
        return base64.b64decode(data).decode("utf-8")
