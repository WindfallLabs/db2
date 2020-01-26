# !/usr/bin/env python2
"""
Misc functions and helpers.
"""

from dateutil.parser import parse as parse_date
from decimal import Decimal

import pandas as pd
import sqlparse

import db2


def sqlite_adapt_datetime(date_time):
    """Adapts ``datetime.datetime`` types to SQLite TEXT with."""
    return date_time.strftime(db2.SQLITE_DATETIME_FORMAT)


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
    return parsed_sql.get_type() == u"SELECT" and not type(
        tokens[0]).__name__ == "Function"


# =============================================================================
# SQL Functions:
# =============================================================================

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
        u'Jan'


    Also see SQLite_ and datetime_ references.

    .. _SQLite: https://sqlite.org/lang_datefunc.html
    .. _datetime: https://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
    """
    return parse_date(timestring).strftime(directive)
