# !/usr/bin/env python2
"""
Misc functions and helpers.
"""

from dateutil.parser import parse as parse_date
from decimal import Decimal

import pandas as pd


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
    """
    if any(set(col.apply(lambda x: isinstance(x, Decimal)))):
        return pd.to_numeric(col)
    return col


# =============================================================================
# SQL Functions:
# =============================================================================

def pystrftime(directive, timestring):
    """
    Extends the built-in function 'strftime' with Python's datetime module.
    This adds support for directives not natively supported in some SQL
    flavors. For example, this function adds the following to SQLite:

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
