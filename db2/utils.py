# !/usr/bin/env python2
"""
Misc functions and helpers.
"""

from dateutil.parser import parse as parse_date
from decimal import Decimal


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
    # Python:
    >>> pystrftime("%b", "2020-01-01")
    u'Jan'
    # SQL function:
    >>> d = db2.DemoDB()
    # Create an SQL function "pystrftime" using the pystrftime Python function
    # that takes 2 arguments.
    >>> d.dbapi_con.create_function("pystrftime", 2, db2.utils.pystrftime)
    >>> d.sql("SELECT pystrftime('%b', '2020-01-01') AS abbr;")["abbr"].iat[0]
    u'Jan'

    Sources
    -------
    https://sqlite.org/lang_datefunc.html
    https://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
    """
    return parse_date(timestring).strftime(directive)
