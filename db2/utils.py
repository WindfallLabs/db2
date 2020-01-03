# -*- coding: utf-8 -*-
# !/usr/bin/env python2
"""
db2: Utilities and Helpers
"""

from collections import OrderedDict

from sqlalchemy.ext.declarative import declarative_base


class RowConverter(object):
    """
    Converts a row (collection) to a mapping instance.

    Parameters
    ----------
    db_obj: DB
        A database object inherit session and engine from
    table_name: str
        The name of a table
    row: collection
        A dict, OrderedDict, list, list of tuples, or namedtuple containing
        sufficient table data to INSERT.

    Returns a mapping instance.

    Examples
    --------
    >>> d = DB(dbname=":memory:", dbtype="sqlite")
    >>> d.engine.execute("CREATE TABLE Artist (ArtistId INTEGER PRIMARY KEY, Name TEXT);")  # doctest:+ELLIPSIS
    <sqlalchemy.engine.result.ResultProxy object at 0x...>
    >>> d.insert([{"ArtistId": 1, "Name": "AC/DC"}], "Artist")
    >>> d.engine.execute("SELECT * FROM Artist").fetchall() == [(1, "AC/DC")]
    True
    """
    def __init__(self, db_obj, table_name, row):
        self.db = db_obj
        self.table_name = table_name
        self._row = row
        self.row_type = type(row)
        # Dynamically create a mapping for existing tables
        self.mapping = type(
            table_name,
            (declarative_base(bind=self.db.engine),),
            {"__tablename__": table_name,
             "__table_args__": {"autoload": True}})

    @staticmethod
    def _is_namedtuple(x):
        """
        Checks if input 'nt' is an instance of collections.namedtuple.

        Example
        -------
        >>> from collections import namedtuple
        >>> track = namedtuple("Track", ["TrackId", "Name", "AlbumId", "MediaTypeId"])
        >>> acdc = track(1, "For Those About To Rock (We Salute You)", 1, 1)
        >>> RowConverter._is_namedtuple(acdc) is True
        True
        >>> RowConverter._is_namedtuple(["For Those About To Rock (We Salute You)"]) is False
        True
        """
        # Source: https://stackoverflow.com/questions/2166818
        t = type(x)
        b = t.__bases__
        if len(b) != 1 or b[0] != tuple:
            return False
        f = getattr(t, '_fields', None)
        if not isinstance(f, tuple):
            return False
        return all(type(n) == str for n in f)

    def to_dict(self):
        """
        Converts an input collection to a dictionary.
        """
        # From dict / OrderedDict
        if self.row_type in (dict, OrderedDict):
            return self._row

        # From namedtuple
        elif RowConverter._is_namedtuple(self._row):
            return self._row.__dict__

        # From list or list of tuples
        elif self.row_type is list:
            # List of tuples
            if all([type(i) is tuple for i in self._row]):
                return dict(self._row)
            # Just a list
            cols = [col.name for col in self.mapping.__table__.columns]
            row = dict(zip(cols, self._row))
            return row

    def convert(self):
        return self.mapping(**self.to_dict())

    def __str__(self):
        return "RowConverter: {}(**{})".format(self.table_name, self.to_dict())

    def __repr__(self):
        return "<{}>".format(self.__str__())
