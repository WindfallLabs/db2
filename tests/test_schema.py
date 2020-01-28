# !/usr/bin/env python2
"""
Tests the Schema and TableSchema objects when used as a database attribute.
"""

import unittest

from db2 import SQLiteDB


CHINOOK = "tests/chinook.sqlite"


class TestSchemaObject(unittest.TestCase):
    def setUp(self):
        self.d = SQLiteDB(CHINOOK)

    def test_refresh_schema(self):
        self.assertTrue(not hasattr(self.d.schema, "meta"))
        self.assertEqual(str(self.d.schema),
                         "<Schema (chinook.sqlite): NOT LOADED>")
        self.d.schema.refresh()
        self.assertEqual(str(self.d.schema),
                         "<Schema (chinook.sqlite): 11 Tables Loaded>")
        self.assertTrue(hasattr(self.d.schema, "meta"))
        self.assertTrue(hasattr(self.d.schema, "Artist"))

    def test_table_attrs(self):
        self.d.schema.refresh()
        self.assertTrue(hasattr(self.d.schema.Artist, "table_schema"))
        self.assertTrue(hasattr(self.d.schema, "Artist"))
        self.assertEqual(self.d.schema.Artist.name, "Artist")
        self.assertEqual(self.d.schema.Artist.columns,
                         ["ArtistId", "Name"])
        self.assertEqual(self.d.schema.Artist.count, 275)
