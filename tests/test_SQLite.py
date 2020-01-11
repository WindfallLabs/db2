# !/usr/bin/env python2

import unittest

from db2 import SQLiteDB

CHINOOK = "./chinook.sqlite"


class TestSQLite(unittest.TestCase):
    def test_has_executemany(self):
        d = SQLiteDB(":memory:")
        self.assertTrue(hasattr(d.cur, "executemany"))

    def test_attach_and_detach(self):
        d = SQLiteDB(":memory:")
        # Attach
        d.attach_db(CHINOOK)
        self.assertEqual(
            d.databases["file"].tolist(),
            [":memory:", "./chinook.sqlite"])
        self.assertEqual(
            d.databases["name"].tolist(),
            ["main", "chinook"])
        # Detach
        d.detach_db("chinook")
        self.assertEqual(
            d.databases["file"].tolist(),
            [":memory:"])
