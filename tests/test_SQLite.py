# !/usr/bin/env python2

import os
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


class TestOnDisk_notclosed(unittest.TestCase):
    def setUp(self):
        self.path = "tests/test_ondisk.sqlite"

    def tearDown(self):
        os.remove(self.path)

    def test_writeread(self):
        self.d = SQLiteDB(self.path)
        self.d.sql("CREATE TABLE test (id INT PRIMARY KEY, name TEXT);")
        self.d.sql("INSERT INTO test VALUES (?, ?)",
                   [(1, 'AC/DC'), (2, 'Aerosmith')])
        # Reconnect
        self.d = SQLiteDB(self.path)
        df = self.d.sql("SELECT * FROM test")
        self.assertEqual(df["id"].tolist(), [1, 2])
