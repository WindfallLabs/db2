# !/usr/bin/env python2

from __future__ import unicode_literals

import os
import unittest
from datetime import datetime
from decimal import Decimal

import db2
from db2 import SQLiteDB

CHINOOK = "tests/chinook.sqlite"


class TestSQLite(unittest.TestCase):
    def test_attach_and_detach(self):
        d = SQLiteDB(":memory:")
        full_path = os.path.abspath(CHINOOK)
        # Attach
        d.attach_db(CHINOOK)
        self.assertEqual(
            d.databases["file"].tolist(),
            ["", full_path])
        self.assertEqual(
            d.databases["name"].tolist(),
            ["main", "chinook"])
        # Detach
        d.detach_db("chinook")
        self.assertEqual(d.databases["file"].tolist(), [""])

    def test_decimal_adapter(self):
        d = SQLiteDB(":memory:")
        d.sql("CREATE TABLE decimal_test (id INT PRIMARY KEY, value FLOAT);")
        d.sql("INSERT INTO decimal_test VALUES (?, ?);",
              [(1, Decimal(1)), (2, Decimal(3.14))])
        df = d.sql("SELECT * FROM decimal_test")
        self.assertTrue(not df.empty)

    def test_datetime_adapter_default(self):
        now = datetime.now()
        d = SQLiteDB(":memory:")
        d.sql("CREATE TABLE test_dates (id INT PRIMARY KEY, ddate FLOAT);")
        d.sql("INSERT INTO test_dates VALUES (?, ?);", (1, now))
        ddate = d.sql("SELECT ddate FROM test_dates WHERE id=1")["ddate"].iat[0]
        self.assertEqual(ddate,
                         now.strftime(db2.options["sqlite_datetime_format"]))

    def test_datetime_adapter_custom(self):
        db2.SQLITE_DATETIME_FORMAT = "%d %b, %Y"
        now = datetime.now()
        d = SQLiteDB(":memory:")
        d.sql("CREATE TABLE test_dates (id INT PRIMARY KEY, ddate FLOAT);")
        d.sql("INSERT INTO test_dates VALUES (?, ?);", (1, now))
        ddate = d.sql("SELECT ddate FROM test_dates WHERE id=1")["ddate"].iat[0]
        self.assertEqual(ddate,
                         now.strftime(db2.options["sqlite_datetime_format"]))


class TestOnDisk_notclosed(unittest.TestCase):
    def setUp(self):
        self.path = "tests/test_ondisk.sqlite"
        if os.path.exists(self.path):
            os.remove(self.path)

    def tearDown(self):
        # os.remove(self.path) causes WindowsError, fine on Linux
        pass

    def test_writeread(self):
        self.d = SQLiteDB(self.path)
        self.d.sql("CREATE TABLE test (id INT PRIMARY KEY, name TEXT);")
        self.d.sql("INSERT INTO test VALUES (?, ?)",
                   [(1, 'AC/DC'), (2, 'Accept')])
        # Reconnect
        self.d = SQLiteDB(self.path)
        df = self.d.sql("SELECT * FROM test")
        self.assertEqual(df["id"].tolist(), [1, 2])
