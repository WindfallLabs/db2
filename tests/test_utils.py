# !/usr/bin/env python2
"""
Test utils module
"""

from __future__ import unicode_literals

import unittest
from datetime import datetime
from decimal import Decimal

import sqlparse

import db2
from db2 import utils


class SQLAdapterFunctions(unittest.TestCase):
    def setUp(self):
        pass

    def test_datetime(self):
        dt = datetime(2020, 1, 1, 0, 0, 0, 0)
        self.assertEqual(
            utils.sqlite_adapt_datetime(dt),
            "2020-01-01 00:00:00")


class PandasUtilsTests(unittest.TestCase):
    def setUp(self):
        pass


class ParsedSQLFunctions(unittest.TestCase):
    def setUp(self):
        pass

    def test_is_query(self):
        query1 = sqlparse.parse(
            "SELECT * FROM sqlite_master")[0]
        query2 = sqlparse.parse(
            "SELECT    *     FROM       sqlite_master")[0]
        function = sqlparse.parse(
            "SELECT ExampleFunction()")[0]
        statement1 = sqlparse.parse(
            "CREATE TABLE test (id INT, name TEXT)")[0]
        statement2 = sqlparse.parse(
            "INSERT INTO test VALUES (1, 'Aerosmith')")[0]
        statement3 = sqlparse.parse(
            "UPDATE test SET name = 'AC/DC' WHERE id = 1)")[0]

        # TRUE
        self.assertTrue(utils.is_query(query1))
        self.assertTrue(utils.is_query(query2))
        # FALSE
        self.assertFalse(utils.is_query(function))
        self.assertFalse(utils.is_query(statement1))
        self.assertFalse(utils.is_query(statement2))
        self.assertFalse(utils.is_query(statement3))


class SQLFunctionTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_pystrftime(self):
        d = db2.SQLiteDB(":memory:", functions=[utils.pystrftime])
        month = d.sql("SELECT pystrftime('%b', '2020-01-01') AS abbr;")
        self.assertEqual(month["abbr"].iat[0], "Jan")
