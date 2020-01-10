# !/usr/bin/env python2
"""
Test db module
"""

import unittest
from collections import namedtuple
from sqlite3 import OperationalError

import pandas as pd
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

from db2 import DB, SQLiteDB


class TestDatabaseTables(unittest.TestCase):
    def setUp(self):
        self.d = DB(dbname=":memory:", dbtype="sqlite")

    def tearDown(self):
        del self.d

    def test_table_names_one(self):
        # New db has no tables
        self.assertEqual(self.d.table_names, [])
        # Create 1 table
        self.d.engine.execute("CREATE TABLE test (id INT PRIMARY KEY)")
        self.assertEqual(self.d.table_names, ["test"])

    def test_table_names_two(self):
        # Create 2 tables
        self.assertEqual(self.d.table_names, [])
        self.d.engine.execute("CREATE TABLE test1 (id INT PRIMARY KEY)")
        self.d.engine.execute("CREATE TABLE test2 (id INT PRIMARY KEY)")
        self.assertEqual(self.d.table_names, ["test1", "test2"])


class TestSQL(unittest.TestCase):
    def setUp(self):
        self.d = DB(dbname=":memory:", dbtype="sqlite")

    def tearDown(self):
        del self.d

    def test_insert(self):
        self.d.sql("CREATE TABLE test (id INT PRIMARY KEY, name TEXT)")
        self.d.sql("INSERT INTO test VALUES (0, 'AC/DC')")
        expected = pd.DataFrame(data=[[1, 'AC/DC']], columns=['id', 'name'])
        t = self.d.sql("SELECT * FROM test")
        self.assertEqual(list(t.columns), list(expected.columns))
        self.assertEqual(t["name"].iat[0], expected["name"].iat[0])

    def test_insert_many(self):
        insert_data = [
            {"id": 1, "name": "AC/DC"},
            {"id": 2, "name": "Accept"},
            {"id": 3, "name": "Aerosmith"}
            ]
        self.d.sql("CREATE TABLE test (id INT PRIMARY KEY, name TEXT)")
        self.d.sql(
            "INSERT INTO test VALUES ({{id}}, '{{name}}')",
            insert_data,
            union=False)
        expected = pd.DataFrame(
            data=[
                [1, "AC/DC"],
                [2, "Accept"],
                [3, "Aerosmith"]],
            columns=["id", "name"])
        t = self.d.sql("SELECT * FROM test")
        self.assertEqual(list(t.columns), list(expected.columns))
        self.assertEqual(t["name"].iat[0], expected["name"].iat[0])


class TestDatabaseURLs(unittest.TestCase):
    def test_sqlite(self):
        self.assertEqual(
            DB._create_url(dbname=":memory:", dbtype="sqlite"),
            "sqlite:///:memory:"
            )
        self.assertEqual(
            DB._create_url(dbname="C:/Docs/shinook.sqlite", dbtype="sqlite"),
            "sqlite:///C:/Docs/shinook.sqlite"
            )

    def test_postgres(self):
        self.assertEqual(
            DB._create_url(**{
                "dbtype": "postgres", "user": "dbread",
                "pwd": "my$tr0ngPWD", "host": "localhost", "port": 8080,
                "dbname": "chinook", "driver": None}),
            "postgres://dbread:my$tr0ngPWD@localhost:8080/chinook")

    def test_mssql(self):
        self.assertEqual(
            DB._create_url(**{
                "dbname": "chinook", "user": "dbread", "pwd": "my$tr0ngPWD",
                "host": "localhost", "dbtype": "mssql",
                "driver": 'pymssql'}),
            # Big string
            "mssql+pymssql://dbread:my$tr0ngPWD@localhost/?charset=utf8")
