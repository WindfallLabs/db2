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


class CursorTests(unittest.TestCase):
    def setUp(self):
        self.d = SQLiteDB(":memory:")
        self.d.engine.execute("DROP TABLE IF EXISTS Artist")
        self.d.cur.execute(
            "CREATE TABLE Artist (ArtistId INT PRIMARY KEY, Name TEXT);")

    def tearDown(self):
        del self.d

    def test_trivial(self):
        self.assertTrue(hasattr(self.d, "cur"))

    def test_limit(self):
        self.assertEqual(
            self.d.cur._assign_limit("SELECT * FROM Artists", 5),
            'SELECT * FROM (SELECT * FROM Artists) q LIMIT 5')

    def test_mssql_limit(self):
        # Spoof the dbtype to mssql
        self.d.credentials["dbtype"] = "mssql"
        self.assertEqual(self.d.dbtype, "mssql")
        self.assertEqual(
            self.d.cur._assign_limit("SELECT * FROM Artists", 10),
            'SELECT TOP 10 * FROM (SELECT * FROM Artists) q')

    def test_handlebars_without_semicolon(self):
        # Original query does not include semicolon
        # _apply_handlebars does not add one to single queries
        template = u"INSERT INTO Artist VALUES ({{id}}, '{{name}}')"
        # Apply once
        once = self.d.cur._apply_handlebars(
            template,
            [{"id": 1, "name": "AC/DC"}])
        expected1 = u"INSERT INTO Artist VALUES (1, 'AC/DC')"
        self.assertEqual(once, expected1)

        # Apply for many
        twice = self.d.cur._apply_handlebars(
            template,
            [{"id": 1, "name": "AC/DC"}, {"id": 2, "name": "Accept"}],
            union=False)
        expected2 = (u"INSERT INTO Artist VALUES (1, 'AC/DC');\n"
                     u"INSERT INTO Artist VALUES (2, 'Accept');")
        self.assertEqual(twice, expected2)

    def test_handlebars_with_semicolon(self):
        # Includes semicolon
        semicolon_template = u"INSERT INTO Artist VALUES ({{id}}, '{{name}}');"
        # Apply once
        once = self.d.cur._apply_handlebars(
            semicolon_template,
            [{"id": 1, "name": "AC/DC"}])
        expected1 = u"INSERT INTO Artist VALUES (1, 'AC/DC');"
        self.assertEqual(once, expected1)

        # Apply for many
        twice = self.d.cur._apply_handlebars(
            semicolon_template,
            [{"id": 1, "name": "AC/DC"}, {"id": 2, "name": "Accept"}],
            union=False)
        expected2 = (u"INSERT INTO Artist VALUES (1, 'AC/DC');\n"
                     u"INSERT INTO Artist VALUES (2, 'Accept');")
        self.assertEqual(twice, expected2)

    def test_execute(self):
        self.d.cur.execute(
            "INSERT INTO Artist VALUES (1, 'AC/DC');")
        self.assertTrue("Artist" in self.d.table_names)
        df = self.d.cur.execute("SELECT * FROM Artist")
        self.assertEqual(df["Name"].iat[0], "AC/DC")

    def test_executemany_named(self):
        # SQLite named
        self.d.cur.executemany(
            "INSERT INTO Artist VALUES (:id, :name);",
            ({"id": 1, "name": "AC/DC"}, {"id": 2, "name": "Accept"}))
        df = self.d.cur.execute(
            "SELECT * FROM Artist WHERE ArtistId IN (1, 2)")
        self.assertTrue(df["Name"].tolist(), ["AC/DC", "Accept"])

    def test_executemany_qmark(self):
        # SQLite named
        self.d.cur.executemany(
            "INSERT INTO Artist VALUES (?, ?);",
            [(1, "AC/DC"), (2, "Accept")])
        df = self.d.cur.execute(
            "SELECT * FROM Artist WHERE ArtistId IN (1, 2)")
        self.assertTrue(df["Name"].tolist(), ["AC/DC", "Accept"])

    def test_executemany_handlebars(self):
        # SQLite named
        self.d.cur.executemany(
            "INSERT INTO Artist VALUES ({{id}}, '{{name}}');",
            [{"id": 1, "name": "AC/DC"}, {"id": 2, "name": "Accept"}])
        df = self.d.cur.execute(
            "SELECT * FROM Artist WHERE ArtistId IN (1, 2)")
        self.assertTrue(df["Name"].tolist(), ["AC/DC", "Accept"])

    def test_executescript(self):
        s = ("INSERT INTO Artist VALUES (1, 'AC/DC');"
             "INSERT INTO Artist VALUES (2, 'Accept');"
             "INSERT INTO Artist VALUES (3, 'Aeromith');")
        self.d.cur.executescript(s)
        df = self.d.cur.execute("SELECT * FROM Artist")
        self.assertEqual(len(df), 3)


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
                "driver": '{ODBC DRIVER 13 for SQL Server}'}),
            # Big string
            ("mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BODBC+DRIVER+13+for+SQL"
             "+Server%7D%3BSERVER%3Dlocalhost%3BDATABASE%3Dchinook%3B"
             "UID%3Ddbread%3BPWD%3Dmy%24tr0ngPWD")
            )
