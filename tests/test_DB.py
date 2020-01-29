# !/usr/bin/env python2
"""
Test db module
"""

from __future__ import unicode_literals

import unittest

import pandas as pd

from db2 import DB, SQLiteDB


CHINOOK = "tests/chinook.sqlite"


class TestDatabaseProperties(unittest.TestCase):
    def setUp(self):
        self.d = DB(dbname=":memory:", dbtype="sqlite")

    def test_dbtype(self):
        self.assertTrue(self.d.dbtype == "sqlite")
        self.assertTrue(self.d.dbname == ":memory:")

    def test_table_names_one(self):
        # New db has no tables
        self.assertEqual(self.d.table_names, [])
        # Create 1 table
        self.d.engine.execute("CREATE TABLE test0 (id INT PRIMARY KEY)")
        self.assertEqual(self.d.table_names, ["test0"])

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

    def test_pragma(self):
        self.create_test_table()
        # Test pragma
        self.assertTrue(not self.d.sql("PRAGMA table_info('test');").empty)

    def test_script(self):
        # Test executescript
        self.d.sql(
            "DROP TABLE IF EXISTS test; "
            "CREATE TABLE test (id INT PRIMARY KEY, name TEXT NOT NULL); ")

    def create_test_table(self):
        return self.d.sql(
            "CREATE TABLE test (id INT PRIMARY KEY, name TEXT NOT NULL);")

    def test_select_empty_dataframe(self):
        self.create_test_table()
        # Test execute single select, empty dataframe
        r = self.d.sql("SELECT * FROM test")
        self.assertEqual(r.columns.tolist(), ["id", "name"])
        self.assertTrue(r.empty)

    def test_select_not_empty_dataframe(self):
        # A create statement
        c = self.create_test_table()
        self.assertTrue(not c.empty)
        # An insert statement
        one = self.d.sql("INSERT INTO test VALUES (1, 'One');")
        self.assertTrue(not one.empty)
        # A select statement
        r = self.d.sql("SELECT * FROM test")
        self.assertEqual(r.columns.tolist(), ["id", "name"])
        self.assertTrue(not r.empty)
        self.assertEqual(r["id"].iloc[0], 1)
        self.assertEqual(r["name"].iloc[0], "One")

    def test_executemany_novars(self):
        self.create_test_table()
        # Test executemany, no vars
        self.d.sql("INSERT INTO test VALUES (1, 'One'); "
                   "INSERT INTO test VALUES (2, 'Two');")
        r = self.d.sql("SELECT * FROM test")
        self.assertTrue(r["name"].tolist() == ["One", "Two"])

    def test_executemany_qmark(self):
        self.create_test_table()
        # Test executemany, qmark vars
        self.d.sql("INSERT INTO test VALUES (?, ?)",
                   [(1, "One"), (2, "Two")])
        r = self.d.sql("SELECT * FROM test")
        self.assertTrue(r["name"].tolist() == ["One", "Two"])

    def test_executemany_named(self):
        self.create_test_table()
        # Test executemany, named vars
        self.d.sql("INSERT INTO test VALUES (:id, :name)",
                   [{"id": 1, "name": "One"},
                    {"id": 2, "name": "Two"}])
        r = self.d.sql("SELECT * FROM test")
        self.assertTrue(r["name"].tolist() == ["One", "Two"])

    def test_executemany_handlebars(self):
        self.create_test_table()
        # Test executemany, handlebars
        self.d.sql("INSERT INTO test VALUES ({{id}}, '{{name}}')",
                   [{"id": 1, "name": "One"},
                    {"id": 2, "name": "Two"}])
        r = self.d.sql("SELECT * FROM test")
        self.assertTrue(r["name"].tolist() == ["One", "Two"])

    def test_single_select_novars(self):
        self.create_test_table()
        self.d.sql("INSERT INTO test VALUES (1, 'One'); "
                   "INSERT INTO test VALUES (2, 'Two'); "
                   "INSERT INTO test VALUES (3, 'Three');")
        # Test execute single select, no vars
        r = self.d.sql("SELECT * FROM test LIMIT 2")
        self.assertTrue(r["name"].tolist() == ["One", "Two"])

    def test_single_select_qmark(self):
        self.create_test_table()
        self.d.sql("INSERT INTO test VALUES (1, 'One'); "
                   "INSERT INTO test VALUES (2, 'Two'); "
                   "INSERT INTO test VALUES (3, 'Three');")
        # Test execute single select, qmark
        r = self.d.sql("SELECT * FROM test WHERE id > ?", (2,))
        self.assertTrue(r["name"].tolist() == ["Three"])

    def test_single_select_named(self):
        self.create_test_table()
        self.d.sql("INSERT INTO test VALUES (1, 'One'); "
                   "INSERT INTO test VALUES (2, 'Two'); "
                   "INSERT INTO test VALUES (3, 'Three');")
        # Test execute single select, named
        r = self.d.sql("SELECT * FROM test WHERE id > :id", {"id": 2})
        self.assertTrue(r["name"].tolist() == ["Three"])

    def test_single_select_handlebars(self):
        self.create_test_table()
        self.d.sql("INSERT INTO test VALUES (1, 'One'); "
                   "INSERT INTO test VALUES (2, 'Two'); "
                   "INSERT INTO test VALUES (3, 'Three');")
        # Test execute single select, handlebars
        r = self.d.sql("SELECT * FROM test WHERE id > {{id}}", {"id": 2})
        self.assertTrue(r["name"].tolist() == ["Three"])

    def test_sql_union1(self):
        q = """
        SELECT '{{ name }}' as table_name, sum(1) as cnt
        FROM {{ name }}
        GROUP BY table_name
        """
        data = [
            {"name": "Album"},
            {"name": "Artist"},
            {"name": "Track"}
        ]
        d = SQLiteDB(CHINOOK)
        r = d.sql(q, data)
        self.assertEqual(r.columns.tolist(), ["table_name", "cnt"])
        self.assertEqual(r["cnt"].sum(), 4125)

    def test_sql_union2(self):
        favorites = ["Eric Clapton", "Led Zeppelin", "Jimi Hendrix", "AC/DC"]
        fav_data = [{"fav": band} for band in favorites]
        q = ("SELECT a.Name AS Band, COUNT(b.Title) AS Albums "
             "FROM Artist a "
             "LEFT JOIN Album b "
             "    ON a.ArtistId=b.ArtistId WHERE a.Name = '{{ fav }}'")
        d = SQLiteDB(CHINOOK)
        r = d.sql(q, fav_data)
        self.assertEqual(r.columns.tolist(), ["Band", "Albums"])
        self.assertEqual(r[r["Band"] == "Led Zeppelin"]["Albums"].iat[0], 14)

    def test_sql_create_results(self):
        self.create_test_table()
        # Executing two statements produces a two row dataframe
        one = self.d.sql("INSERT INTO test VALUES (1, 'One');")
        self.assertEqual(one.columns.tolist(), ["SQL", "Result"])
        self.assertEqual(one["Result"].iloc[0], 1)

        # Executing two statements produces a two row dataframe
        two = self.d.sql("INSERT INTO test VALUES (2, 'Two'); "
                         "INSERT INTO test VALUES (3, 'Three');")
        self.assertEqual(len(two), 2)
        # First row
        self.assertEqual(
            two["SQL"].iloc[0], "INSERT INTO test VALUES (2, 'Two');")
        self.assertEqual(two["Result"].iloc[0], 1)
        # Second row
        self.assertEqual(
            two["SQL"].iloc[1], "INSERT INTO test VALUES (3, 'Three');")
        self.assertEqual(two["Result"].iloc[0], 1)


class TestDatabaseURLs(unittest.TestCase):
    def test_url(self):
        d = DB(url="sqlite:///:memory:")
        # TODO: parse the URL for credential components?
        # self.assertEqual(d.dbname, ":memory:")
        self.assertEqual(d.dbtype, "sqlite")

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
