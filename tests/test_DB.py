# -*- coding: utf-8 -*-
# !/usr/bin/env python2
"""
Module Docstring
"""

import unittest
from collections import namedtuple
from sqlite3 import OperationalError

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

from db2 import DB


class TestQueryFunctions(unittest.TestCase):

    def test_limit(self):
        d = DB(dbname=":memory:", dbtype="sqlite")
        self.assertEqual(
            d._assign_limit("SELECT * FROM Artists", 5),
            'SELECT * FROM (SELECT * FROM Artists) q LIMIT 5')

    def test_mssql_limit(self):
        d = DB(dbname=":memory:", dbtype="sqlite")
        # Spoof the dbtype to mssql
        d._config["connection_data"]["dbtype"] = "mssql"
        self.assertEqual(d._config["connection_data"]["dbtype"], "mssql")
        self.assertEqual(
            d._assign_limit("SELECT * FROM Artists", 10),
            'SELECT TOP 10 * FROM (SELECT * FROM Artists) q')


class TestDatabaseTables(unittest.TestCase):
    def setUp(self):
        self.d = DB(dbname=":memory:", dbtype="sqlite")

    def tearDown(self):
        self.d.con.close()
        self.d.engine.dispose()
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
            'mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BODBC+DRIVER+13+for+SQL+Server%7D%3BSERVER%3Dlocalhost%3BDATABASE%3Dchinook%3BUID%3Ddbread%3BPWD%3Dmy%24tr0ngPWD'
            )


class TestORM_Inserts(unittest.TestCase):
    def setUp(self):
        self.d = DB(dbname=":memory:", dbtype="sqlite")
        self.s = "SELECT * FROM Artist"

    def tearDown(self):
        self.d.con.close()
        self.d.engine.dispose()
        del self.d

    def create_table(self):
        self.d.engine.execute(
            "CREATE TABLE Artist (ArtistId INT PRIMARY KEY, Name TEXT)")

    def test_insert_dict(self):
        self.create_table()
        self.d.insert([{"ArtistId": 2, "Name": "Accept"}], "Artist")
        self.assertEqual(
            self.d.engine.execute(self.s).fetchall(),
            [(2, "Accept")])

    def test_insert_tuples(self):
        self.create_table()
        self.d.insert([[("ArtistId", 3), ("Name", "Aerosmith")]], "Artist")
        self.assertEqual(
            self.d.engine.execute(self.s).fetchall(),
            [(3, "Aerosmith")])

    def test_insert_namedtuple(self):
        self.create_table()
        ArtistNT = namedtuple("Artist", ["ArtistId", "Name"])
        self.d.insert([ArtistNT(4, "Alanis Moressette")], "Artist")
        self.assertEqual(
            self.d.engine.execute(self.s).fetchall(),
            [(4, "Alanis Moressette")])

    def test_insert_list(self):
        self.create_table()
        self.d.insert([[5, "Alice In Chains"]], "Artist")
        self.assertEqual(
            self.d.engine.execute(self.s).fetchall(),
            [(5, "Alice In Chains")])


'''
session.add_all([
    Album(AlbumId=1, Title="For Those About To Rock We Salute You", ArtistId=1),
    Album(AlbumId=2, Title="Balls to the Wall", ArtistId=2),
    Album(AlbumId=3, Title="Restless and Wild", ArtistId=2),
    Album(AlbumId=4, Title="Let There Be Rock", ArtistId=1),
    Album(AlbumId=5, Title="Big Ones", ArtistId=3)
])

session.commit()
'''
