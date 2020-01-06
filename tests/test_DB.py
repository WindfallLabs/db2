# -*- coding: utf-8 -*-
# !/usr/bin/env python2
"""
Module Docstring
"""

import unittest
from collections import namedtuple
from sqlite3 import OperationalError

import pandas as pd
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
        d.credentials["dbtype"] = "mssql"
        self.assertEqual(d.dbtype, "mssql")
        self.assertEqual(
            d._assign_limit("SELECT * FROM Artists", 10),
            'SELECT TOP 10 * FROM (SELECT * FROM Artists) q')


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
