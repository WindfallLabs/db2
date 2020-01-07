# -*- coding: utf-8 -*-
# !/usr/bin/env python2
"""
Module Docstring
"""

import unittest
from collections import OrderedDict, namedtuple

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from db2 import RowConverter


class RowConverter_Tests(unittest.TestCase):
    """Ensure that RowConverter converts various containers to dicts."""
    def setUp(self):
        # Mock database object
        class DB(object):
            def __init__(self, dbname=None, dbtype=None):
                self.engine = create_engine("sqlite:///:memory:")
                self.session = sessionmaker(bind=self.engine)()

            def create_mapping(self, mapping):
                mapping.__table__.create(self.engine)

        self.d = DB(dbname=":memory:", dbtype="sqlite")

        Base = declarative_base()

        class Artist(Base):
            __tablename__ = "Artist"
            ArtistId = Column(Integer, primary_key=True)
            Name = Column(String)

        self.d.create_mapping(Artist)

    def test_convert_dict(self):
        self.assertDictEqual(
            RowConverter(
                self.d,
                "Artist",
                {"ArtistId": 2, "Name": "Accept"}).to_dict(),
            {"ArtistId": 2, "Name": "Accept"})

    def test_convert_tuples(self):
        self.assertDictEqual(
            RowConverter(
                self.d,
                "Artist",
                [("ArtistId", 3), ("Name", "Aerosmith")]).to_dict(),
            {"ArtistId": 3, "Name": "Aerosmith"})

    def test_convert_namedtuple(self):
        ArtistRowNT = namedtuple("Artist", ["ArtistId", "Name"])
        alanis = ArtistRowNT(4, "Alanis Moressette")
        self.assertEqual(
            RowConverter(
                self.d,
                "Artist",
                alanis).to_dict(),
            OrderedDict([('ArtistId', 4), ('Name', 'Alanis Moressette')]))
