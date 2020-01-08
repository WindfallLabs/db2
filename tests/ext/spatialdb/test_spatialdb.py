# !/usr/bin/env python2
"""
Test ext.spatialdb module
"""

import os
import unittest

import geopandas as gpd

from db2.ext.spatialdb import SpatiaLiteDB, get_sr_from_web


os.environ["SPATIALITE_SECURITY"] = "relaxed"


class UtilTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_sr_from_web(self):
        with open("./tests/ext/spatialdb/data/mtstplane_102700.txt", "r") as f:
            test_sr = f.readlines()
        post_sr = get_sr_from_web(102700, "esri", "postgis")
        lite_sr = get_sr_from_web(102700, "esri", "spatialite")
        # Test SpatiaLite-PostGIS similarity/derived output
        self.assertEqual(post_sr, test_sr[0].strip())
        self.assertEqual(lite_sr, test_sr[1].strip())
        # Execute it
        d = SpatiaLiteDB(":memory:")
        self.assertFalse(d.has_srid(102700))  # not in db yet
        d.get_spatial_ref_sys(102700, "esri")
        self.assertTrue(d.has_srid(102700))  # now it is
        # If run again, do nothing
        self.assertEqual(d.get_spatial_ref_sys(102700, "esri"), 0)


class MainTests(unittest.TestCase):
    def setUp(self):
        self.test_data = "./tests/ext/spatialdb/data/ContUSWildCentroids.shp"

    def test_sql_empty_df(self):
        d = SpatiaLiteDB(":memory:")
        df = d.sql("SELECT * FROM ElementaryGeometries")
        cols = [
            'db_prefix',
            'f_table_name',
            'f_geometry_column',
            'origin_rowid',
            'item_no',
            'geometry']
        self.assertTrue(df.empty and df.columns.tolist() == cols)

    def test_load_geodataframe(self):
        d = SpatiaLiteDB(":memory:")
        gdf = gpd.read_file(self.test_data)
        d.load_geodataframe(gdf, "wild", 4326)
        self.assertTrue("wild" in d.table_names)
        self.assertTrue("wild" in d.geometries["f_table_name"].tolist())
        self.assertEqual(
            d.sql(("SELECT DISTINCT IsValid(geometry) "
                   "FROM wild")).iloc[0]["IsValid(geometry)"], 1)

    def test_import_shp(self):
        os.environ["SPATIALITE_SECURITY"] = "relaxed"  # TODO: rm
        d = SpatiaLiteDB(":memory:")
        d.import_shp(self.test_data, "wild", srid=4326)
        self.assertTrue("wild" in d.table_names)

    def test_sql(self):
        d = SpatiaLiteDB(":memory:")
        gdf = gpd.read_file(self.test_data)
        d.load_geodataframe(gdf, "wild", srid=4326)
        df = d.sql("SELECT * FROM wild LIMIT 5")
        self.assertEqual(len(df), 5)
        self.assertTrue("geometry" in df.columns)
