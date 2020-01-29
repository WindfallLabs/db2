# !/usr/bin/env python2
"""
Test ext.spatialdb module
"""

from __future__ import unicode_literals

import os
import unittest

import geopandas as gpd

from db2.ext import spatialdb as sdb


WILDERNESS = test_data = "./tests/ext/spatialdb/data/ContUSWildCentroids.shp"


class UtilTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_sr_from_web(self):
        with open("./tests/ext/spatialdb/data/mtstplane_102700.txt", "r") as f:
            test_sr = f.readlines()
        post_sr = sdb.get_sr_from_web(102700, "esri", "postgis")
        lite_sr = sdb.get_sr_from_web(102700, "esri", "spatialite")
        # Test SpatiaLite-PostGIS similarity/derived output
        self.assertEqual(post_sr, test_sr[0].strip())
        self.assertEqual(lite_sr, test_sr[1].strip())
        # Execute it
        d = sdb.SpatiaLiteDB(":memory:")
        self.assertFalse(d.has_srid(102700))  # not in db yet
        d.get_spatial_ref_sys(102700, "esri")
        self.assertTrue(d.has_srid(102700))  # now it is
        # If run again, do nothing
        self.assertEqual(d.get_spatial_ref_sys(102700, "esri"), 0)


class MainTests(unittest.TestCase):
    def test_sql_empty_df(self):
        d = sdb.SpatiaLiteDB(":memory:")
        df = d.sql("SELECT * FROM ElementaryGeometries")
        cols = [
            'db_prefix',
            'f_table_name',
            'f_geometry_column',
            'origin_rowid',
            'item_no',
            'geometry']
        self.assertTrue(df.empty and df.columns.tolist() == cols)

    def test_sql(self):
        d = sdb.SpatiaLiteDB(":memory:")
        df = d.sql("SELECT * FROM spatial_ref_sys WHERE srid = 4326")
        self.assertTrue(not df.empty and df["srid"].iat[0] == 4326)


class ImportTests_Memory(unittest.TestCase):
    def setUp(self):
        pass

    def test_load_geodataframe(self):
        d = sdb.SpatiaLiteDB(":memory:")
        gdf = gpd.read_file(WILDERNESS)
        d.load_geodataframe(gdf, "wild", 4326, validate=False)
        self.assertTrue("wild" in d.table_names)
        self.assertTrue("wild" in d.geometries["f_table_name"].tolist())
        self.assertEqual(
            d.sql(("SELECT DISTINCT IsValid(geometry) "
                   "FROM wild")).iloc[0]["IsValid(geometry)"], 1)

    def test_import_shp(self):
        d = sdb.SpatiaLiteDB(":memory:")
        r = d.import_shp(WILDERNESS, "wild", srid=4326)
        self.assertTrue("wild" in d.table_names)
        self.assertEqual(r.columns.tolist(), ["SQL", "Result"])
        self.assertEqual(r["Result"].iat[0], 742)

    def test_get_geom_data(self):
        d = sdb.SpatiaLiteDB(":memory:")
        d.import_shp(WILDERNESS, "wild", srid=4326)
        self.assertEqual(d.get_geometry_data("wild")["srid"], 4326)
        self.assertEqual(d.get_geometry_data("wild")["ref_sys_name"], "WGS 84")

class ImportTests_OnDisk(unittest.TestCase):
    def setUp(self):
        self.path = "tests/ext/spatialdb/test_ondisk.sqlite"
        if os.path.exists(self.path):
            os.remove(self.path)

    def tearDown(self):
        # os.remove(self.path) causes WindowsError, fine on Linux
        pass

    def test_import_shp(self):
        self.d = sdb.SpatiaLiteDB(self.path)
        r = self.d.import_shp(WILDERNESS, "wild", srid=4326)
        self.assertTrue("wild" in self.d.table_names)
        self.assertEqual(r.columns.tolist(), ["SQL", "Result"])
        self.assertEqual(r["Result"].iat[0], 742)


'''
class ExportTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_export_shp(self):
        pass

    def test_export_dbf(self):
        d = SpatiaLiteDB(":memory:")
        d.import_shp(WILDERNESS, "wild", srid=4326)
        d.export_dbf("wild", "OUTPUT_PATH", charset="UTF8", colname_case="lower")
'''
