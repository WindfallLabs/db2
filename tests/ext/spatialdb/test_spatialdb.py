# !/usr/bin/env python2
"""
Test ext.spatialdb module
"""

import os
import unittest

import geopandas as gpd

from db2.ext import spatialdb as sdb


os.environ["SPATIALITE_SECURITY"] = "relaxed"

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

    def test_security(self):
        # Value set before tests
        # Test get
        self.assertTrue(
            sdb.SPATIALITE_SECURITY.get() == "relaxed")
        # Test set
        sdb.SPATIALITE_SECURITY.set("strict")
        self.assertTrue(
            os.environ["SPATIALITE_SECURITY"] == "strict")
        sdb.SPATIALITE_SECURITY.set("relaxed")
        self.assertEqual(
            str(sdb.SPATIALITE_SECURITY.__repr__()),
            "<_SpatiaLiteSecurity: SPATIALITE_SECURITY = 'relaxed'>")


        # Can't make a new singleton
        with self.assertRaises(TypeError) as e:
            new = sdb.SPATIALITE_SECURITY.__new__()


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
        gdf = gpd.read_file(WILDERNESS)
        d.load_geodataframe(gdf, "wild", srid=4326)
        df = d.sql("SELECT * FROM wild LIMIT 5")
        self.assertEqual(len(df), 5)
        self.assertTrue("geometry" in df.columns)

class ImportTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_load_geodataframe(self):
        d = sdb.SpatiaLiteDB(":memory:")
        gdf = gpd.read_file(WILDERNESS)
        d.load_geodataframe(gdf, "wild", 4326)
        self.assertTrue("wild" in d.table_names)
        self.assertTrue("wild" in d.geometries["f_table_name"].tolist())
        self.assertEqual(
            d.sql(("SELECT DISTINCT IsValid(geometry) "
                   "FROM wild")).iloc[0]["IsValid(geometry)"], 1)

    def test_import_shp(self):
        os.environ["SPATIALITE_SECURITY"] = "relaxed"  # TODO: rm
        d = sdb.SpatiaLiteDB(":memory:")
        d.import_shp(WILDERNESS, "wild", srid=4326)
        self.assertTrue("wild" in d.table_names)

    #def test_import_dbf(self):
    #    pass

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
