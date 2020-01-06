import unittest

from db2.ext.spatialdb import SpatiaLiteDB, get_sr_from_web


class UtilTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_sr_from_web(self):
        with open("./tests/ext/data/mtstplane_102700.txt", "r") as f:
            test_sr = f.readlines()
        post_sr = get_sr_from_web(102700, "esri", "postgis")
        lite_sr = get_sr_from_web(102700, "esri", "spatialite")
        self.assertEqual(post_sr, test_sr[0].strip())
        self.assertEqual(lite_sr, test_sr[1].strip())


class MainTests(unittest.TestCase):
    def setUp(self):
        self.test_data = "/media/garin/MAPS_MEDIA1/Maps/GIS/GeneralData/ContUSWildCentroids.shp"

    def test_load_geodataframe(self):
        import geopandas as gpd
        d = SpatiaLiteDB(":memory:")
        gdf = gpd.read_file(self.test_data)
        d.load_geodataframe(gdf, "wild", srid=4326)
        self.assertTrue("wild" in d.table_names)

    def test_sql(self):
        import geopandas as gpd
        d = SpatiaLiteDB(":memory:")
        gdf = gpd.read_file(self.test_data)
        d.load_geodataframe(gdf, "wild", srid=4326)
        df = d.sql("SELECT * FROM wild LIMIT 5")
        self.assertEqual(len(df), 5)
