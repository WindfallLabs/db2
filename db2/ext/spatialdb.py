# -*- coding: utf-8 -*-
# !/usr/bin/env python2
"""
Module Docstring

GDAL Installation
#!/usr/bin/env bash

add-apt-repository ppa:ubuntugis/ppa && sudo apt-get update
apt-get update
apt-get install gdal-bin
apt-get install libgdal-dev
export CPLUS_INCLUDE_PATH=/usr/include/gdal
export C_INCLUDE_PATH=/usr/include/gdal
pip install GDAL==$(gdal-config --version) --global-option=build_ext --global-option="-I/usr/include/gdal"
"""

import re
import struct
import sys
import urllib2
from collections import OrderedDict
from sqlite3 import IntegrityError

import fiona
import geopandas as gpd
import pandas as pd
import shapely
from geopandas.io.file import infer_schema
from sqlalchemy import func, select

from .. import SQLiteDB


if sys.platform.startswith("linux"):
    MOD_SPATIALITE = "/usr/local/lib/mod_spatialite.so"
else:
    MOD_SPATIALITE = "mod_spatialite"


def get_sr_from_web(srid, auth, sr_format):
    """
    Get spatial reference data from spatialreference.org

    Parameters
    ----------
    srid: int
        Spatial Reference ID
    auth: str
        Name of authority {epsg, esri, sr-org}
    sr_format: str
        Desired format of spatial reference data
    """
    _formats = [
        'html',
        'prettywkt',
        'proj4',
        'json',
        'gml',
        'esriwkt',
        'mapfile',
        'mapserverpython',
        'mapnik',
        'mapnikpython',
        'geoserver',
        'postgis',
        'spatialite',  # Derivative of PostGIS
        'proj4js'
        ]

    _authorities = [
        "epsg",
        "esri",
        "sr-org"]

    # Validate inputs
    srid = int(srid)
    auth = auth.lower()
    sr_format = sr_format.lower()
    if auth not in _authorities:
        raise ValueError("{} is not a valid authority".format(auth))
    if sr_format not in _formats:
        raise ValueError("{} is not a valid format".format(sr_format))
    site = "https://spatialreference.org/ref/{0}/{1}/{2}/".format(
        auth, srid, sr_format)

    # SpatiaLite (derive from PostGIS)
    if sr_format == "spatialite":
        site = site.replace("spatialite", "postgis")
        data = urllib2.urlopen(site).read()
        # The srid value has a leading 9 in the PostGIS INSERT statement
        data = re.sub("9{}".format(srid), str(srid), data, count=1)
    else:
        data = urllib2.urlopen(site).read()
    return data


'''
class _ColumnFactory():
    def create_column(self, typ):
        if typ.startswith("date"):
            typ = typ.replace("date", "str")
        col_type = typ.split(":")[0]

        if col_type == "str":
            return Column(
                Text(typ.split(":")[1]))
        elif col_type == "int":
            if typ.split(":")[1] == "primary":
                return Column(Integer, primary_key=True)
            return Column(Integer)
        elif col_type == "float":
            return Column(Float)
        elif col_type == "geometry":
            geom_type = typ.split(":")[1].replace("3D", "").strip().upper()
            return Column(
                Geometry(geometry_type=geom_type, management=True))


schema = OrderedDict([("__tablename__", "test"), (u"PK_UID", "int:primary")])
schema.update(shp.schema["properties"])
schema[u"geometry"] = u"geometry:{}".format(shp.schema["geometry"])

for k, v in schema.items():
    if k == "__tablename__":
        continue
    schema[k] = _ColumnFactory().create_column(v)
'''


class SpatiaLiteBlobElement(object):
    """
    SpatiaLite Blob Element
    """
    def __init__(self, geom_buffer):
        """
        Decodes a SpatiaLite BLOB geometry into a Spatial Reference and
        Well-Known Binary representation
        See specification: https://www.gaia-gis.it/gaia-sins/BLOB-Geometry.html

        Parameters
        ----------
        geom_buffer: buffer
            The geometry type native to SpatiaLite (BLOB geometry)
        """
        self.blob = geom_buffer
        # Get as bytearray
        array = bytearray(geom_buffer)
        # List of Big- or Little-Endian identifiers
        endian = [">", "<"][array[1]]

        # Decode the Spatial Reference ID
        self.srid = "{}".format(struct.unpack(endian + 'i', array[2:6])[0])

        # Create WKB from Endian (pos 1) and SpatiaLite-embeded WKB data
        # at pos 39+
        self.wkb = str(geom_buffer[1] + array[39:-1])

    @property
    def as_shapely(self):
        """Return SpatiaLite BLOB as shapely object."""
        return shapely.wkb.loads(self.wkb)

    @property
    def as_wkt(self):
        """Return SpatiaLite BLOB as Well Known Text."""
        return shapely.wkt.dumps(self.as_shapely)

    @property
    def as_ewkt(self):
        """Return SpatiaLite BLOB as Extended Well-Known Text."""
        return "SRID={};{}".format(self.srid, self.as_wkt)

    def __str__(self):
        return self.ewkt


class GeoDataFrameToSQLHandler(object):
    def __init__(self, gdf, table_name, srid=-1, primary_key="",
                 from_text="ST_GeomFromText", cast_to_multi=False):
        self.gdf = gdf.copy()
        self.table_name = table_name
        self.primary_key = "PK_UID" if not primary_key else primary_key
        if self.primary_key == "PK_UID" or self.primary_key not in self.gdf.columns:
            _new_cols = self.gdf.columns.insert(0, self.primary_key)
            self.gdf[self.primary_key] = xrange(0, len(self.gdf))
            self.gdf = self.gdf[_new_cols]

        # Check for multiple geometry types
        geom_types = list(set(gdf["geometry"].geom_type))
        if len(geom_types) > 1:
            # Cast all to Multi type
            if cast_to_multi is True:
                self.gdf["geometry"] = self.gdf["geometry"].apply(
                    lambda x: gpd.tools.collect(x, True))
            else:
                raise IntegrityError(
                    "only geometries of a single type are allowed. Found: {}".format(geom_types))

        # Handle SRIDs
        self.srid = srid
        # Get SRID from GeoDataFrame if in the crs
        if self.srid == -1 and self.gdf.crs["init"].startswith("epsg"):
            self.srid = int(self.gdf.crs["init"].split(":")[1])

        self.dim = list(set(self.gdf["geometry"].apply(lambda x: x._ndim)))[0]

        self.from_text = from_text
        self.sql_types = {"str": "TEXT", "int": "INTEGER", "float": "REAL"}
        self.schema = infer_schema(self.gdf)
        self.column_types = OrderedDict(
            [[k, self.sql_types[v]] for k, v in
             self.schema["properties"].items()])
        if isinstance(self.schema["geometry"], list):
            self.geom_type = max(self.schema["geometry"], key=len).upper()
        else:
            self.geom_type = self.schema["geometry"].upper()
        self.gdf["geometry"] = self.gdf["geometry"].apply(lambda x: x.wkt)

    @property
    def col_str(self):
        cols = self.column_types.copy()
        cols[self.primary_key] = cols[self.primary_key] + " PRIMARY KEY"
        col_str = "({})".format(", ".join(
            [" ".join([k, v]) for k, v in cols.items()]))
        return col_str

    @property
    def create_sql(self):
        create = "CREATE TABLE {} {};".format(self.table_name, self.col_str)
        return create

    @property
    def add_geom_col_sql(self):
        add_geom = "SELECT AddGeometryColumn({});"
        params = "'{}', '{}', {}, '{}', '{}'".format(
            self.table_name,
            "geometry",  # Column name
            self.srid,
            self.geom_type,
            self.dim)
        return add_geom.format(params)

    @property
    def insert_sql(self):
        return "INSERT INTO {} ({}) VALUES ({});".format(
            self.table_name, ", ".join(self.gdf.columns), self._qmarks)

    @property
    def _qmarks(self):
        qmarks = ", ".join(["?"] * (len(self.gdf.columns) - 1))
        geom_from_text = ", {}(?, {})".format(self.from_text, self.srid)
        qmarks += geom_from_text
        return qmarks

    def execute(self, db, if_exists="fail"):
        dataframes = []
        if db._echo:
            print(self.create_sql)
        db.session.execute(self.create_sql)
        dataframes.append(pd.DataFrame(
            data=[[self.create_sql, 1]], columns=["SQL", "Result"]))
        if db._echo:
            print(self.add_geom_col_sql)
        db.session.execute(self.add_geom_col_sql)
        dataframes.append(pd.DataFrame(
            data=[[self.add_geom_col_sql, 1]], columns=["SQL", "Result"]))
        db.session.commit()
        if db._echo:
            print("INSERTing {} rows using:".format(len(self.gdf)))
            print(self.insert_sql)
        for row in self.gdf.apply(OrderedDict, axis=1):
            db.con.execute(self.insert_sql, row.values())
        dataframes.append(pd.DataFrame(
            data=[[self.insert_sql, len(self.gdf)]],
            columns=["SQL", "Result"]))
        return pd.concat(dataframes).reset_index(drop=True)


class SpatiaLiteDB(SQLiteDB):
    """
    Utility for exploring and querying a SpatiaLite database.

    Parameters
    ----------
    filename: str
        Path to SQLite database or ":memory:" for in-memory database
    echo: bool
        Whether or not to repeat queries and messages back to user
    extensions: list
        List of extensions to load on connection. Default: ['mod_spatialite']
    """
    def __init__(self, dbname, echo=False, extensions=[MOD_SPATIALITE]):
        super(SpatiaLiteDB, self).__init__(
            dbname=dbname,
            echo=echo,
            extensions=extensions)

        # Initialize spatial metadata if the database is new
        if "geometry_columns" not in self.table_names:
            # Source: geoalchemy2 readthedocs tutorial
            self.con.execute(select([func.InitSpatialMetaData(1)]))

    def has_srid(self, srid):
        """Check if a spatial reference system is in the database."""
        return len(self.engine.execute(
            "SELECT * FROM spatial_ref_sys WHERE srid=?", (srid,)
            ).fetchall()) == 1


    def load_geodataframe(self, gdf, table_name, srid=-1, primary_key=""):
        """
        Create a spatial table from a geopandas.GeoDataFrame
        """
        if not self.has_srid(srid):
            self.get_spatial_ref_sys(srid)
        gdf_sql = GeoDataFrameToSQLHandler(
            gdf, table_name, srid, primary_key, cast_to_multi=True)
        return gdf_sql.execute(self)

    def get_spatial_ref_sys(self, srid, auth="esri"):
        """
        Execute the INSERT statement for the spatial reference data from
        spatialreference.org. Does nothing if the spatial reference data exists

        Parameters
        ----------
        srid: int
            Spatial Reference ID
        auth: str
            Name of authority {epsg, esri, sr-org}
            Default 'esri' because spatial_ref_sys table already has most epsg
            spatial references
        """
        if self.has_srid(srid):
            return 0
        sr_data = get_sr_from_web(srid, auth, "spatialite")
        self.engine.execute(sr_data)
        return 1

    def sql(self, q, data=None, union=True, limit=None):
        """"""
        # Execute the query using the sql method of the super class
        df = super(SpatiaLiteDB, self).sql(q, data, union, limit)

        # Post-process the dataframe
        if "geometry" in df.columns:
            # Decode SpatiaLite BLOB and
            df["geometry"] = df["geometry"].apply(SpatiaLiteBlobElement)
            # Get Spatial Reference while geometry values are
            # SpatiaLiteBlobElement objects
            srid = df["geometry"].iat[0].srid
            # Convert SpatiaLiteBlobElement to shapely object
            df["geometry"] = df["geometry"].apply(lambda x: x.as_shapely)
            # Convert to GeoDataFrame
            df = gpd.GeoDataFrame(df)

            # Get spatial reference authority and proj4text
            auth, proj = self.engine.execute(
                "SELECT auth_name, proj4text FROM spatial_ref_sys WHERE auth_srid = ?",
                (srid,)
                ).fetchone()

            # Set crs attribute of GeoDataFrame
            if auth != "epsg":
                df.crs = fiona.crs.from_string(proj)
            else:
                df.crs = fiona.crs.from_epsg(srid)
        return df

    @property
    def geometries(self):
        """Return the contents of table `geometry_columns` and srs info."""
        return self.sql("SELECT * FROM geometry_columns g LEFT JOIN spatial_ref_sys s ON g.srid=s.srid")

    def __str__(self):
        return "SpatialDB[SQLite/SpatiaLite] > {dbname}".format(
            dbname=self.dbname)

    def __repr__(self):
        return self.__str__()


'''
type(table_name,(declarative_base(bind=self.engine),),
            {"__tablename__": table_name, "__table_args__": {"autoload": True}})
'''

# TODO: make SpatialDB superclass or abstract class(?)
'''
class PostGISDB(SpatialDB):
    def __init__(self):
        super(PostGISDB, )
'''
