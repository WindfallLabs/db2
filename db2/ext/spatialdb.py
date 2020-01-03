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

import struct
import sys
from collections import OrderedDict
from sqlite3 import IntegrityError

import json
import re
import requests

import fiona
import shapely
import pandas as pd
import geopandas as gpd
from geopandas.io.file import infer_schema
from sqlalchemy import Column, Text, Integer, Float
from sqlalchemy import func, select
from sqlalchemy.exc import ResourceClosedError
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
from geoalchemy2.elements import _SpatialElement, WKTElement

from db2 import SQLiteDB

if sys.platform.startswith("linux"):
    #MOD_SPATIALITE = "mod_spatialite"
    MOD_SPATIALITE = "/usr/local/lib/mod_spatialite.so"
else:
    MOD_SPATIALITE = "mod_spatialite"


# URL to Spatial Reference System (srs) string
_site = "https://spatialreference.org/ref/{auth}/{srid}/{fmt}/"

# Supported Authorities
_authorities = ["epsg", "esri", "sr-org"]

# Supported Formats
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
    'spatialite',  # Customized in the get function: derrivitive of PostGIS
    'proj4js'
    ]


class SpatialReferenceResponse(object):
    """Response object returned by spatialreferenceapi.get()."""
    def __init__(self, auth, srid, sr_format, text):
        self.auth = auth
        self.srid = srid
        self.sr_format = sr_format
        self.text = text
        self.url = _site.format(auth=auth, srid=srid, fmt=sr_format)

    def to_json(self):
        """Returns the object as a json string."""
        return json.dumps(self.__dict__)

    def __getitem__(self, key):
        # Allow the object to be treated as a dictionary supporting:
        #  sr["srid"] == 102700
        return getattr(self, key)

    def __str__(self):
        return self.to_json()

    def __repr__(self):
        return "<SpatialReferenceResponse(SRID={})>".format(self.srid)


def get_spatialreference_data(srid, auth, sr_format, raise_errors=True):
    """GET function for spatialreference.org.
    Args:
        srid (int/str): Spatial Reference ID
        auth (str): Spatial Reference Authority
        sr_format (str): the requested format of the spatial reference info
        raise_errors (bool): Turn raising errors on/off (default: True)
    Returns a SpatialReferenceResponse object with the requested format of the
    spatial reference system.
    """
    site = "https://spatialreference.org/ref/{0}/{1}/{2}/"
    # Validate inputs
    srid = int(srid)
    auth = auth.lower()
    sr_format = sr_format.lower()
    if auth not in _authorities:
        raise ValueError("{} is not a valid authority".format(auth))
    if sr_format not in _formats:
        raise ValueError("{} is not a valid format".format(sr_format))

    # SpatiaLite is PostGIS with an alteration
    if sr_format == "spatialite":
        r = requests.get(site.format(auth, srid, "postgis"))
        txt = re.sub("9{}".format(srid), str(srid), r.text, count=1)
    # All other types
    else:
        r = requests.get(site.format(auth, srid, sr_format))
        txt = r.text

    # Raise errors on unsuccessful calls (if raise_errors is True)
    if raise_errors:
        if r.status_code == 404:
            raise requests.HTTPError("404 - Not Found")
        elif r.status_code != 200:
            raise requests.HTTPError("Error: Status Code {}".format(
                r.status_code))

    # Return the response as a customized object
    return SpatialReferenceResponse(auth, srid, sr_format, txt)

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
        self.blob = geom_buffer
        # Get as bytearray
        array = bytearray(geom_buffer)
        # List of Big- or Little-Endian identifiers
        endian = [">", "<"][array[1]]
        # Decode the Spatial Reference ID;  # Could be returned
        self.srid = "{}".format(struct.unpack(endian + 'i', array[2:6])[0])
        # Create WKB from Endian (pos 1) and SpatiaLite-embeded WKB data at pos 39+
        self.wkb = str(geom_buffer[1] + array[39:-1])

    def __repr__(self):
        return shapely.wkb.loads(self.wkb)

    def __str__(self):
        return shapely.wkt.dumps(self.__repr__())

    @property
    def wkt(self):
        """
        Well-Known Text
        """
        return self.__str__()

    @property
    def ewkt(self):
        """
        Extended Well-Known Text
        """
        return "SRID={};{}".format(self.srid, self.wkt)


class GeoDataFrameToSQLHandler(object):
    def __init__(self, gdf, table_name, primary_key="", srid=-1, dim="XY", from_text="ST_GeomFromText"):
        self.gdf = gdf.copy()
        self.table_name = table_name
        self.primary_key = "PK_UID" if not primary_key else primary_key
        if self.primary_key == "PK_UID" or self.primary_key not in self.gdf.columns:
            _new_cols = self.gdf.columns.insert(0, self.primary_key)
            self.gdf[self.primary_key] = xrange(0, len(self.gdf))
            self.gdf = self.gdf[_new_cols]
        self.srid = srid
        self.dim = dim
        self.from_text = from_text
        self.sql_types = {"str": "TEXT", "int": "INTEGER", "float": "REAL"}
        self.schema = infer_schema(self.gdf)
        self.column_types = OrderedDict([[k, self.sql_types[v]] for k, v in self.schema["properties"].items()])
        if isinstance(self.schema["geometry"], list):
            self.geom_type = max(self.schema["geometry"], key=len).upper()
        else:
            self.geom_type = self.schema["geometry"].upper()
        self.gdf["geometry"] = self.gdf["geometry"].apply(lambda x: x.wkt)

    @property
    def col_str(self):
        cols = self.column_types.copy()
        cols[self.primary_key] = cols[self.primary_key] + " PRIMARY KEY"
        col_str = "({})".format(", ".join([" ".join([k, v]) for k, v in cols.items()]))
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
        return "INSERT INTO {} ({}) VALUES ({});".format(self.table_name, ", ".join(self.gdf.columns), self._qmarks)

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
        dataframes.append(pd.DataFrame(data=[[self.create_sql, 1]], columns=["SQL", "Result"]))
        if db._echo:
            print(self.add_geom_col_sql)
        db.session.execute(self.add_geom_col_sql)
        dataframes.append(pd.DataFrame(data=[[self.add_geom_col_sql, 1]], columns=["SQL", "Result"]))
        db.session.commit()
        if db._echo:
            print("INSERTing {} rows using:".format(len(self.gdf)))
            print(self.insert_sql)
        for row in self.gdf.apply(OrderedDict, axis=1):
            db.con.execute(self.insert_sql, row.values())
        dataframes.append(pd.DataFrame(data=[[self.insert_sql, len(self.gdf)]], columns=["SQL", "Result"]))
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
            #self.con.execute("SELECT InitSpatialMetaData(1);")

    def load_geodataframe(self, gdf, table_name, primary_key="", srid=-1, dim="XY", from_text="ST_GeomFromText"):
        """
        Create a spatial table from a geopandas.GeoDataFrame

        """
        geom_types = [t.__name__ for t in list(set(gdf["geometry"].apply(type)))]
        if len(geom_types) > 1:
            # TODO: fix_types option to correct multi types
            raise IntegrityError("only geometries of a single type are allowed. Found: {}".format(geom_types))
        gdf_sql = GeoDataFrameToSQLHandler(gdf, table_name, primary_key, srid, dim, from_text)
        return gdf_sql.execute(self)

    def get_spatial_ref_sys(self, srid, auth="epsg", insert=True):
        sr = get_spatialreference_data(srid, auth, "spatialite")
        if insert:
            self.engine.execute(sr.text)
            return
        return sr

    def sql(self, q, data=None, union=True, limit=None):
        q = unicode(q)

        if data:
            q = self._apply_handlebars(q, data, union)
        if limit:
            q = self._assign_limit(q, limit)

        # Execute multiple statements individually
        if isinstance(data, list) and union is False:
            dataframes = []
            for qi in q.split(";"):
                qi = qi.strip()
                if qi:
                    try:
                        dataframes.append(pd.read_sql(qi, self.con))
                    except ResourceClosedError:
                        dataframes.append(pd.DataFrame(data=[[qi, 1]], columns=["SQL", "Result"]))
            df = pd.concat(dataframes).reset_index(drop=True)

        else:
            try:
                # Return query results
                df = pd.read_sql(q, self.con)

            except ResourceClosedError:
                # Return a DataFrame indicating successful statement
                df = pd.DataFrame(data=[[q, 1]], columns=["SQL", "Result"])

        if "geometry" in df.columns:
            df["geometry"] = df["geometry"].apply(SpatiaLiteBlobElement)
        return df

    @property
    def geometries(self):
        return self.sql("SELECT * FROM geometry_columns")

'''
type(table_name,(declarative_base(bind=self.engine),),
            {"__tablename__": table_name, "__table_args__": {"autoload": True}})
'''
