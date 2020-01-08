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

import os
import re
import struct
import sys
import urllib2
from collections import OrderedDict
from sqlite3 import IntegrityError

import fiona
import geopandas as gpd
import pandas as pd
import shapely.wkt
from geopandas.io.file import infer_schema
from sqlalchemy import func, select

from db2 import SQLiteDB
#from _utils import SpatiaLiteSecurity as _SpatiaLiteSecurity


if sys.platform.startswith("linux"):
    MOD_SPATIALITE = "/usr/local/lib/mod_spatialite.so"
else:
    MOD_SPATIALITE = "mod_spatialite"

GEOM_TYPES = {
    1: "POINT",
    2: "LINESTRING",
    3: "POLYGON",
    # ...
    6: "MULTIPOLYGON"
    }

# Implement SPATIALITE_SECURITY handler
#SPATIALITE_SECURITY = _SpatiaLiteSecurity()


class SpatiaLiteError(Exception):
    """
    An explicit exception for use when SpatiaLite doesn't work as expected.

    Example:
    # This exemption is useful when the environment variable
    # 'SPATIALITE_SECURITY' is required to be set to 'relaxed' but isn't.
    # Functions such as ImportSHP will just not run, and users might beat their
    # head against the wall wondering why their data doesn't exist.
    """
    pass


def enable_relaxed_security(enable=True):
    """
    Set the 'SPATIALITE_SECURITY' environment variable to 'relaxed'.
    This is required by many of SpatiaLite's Import/Export functions.
    """
    if enable:
        os.environ["SPATIALITE_SECURITY"] = "relaxed"
        #SPATIALITE_SECURITY.set("relaxed")
        return
    #SPATIALITE_SECURITY.set("strict")
    return


def check_security():
    """Raises a SpatiaLiteError when not set to 'relaxed'."""
    #if not SPATIALITE_SECURITY.state.value == "relaxed":
    if not os.environ["SPATIALITE_SECURITY"] == "relaxed":
        raise SpatiaLiteError(
            "SPATIALITE_SECURITY variable not set to 'relaxed'")


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
        array = bytearray(self.blob)
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

        self.spatialite_security = "relaxed"  # TODO: State?

    def has_srid(self, srid):
        """Check if a spatial reference system is in the database."""
        return len(self.engine.execute(
            "SELECT * FROM spatial_ref_sys WHERE srid=?", (srid,)
            ).fetchall()) == 1

    def load_geodataframe(self, gdf, table_name, srid, if_exists="fail",
                          geom_func="ST_GeomFromText(:geometry, :srid)"):
        """
         Creates a database table from a geopandas.GeoDataFrame

         Parameters
         ----------

         gdf: pandas.GeoDataFrame
             GeoDataFrame to load into database as a spatial table.
         table_name: str
             The name of the table to create from the gdf
         srid: int
             Spatial Reference ID for the geometry
         if_exists: {'fail', 'replace', 'append'}, default 'fail'
             How to behave if the table already exists.
                 fail: Raise a ValueError.
                 replace: Drop the table before inserting new values.
                 append: Insert new values to the existing table.
         geom_func: str
             This specifies what spatial function and argument to use to
             transform the geometry column into SpatiaLite BLOB geometries.
             By default it uses ST_GeomFromText(WKT geometry, SRID), but can
             be tailored for use with other representations.
             e.g. 'GeomFromEWKT(:geometry)', 'GeomFromGeoJSON(:geometry)', etc.
        """
        # Get everything from GeoDataFrame except 'geometry' column
        no_geom = gdf[filter(lambda x: x != "geometry", gdf.columns)]
        # Use pandas to CREATE the table
        no_geom.to_sql(table_name, self.engine, if_exists=if_exists)
        # Get geometry type from 'geometry' column
        geom_types = set(gdf["geometry"].geom_type)
        # SpatiaLite can only accept one geometry type
        if len(geom_types) > 1:
            # Cast geometries to Multi-type
            gdf["geometry"] = gdf["geometry"].apply(
                lambda x: gpd.tools.collect(x, True))
        geom_type = max(geom_types, key=len).upper()
        # Create the geometry column (will be empty)
        self.sql("SELECT AddGeometryColumn(?, ?, ?, ?);",
                 (table_name, "geometry", srid, geom_type))
        # Create a new dataframe to use to UPDATE the 'geometry' column
        update_gdf = pd.DataFrame()
        # Set 'geometry' column to Well-Known Text
        if geom_func.startswith("ST_GeomFromText"):
            update_gdf["geometry"] = gdf["geometry"].apply(shapely.wkt.dumps)
        else:
            update_gdf["geometry"] = gdf["geometry"]
        # This PK column will be used to update geometries by joining on rowid
        update_gdf["PK"] = range(1, len(update_gdf) + 1)
        # Create the UPDATE statement
        update = self.cur._apply_handlebars(
            (u"UPDATE {{table_name}} "
             u"SET geometry={{geom_func}} "
             u"WHERE rowid=:rowid"),
            {"table_name": table_name, "geom_func": geom_func})
        update_gdf["update"] = update
        # Pass srid to geom_func if it takes it as an argument
        if ":srid" in update:
            update_gdf.apply(
                lambda x: self.sql(
                    x["update"],
                    {"geometry": x["geometry"],
                     "srid": srid,
                     "rowid": x["PK"]}),
                axis=1)
        else:
            update_gdf.apply(
                lambda x: self.sql(
                    x["update"],
                    {"geometry": x["geometry"],
                     "rowid": x["PK"]}),
                axis=1)
        return pd.DataFrame(data=[[update, 1]], columns=["SQL", "Result"])

    def import_shp(self, filename, table_name, charset="UTF-8", srid=-1,
                   geom_column="geometry", pk_column="PK",
                   geometry_type="AUTO", coerce2D=0, compressed=0,
                   spatial_index=0, text_dates=0):
        """
        Import a shapefile using the SpatiaLite function ImportSHP.
        Faster than 'load_geodataframe' but much more sensitive to data errors.

        Parameters
        ----------

        """
        # Validate parameters
        check_security()
        filename = os.path.splitext(filename)[0].replace("\\", "/")
        if not os.path.exists(filename + ".shp"):
            raise AttributeError("cannot find path specified")
        if not self.has_srid(srid):
            self.get_spatial_ref_sys(srid)
        # Execute
        df = self.sql(
            "SELECT ImportSHP(?,?,?,?,?,?,?,?,?,?,?);",
            (filename, table_name, charset, srid, geom_column, pk_column,
             geometry_type, coerce2D, compressed, spatial_index, text_dates))
        if table_name not in self.table_names:
            # TODO: Hopefully this can someday be more helpful
            raise SpatiaLiteError("import failed")
        return df

    def export_shp(self, table_name, filename, geom_column="geometry",
                   charset="UTF-8", geometry_type="AUTO"):
        """
        Export a shapefile using the SpatiaLite function ExportSHP.
        Note that parameters have been altered to improve functionality and
        make consistant with ImportSHP.
        """
        # Validate parameters
        check_security()
        if table_name not in self.table_names:
            raise AttributeError("table '{}' not found".format(table_name))
        filename = os.path.splitext(filename)[0].replace("\\", "/")
        geom_data = self.get_geometry_data(table_name)
        if geometry_type == "AUTO":
            geometry_type = GEOM_TYPES[geom_data["geometry_type"]]
        # Execute
        df = self.sql(
            "SELECT ImportSHP(?,?,?,?);",
            # ExportSHP parameter order
            (table_name, geom_column, filename, charset)) #, geometry_type))
        if not os.path.exists(filename + ".shp"):
            # TODO: Hopefully this can someday be more helpful
            raise SpatiaLiteError("export failed")
        return df  # TODO: WIP not working


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
        if df.empty:
            return df

        # Post-process the dataframe
        if "geometry" in df.columns:
            # Decode SpatiaLite BLOB and
            df["geometry"] = df["geometry"].apply(
                lambda x: SpatiaLiteBlobElement(x) if x else None)
            # Check for NULL geometries and if any are, bail
            if any(df["geometry"].isna()):
                print("NULL geometries found! Returning DataFrame...")
                return df
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

    def get_geometry_data(self, table_name):
        return self.geometries[self.geometries["f_table_name"] == table_name
                               ].iloc[0].to_dict()

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
