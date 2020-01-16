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
import sys

import fiona
import geopandas as gpd
import pandas as pd
import shapely.wkt
from sqlalchemy import func, select

from db2 import SQLiteDB
from ._utils import get_sr_from_web, SpatiaLiteBlobElement


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


class _SpatiaLiteSecurity(object):
    """
    Object for locally handling the SPATIALITE_SECURITY environment variable.
    """
    __instance = None

    @staticmethod
    def getInstance():
        if _SpatiaLiteSecurity.__instance is None:
            _SpatiaLiteSecurity()
        return _SpatiaLiteSecurity.__instance

    def __init__(self):
        """Default to 'strict' security."""
        # Singleton logic
        if _SpatiaLiteSecurity.__instance is not None:
            raise TypeError("instance of singleton class 'SpatiaLiteSecurity'"
                            "already exists.")
        else:
            _SpatiaLiteSecurity.__instance = self

        # Set Default value for "SPATIALITE_SECURITY" if not set
        os.environ.setdefault("SPATIALITE_SECURITY", "strict")

    def get(self):
        return os.environ["SPATIALITE_SECURITY"]

    def set(self, value="strict"):
        os.environ["SPATIALITE_SECURITY"] = value
        return

    def __str__(self):
        return "SPATIALITE_SECURITY = '{}'".format(self.get())

    def __repr__(self):
        return "<_SpatiaLiteSecurity: {}>".format(self.__str__())


# Implement SPATIALITE_SECURITY handler
SPATIALITE_SECURITY = _SpatiaLiteSecurity()


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


def check_security():
    """Raises a SpatiaLiteError when not set to 'relaxed'."""
    if not os.environ["SPATIALITE_SECURITY"] == "relaxed":
        raise SpatiaLiteError(
            "SPATIALITE_SECURITY variable not set to 'relaxed'")


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

    def load_geodataframe(self, gdf, table_name, srid, validate=True,
                          if_exists="fail", srid_auth="esri", **kwargs):
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
        srid_auth: {'epsg', 'sr-org', 'esri'}, default 'esri'
            If the 'srid' argument value is not in the database, it is
            retrieved from the web. This argument allows users to specify the
            spatial reference authority. Default is 'esri' since most
            'epsg' systems already exist in the spatial_ref_sys table.
        Any other kwargs are passed to the 'to_sql()' method of the dataframe.
            Note that the 'index' argument is set to False.
        """
        # TODO: check_security()
        # Get SRID if needed
        if not self.has_srid(srid):
            self.get_spatial_ref_sys(srid, srid_auth)
        # Get geometry type from 'geometry' column
        geom_types = set(gdf["geometry"].geom_type)
        # SpatiaLite can only accept one geometry type
        if len(geom_types) > 1:
            # Cast geometries to Multi-type
            gdf["geometry"] = gdf["geometry"].apply(
                lambda x: gpd.tools.collect(x, True))
        geom_type = max(geom_types, key=len).upper()
        # Convert geometry to WKT
        gdf["geometry"] = gdf["geometry"].apply(lambda x: x.wkt)
        # Load the table using pandas
        kwargs.setdefault("index", False)
        gdf.to_sql(table_name, self.dbapi_con, **kwargs)
        # Convert from WKT to SpatiaLite geometry
        self.sql("UPDATE {{tbl}} "
                 "SET geometry = GeomFromText(geometry, {{srid}});",
                 data={"tbl": table_name, "srid": srid})
        # Recover geometry as a spatial column
        self.sql("SELECT RecoverGeometryColumn(?, ?, ?, ?);",
                 (table_name, "geometry", srid, geom_type))
        if validate:
            self.sql("UPDATE {{tbl}} "
                     "SET geometry = MakeValid(geometry) "
                     "WHERE NOT IsValid(geometry);",
                     data={"tbl": table_name})
        return pd.DataFrame([["Load GDF", len(gdf)]], columns=["SQL", "Result"])

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
        make consistent with ImportSHP.
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
        df = super(SpatiaLiteDB, self).sql(q, data)  # TODO: , union, limit)
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
                ("SELECT auth_name, proj4text "
                 "FROM spatial_ref_sys "
                 "WHERE auth_srid = ?"),
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
        return self.sql(
            ("SELECT * FROM geometry_columns g "
             "LEFT JOIN spatial_ref_sys s "
             "ON g.srid=s.srid"))

    def get_geometry_data(self, table_name):
        return self.geometries[self.geometries["f_table_name"] == table_name
                               ].iloc[0].to_dict()

    def __str__(self):
        return "SpatialDB[SQLite/SpatiaLite] > {dbname}".format(
            dbname=self.dbname)

    def __repr__(self):
        return self.__str__()


# TODO: make SpatialDB superclass or abstract class(?)
'''
class PostGISDB(SpatialDB):
    def __init__(self):
        super(PostGISDB, )
'''
