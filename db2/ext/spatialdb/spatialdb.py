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

from __future__ import unicode_literals

import os
import sys

import fiona
import geopandas as gpd
import pandas as pd
import shapely.wkt
from sqlalchemy import func, select

from db2 import SQLiteDB
from ._utils import get_sr_from_web, SpatiaLiteBlobElement

# Assume users want access to functions like ImportSHP, ExportSHP, etc.
os.environ["SPATIALITE_SECURITY"] = "relaxed"

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

# TODO: something that allows users the option to raise errors on column names
# that are greater than 10 chars long


class SpatiaLiteError(Exception):
    """
    An explicit exception for use when SpatiaLite doesn't work as expected.
    """
    pass


class SpatiaLiteDB(SQLiteDB):
    """
    Utility for exploring and querying a SpatiaLite database.

    Parameters
    ----------
    dbname: str
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

        self.relaxed_security = os.environ["SPATIALITE_SECURITY"]

        # Initialize spatial metadata if the database is new
        if "geometry_columns" not in self.table_names:
            # NOTE: Use SQLAlchemy rather than the DB API con
            # Source: geoalchemy2 readthedocs tutorial
            self.engine.execute(select([func.InitSpatialMetaData(1)]))

    def has_srid(self, srid):
        """
        Check if a spatial reference system is in the database.

        Parameters
        ----------
        srid: int
            Spatial Reference ID

        Returns
        -------
        bool
            True if the SRID exists in spatial_ref_sys table, otherwise False.
        """
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
            GeoDataFrame to load into database as a spatial table. This could
            also be a normal DataFrame with geometry stored as Well-Known Text
            in a Series called 'wkt'.
        table_name: str
            The name of the table to create from the gdf
        srid: int
            Spatial Reference ID for the geometry
        if_exists: str ({'fail', 'replace', 'append'}, default 'fail')
            How to behave if the table already exists.

                * fail: Raise a ValueError.
                * replace: Drop the table before inserting new values.
                * append: Insert new values to the existing table.

        srid_auth: str ({'epsg', 'sr-org', 'esri'}, default 'esri')
            If the 'srid' argument value is not in the database, it is
            retrieved from the web. This argument allows users to specify the
            spatial reference authority. Default is 'esri' since most
            'epsg' systems already exist in the spatial_ref_sys table.
        Any other kwargs are passed to the 'to_sql()' method of the dataframe.
            Note that the 'index' argument is set to False.
        """
        # TODO: check_security()
        rcols = ["SQL", "Result"]
        r = pd.DataFrame(columns=rcols)
        # Get SRID if needed
        if not self.has_srid(srid):
            self.get_spatial_ref_sys(srid, srid_auth)
            r = r.append(
                pd.DataFrame([["get_spatial_ref_sys", 1]], columns=rcols))
        # Auto-convert Well-Known Text to shapely
        if "geometry" not in gdf.columns and "wkt" in gdf.columns:
            # Load geometry from WKT series
            gdf["geometry"] = gpd.GeoSeries(gdf["wkt"].apply(
                shapely.wkt.loads))
            # Drop wkt series
            gdf = gpd.GeoDataFrame(gdf.drop("wkt", axis=1))
            r = r.append(pd.DataFrame([["wkt.loads", 1]], columns=rcols))
        # Get geometry type from 'geometry' column
        geom_types = set(gdf["geometry"].geom_type)
        # SpatiaLite can only accept one geometry type
        if len(geom_types) > 1:
            # Cast geometries to Multi-type
            gdf["geometry"] = gdf["geometry"].apply(
                lambda x: gpd.tools.collect(x, True))
            r = r.append(pd.DataFrame([["collect()", 1]], columns=rcols))
        geom_type = max(geom_types, key=len).upper()
        # Convert geometry to WKT
        gdf["geometry"] = gdf["geometry"].apply(lambda x: x.wkt)
        # Load the table using pandas
        #gdf.to_sql(table_name, self.dbapi_con, **kwargs)
        gdf.to_sql(table_name, self.con, **kwargs)
        # Convert from WKT to SpatiaLite geometry
        r = r.append(self.sql(
            "UPDATE {{tbl}} SET geometry = GeomFromText(geometry, {{srid}});",
            data={"tbl": table_name, "srid": srid})
            )

        # Recover geometry as a spatial column
        self.sql("SELECT RecoverGeometryColumn(?, ?, ?, ?);",
                 (table_name, "geometry", srid, geom_type))
        if table_name not in self.geometries["f_table_name"].tolist():
            r = r.append(
                pd.DataFrame([["RecoverGeometryColumn", 0]], columns=rcols))
        else:
            r = r.append(
                pd.DataFrame([["RecoverGeometryColumn", 1]], columns=rcols))
        # Optionally validate geometries
        if validate:
            r = r.append(self.sql("UPDATE {{tbl}} "
                                  "SET geometry = MakeValid(geometry) "
                                  "WHERE NOT IsValid(geometry);",
                                  data={"tbl": table_name}))
        r = r.append(
            pd.DataFrame([["Load GeoDataFrame", len(gdf)]], columns=rcols))
        return r.reset_index(drop=True)

    def import_shp(self, filename, table_name, charset="UTF-8", srid=-1,
                   geom_column="geometry", pk_column="PK",
                   geom_type="AUTO", coerce2D=0, compressed=0,
                   spatial_index=0, text_dates=0):
        """
        Will import an external Shapfile into an internal Table.

        This method wraps SpatiaLite's ImportSHP function. It is faster than
        ``load_geodataframe`` but more sensitive. For more information check
        the `SpatiaLite's Functions Reference List`_.

        Parameters
        ----------
        filename: str
            Absolute or relative path leading to the Shapefile (omitting any
            .shp, .shx or .dbf suffix).
        table_name: str
            Name of the table to be created.
        charset: str
            The character encoding adopted by the DBF member, as
            e.g. UTF-8 or CP1252
        srid: int
            EPSG SRID value; -1 by default.
        geom_column: str
            Name to assign to the Geometry column; 'geometry' by default.
        pk_column: str
            Name of a DBF column to be used in the Primary Key role; an
            INTEGER AUTOINCREMENT PK will be created by default.
        geom_type: str
            One between: AUTO, POINT|Z|M|ZM, LINESTRING|Z|M|ZM, POLYGON|Z|M|ZM,
            MULTIPOINT|Z|M|ZM, LINESTRING|Z|M|ZM, MULTIPOLYGON|Z|M|ZM; by
            default AUTO.
        coerce2D: int {0, 1}
            Cast to 2D or not; 0 by default.
        compressed: int {0, 1}
            Compressed geometries or not; 0 by default.
        spatial_index: int {0, 1}
            Immediately building a Spatial Index or not; 0 by default.
        text_dates: int {0, 1}
            Interpret DBF dates as plaintext or not: 0 by default
            (i.e. as Julian Day).

        Returns
        -------
        DataFrame:
            DataFrame containing SQL passed and number of inserted features.


        .. _`SpatiaLite's Functions Reference List`: https://www.gaia-gis.it/gaia-sins/spatialite-sql-4.3.0.html
        """
        # Validate parameters
        if not self.relaxed_security:
            raise SpatiaLiteError("This function requires relaxed security")
        filename = os.path.splitext(filename)[0].replace("\\", "/")
        if not os.path.exists(filename + ".shp"):
            raise AttributeError("cannot find path specified")
        if not self.has_srid(srid):
            self.get_spatial_ref_sys(srid)
        # Execute
        df = self.sql(
            "SELECT ImportSHP(?,?,?,?,?,?,?,?,?,?,?);",
            (filename, table_name, charset, srid, geom_column, pk_column,
             geom_type, coerce2D, compressed, spatial_index, text_dates))
        if table_name not in self.table_names:
            # TODO: Hopefully this can someday be more helpful
            raise SpatiaLiteError("import failed")
        return df

    def export_shp(self, table_name, filename, geom_column="geometry",
                   charset="UTF-8", geom_type="AUTO"):
        """
        Will export an internal Table as an external Shapefile.

        This method wraps SpatiaLite's ExportSHP function. Note that this
        function's parameters differ slightly from
        `SpatiaLite's Functions Reference List`_ in order to improve
        functionality and make it more consistent with ImportSHP's parameters.

        Parameters
        ----------
        table_name: str
            Name of the table to be exported.
        filename: str
            Absolute or relative path leading to the Shapefile (omitting any
            .shp, .shx or .dbf suffix).
        geom_column: str
            Name of the Geometry column. Default 'geometry'
        charset: str
            The character encoding adopted by the DBF member, as
            e.g. UTF-8 or CP1252
        geom_type: str
            Useful when exporting unregistered Geometries, and can be one
            between: POINT, LINESTRING, POLYGON or MULTIPOINT; AUTO option
            queries the database.

        Returns
        -------
        DataFrame:
            DataFrame containing the results of executing the SQL. (WIP)


        .. _`SpatiaLite's Functions Reference List`: https://www.gaia-gis.it/gaia-sins/spatialite-sql-4.3.0.html
        """
        # Validate parameters
        if not self.relaxed_security:
            raise SpatiaLiteError("This function requires relaxed security")
        if table_name not in self.table_names:
            raise AttributeError("table '{}' not found".format(table_name))
        filename = os.path.splitext(filename)[0].replace("\\", "/")
        geom_data = self.get_geometry_data(table_name)
        if geom_type == "AUTO":
            geom_type = GEOM_TYPES[geom_data["geometry_type"]]
        # Execute
        df = self.sql(
            "SELECT ExportSHP(?,?,?,?);",
            # ExportSHP parameter order
            (table_name, geom_column, filename, charset)) #, geometry_type))
        if not os.path.exists(filename + ".shp"):
            # TODO: Hopefully this can someday be more helpful
            raise SpatiaLiteError("export failed")
        return df


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

    def create_table_as(self, table_name, sql, srid=None, **kwargs):  # TODO: add tests
        """
        Handles ``CREATE TABLE {{table_name}} AS {{select_statement}};`` via
        pandas to preserve column type affinity. (WIP)

        Parameters
        ----------
        table_name: str
            Name of table to create
        sql: str
            SQL `SELECT` statement used to create a new
        srid: int
            Spatial Reference ID if the resulting table should be spatial

        Returns
        -------
        None
            (WIP)
        """
        df = self.sql(sql)
        if srid is not None:  # "geometry" in df.columns or "wkt" in df.columns:
            return self.load_geodataframe(df, table_name, srid, **kwargs)
        return self.load_dataframe(df, table_name)

    @property
    def geometries(self):
        """
        Returns a dictionary containing the ``geometry_columns`` table joined
        with related records in the ``spatial_ref_sys`` table.
        """
        return self.sql(
            ("SELECT * FROM geometry_columns g "
             "LEFT JOIN spatial_ref_sys s "
             "ON g.srid=s.srid"))

    def get_geometry_data(self, table_name):
        """(WIP)."""
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
