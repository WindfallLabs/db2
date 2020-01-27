.. code-block:: python

    """
    This example connects to a Microsoft SQL Server database (with ESRI SDE)
    and copies data into an in-memory SpatiaLite database in order to find
    parcels that could be zoned to a higher zoning district as allowed by
    land use.

    Data used is zoning, land use, and parcel data for the City of Missoula, MT
    """
    from db2 import MSSQLDB
    from db2.ext.spatialdb import SpatiaLiteDB

    # Create in-memory SpatiaLite database
    d = SpatiaLiteDB(":memory:")

    # Connect to MS SQL database "gisrep" on SQLA\GIS server
    gisrep = MSSQLDB("dbread", "dbread", r"SQLA\GIS", "gisrep")

    # Query Zoning data
    zoning = gisrep.sql("SELECT "
                        "    z.Base,"
                        "    z.Overlay,"
                        "    z.Shape.STAsText() AS wkt,"
                        "    ISNULL(CAST(zd.Duac AS FLOAT), 0.0) AS Duac,"
                        "    zd.Duac_Ratio "
                        "FROM sde.CityZoning_evw z "
                        "LEFT JOIN sde.ZONINGDESC zd"
                        "	ON z.Base=zd.Base_Zone"
                        )

    # and load it into SpatiaLite
    d.load_geodataframe(zoning, "zoning", 102700)

    # Query Land Use data
    landuse = gisrep.sql("SELECT"
                         "    LU_Abbr,"
                         "    name,"
                         "    ISNULL(CAST(du_min AS FLOAT), 0.0) AS du_min,"
                         "    Shape.STAsText() AS wkt "
                         "FROM sde.Landuses_evw;"
                         )

    # and load it into SpatiaLite
    d.load_geodataframe(landuse, "landuse", 102700)


    # Query Parcel data
    parcels = gisrep.sql("SELECT ParcelID, Shape.STAsText() AS wkt "
                         "FROM sde.parcels_evw "
                         "WHERE Shape IS NOT NULL")

    # and load it into SpatiaLite
    d.load_geodataframe(parcels, "parcels", 102700)

    # Create a new table 'parcel_pts' from Parcel centroids with a SELECT query
    d.create_table_as(
        "parcel_pts",
        "SELECT ParcelID, ST_AsText(PointOnSurface(geometry)) AS wkt FROM parcels",
        102700)

    # Put together a big query to join spatially join it all together
    joined = ("SELECT "
              "    pt.ParcelID, z.Base, z.Duac, lu.LU_Abbr, lu.name, lu.du_min, "
              "    CASE WHEN z.Duac < lu.du_min THEN 1 ELSE 0 END AS under_zoned, "
              "    ST_AsText(p.geometry) AS wkt "
              "FROM parcel_pts pt "
              "LEFT JOIN zoning z "
              "    ON Intersects(pt.geometry, z.geometry) "
              "LEFT JOIN landuse lu "
              "    ON Intersects(pt.geometry, lu.geometry)"
              "LEFT JOIN parcels p "
              "    ON pt.ParcelID=p.ParcelID "
              "WHERE "
              # Ignore Special Districts
              "    z.Base NOT LIKE 'SD/%' "
              # Ignore Planned Unit Developments
              "    AND z.Base NOT LIKE 'PUD/%' "
              # Ignore Parks and Open Space
              "    AND z.Base NOT LIKE 'OP%' "
              # Ignore Right Of Way and Industrial Zones
              "    AND z.Base NOT IN ('ROW', 'M1-2', 'M2-4') "
              "    AND wkt IS NOT NULL;")

    # Create a new table 'underzoned_parcels' using the 'joined' query
    # NOTE: this takes a bit of time...
    d.create_table_as("underzoned_parcels", joined, 102700)

    # Export table as a shapefile
    d.export_shp("underzoned_parcels", "C:/workspace/underzoned_parcels")
