# Changes

## Version 0.0.1 (January 3 - 5, 2020)

* Created project structure with `cookiecutter https://github.com/audreyr/cookiecutter-pypackage.git` ([source](https://www.pydanny.com/cookie-project-templates-made-easy.html))
* Created `db2` to carry on in the spirit of [db.py](https://github.com/yhat/db.py) by yhat.
* Created `db2.utils` to house misc helper code
    * Created `RowConverter` to convert collections to SQLAlchemy inserts  (_TESTED_)
* Created `db2.db` to house the database classes
* Created `db2/tests`
* Reconfigured `DB` object for use as a superclass for subclassed database objects (e.g. `SQLiteDB`)
    * Removed `DB.__init__` parameter `filename` (SQLite only) in favor for `dbname`
    * Changed `DB.credentials` from property to dictionary (reduce API)
    * Added support for loading extensions on `connect()` via SQLAlchemy `listen` function
    * Added `DB._create_url` to build URL from `DB.credentials`
    * Removed `DB.cur` attribute
    * Added `DB.engine`
    * Changed `DB.con` to result of `engine.connect()`
    * Added `DB.session` but I'm unsure if this is needed
    * Renamed `.query` method to `.sql`
        * Now supports statements that would normally fail in `db.py` (_TESTED_)
            * Returns a DataFrame with submitted statement and '1' if successful
        * Returns a DataFrame for all queries submitted (if `union=False`)
        * WIP: returned DataFrames are saved as `DB._last_result`
    * Added `DB.get_table_mapping` to easily and dynamically load existing tables as mappings
    * Added `DB.create_mapping` to easily create tables from a mapping
    * Added `DB.insert` to flexibly insert records/rows into a table
    * Added `DB.close` method
* Created `db2.ext` library to house non-core code (extensions)
* Created `db2.ext.spatialdb` extension (my reason for taking this project on)
    * Added `GeoDataFrameToSQLHandler` to dump GeoDataFrame objects to SQL text
    * Added `get_sr_from_web` to get srs data from [spatialreference.org](https://www.spatialreference.org) (_TESTED_)
    * Added `SpatiaLiteBlobElement` to handle Blob geometry decoding
    * Added `SpatiaLiteDB` subclass of `db2.SQLiteDB`
        * Added `SpatiaLiteDB.geometries` property to quickly get spatial table information
        * Added `SpatiaLiteDB.load_geodataframe` to CREATE and INSERT data into the database from `geopandas.GeoDataFrame` objects (_TESTED_)
            * Checks and handles for single geometry type integrity
        * Altered `SpatiaLiteDB.sql` to return a valid GeoDataFrame
            * Automatically converts SpatiaLite Blob geometries to shapely
            * Sets `GeoDataFrame.crs` attribute
