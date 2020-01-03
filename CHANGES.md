# Changes

## Version 0.0.1 (January 3, 2020)

* Created project structure with `cookiecutter https://github.com/audreyr/cookiecutter-pypackage.git` ([source](https://www.pydanny.com/cookie-project-templates-made-easy.html))
* Created `db2` to carry on in the spirit of [db.py](https://github.com/yhat/db.py) by yhat.
* Created `db2.utils`
* Created `db2.db`
* Created `db2/tests`
    * NOTE: currently uses `nosetests`
    * TODO: change to tox / py.test
* Reconfigured `DB` object for use as a superclass for subclassed database objects (e.g. `SQLiteDB`)
    * Removed `DB` parameter `filename` (SQLite only) in favor for `dbname`
    * Added `DB._config` to hold `connection_data` dictionary and `encoding`
        * GOAL: keep a simple API
    * Added support for loading extensions on `connect()` via SQLAlchemy `listen` function
    * Added `DB._create_url` to build URL from `DB._config['connection_data']`
    * Removed `DB.cur` attribute
    * Added `DB.engine`
    * Changed `DB.con` to `engine.connect()`
    * Added `DB.session` but I'm unsure if this is needed
    * Renamed `.query` method to `.sql`
        * Now supports statements that would normally fail in `db.py`
            * Returns a `pandas.DataFrame` with submitted statement and '1' if successful
        * Returns a `pandas.DataFrame` for all queries submitted (if `union=False`)
        * WIP: returned DataFrames are saved as `DB._last_result`
    * Added `DB.get_table_mapping` to easily and dynamically load existing tables as mappings
    * Added `DB.create_mapping` to easily create tables from a mapping
    * Added `DB.insert` to flexibly insert records/rows into a table
    * Added `DB.close` method
    * Kept `DB.__str__` and `DB.__repr__` unchanged
* Created `db2.ext` library to house non-core code (extensions)
* Created `db2.ext.spatialdb` extension
    * Added `GeoDataFrameToSQLHandler` to dump `geopandas.GeoDataFrame` objects to SQL text
    * Added `SpatiaLiteBlobElement` to handle Blob geometry decoding
    * Added `SpatiaLiteDB` subclass of `db2.SQLiteDB`
        * Added `SpatiaLiteDB.geometries` property to quickly get spatial table information
        * Added `SpatiaLiteDB.load_geodataframe` to CREATE and INSERT data into the database from `geopandas.GeoDataFrame` objects
            * Checks for geometry type integrity
        * Changed `.sql` to automatically read SpatiaLite Blob geometries with shapely
