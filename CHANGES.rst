Changes
=======

Milestones required to bump version to 0.1.0
--------------------------------------------

**GOAL:** A one-time, feature-complete release for Python2.7 (anything after 0.1.0 will be Python3)

Core db2.py:

* [X] Documentation with Sphinx
    * Hosted as GitHub Wiki?
* [ ] Setup Tox, Travis/AppVeyor
* [ ] setup.py, and related configs work
* [ ] Test PostgreSQL, MSSQL with Docker
    * Support for MySQL, Oracle, Redshift?
* [ ] db.py's Table object
    * Auto-complete table names
    * ``__repr__`` returns ``DataFrame``
    * etc.
* [ ] IPython/Jupyter Notebook support
* [ ] I/O of MS Excel files (export to named sheets, etc.)
* [ ] A good amount of examples


Version 0.0.2 (February 2020)
-----------------------------

* Split db2 project from SpatialDB project


Version 0.0.1 (January, 2020)
-----------------------------

Differences between db.py and db2.py:

* Reconfigured ``DB`` object for use as a superclass for other database subclasses (e.g. ``SQLiteDB``)
    * Removed ``DB.__init__`` parameter ``filename`` (SQLite only) in favor for ``dbname``
    * Changed ``DB.credentials`` from a property to a dictionary (to reduce API)
        * Both ``DB.dbname`` and ``DB.dbtype`` properties still exist
* Built with SQLAlchemy
    * Added ``DB._create_url`` to build URL from ``DB.credentials``
    * Added ``DB.engine``
    * Changed ``DB.con`` to result of ``engine.connect()``
    * Added ``DB.dbapi_con`` to access DBAPI2 connections
    * Added ``DB.session`` but I'm unsure if this is needed
    * Added ``DB.get_table_mapping`` to easily and dynamically load existing tables as mappings
    * Added ``DB.create_mapping`` to easily create tables from a mapping
* Replaced ``DB.query_from_file`` with ``DB.execute_script_file``
* Added ``DB.close`` method
* Renamed ``DB.query`` method to ``DB.sql``
    * Always returns DataFrames
        * Now supports statements with no outputs
        * Returns a results DataFrame for multiple statements


New in db2.py:

* Added SQLAlchemy ``listen`` call to ``DB.__init__``
    * Now supports loading SQLite extensions on ``connect()``
* Created ``db2.ext`` library to house non-core code (extensions)

