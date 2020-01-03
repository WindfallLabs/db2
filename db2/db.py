# -*- coding: utf-8 -*-
# !/usr/bin/env python2
"""
Module Docstring
"""

__author__ = "Garin Wally"
__version__ = "0.1.0"
__license__ = "MIT"


import sys
from collections import OrderedDict

try:
    from urllib import quote_plus
except ImportError:
    from urllib.parse import quote_plus

import pandas as pd
import pybars
from sqlalchemy import create_engine
from sqlalchemy.event import listen
from sqlalchemy.exc import ResourceClosedError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import MetaData

from .utils import RowConverter

pd.set_option('display.max_columns', 25)


class DB(object):
    """
    Utility for exploring and querying a database.
    Adapted from code originally written by yhat for use with SQLAlchemy.

    Parameters
    ----------
    url: str
        The DB-API URL string used to connect to the database
    username: str
        Your username for the database
    password: str
        Your password for the database
    hostname: str
        Hostname your database is running on (i.e. "localhost", "10.20.1.248")
    port: int
        Port the database is running on. defaults to default port for db.
            portgres: 5432
            redshift: 5439
            mysql: 3306
            sqlite: n/a
            mssql: 1433
    dbname: str
        Name of the database or path to sqlite database
    dbtype: str
        Type of database (i.e. dialect)
    schemas: list
        List of schemas to include. Defaults to all.
    profile: str
        Preconfigured database credentials / profile for how you like your
        queries
    exclude_system_tables: bool
        Whether or not to include "system" tables (the ones that the database
        needs in order to operate). This includes things like schema
        definitions. Most of you probably don't need this, but if you're a db
        admin you might actually want to query the system tables.
    limit: int, None
        Default number of records to return in a query. This is used by the
        DB.query method. You can override it by adding limit={X} to the `query`
        method, or by passing an argument to `DB()`. None indicates that there
        will be no limit (That's right, you'll be limitless. Bradley Cooper
        style.)
    keys_per_column: int, None
        Default number of keys to display in the foreign and reference keys.
        This is used to control the rendering of PrettyTable a bit. None means
        that you'll have verrrrrrrry wide columns in some cases.
    driver: str, None
        Driver for mssql/pyodbc connections.
    encoding: str
        Specify the encoding.
    echo: bool
        Whether or not to repeat queries and messages back to user
    extensions: list
        List of extensions to load on connection
    """
    def __init__(self, url=None, username=None, password=None, hostname=None,
                 port=None, dbname=None, dbtype=None, driver=None,
                 encoding="utf8", echo=False, extensions=[]):  # , *args, **kwargs):
        """"""
        # Employ a configuration dictionary to keep the API tidy
        self._config = {
            "encoding": encoding,
            "connection_data": {
                "user": username,
                "pwd": password,
                "host": hostname,
                "port": ":{}".format(port) if port else None,
                "dbname": dbname,
                "dbtype": str(dbtype).lower() if dbtype else None,
                "driver": str(driver).lower() if driver else None
                }
            }
        self._echo = echo
        self._extensions = extensions

        # Prepare connection to database (create database URL if needed)
        if url:
            self._url = url
        else:
            self._url = DB._create_url(**self._config["connection_data"])

        # Create engine
        self.engine = create_engine(self._url)

        # Load extensions
        if self._extensions:
            listen(self.engine, 'connect', self._load_extensions)

        # Connect
        self.con = self.engine.connect()

        # Create session (WIP)
        self.session = sessionmaker(bind=self.engine)()

        # Misc
        self._handlebars = pybars.Compiler()
        self._last_result = None

    @staticmethod
    def _create_url(**kwargs):
        """Unpacks a dictionary of a DB object's connection data to create a
        database URL.

        Example
        -------
        >>> DB._create_url(**{
        ...     "dbtype": "postgres", "user": "test_user",
        ...     "pwd": "my$tr0ngPWD", "host": "localhost", "port": 8080,
        ...     "dbname": "students", "driver": None})
        'postgres://test_user:my$tr0ngPWD@localhost:8080/students'
        """
        # URL template
        temp = "{dbtype}{driver}://{user}:{pwd}@{host}{port}/{dbname}"

        kwargs.setdefault("driver")

        # SQLite
        if kwargs["dbtype"] == "sqlite" and kwargs["dbname"] is not None:
            return "sqlite:///{}".format(kwargs["dbname"])
        # MSSQL
        elif kwargs["dbtype"] == "mssql":
            mstemp = 'DRIVER={driver};SERVER={host};DATABASE={dbname};UID={user};PWD={pwd}'
            params = quote_plus(mstemp.format(**kwargs))
            return "mssql+pyodbc:///?odbc_connect={}".format(params)
        # Others
        kwargs = {k: v if v else "" for k, v in kwargs.items()}
        kwargs["port"] = ":{}".format(kwargs["port"]) if kwargs["port"] else ""
        return temp.format(**kwargs)

    def _load_extensions(self, conn, _):
        """Loads all specified extensions."""
        conn.enable_load_extension(True)
        for ext in self._extensions:
            conn.load_extension(ext)
        return

    @property
    def table_names(self):
        """
        A list of tables in the database (alias for self.engine.table_names())
        """
        return self.engine.table_names()

    def get_table_mapping(self, table_name):
        """
        Return an existing table as a mapping

        Example
        -------
        >>> d = DB(dbname=":memory:", dbtype="sqlite")
        >>> d.engine.execute("CREATE TABLE Artist (ArtistId INT PRIMARY KEY, Name TEXT);") # doctest:+ELLIPSIS
        <sqlalchemy.engine.result.ResultProxy object at 0x...>
        >>> Artist = d.get_table_mapping("Artist")
        >>> Artist
        <class 'db.Artist'>
        >>> d.insert([Artist(ArtistId=1, Name="AC/DC")])
        >>> assert d.engine.execute("SELECT * FROM Artist").fetchall() == [(1, "AC/DC")]
        """
        if table_name not in self.table_names:
            raise AttributeError("target table '{}' does not exist".format(table_name))
        return type(
            table_name,
            (declarative_base(bind=self.engine),),
            {"__tablename__": table_name, "__table_args__": {"autoload": True}})

    def _assign_limit(self, q, limit=1000):
        q = q.rstrip().rstrip(";")

        if self._config["connection_data"]["dbtype"] == "mssql":
            new = "SELECT TOP {limit} * FROM ({q}) q"
        else:
            new = "SELECT * FROM ({q}) q LIMIT {limit}"
        return new.format(q=q, limit=limit)

    def _apply_handlebars(self, q, data, union=True):
        """
        Create queries using Handlebars style templates

        Example
        -------
        >>> d = DB(dbname=":memory:", dbtype="sqlite")
        >>> d._apply_handlebars("SELECT {{n}} FROM Artist LIMIT 5;", data={"n": "Name"}, union=True)
        u'SELECT Name FROM Artist LIMIT 5;'
        >>> template = "SELECT '{{ name }}' AS table_name, COUNT(*) AS cnt FROM {{ name }} GROUP BY table_name"
        >>> data = [{"name": "Album"}, {"name": "Artist"}, {"name": "Track"}]
        >>> print(d._apply_handlebars(template, data, True))
        SELECT 'Album' AS table_name, COUNT(*) AS cnt FROM Album GROUP BY table_name
        UNION ALL SELECT 'Artist' AS table_name, COUNT(*) AS cnt FROM Artist GROUP BY table_name
        UNION ALL SELECT 'Track' AS table_name, COUNT(*) AS cnt FROM Track GROUP BY table_name
        >>> del d
        """
        if (sys.version_info < (3, 0)):
            q = unicode(q)
        template = self._handlebars.compile(q)
        if isinstance(data, list):
            query = [template(item) for item in data]
            query = ["".join(item) for item in query]
            if union is True:
                query = "\nUNION ALL ".join(query)
            else:
                query = ";\n".join(query)
        elif isinstance(data, dict):
            query = "".join(template(data))
        else:
            return q
        return query

    def sql(self, q, data=None, union=True, limit=None, func=None):
        """
        Execute an SQL query or statement

        Parameters
        ----------
        q: str
            An SQL query or statement string to execute
        data: list, dict
            Optional argument for handlebars-queries. Data will be passed to the template and rendered using handlebars.
        union: bool
            Whether or not "UNION ALL" handlebars templates. This will return any handlebars queries as a single
            DataFrame.
        limit: int
            Number of records to return (if not specified in query)

        Returns a pandas DataFrame

        Example
        -------
        >>> d = DB(dbname=":memory:", dbtype="sqlite")
        >>> d.engine.execute("CREATE TABLE Artist (ArtistId INT PRIMARY KEY, Name TEXT);") # doctest:+ELLIPSIS
        <sqlalchemy.engine.result.ResultProxy object at 0x...>
        >>> d.sql(
        ...     "INSERT INTO Artist VALUES ({{artistid}}, '{{name}}');",
        ...     data=[{"artistid": 1, "name": "AC/DC"}, {"artistid": 2, "name": "Accept"}],
        ...     union=False)
                                                   SQL  Result
            0   INSERT INTO Artist VALUES (1, 'AC/DC')       1
            1  INSERT INTO Artist VALUES (2, 'Accept')       1

        >>> d.sql("SELECT ArtistId FROM Artist WHERE Name = '{{n}}'", data=[{"n": "AC/DC"}, {"n": "Accept"}], union=False)
                                                         SQL  Result
        0   SELECT ArtistId FROM Artist WHERE Name = 'AC/DC'       1
        1  SELECT ArtistId FROM Artist WHERE Name = 'Accept'       2
        """
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
        if func:
            return func(df)
        return df

    def create_mapping(self, mapping):
        """Creates a table from a mapping object."""
        mapping.__table__.create(self.engine)
        return

    def insert(self, rows, table_name=None, commit_each=True):
        """
        Insert rows of data into a database table via a session.

        Parameters
        ----------
        rows: list
            Insert a list of row data into the database session
        table_name: str
            The name of the table to insert data into
        commit_each: bool
            If True, each inserted row is added and committed individually, maintaining order (Default).
            If False, all rows are added at once.

        Example
        -------
        >>> d = DB(dbname=":memory:", dbtype="sqlite")
        >>> d.engine.execute("CREATE TABLE Artist (ArtistId INT PRIMARY KEY, Name TEXT);") # doctest:+ELLIPSIS
        <sqlalchemy.engine.result.ResultProxy object at 0x...>
        >>> d.insert([[1, "AC/DC"]], "Artist")
        >>> d.insert([{"ArtistId": 2, "Name": "Accept"}, {"ArtistId": 3, "Name": "Aerosmith"}], "Artist")
        >>> d.insert([[("ArtistId", 4), ("Name", "Alanis Moressette")]], "Artist")
        >>> d.engine.execute("SELECT * FROM Artist").fetchall()
        [(1, u'AC/DC'), (2, u'Accept'), (3, u'Aerosmith'), (4, u'Alanis Moressette')]
        >>> del d
        """
        if type(rows) is not list:
            raise AttributeError("parameter 'rows' must be list")

        bulk_inserts = []
        for row in rows:
            # Convert non-mapping instance collections to mapping instance
            if not hasattr(row, "__tablename__"):
                if table_name is None:
                    raise AttributeError("table name cannot be None")
                row = RowConverter(self, table_name, row).convert()

            # Insert individually
            if commit_each:
                self.session.add(row)
                self.session.commit()

            # Bulk insert
            else:
                bulk_inserts.append(row)

        if not commit_each:
            self.session.add_all(bulk_inserts)
            self.session.commit()
        return

    def close(self):
        self.con.close()
        self.engine.dispose()
        return

    def __del__(self):
        self.close()

    def __str__(self):
        return "DB[{dbtype}][{host}]:{port} > {user}@{dbname}".format(**self._config["connection_data"])

    def __repr__(self):
        return self.__str__()

'''

    def execute(self, sql):
        pass

    def executemany(self, statement, ):
        pass

    def executescript(self, sql_script):
        pass

    def delete_mapping(self, mapping):
        mapping.__table__.delete(self.engine)
        return

    def df_to_sql(self, df, table_name, srid=-1, if_exists="fail"):
        """Create an SQL table from a (geo)pandas DataFrame."""
        # Ensure that the index column is sequential ints
        df = df.reset_index().rename({"index": "PK_UID"}, axis=1)
        dtype = {}
        if type(df) is gpd.GeoDataFrame:
            # Get longest geometry type
            # e.g. 'MultiPolygon' from {'MultiPolygon', 'Polygon'}
            # TODO: df.enforce_single_multitype(True)  # https://github.com/geopandas/geopandas/issues/834
            # But for now
            geom_type = max(set(df.geometry.geom_type), key=len)
            # Convert from Shapely to EWKT; ensures type constraints
            to_ewkt = ShapelyToEWKT(srid, geom_type)
            df["geometry"] = df["geometry"].apply(to_ewkt)
            #df["geometry"] = df["geometry"].apply(lambda x: WKTElement(str(x), srid))
            # Define geometry column
            dtype = {"geometry": Geometry(
                geom_type,
                srid=srid,
                spatial_index=False,
                management=True)}
        df.to_sql(
            table_name,
            self.engine,
            if_exists=if_exists,
            index=False,
            dtype=dtype)
        return

    def query(self, q, data=None, union=True, limit=None):
        """Return query results as pandas DataFrame."""
        # Get ORM-style query as DataFrame
        if isinstance(q, sqlalchemy.sql.selectable.Select):
            # Convert Select object to SQL string
            q = q.__str__()

        if data:
            q = self._apply_handlebars(q, data, union)
        if limit:
            q = self._assign_limit(q, limit)
        print(q)  # TODO: rm DEBUG
        # Execute query and return DataFrame
        df = pd.read_sql(q, self.engine)
        if "geometry" in df.columns:
            srid = int(
                spatialite_to_ewkt(
                    df.geometry.iat[0]).split(";")[0].replace("SRID=", ""))
            df["geometry"] = df.geometry.apply(lambda x: load_spatialite(x)[0])
            crs = fiona.crs.from_epsg(srid)
            df = gpd.GeoDataFrame(df, geometry="geometry", crs=crs)
        return df

    @property
    def tables(self):
        return self.table_models.keys()
'''


class SQLiteDB(DB):
    """
    Utility for exploring and querying an SQLite database.

    Parameters
    ----------
    filename: str
        Path to SQLite database or ":memory:" for in-memory database
    echo: bool
        Whether or not to repeat queries and messages back to user
    extensions: list
        List of extensions to load on connection
    """
    def __init__(self, dbname, echo=False, extensions=[]):
        super(SQLiteDB, self).__init__(
            dbname=dbname,
            dbtype="sqlite",
            echo=echo,
            extensions=extensions)


class PostgresDB(DB):
    def __init__(self, username, password, hostname, dbname, dbtype="postgres",
                 port=5432, schemas=None, profile="default",
                 exclude_system_tables=True, limit=1000, keys_per_column=None,
                 driver="psycopg2"):
        super(PostgresDB, self).__init__()


class MSSQLDB(DB):
    """
    Utility for exploring and querying a Microsoft SQL database.
    """
    def __init__(self, username, password, hostname, dbname, port=1433,
                 driver='{ODBC DRIVER 13 for SQL Server}', echo=False):
        super(MSSQLDB, self).__init__(
            username=username,
            password=password,
            hostname=hostname,
            dbname=dbname,
            dbtype="mssql",
            driver=driver,
            echo=echo)
