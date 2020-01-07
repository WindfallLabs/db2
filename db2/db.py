# -*- coding: utf-8 -*-
# !/usr/bin/env python2
"""
db module
"""

import re
import sys

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

pd.set_option('display.max_columns', 25)


class Cursor(object):
    def __init__(self, db):
        """
        Non-standard Cursor object.
        Allows backwards compatibility for existing script that use db.py.

        Parameters
        ----------
        db: db2.DB object
            Database object to register with
        """
        self.db = db
        self.con = db.con  # TODO: or should this be engine?
        self._handlebars = pybars.Compiler()
        self.return_limit = 3

    @property
    def native_placeholders(self):
        """
        The placeholders supported by the database type as regex strings

        SQLite supports two kinds of placeholders:
            question marks (qmark style) and named placeholders (named style).
        <ADD OTHERS HERE>
        """
        if self.db.dbtype == "sqlite":
            return ["\?", "\:\w+"]  # e.g. ["?", ":id"]
        # TODO: Add additional database placehold support here
        # elif self.dbtype == "postgres":
        #    return ["\$\d+"]  # Is this right?
        else:
            raise NotImplementedError(
                "'{}' is not currently supported".format(self.dbtype))

    def _assign_limit(self, q, limit=1000):
        q = q.rstrip().rstrip(";")

        if self.db.dbtype == "mssql":
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

        template = self._handlebars.compile(q)
        has_semicolon = True if q.endswith(";") else False
        if isinstance(data, list):
            query = [template(item) for item in data]
            query = ["".join(item) for item in query]
            if union is True:
                query = "\nUNION ALL ".join(query)
            else:
                has_semicolon = True
                query = ";\n".join([i.strip(";") for i in query])
        elif isinstance(data, dict):
            query = "".join(template(data))
        else:
            return q
        return query + (";" if not query.endswith(";")
                        and has_semicolon is True else "")

    def _query_has_native_placeholders(self, q):
        """
        Check query for qmark or named placeholders.

        Parameters
        ----------
        q: str
            SQL statement

        Examples
        --------
        >>> cur = Cursor(SQLiteDB)

        # qmark style
        >>> cur._query_has_native_placeholders(
        ...     "SELECT * FROM test_table WHERE id = ?")
        True

        # named style
        >>> cur._query_has_native_placeholders(
        ...     "SELECT * FROM test_table WHERE id = :id")
        True

        # but not for Handlebars style
        >>> Cursor._query_has_native_placeholders(
        ...     "SELECT * FROM test_table WHERE id = {{id}}")
        False
        """
        if re.findall(re.compile("|".join(self.native_placeholders)), q):
            return True
        return False

    def execute(self, sql, data=None, union=False, limit=None):
        """
        Executes an SQL statement. The SQL statement may be parameterized
        (i. e. placeholders instead of SQL literals). db2 also adds support for
        native placeholders and handlebars style placeholders.

        Parameters
        ----------
        sql: unicode str
            SQL statement to execute.
        data: tuple or dict
            Input data to submit to parameterized statement
        union:

        limit:

        Examples
        --------
        >>> d = DB(dbname=":memory:", dbtype="sqlite")
        # Execute an unparameterized statement:
        >>> d.cur.execute(
        ...     "CREATE TABLE Artist (ArtistId INT PRIMARY KEY, Name TEXT)")

        # Execute statements parameterized in the qmark style:
        >>> d.cur.execute("INSERT INTO Artist VALUES (?, ?)", (1, "AC/DC"))
        >>> d.cur.execute("INSERT INTO Artist VALUES (?, ?)", (2, "Accept"))

        # Execute statements parameterized in the named style:
        >>> d.cur.execute("SELECT * FROM Artist where Name=:who",
        ...               {"who": "AC/DC")

        # Execute statements parameterized in the handlebars style:
        # (similar to named style, but can be used where native placeholders
        # are not normally allowed)
        >>> d.cur.execute("SELECT {{col}} FROM Artist WHERE AritstId = 2",
        ...               {"col": "Name"})

        """
        if (sys.version_info < (3, 0)):
            sql = unicode(sql)

        # Apply limit if supplied
        if limit:
            sql = self._assign_limit(sql, limit)

        try:
            # Check for native placeholders first
            if self._query_has_native_placeholders(sql) \
                    and isinstance(data, (tuple, dict)):
                df = pd.read_sql(sql, self.con, params=data)
            else:
                sql = self._apply_handlebars(sql, data, union)
                df = pd.read_sql(sql, self.con)

        except ResourceClosedError:
            # Just because it errors doens't mean stmts didn't execute
            df = pd.DataFrame(data=[[sql, 1]],
                              columns=["SQL", "Result"])
        return df

    def executemany(self, sql, data):
        """
        Executes a parameterized SQL statement over many data values.
        This is already supported by SQLAlchemy's 'execute'.

        Parameters
        ----------
        sql: unicode str
            SQL statement to execute for each datum
        data: list
            Data to iterate sql statement over

        Examples
        --------
        >>> d = DB(dbname=":memory:", dbtype="sqlite")
        >>> d.cur.execute(
        ...     "CREATE TABLE Artist (ArtistId INT PRIMARY KEY, Name TEXT)")

        # Execute statement over data (in any placeholder style)
        >>> d.cur.executemany("INSERT INTO Artist VALUES (?, ?)",
        ...                   [(1, "AC/DC"),
        ...                    (2, "Accept")])
        """
        dataframes = []
        cnt = 0
        for d in data:
            # Iterate the stmt over the data applying handlebars style
            dataframes.append(self.execute(sql, d))
            cnt += 1
        return_df = pd.concat(dataframes).reset_index(drop=True)
        if len(return_df) <= self.return_limit:
            return return_df
        # Show parameterized rather than each stmt in result
        return pd.DataFrame(
            data=[[sql, cnt]],
            columns=["SQL", "Result"])

    def executescript(self, sql, data=None):
        """
        Executes multiple SQL statements at once.

        Parameters
        ----------
        sql: str
            Multiple SQL statements to execute.
        data: dict
            This only supports handlebars style parameterization
        """
        dataframes = []
        for stmt in [i.strip() for i in sql.split(";") if i]:
            dataframes.append(self.execute(stmt, data))

        return pd.concat(dataframes).reset_index(drop=True)

    def executeunified(self, sql, data, union=False, limit=None):
        """
        Automatically applies the appropriate method to execute an SQL
        statement.
        """
        # Execute script
        if len(re.findall(";", sql)) > 1:
            return self.executescript(sql, data)
        # Execute many (data is list)
        elif isinstance(data, list):
            return self.executemany(sql, data)
        # Execute single statement
        else:
            return self.execute(sql, data, union, limit)


class DB(object):
    """
    Utility for exploring and querying a database.
    Adapted from code originally written by yhat for use with SQLAlchemy.
    """
    def __init__(self, url=None, username=None, password=None, hostname=None,
                 port=None, dbname=None, dbtype=None, driver=None,
                 encoding="utf8", echo=False, extensions=[]):
        """

        Parameters
        ----------
        url: str
            The DB-API URL string used to connect to the database
        username: str
            Your username for the database
        password: str
            Your password for the database
        hostname: str
            Hostname your database is running on
            (i.e. "localhost", "10.20.1.248")
        port: int
            Port the database is running on. defaults to default port for db.
                postgres: 5432
                redshift: 5439
                mysql: 3306
                sqlite: n/a
                mssql: 1433
        dbname: str
            Name of the database or path to sqlite database
        dbtype: str
            Type of database (i.e. dialect)
        driver: str, None
            Driver for mssql/pyodbc connections.
        encoding: str
            Specify the encoding.
        echo: bool
            Whether or not to repeat queries and messages back to user
        extensions: list
            List of extensions to load on connection
        """
        self._encoding = encoding
        self.credentials = {
                "user": username,
                "pwd": password,
                "host": hostname,
                "port": ":{}".format(port) if port else None,
                "dbname": dbname,
                "dbtype": str(dbtype).lower() if dbtype else None,
                "driver": str(driver).lower() if driver else None
                }
        self._echo = echo
        self._extensions = extensions

        # Prepare connection to database (create database URL if needed)
        if url:
            self._url = url
        else:
            self._url = DB._create_url(**self.credentials)

        # Create engine
        self.engine = create_engine(self._url)

        # Load extensions  # TODO: is this SQLite-specific?
        if self._extensions:
            listen(self.engine, 'connect', self._load_extensions)

        # Connect
        self.con = self.engine.connect()
        self.cur = Cursor(self)

        # Create session (WIP)
        self.session = sessionmaker(bind=self.engine)()

        # Misc
        self._handlebars = pybars.Compiler()
        self._last_result = None

    @staticmethod
    def _create_url(**kwargs):
        """
        Unpacks a dictionary of a DB object's connection data to create a
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
            mstemp = ('DRIVER={driver};SERVER={host};DATABASE={dbname};'
                      'UID={user};PWD={pwd}')
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
    def dbname(self):
        return self.credentials["dbname"]

    @property
    def dbtype(self):
        return self.credentials["dbtype"]

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
            raise AttributeError("target table '{}' does not exist".format(
                table_name))
        return type(
            table_name,
            (declarative_base(bind=self.engine),),
            {"__tablename__": table_name,
             "__table_args__": {"autoload": True}})

    def sql(self, sql, data=None, union=False, limit=None, filename=None):
        """
        Execute SQL
        Unifies 'DB.cur.execute', 'DB.cur.executemany', and
        'DB.cur.executescript' into a single, flexible method

        Parameters
        ----------
        sql: str
            An SQL query or statement string to execute
        data: list, dict
            Optional argument for handlebars-queries. Data will be passed to
            the template and rendered using handlebars. Any items in data not
            specified in q are ignored.
        union: bool
            Whether or not to join statements with "UNION ALL". Allowing for
            many SELECT statements to return as one dataframe.
        limit: int
            Number of records to return (if not specified in query)
        filename: str (path)
            Optionally execute SQL from a file

        Returns a pandas DataFrame

        Example
        -------
        >>> d = DB(dbname=":memory:", dbtype="sqlite")

        # Execute a statement
        >>> d.sql("CREATE TABLE Artist (ArtistId INT PRIMARY KEY, Name TEXT);") # doctest:+ELLIPSIS
                                                         SQL  Result
        0  CREATE TABLE Artist (ArtistId INT PRIMARY KEY,...

        # Execute many statements by iterating over data
        # via handlebars
        >>> d.sql("INSERT INTO Artist VALUES ({{artistid}}, '{{name}}');",
        ...       data=[{"artistid": 1, "name": "AC/DC"},
        ...             {"artistid": 2, "name": "Accept"}],
        ...       union=False)
                                               SQL  Result
        0   INSERT INTO Artist VALUES (1, 'AC/DC')       1
        1  INSERT INTO Artist VALUES (2, 'Accept')       1

        # via qmarks
        >>> d.sql("INSERT INTO Artist VALUES (?, ?);",
        ...       data=[(3, "Aerosmith"),
        ...             (4, "Alanis Moressette")])

        >>> d.sql("SELECT ArtistId FROM Artist WHERE Name = '{{n}}'",
        ...       data=[{"n": "AC/DC"},
        ...             {"n": "Accept"}],
        ...       union=False)
                                                         SQL  Result
        0   SELECT ArtistId FROM Artist WHERE Name = 'AC/DC'       1
        1  SELECT ArtistId FROM Artist WHERE Name = 'Accept'       2
        """
        if filename:
            with open(filename, "r") as f:
                sql = f.read()

        df = self.cur.executeunified(sql, data, union, limit)
        return df

    def create_mapping(self, mapping):
        """Creates a table from a mapping object."""
        mapping.__table__.create(self.engine)
        return

    def close(self):
        self.con.close()
        self.engine.dispose()
        return

    def __del__(self):
        self.close()

    def __str__(self):
        return "DB[{dbtype}][{host}]:{port} > {user}@{dbname}".format(
            **self.credentials)

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

    def __str__(self):
        return "SQLite[SQLite] > {dbname}".format(dbname=self.dbname)


class PostgresDB(DB):
    def __init__(self, username, password, hostname, dbname, dbtype="postgres",
                 port=5432, schemas=None, profile="default", echo=False,
                 exclude_system_tables=True, limit=1000, keys_per_column=None,
                 driver="psycopg2"):
        super(PostgresDB, self).__init__(
            username=username,
            password=password,
            hostname=hostname,
            dbname=dbname,
            dbtype="postgres",
            driver=driver,
            echo=echo)


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
