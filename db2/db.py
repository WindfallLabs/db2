# !/usr/bin/env python2
"""
db module
"""

import os
import re
import sys
from collections import OrderedDict
from sqlite3 import Row

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


__all__ = [
    "DB",
    "SQLiteDB",
    "PostgresDB",
    "MSSQLDB"
    ]

# Display more columns; 25 by default
pd.set_option('display.max_columns', 25)
# Suppress scientific notation of floats to 6 digits (default)
pd.set_option("display.float_format", "{:20,.6f}".format)


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
        # Credentials
        # TODO: copy over db.py's utils.py and implement profiles
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

        # Get DBAPI connection and cursor objects on connect
        listen(self.engine, 'connect', self._on_connect)
        # Connect
        self.con = self.engine.connect()  # Also creates self.dbapi_con
        # DBAPI Cursor
        self.cur = self.dbapi_con.cursor()

        # Create session (WIP)
        self.session = sessionmaker(bind=self.engine)()

        # Misc
        self._handlebars = pybars.Compiler()
        self._last_result = None
        self._max_return_rows = 10

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
            return "mssql+{driver}://{user}:{pwd}@{host}/?charset=utf8".format(
                **kwargs)
        # Others
        kwargs = {k: v if v else "" for k, v in kwargs.items()}
        kwargs["port"] = ":{}".format(kwargs["port"]) if kwargs["port"] else ""
        return temp.format(**kwargs)

    def _on_connect(self, conn, _):
        """Get DBAPI2 Connection."""
        setattr(self, "dbapi_con", conn)
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

    @property
    def _placeholders(self):  # TODO: required overwrite in indiv DB classes
        """
        A list of placeholders supported by the database type as regex strings.
        """
        return ["\?", "\:\w+"]  # Default for SQLite (?, and :var style)


    def _apply_handlebars(self, sql, data, union=True):
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
            sql = unicode(sql)
        template = self._handlebars.compile(sql)
        has_semicolon = True if sql.endswith(";") else False
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
            return sql
        return query + (";" if not query.endswith(";")
                        and has_semicolon is True else "")

    def sql(self, sql, data=None, union=False, limit=None):
        """
        Executes an SQL statement. The SQL statement may be parameterized
        (i. e. placeholders instead of SQL literals) using either native
        placeholders or handlebars style placeholders.

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
        >>> d.sql(
        ...     "CREATE TABLE Artist (ArtistId INT PRIMARY KEY, Name TEXT)")

        # Execute statements parameterized in the qmark style:
        >>> d.sql("INSERT INTO Artist VALUES (?, ?)", (1, "AC/DC"))
        >>> d.sql("INSERT INTO Artist VALUES (?, ?)", (2, "Accept"))

        # Execute statements parameterized in the named style:
        >>> d.sql("SELECT * FROM Artist where Name=:who",
        ...               {"who": "AC/DC")

        # Execute statements parameterized in the handlebars style:
        # (similar to named style, but can be used where native placeholders
        # are not normally allowed)
        >>> d.sql("SELECT {{col}} FROM Artist WHERE AritstId = 2",
        ...               {"col": "Name"})

        """
        # Ensure SQL is unicode
        if (sys.version_info < (3, 0)):
            sql = unicode(sql)

        # Ensure SQL ends with a semicolon
        if not sql.endswith(";"):
            sql = sql + ";"

        dataframes = []
        # Execute script
        if len(re.findall(";", sql)) > 1:
            for stmt in [i.strip() for i in sql.split(";") if i]:
                dataframes.append(self.sql(stmt, data))
            return pd.concat(dataframes).reset_index(drop=True)

        # Execute many (data is list)
        elif isinstance(data, list):
            cnt = 0
            for d in data:
                # Iterate the stmt over the data applying handlebars style
                dataframes.append(self.sql(sql, d))
                cnt += 1
            return_df = pd.concat(dataframes).reset_index(drop=True)
            if len(return_df) <= self._max_return_rows:
                return return_df
            # Show parameterized rather than each stmt in result
            return pd.DataFrame(
                data=[[sql, cnt]],
                columns=["SQL", "Result"])

        # Execute single statement
        else:
            # Apply limit if supplied
            # Code for yhat's method 'DB._apply_limit' is embeded here
            if limit:  # TODO: and security is 'relaxed'
                sql = sql.rstrip().rstrip(";")

                if self.dbtype == "mssql":
                    sql = "SELECT TOP {limit} * FROM ({sql}) q".format(
                        sql=sql, limit=limit)
                else:
                    sql = "SELECT * FROM ({sql}) q LIMIT {limit}".format(
                        sql=sql, limit=limit)

            try:
                # Check for native placeholders first
                if re.findall(re.compile("|".join(self._placeholders)), sql) \
                        and isinstance(data, (tuple, dict)):
                    df = pd.read_sql(sql, self.con, params=data)
                # Then handlebars
                else:
                    sql = self._apply_handlebars(sql, data, union)
                    df = pd.read_sql(sql, self.con)

            except (TypeError, ResourceClosedError):
                # Just because it errors doesn't mean stmts didn't execute
                df = pd.DataFrame(data=[[sql, 1]],
                                  columns=["SQL", "Result"])
            return df

    def load_dataframe(self, df, table_name):
        """
        Loads a DataFrame as a database table.
        """
        # TODO: enforce datatypes and column name requirements
        df.to_sql(table_name, self.engine)
        return

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


class SQLiteDB(DB):
    """
    Utility for exploring and querying an SQLite database.

    Parameters
    ----------
    dbname: str
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
        # TODO: consider using Row factories
        # self._set_row_factory(Row)

        # Similar functionality to sqlite command ".databases"
        self.databases = pd.DataFrame(
            OrderedDict([("name", "main"), ("file", self.dbname)]),
            index=[0])

    def _on_connect(self, conn, _):
        """Get DBAPI2 Connection and load all specified extensions."""
        setattr(self, "dbapi_con", conn)
        self.dbapi_con.enable_load_extension(True)
        for ext in self._extensions:
            self.dbapi_con.load_extension(ext)
        return

    def _set_row_factory(self, factory):
        """Change how cursors return database records."""
        self.dbapi_con.row_factory = factory
        self.cur = self.dbapi_con.cursor()
        return

    def attach_db(self, db_path, name=None):
        """
        Attaches a database with `ATTACH DATABASE :file AS :name;`.
        """
        # Default name to filename
        if name is None:
            name = os.path.basename(db_path).split(".")[0]
        self.cur.execute("ATTACH :file AS :name;",
                         {"file": db_path, "name": name})

        # Append alias and path to self.databases dataframe
        self.databases.loc[len(self.databases)] = [name, db_path]
        return self.cur.fetchall()

    def detach_db(self, name):
        """
        Detaches an attached database with  `DETACH DATABASE :name;`.
        """
        self.cur.execute("DETACH DATABASE :name;", {"name": name})

        # Remove the alias from self.databases dataframe
        self.databases.drop(
            self.databases[self.databases["name"] == name].index,
            inplace=True)
        return self.cur.fetchall()

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
                 driver='pymssql', echo=False):
        super(MSSQLDB, self).__init__(
            username=username,
            password=password,
            hostname=hostname,
            dbname=dbname,
            dbtype="mssql",
            driver=driver,
            echo=echo)
        self.sql("USE {{dbname}};", {"dbname": dbname})

    @property
    def table_names(self):
        return self.sql(
            "SELECT TABLE_NAME "
            "FROM {{dbname}}.INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE'",
            {"dbname": self.dbname})["TABLE_NAME"].tolist()
