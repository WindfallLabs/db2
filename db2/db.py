# !/usr/bin/env python2
"""
This module contains the DB superclass and all related subclasses.
"""

from __future__ import unicode_literals

import datetime
import os
import re
import sqlite3
import sys
from collections import OrderedDict
from decimal import Decimal

try:
    from urllib import quote_plus
except ImportError:
    from urllib.parse import quote_plus

import pandas as pd
import pybars
import sqlparse
from pymssql import OperationalError
from sqlalchemy import create_engine
from sqlalchemy.event import listen
from sqlalchemy.exc import ResourceClosedError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import utils


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


# =============================================================================
# SQLITE CONFIG
# =============================================================================

# Register type adapters (defined in db2.utils)
sqlite3.register_adapter(datetime.datetime, utils.sqlite_adapt_datetime)
sqlite3.register_adapter(Decimal, utils.sqlite_adapt_decimal)


# =============================================================================
# DATABASE OBJECTS / DB SUPERCLASS
# =============================================================================

class DB(object):
    """
    Utility for exploring and querying a database. Cheers yhat / Greg Lamp!

    Parameters
    ----------
    url: str
        The URL used to connect to the database
    username: str
        Your username for the database
    password: str
        Your password for the database
    hostname: str
        Hostname your database is running on
        (i.e. "localhost", "10.20.1.248")
    port: int
        Port number that the database is running on

            * postgres: 5432
            * redshift: 5439
            * mysql: 3306
            * sqlite: n/a
            * mssql: 1433
    dbname: str
        Name of the database or path to SQLite database
    dbtype: str
        Type of database (i.e. dialect)
    driver: str, None
        Driver for mssql/pyodbc connections.
    encoding: str
        Specify the encoding.
    echo: bool
        Whether or not to repeat queries and messages back to user
    extensions: list
        List of SQLite extensions to load on connection
    """
    def __init__(self, url=None, username=None, password=None, hostname=None,
                 port=None, dbname=None, dbtype=None, driver=None,
                 encoding="utf8", echo=False):

        self._encoding = encoding
        # Credentials
        # TODO: copy over db.py's utils.py for profile save/load handling
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

        # TODO: self.credentials doesn't get populated if URL is provided
        if url:
            self._url = url
            self.credentials["dbtype"] = url.split(":")[0].split("+")[0]
        # Prepare database URL from credentials
        else:
            self._url = DB._create_url(**self.credentials)

        # Create engine
        self.engine = create_engine(self._url)

        # Get DBAPI connection and cursor objects on connect
        listen(self.engine, 'connect', self._on_connect)
        # Connect
        self.con = self.engine.connect()  # Also creates self.con
        # DBAPI Cursor
        #self.cur = self.con.cursor()

        # Create session (WIP)
        self.session = sessionmaker(bind=self.engine)()

        # Misc
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
        setattr(self, "con", conn)
        return

    @property
    def dbname(self):
        """
        Returns the database name.
        """
        return self.credentials["dbname"]

    @property
    def dbtype(self):
        """
        Returns the type of database.
        """
        return self.credentials["dbtype"]

    @property
    def table_names(self):
        """
        Alias for ``self.engine.table_names()``
        Returns a list of tables in the database.
        """
        return self.engine.table_names()

    def get_table_mapping(self, table_name):
        """
        Return an existing table as a mapping

        Parameters
        ----------
        table_name: str
            The name of the table to get as an SQLAlchemy mapping.

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

    @staticmethod
    def _apply_handlebars( sql, data, union=True):
        """
        Create queries using Handlebars style templates

        Parameters
        ----------
        sql: str
            SQL statement
        data: dict, list of dicts
            Variables to iterate the SQL statement over

        Returns
        Modified SQL string.

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
        template = pybars.Compiler().compile(sql)
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

    @staticmethod
    def clean_sql(sql, rm_comments=True, rm_blanks=True,
                  rm_indents=False, **kwargs):
        """
        Cleaning utility (WIP).

        Parameters
        ----------
        sql: str
            SQL statement(s)
        rm_comments: bool
            Optinally remove comments from SQL statement(s)
        rm_blanks: bool
            Optinally remove blank lines from SQL statement(s)
        rm_indents: bool
            Optinally remove indents from SQL statement(s)
        kwargs: dict
            Passed to ``sqlparse.format``

        Returns
        -------
        str:
            The cleaned SQL statement(s).
        """
        # Remove comments
        if rm_comments:
            # Get anything before -- comments
            sql = "\n".join([s.split("--")[0] for s in sql.split("\n")])
            # Get anything inside /* */ comments
            sql = re.sub("(((/\*)+?[\w\W]+?(\*/)+))", "", sql)
        # Remove blank lines
        if rm_blanks:
            sql = "\n".join([s for s in sql.split("\n") if s.strip()])
        if rm_indents:
            sql = "\n".join([s.strip() for s in sql.split("\n") if s.strip()])
        if kwargs:
            sql = sqlparse.format(sql, **kwargs)
        return sql

    def _concat_dfs(sqlfunc):
        """Decorates DB.sql()."""
        def sql_wrapper(d, sql, data=None, union=False):
            """
            Executes one or more SQL statements.

            Parameters
            ----------
            sql: str
                The SQL to be executed. May be a single statement/query or
                script of multiple statements. Placeholders are allowed
                (may vary by dbtype).
            data: dict, tuple; or list or tuple of tuples or dicts
                A container of variables to pass to placeholders in the SQL at
                runtime.

            Returns
            -------
            DataFrame:
                A DataFrame containing the results of a SELECT query, or an
                echo of the statement and number of successful operations.
            """
            dfs = []
            # Allow UNIONing multiple statements iterated over data
            if union:
                sql = DB._apply_handlebars(sql, data, union=True)
                return sqlfunc(d, sql)

            parsed = sqlparse.parse(sql)

            # Iterate over statements passed. Single statements that iterate
            # over data (executemany) will occur inside the sqlfunc
            for stmt in parsed:
                dfs.append(sqlfunc(d, stmt.value, data))
            # If a select query is in the tuple of parsed statements, we don't
            # want to concat with a success DataFrame showing SQL and Result
            try:
                return dfs[parsed.index(
                    [s for s in parsed if utils.is_query(s)][0])]
            except IndexError:
                pass
            return pd.concat(dfs).reset_index(drop=True)
        return sql_wrapper

    @_concat_dfs
    def sql(self, sql, data=None, union=False):
        # Apply handlebars to single statement
        many = False
        if isinstance(data, dict) and "{{" in sql:
            sql = self._apply_handlebars(sql, data)
            if self._echo:
                print(sql)
            rprox = self.engine.execute(sql)

        # Use placeholders/variables
        elif data is not None:
            # Execute many (iterate SQL over input data)
            if (isinstance(data, (list, tuple))
                    and isinstance(data[0], (dict, tuple))):
                many = True
                for dat in data:
                    # Iteratively apply handlebars to statement
                    if "{{" in sql:
                        s = self._apply_handlebars(sql, dat)  # TODO: log SQL
                        if self._echo:
                            print(sql)
                        rprox = self.engine.execute(s)
                    else:
                        if self._echo:
                            print(sql)
                        rprox = self.engine.execute(sql, dat)
            # Execute single with placeholders/variables
            else:
                if self._echo:
                    print(sql)
                rprox = self.engine.execute(sql, data)
        else:
            # Execute single statement without placeholders/variables
            if self._echo:
                print(sql)
            rprox = self.engine.execute(sql)

        try:
            # Get column names from cursor
            columns = rprox.keys()
        except (TypeError, AttributeError):
            # If the SQL doesn't return column names in the cursor.description
            columns = ["SQL", "Result"]

        # Get the results
        try:
            results = rprox.fetchall()
        #except OperationalError:
        except ResourceClosedError:
            results = []

        # Executed SQL did not return data
        if results in ([], None):
            # If columns weren't returned
            if columns == ["SQL", "Result"]:
                if many is True:
                    results = [[sql, len(data)]]
                else:
                    results = [[sql, 1]]
            # SQL returned an empty table
            else:
                results = None

        return pd.DataFrame(results, columns=columns)

    def execute_script_file(self, filename, data=None):
        """
        Executes an SQL script from a file.

        Parameters
        ----------

        filename: str
            Path to the file containing SQL to be executed.
        data: dict
            Dictionary mapping script variables to values via PyBars.

        Returns
        -------
        DataFrame:
            The DataFrame produced from executing the SQL statement(s).
        """
        if not os.path.exists(filename):
            raise AttributeError("input file not found")
        with open(filename, "r") as f:
            script = self._apply_handlebars(f.read(), data)
        return self.sql(script)

    def load_dataframe(self, df, table_name, **kwargs):
        """
        Loads a DataFrame as a database table. (WIP)

        More than just a pass-through to ``DataFrame.to_sql()``, this method
        can be used to ensure that data conforms to various rules. For
        instance, users may specify that all column names should be lowercase,
        etc.

        Parameters
        ----------
        df: DataFrame
            DataFrame object to load into database as a table
        table_name: str
            Name of table to create
        **kwargs: dict
            Passed to ``df.to_sql()``
        """
        kwargs.setdefault("index", False)
        # TODO: enforce datatypes and column name requirements

        df.to_sql(table_name, self.engine, **kwargs)
        return

    def create_mapping(self, mapping):
        """Creates a table from a mapping object."""
        mapping.__table__.create(self.engine)
        return

    def close(self):
        """Close the database connection."""
        #self.con.close()
        self.engine.dispose()
        return

    def __del__(self):
        self.close()

    def __str__(self):
        return "DB[{dbtype}][{host}]:{port} > {user}@{dbname}".format(
            **self.credentials)

    def __repr__(self):
        return self.__str__()


# =============================================================================
# DB SUBCLASSES
# =============================================================================

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
    def __init__(self, dbname, echo=False, extensions=None, functions=None):
        self._extensions = extensions
        self._functions = functions
        super(SQLiteDB, self).__init__(
            dbname=dbname,
            dbtype="sqlite",
            echo=echo)

        # Similar functionality to sqlite command ".databases"
        self.databases = pd.DataFrame(
            OrderedDict([("name", "main"), ("file", self.dbname)]),
            index=[0])

    def _on_connect(self, conn, _):
        """Get DBAPI2 Connection and load all specified extensions."""
        conn.enable_load_extension(True)
        if isinstance(self._extensions, list):
            for ext in self._extensions:
                conn.load_extension(ext)
        # Load Python functions into the database for use in SQL
        if isinstance(self._functions, list):
            for func in self._functions:
                # For each function in the list
                utils.make_sqlite_function(conn, func)
        return

    def attach_db(self, db_path, name=None):
        """
        Attaches a database with ``ATTACH DATABASE :file AS :name;``.

        Parameters
        ----------
        db_path: str
            Path to database to be attached
        name: str default: None
            Name or alias of the attached database

        Returns
        -------
        The fetched cursor result.
        """
        # Default name to filename
        if name is None:
            name = os.path.basename(db_path).split(".")[0]
        self.engine.execute("ATTACH :file AS :name;",
                            {"file": db_path, "name": name})

        # Append alias and path to self.databases dataframe
        self.databases.loc[len(self.databases)] = [name, db_path]
        return 1

    def detach_db(self, name):
        """
        Detaches an attached database by name via ``DETACH DATABASE :name;``.
        """
        self.engine.execute("DETACH DATABASE :name;", {"name": name})

        # Remove the alias from self.databases dataframe
        self.databases.drop(
            self.databases[self.databases["name"] == name].index,
            inplace=True)
        return 1

    def create_table_as(self, table_name, sql, **kwargs):  # TODO: add tests
        """
        Handles ``CREATE TABLE {{table_name}} AS {{select_statement}};`` via
        pandas to preserve column type affinity. (WIP)

        Parameters
        ----------
        table_name: str
            Name of table to create
        sql: str
            SQL `SELECT` statement used to create a new table

        Returns
        -------
        None
        """
        df = self.sql(sql)
        self.load_dataframe(df, table_name)
        return

    def __str__(self):
        return "SQLite[SQLite] > {dbname}".format(dbname=self.dbname)


class PostgresDB(DB):
    """
    Utility for exploring and querying a PostgreSQL database. (WIP)
    """
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
    Utility for exploring and querying a Microsoft SQL database. (WIP)
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

        _ = self.table_names  # Establishes connection I guess...

    @property
    def table_names(self):
        return self.sql(
            "SELECT TABLE_NAME "
            "FROM {{dbname}}.INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE'",
            {"dbname": self.dbname})["TABLE_NAME"].tolist()
