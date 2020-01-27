===============
Getting Started
===============

Getting started with db2.py is simple! To make it easy to follow along, this
tutorial uses two SQLite databases: an in-memory database we create about
animals (variable called 'mem') and a database on-disk called 'chinook'
(chinook download_ and tutorial_).

Please note that we test these examples with ``doctest`` to ensure they work
as expected. You may ignore the doctest directives such as
``#doctest: +NORMALIZE_WHITESPACE``, etc.

.. _download: https://github.com/lerocha/chinook-database
.. _tutorial: https://www.sqlitetutorial.net/sqlite-sample-database/

Connecting to Databases
-----------------------

Create a new in-memory SQLite database

    >>> import db2
    >>> mem = db2.SQLiteDB(":memory:")

Or connect to an existing SQLite database (on-disk)

    >>> chinook = db2.SQLiteDB("./tests/chinook.sqlite")

With those simple lines of code, we've created an object that can be used to
interact with our databases. No connection objects or cursors to manage.
On that subject, db2.py leverages SQLAlchemy to handle database connections.
When a connection is made, the DB object has access to the SQLAlchemy connection
``.sqla_con`` as well as to the connection object provided by the database's
python module (sqlite3, pymssql, etc.) via the ``.con`` attribute.


Exploring your Databases
------------------------

Use the ``table_names`` property to list tables.

    >>> chinook.table_names  #doctest: +NORMALIZE_WHITESPACE
    [u'Album', u'Artist', u'Customer', u'Employee', u'Genre', u'Invoice',
     u'InvoiceLine', u'MediaType', u'Playlist', u'PlaylistTrack', u'Track']

# TODO
Explore the schema with the ``tables`` object...


Executing SQL Statements/Queries
--------------------------------

Each database object provides a cursor object as a class attribute ``.cur``. It
will provide the same functionality as the database's underlying Python module.
For example, the sqlite3 module provides a ``executescript()`` method on the
cursor object, while pymssql does not.

db2.py provides an ``.sql()`` method that will `always` return a pandas
DataFrame.

    >>> chinook.sql("SELECT * FROM Artist LIMIT 3")
       ArtistId       Name
    0         1      AC/DC
    1         2     Accept
    2         3  Aerosmith

Even in cases where little or nothing is returned, such as function calls and
DDL statements, db2.py returns a DataFrame containing the submitted statement
and often a 1 to indicate success, or the number of features affected.

    >>> mem.sql("CREATE TABLE animals (id INT PRIMARY KEY, name TEXT);")
                                                     SQL  Result
    0  CREATE TABLE animals (id INT PRIMARY KEY, name...       1

The ``.sql()`` method is designed for flexibility and will handle a single
statement or query, many statements, or entire scripts. Here's an example of a
single statement that is executed 3 times over the input data (same functionality as ``.cur.executemany()``). The
resulting dataframe shows the SQL we passed, and the number of INSERTed records.

    >>> mem.sql("INSERT INTO animals VALUES (?, ?);",
    ... [(1, 'Cat'), (2, 'Dog'), (3, 'Capybara')])
                                      SQL  Result
    0  INSERT INTO animals VALUES (?, ?);       3

Variables or placeholders like the '?' above can be sometimes be used in SQL,
but not all databases work the same. Below shows a named parameter style that
is supported by SQLite, but may not be supported by all databases.

    >>> mem.sql("INSERT INTO animals VALUES (:id, :name);",
    ...         {"id": 4, "name": 'Chicken'})
                                            SQL  Result
    0  INSERT INTO animals VALUES (:id, :name);       1

If we want to use named-parameter style in a database that does not support it,
db2.py uses the pybars module to enable handlebars-style variable support for
any database. First we'll INSERT a single record of ``(5, "Duck")`` into the
database. The resulting DataFrame shows the resolved SQL statement while the
results of the following INSERT of two records shows the parameterized SQL.

    >>> mem.sql("INSERT INTO animals VALUES ({{ id }}, '{{ name }}');",
    ...         {"id": 5, "name": "Duck"})
                                           SQL  Result
    0  INSERT INTO animals VALUES (5, 'Duck');       1


    >>> mem.sql("INSERT INTO animals VALUES ({{ id }}, '{{ name }}');",
    ...         [{"id": 6, "name": "Aardvark"}, {"id": 7, "name": "Snake"}])
                                                     SQL  Result
    0  INSERT INTO animals VALUES ({{ id }}, '{{ name...       2


Also note that this method allows you to use variable names where they may not
be allowed with other parameterization methods.

    >>> chinook.sql("SELECT a.Name, b.Title FROM {{ artist_table }} a "
    ...             "LEFT JOIN Album b ON a.ArtistId=b.ArtistId "
    ...             "WHERE a.Name = '{{ band_name }}'",
    ...             {"artist_table": "Artist", "band_name": "Santana"})
          Name                     Title
    0  Santana              Supernatural
    1  Santana  Santana - As Years Go By
    2  Santana              Santana Live


Finally, entire scripts can be run using this method. Note that each statement
will be run individually and may or may not utilize the passed variables. When
complete, a DataFrame will return with all statements and results.

    >>> s = ("CREATE TABLE new_animals (id INT, name TEXT);"
    ...      "INSERT INTO new_animals VALUES ({{ id }}, '{{ animal }}');")
    >>> mem.sql(s, {"id": 1, "animal": "Cat"})
                                                 SQL  Result
    0  CREATE TABLE new_animals (id INT, name TEXT);       1
    1     INSERT INTO new_animals VALUES (1, 'Cat');       1

But you cannot combine single-execution statements with ones that iterate over
data. The example below will fail because it will execute the CREATE statement
for both cat and dog records:

    >>> s = ("CREATE TABLE new_animals (id INT, name TEXT);"
    ...      "INSERT INTO new_animals VALUES ({{ id }}, '{{ animal }}');")
    >>> mem.sql(s, [{"id": 1, "animal": "Cat"}, {"id": 2, "animal": "Dog"}])  #doctest: +SKIP

In short, don't pass containers (i.e. lists or tuples) as data to a script.

Use the ``.execute_script_file()`` method to execute script files from a path.
