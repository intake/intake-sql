Quickstart
==========

``intake-odbc`` provides quick and easy access to tabular data stored in
ODBC data sources, which include a wide variety of traditional relational
database systems such as MySQL and Microsoft SQL Server. Some DB systems
such as `PostgreSQL`_ may have better/faster plugin implementations.

.. _PostgreSQL: https://github.com/ContinuumIO/intake-postgres

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c intake intake-odbc

.. _intake: https://github.com/ContinuumIO/intake

Setting up ODBC
~~~~~~~~~~~~~~~

Configuring ODBC is beyond the scope of this document, and generally not something
that it performed by an end-user, and generally requires the installation of
backend-specific drivers system-wide.

Specific documentation on the connection string and keyword arguments can be found
on the `TurbODBC`_ website.

.. _TurbODBC: http://turbodbc.readthedocs.io/en/latest/pages/odbc_configuration.html

Usage
-----

Ad-hoc
~~~~~~

After installation, the functions ``intake.open_odbc``
and ``intake.open_odbc_partitioned`` will become available.
Assuming you have an ODBC set up with a fully-configured connection named
``"MSSQL"`` the following would fetch the contents of ``mytable`` into a pandas
dataframe::

   import intake
   source = intake.open_odbc('Driver={MSSQL}', 'SELECT * FROM mytable')
   dataframe = source.read()

Two key arguments are required to define an ODBC data source: the DB connection
parameters, and a SQL query to execute. The former may be as simple as a TurbODBC
connection string, but can commonly be a set of keyword arguments, all of which are
passed on to ``turbodbc.connect()``. The query must have a valid syntax to be
executed by the backend of choice.

In addition, the following arguments are meaningful for the non-partitioned
data source:

- head_rows: how many rows are read from the start of the data to infer data
 types for discovery

- mssql: a special flag to mark datasets which are backed by MS SQL Server, which
 requires a different spelling of the ``LIMIT`` statement.

When using the partitioned ODBC source, further details are required in order to
build the queries for each partition. It requires an index column to use for
the ``WHERE`` statement, and the bounding values for each partition. The most
explicit way to provide the boundaries is with the ``divisions`` keyword, or
boundaries will be calculated as ``npartitions`` equally spaced boundaried between
the min/max values. Note that some partitions may be empty.

Further arguments when using partitioning:

- index: Column to use for partitioning

- max, min: the range of values of the index column to consider for building partitiona;
 will execute a separate query  to find these, if not given

- npartitions: the number of partitions to create

- divisions: explicit partition boundary values

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

To include in a catalog, the plugin must be listed in the plugins of the catalog::

   plugins:
     source:
       - module: intake_odbc

and entries must specify ``driver: odbc`` or ``driver: odbc_partioned``.
Further arguments should be provided, as for the ``intake.open_*`` ad-hoc
commands, above. In particular, the connection parameters and query string
are required, and also the index column, if using partitioning.

It should be noted that SQL query strings are generally quite long; the appropriate
syntax may look like::

     - name: product_types
        description: Randomly generated data
        driver: odbc_partitioned
        args:
          uri:
          sql_expr: |
              SELECT t.productid AS pid, t.productname, t.price, tt.typename
              FROM testtable t
              INNER JOIN testtypetable tt ON t.typeid=tt.typeid
          dsn: MSSQL
          mssql: true

Where in this case we provide a keyword argument to specify the connection and so
leave the ``uri`` field empty, and the special YAML syntax with ``"|"`` is used to
indicate the multi-line query, delimited by indentation.

**Warning**, while it is reasonable to include user parameters in the SQL query body,
free-form strings or environment variables should not be used, since they will allow
arbitrary code execution on the DB server (SQL injection). Similarly, the details of
ODBC connections are unlikely to be useful as user parameters, except possibly
to take the DB username and password from the environment.


Using a Catalog
~~~~~~~~~~~~~~~

Assuming a catalog file called ``cat.yaml``, containing an ODBC source ``pdata``, one could
load it into a dataframe as follows::

   import intake
   cat = intake.Catalog('cat.yaml')
   df = cat.pdata.read()

The source may or may not be partitioned, depending on the plugin which was used and
the parameters. Use ``.discover()`` to find out whether there is partitioning, and if there
is, the partitions can be accessed independently.

``Dask`` can be used to read a partitioned source in parallel (see method ``.to_dask()``);
note that there is some overhead to establishing connections from each worker, and the
same ODBC drivers and configuration must exist on each machine, in the case of a
distributed cluster.