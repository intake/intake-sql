Quickstart
==========

This guide illustrates how to get started using *Intake-Postgres*, an *Intake* plugin that adds support for ingesting data from the `PostgreSQL <https://www.postgresql.org>`_ RDBMS. Before continuing, please complete the *Intake* quickstart.


Installation
------------

If you have a `conda-based installation <https://conda.io/docs/installation.html>`_, install *Intake* and the *Intake-Postgres* plugin with the following command::

    conda install -c intake intake-postgres


Usage (via *catalog.yml*)
-------------------------

Usage of *Intake-Postgres* is easiest to illustrate with an example.

In the *catalog.yml* file:

.. code-block:: yaml

    plugins:
      source:
        - module: intake_postgres
    
      sources:
        all_users:
          driver: postgres
          args:
            uri: 'postgresql://postgres@localhost:5432/postgres'
            sql_expr: 'select * from users'

There are two things to note in the above example:

1. `intake_postgres` is included under "plugins".
   This only needs to be done once for each *catalog.yml* file.
2. Any "sources" entry which includes the field `driver: postgres` includes some additional fields that are specific to the *Intake-Postgres* plugin.
   Specifically, we need to provide a `uri` to the database, and a `sql_expr` (SQL query expression).

Intake can then be accessed as normal, and provided that *Intake-Postgres* is installed:

    >>> import intake
    >>> catalog = intake.Catalog('catalog.yml')
    >>> ds = catalog.all_users
    >>> ds.discover()
    >>> df = ds.read()
    >>> df.tail()

The code above reads the *catalog.yml* file as normal, calls `discover()` on the *Intake-Postgres* data source, and then reads it into a dataframe for further analysis.


Usage (via Python library)
--------------------------

*Intake-Postgres* can also be accessed directly as a library. This usage pattern is for users who desire to call *Intake-Postgres* from inside another application, or just want more control over how data is ingested.

Here is the same example as above, except accessing *Intake-Postgres* as a library instead of through the *catalog.yml*:

    >>> import intake_postgres
    >>> plugin = intake_postgres.Plugin()
    >>> ds = plugin.open('postgresql://postgres@localhost:5432/postgres', 'select * from users')
    >>> source.discover()
    >>> df = source.read()
    >>> df.tail()
