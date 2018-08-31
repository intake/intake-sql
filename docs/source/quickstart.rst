Quickstart
==========

``intake-sql`` provides quick and easy access to tabular data stored in
sql data sources.

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c intake intake-sql

.. _intake: https://github.com/ContinuumIO/intake

In addition, you will also need other packages, depending on the database you wish to talk
to. For example, if your database if Hive, you will also need to install `pyhive`.

The plugins
-----------

``intake-sql`` provides three data plugins to access your data, plus a catalogue plugin. These
will be briefly described here, but see also the API documentation for specifics on parameter
usage. All data plugins produce data-frames as output.

This package makes use of sqlalchemy_ for the connection and data transfer, and the specifics
of what should appear in a connection string, the list of supported backends and optional
dependencies can all be found in its documentation.

.. _sqlalchemy: https://www.sqlalchemy.org/

The plugins:

- ``'sql'``: this is the one-shot plugin, which requires the least configuration. It passes
  on your parameters to ``pd.read_sql``, and so you can specify a table or full-syntax query.
  Since there is no partitioning, the query should be on a small dataset, or an aggregating query,
  whose output table size fits comfortably in memory.

- ``'sql-auto'``: this is restricted to reading from tables, as opposed to general
  queries but, given a column to use for indexing, it can automatically determine the appropriate
  partitioning in various ways. The index column should be something with high cardinality and
  indexed in the database systems.

- ``'sql-manual'``: in this case, the parameters must specify exactly the set of clauses to apply
  to create the data partitioning. This will typically be in the form of WHERE clauses. By requiring
  this extra specificity, this plugin requires more up-front work, but is more flexible than
  'sql-auto', taking general queries rather than only tables/views.

- ``'sql-cat'``: this is a "catalogue" source, and will query the target database, and create
  'sql-auto' data source entries for any tables which have at least one column defined as a
  Primary Key.
