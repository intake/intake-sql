from intake.source import base

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class ODBCPlugin(base.Plugin):
    """
    Read a single ODBC query into a pandas dataframe
    """

    def __init__(self):
        super(ODBCPlugin, self).__init__(name='odbc',
                                         version='0.1',
                                         container='dataframe',
                                         partition_access=False)

    def open(self, uri, sql_expr, **kwargs):
        """
        Create ODBCSource for given connection and SQL statement

        Parameters
        ----------
        uri : str
            Full SQLAlchemy URI for the database connection.
        sql_expr : string or SQLAlchemy Selectable (select or text object):
            SQL query to be executed.
        """
        from intake_odbc.intake_odbc import ODBCSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return ODBCSource(uri=uri,
                          sql_expr=sql_expr,
                          odbc_kwargs=source_kwargs,
                          metadata=base_kwargs['metadata'])


class ODBCPartPlugin(base.Plugin):
    """
    Read a single ODBC query into dataframe partitions
    """

    def __init__(self):
        super(ODBCPartPlugin, self).__init__(name='odbc_partitioned',
                                             version='0.1',
                                             container='dataframe',
                                             partition_access=True)

    def open(self, uri, sql_expr, **kwargs):
        """
        Create ODBCPartitionedSource for given connection and SQL statement

        Parameters
        ----------
        uri : str
            Full SQLAlchemy URI for the database connection.
        sql_expr : string or SQLAlchemy Selectable (select or text object):
            SQL query to be executed.
        """
        from intake_odbc.intake_odbc import ODBCPartitionedSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return ODBCPartitionedSource(uri=uri,
                                     sql_expr=sql_expr,
                                     odbc_kwargs=source_kwargs,
                                     metadata=base_kwargs['metadata'])
