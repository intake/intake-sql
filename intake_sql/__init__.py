from intake.source import base

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
__all__ = ['SQLPlugin', 'SQLAutoPartitionPlugin', 'SQLManualPartition']


class SQLPlugin(base.Plugin):
    """
    Read a single sql query into a pandas dataframe
    """

    def __init__(self):
        super(SQLPlugin, self).__init__(name='sql',
                                        version=__version__,
                                        container='dataframe',
                                        partition_access=False)

    def open(self, uri, sql_expr, **kwargs):
        """
        Create SQLSource for given connection and SQL statement
        """
        from intake_sql.intake_sql import SQLSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return SQLSource(uri, sql_expr, sql_kwargs=source_kwargs,
                         metadata=base_kwargs['metadata'])


class SQLAutoPartitionPlugin(base.Plugin):
    """
    Read sql table into a partitioned dataframe with automatic partitioning
    """

    def __init__(self):
        super(SQLAutoPartitionPlugin, self).__init__(name='sql_auto',
                                                     version=__version__,
                                                     container='dataframe',
                                                     partition_access=True)

    def open(self, uri, sql_expr, index, **kwargs):
        """
        Create SQLAutoPartitionPlugin for given connection and SQL statement
        """
        from intake_sql.intake_sql import SQLSourceAutoPartition
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return SQLSourceAutoPartition(uri, sql_expr, index,
                                      sql_kwargs=source_kwargs,
                                      metadata=base_kwargs['metadata'])


class SQLManualPartition(base.Plugin):
    """
    Read arbitrary sql query into a dataframe with explicit partitioning
    """

    def __init__(self):
        super(SQLManualPartition, self).__init__(name='sql_manual',
                                                 version=__version__,
                                                 container='dataframe',
                                                 partition_access=False)

    def open(self, uri, sql_expr, where_values, **kwargs):
        """
        Create SQLSourceManualPartition for given connection and SQL statement
        """
        from intake_sql.intake_sql import SQLSourceManualPartition
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return SQLSourceManualPartition(uri, sql_expr, where_values,
                                        sql_kwargs=source_kwargs,
                                        metadata=base_kwargs['metadata'])
