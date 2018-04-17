from intake.source import base
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class Plugin(base.Plugin):
    """Create PostgresSource objects"""
    def __init__(self):
        super(Plugin, self).__init__(name='postgres',
                                     version=__version__,
                                     container='dataframe',
                                     partition_access=False)

    def open(self, uri, sql_expr, **kwargs):
        """
        Parameters:
            uri : str
                Full SQLAlchemy URI for the database connection.
            sql_expr : string or SQLAlchemy Selectable (select or text object):
                SQL query to be executed.
            kwargs (dict):
                Additional parameters to pass as keyword arguments to
                ``PostgresAdapter`` constructor.
        """
        from intake_postgres.intake_postgres import PostgresSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return PostgresSource(uri=uri,
                              sql_expr=sql_expr,
                              pg_kwargs=source_kwargs,
                              metadata=base_kwargs['metadata'])
