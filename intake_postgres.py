from intake.source import base
from dask import dataframe as dd
import pandas as pd
import sqlalchemy as sa



class Plugin(base.Plugin):
    def __init__(self):
        super(Plugin, self).__init__(name='postgres', version='0.1', container='dataframe', partition_access=True)

    def open(self, uri, sql_expr, **kwargs):
        """
        Parameters:
            uri : str
                Full SQLAlchemy URI for the database connection.
            sql_expr : string or SQLAlchemy Selectable (select or text object):
                Table name (if string); otherwise, SQLAlchemy query to be executed.
            kwargs (dict):
                Additional parameters to pass to ``dask.dataframe.from_sql_table()``.
                Can nest another ``kwargs`` into it, to pass parameters to ``pandas.read_sql_table()``.
        """
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return PostgresSource(uri=uri, sql_expr=sql_expr, pg_kwargs=source_kwargs, metadata=base_kwargs['metadata'])


class PostgresSource(base.DataSource):
    def __init__(self, uri, sql_expr, pg_kwargs, metadata):
        self._init_args = dict(uri=uri, sql_expr=sql_expr, pg_kwargs=pg_kwargs, metadata=metadata)

        self._uri = uri
        self._sql_expr = sql_expr
        self._pg_kwargs = pg_kwargs
        self._dataframe = None

        super(PostgresSource, self).__init__(container='dataframe', metadata=metadata)

    def _get_schema(self):
        if self._dataframe is None:
            engine = sa.create_engine(self._uri)
            first_row = pd.read_sql_query('({}) limit 1'.format(self._sql_expr),
                                          engine)
        else:
            first_row = self._dataframe.iloc[0]
        return base.Schema(datashape=None,
                           dtype=list(zip(first_row.dtypes.index, first_row.dtypes)),
                           shape=(None, len(first_row.dtypes.index)),
                           npartitions=1,
                           extra_metadata={})

    def _get_partition(self, _):
        if self._dataframe is None:
            engine = sa.create_engine(self._uri)
            self._dataframe = pd.read_sql_query(self._sql_expr, engine)
        return self._dataframe

    def _close(self):
        self._dataframe = None
